package subscription

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	box "github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/service"
	"github.com/stretchr/testify/require"
)

func TestReauditImportSourceRemovalThenConsumerRefresh(t *testing.T) {
	for _, kind := range []string{"selector", "urltest"} {
		for _, replacement := range []string{`{"outbounds":[]}`, `{"outbounds":[{"type":"test","tag":"inner"}]}`} {
			t.Run(kind+"/"+replacement, func(t *testing.T) {
				var sourceBody, consumerBody atomic.Value
				document := func(node, source string) string {
					extra := `,"default":"subscription/source/node"`
					if kind == "urltest" {
						extra = `,"url":"http://127.0.0.1:1"`
					}
					return fmt.Sprintf(`{"outbounds":[{"type":%q,"tag":"pick","import":[{"group":%q}]%s},{"type":"test","tag":%q}]}`, kind, source, extra, node)
				}
				sourceBody.Store(`{"outbounds":[{"type":"test","tag":"node"},{"type":"selector","tag":"inner","outbounds":["node"]}]}`)
				consumerBody.Store(document("old", "subscription/source/inner"))
				remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/source" {
						fmt.Fprint(w, sourceBody.Load())
					} else {
						fmt.Fprint(w, consumerBody.Load())
					}
				}))
				defer remote.Close()
				_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"subscription","tag":"source","url":%q,"http_client":{}},{"type":"subscription","tag":"consumer","url":%q,"http_client":{}}]}`, remote.URL+"/source", remote.URL+"/consumer"))
				pick := importGroup(t, ctx, memberTag("consumer", "pick"))
				require.Equal(t, []string{memberTag("source", "node")}, pick.All())
				sourceBody.Store(replacement)
				require.NoError(t, getSubscription(t, ctx, "source").Refresh(ctx))
				require.Empty(t, pick.All())
				consumerBody.Store(document("new", "subscription/source/inner"))
				require.NoError(t, getSubscription(t, ctx, "consumer").Refresh(ctx))
				require.Empty(t, pick.All())
				require.Equal(t, []string{memberTag("consumer", "pick"), memberTag("consumer", "new")}, getSubscription(t, ctx, "consumer").All())
				consumerBody.Store(document("new", "subscription/source/typo"))
				require.ErrorContains(t, getSubscription(t, ctx, "consumer").Refresh(ctx), "missing outbound reference")
				sourceBody.Store(`{"outbounds":[{"type":"selector","tag":"inner","import":[{"group":"consumer"}]}]}`)
				require.ErrorContains(t, getSubscription(t, ctx, "source").Refresh(ctx), "circular")
				sourceBody.Store(`{"outbounds":[{"type":"test","tag":"returned"},{"type":"selector","tag":"inner","outbounds":["returned"]}]}`)
				require.NoError(t, getSubscription(t, ctx, "source").Refresh(ctx))
				require.Equal(t, []string{memberTag("source", "returned")}, pick.All())
			})
		}
	}
}

type reauditCanceledDial struct {
	outbound.Adapter
	entered, canceled chan struct{}
	resume            <-chan struct{}
}

func (d *reauditCanceledDial) DialContext(ctx context.Context, _ string, _ M.Socksaddr) (net.Conn, error) {
	close(d.entered)
	<-ctx.Done()
	close(d.canceled)
	<-d.resume
	return nil, ctx.Err()
}

func (d *reauditCanceledDial) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, net.ErrClosed
}

func TestReauditCloseJoinsManualSubscriptionURLTest(t *testing.T) {
	ctx := testContext()
	entered, canceled, resume := make(chan struct{}), make(chan struct{}), make(chan struct{})
	outbound.Register[testOptions](service.FromContext[adapter.OutboundRegistry](ctx).(*outbound.Registry), "cancel-gate", func(_ context.Context, _ adapter.Router, _ log.ContextLogger, tag string, _ testOptions) (adapter.Outbound, error) {
		return &reauditCanceledDial{outbound.NewAdapter("cancel-gate", tag, []string{"tcp"}, nil), entered, canceled, resume}, nil
	})
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"outbounds":[{"type":"cancel-gate","tag":"node"}]}`)
	}))
	defer remote.Close()
	instance, ctx := startBoxWithContext(t, ctx, configFor(remote.URL, ""))
	s := getSubscription(t, ctx, "sub")
	testDone, closeDone := make(chan error, 1), make(chan error, 1)
	go func() { _, err := s.URLTest(ctx); testDone <- err }()
	<-entered
	go func() { closeDone <- instance.Close() }()
	<-canceled
	returnedEarly := false
	select {
	case <-closeDone:
		returnedEarly = true
	case <-time.After(25 * time.Millisecond):
	}
	close(resume)
	require.ErrorIs(t, <-testDone, context.Canceled)
	if !returnedEarly {
		require.NoError(t, <-closeDone)
	}
	require.False(t, returnedEarly, "Close returned before an admitted manual probe had released its snapshot")
}

func TestReauditRetirementCancelsManualChildURLTest(t *testing.T) {
	ctx := testContext()
	entered, canceled, resume := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(resume) }) }
	defer release()
	outbound.Register[testOptions](service.FromContext[adapter.OutboundRegistry](ctx).(*outbound.Registry), "cancel-gate", func(_ context.Context, _ adapter.Router, _ log.ContextLogger, tag string, _ testOptions) (adapter.Outbound, error) {
		return &reauditCanceledDial{outbound.NewAdapter("cancel-gate", tag, []string{"tcp"}, nil), entered, canceled, resume}, nil
	})
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/parent" {
			fmt.Fprintf(w, `{"outbounds":[{"type":"subscription","tag":"child","url":"http://%s/child","http_client":{}}]}`, r.Host)
		} else {
			fmt.Fprint(w, `{"outbounds":[{"type":"cancel-gate","tag":"node"}]}`)
		}
	}))
	defer remote.Close()
	_, ctx = startBoxWithContext(t, ctx, configFor(remote.URL+"/parent", ""))
	root := getSubscription(t, ctx, "sub")
	child := importGroup(t, ctx, memberTag("sub", "child")).(adapter.Subscription)
	testCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	testDone, refreshDone := make(chan error, 1), make(chan error, 1)
	go func() { _, err := child.URLTest(testCtx); testDone <- err }()
	<-entered
	go func() { refreshDone <- root.Refresh(ctx) }()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		cancel()
		release()
		<-testDone
		<-refreshDone
		t.Fatal("retired child left its manual probe running")
	}
	returnedEarly := false
	select {
	case <-refreshDone:
		returnedEarly = true
	case <-time.After(25 * time.Millisecond):
	}
	release()
	require.ErrorIs(t, <-testDone, context.Canceled)
	if !returnedEarly {
		require.NoError(t, <-refreshDone)
	}
	require.False(t, returnedEarly, "retirement returned before the old child's probe exited")
}

func TestReauditCachedImportDoesNotAliasHostGroup(t *testing.T) {
	var body atomic.Value
	body.Store(`{"outbounds":[{"type":"test","tag":"node"},{"type":"selector","tag":"source","outbounds":["node"]},{"type":"selector","tag":"pick","import":[{"group":"source"}]}]}`)
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body.Load()) }))
	defer remote.Close()
	config := fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"test","tag":"host-node"},{"type":"selector","tag":"source","outbounds":["host-node"]},{"type":"subscription","tag":"sub","url":%q,"cache_path":%q,"http_client":{}}]}`, remote.URL, filepath.Join(t.TempDir(), "subscription.json"))
	instance, ctx := startBox(t, config)
	body.Store(`{"outbounds":[{"type":"selector","tag":"pick","import":[{"group":"source"}]}]}`)
	require.NoError(t, getSubscription(t, ctx, "sub").Refresh(ctx))
	require.NoError(t, instance.Close())
	ctx = testContext()
	opts, err := json.UnmarshalExtendedContext[option.Options](ctx, []byte(config))
	require.NoError(t, err)
	instance, err = box.New(box.Options{Options: opts, Context: ctx})
	require.NoError(t, err)
	defer instance.Close()
	require.ErrorContains(t, instance.Start(), "import group not found: subscription/sub/source", "cached identity must not fall through to the host; cold-start availability remains strict")
}

func TestReauditLocalImportRemovalPreservesIdentity(t *testing.T) {
	var body atomic.Value
	body.Store(`{"outbounds":[{"type":"test","tag":"node"},{"type":"selector","tag":"source","outbounds":["node"]},{"type":"selector","tag":"pick","import":[{"group":"source"}]}]}`)
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body.Load()) }))
	defer remote.Close()
	_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"test","tag":"host-node"},{"type":"selector","tag":"source","outbounds":["host-node"]},{"type":"subscription","tag":"sub","url":%q,"http_client":{}}]}`, remote.URL))
	pick := importGroup(t, ctx, memberTag("sub", "pick"))
	require.Equal(t, []string{memberTag("sub", "node")}, pick.All())
	body.Store(`{"outbounds":[{"type":"selector","tag":"pick","import":[{"group":"source"}]}]}`)
	for range 2 {
		require.NoError(t, getSubscription(t, ctx, "sub").Refresh(ctx))
		require.Empty(t, pick.All(), "a deleted local source must not resolve to the host's same-name group")
	}
	body.Store(`{"outbounds":[{"type":"test","tag":"node"},{"type":"selector","tag":"source","outbounds":["node"]},{"type":"selector","tag":"pick","import":[{"group":"source"}]}]}`)
	require.NoError(t, getSubscription(t, ctx, "sub").Refresh(ctx))
	require.Equal(t, []string{memberTag("sub", "node")}, pick.All())
}

func TestReauditImportMissingDefaultIsPreference(t *testing.T) {
	var body atomic.Value
	body.Store(`{"outbounds":[{"type":"test","tag":"node"},{"type":"selector","tag":"source","outbounds":["node"]},{"type":"selector","tag":"pick","import":[{"group":"source"}],"default":"subscription/sub/removed"}]}`)
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body.Load()) }))
	defer remote.Close()
	_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"subscription","tag":"sub","url":%q,"http_client":{}},{"type":"selector","tag":"top","import":[{"group":"sub"}],"default":"top"}]}`, remote.URL))
	require.Equal(t, memberTag("sub", "node"), importGroup(t, ctx, memberTag("sub", "pick")).Selected("tcp").Tag())
}

func TestReauditNestedImportSurvivesParentRefresh(t *testing.T) {
	var sourceBody atomic.Value
	sourceBody.Store(`{"outbounds":[{"type":"test","tag":"node"},{"type":"selector","tag":"inner","outbounds":["node"]}]}`)
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/source":
			fmt.Fprint(w, sourceBody.Load())
		case "/parent":
			fmt.Fprintf(w, `{"outbounds":[{"type":"subscription","tag":"child","url":"http://%s/child","http_client":{},"import":[{"group":"subscription/source/inner"}]}]}`, r.Host)
		case "/child":
			fmt.Fprint(w, `{"outbounds":[{"type":"selector","tag":"pick","import":[{"group":"subscription/source/inner"}]}]}`)
		}
	}))
	defer remote.Close()
	_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"subscription","tag":"source","url":%q,"http_client":{}},{"type":"subscription","tag":"parent","url":%q,"http_client":{}}]}`, remote.URL+"/source", remote.URL+"/parent"))
	pick := importGroup(t, ctx, memberTag(memberTag("parent", "child"), "pick"))
	require.Equal(t, []string{memberTag("source", "node")}, pick.All())
	sourceBody.Store(`{"outbounds":[]}`)
	require.NoError(t, getSubscription(t, ctx, "source").Refresh(ctx))
	for range 2 {
		require.NoError(t, getSubscription(t, ctx, "parent").Refresh(ctx))
		require.Empty(t, pick.All())
	}
	sourceBody.Store(`{"outbounds":[{"type":"test","tag":"node"},{"type":"selector","tag":"inner","outbounds":["node"]}]}`)
	require.NoError(t, getSubscription(t, ctx, "source").Refresh(ctx))
	require.Equal(t, []string{memberTag("source", "node")}, pick.All())
}

func TestReauditConcurrentSourceAndParentRefreshCleanup(t *testing.T) {
	var body atomic.Value
	present := `{"outbounds":[{"type":"test","tag":"node"},{"type":"selector","tag":"inner","outbounds":["node"]}]}`
	body.Store(present)
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/source":
			fmt.Fprint(w, body.Load())
		case "/parent":
			fmt.Fprintf(w, `{"outbounds":[{"type":"subscription","tag":"child","url":"http://%s/child","http_client":{}}]}`, r.Host)
		default:
			fmt.Fprint(w, `{"outbounds":[{"type":"selector","tag":"pick","import":[{"group":"subscription/source/inner"}]}]}`)
		}
	}))
	defer remote.Close()
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	defer target.Close()
	instance, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"subscription","tag":"source","url":%q,"http_client":{}},{"type":"subscription","tag":"parent","url":%q,"http_client":{}}]}`, remote.URL+"/source", remote.URL+"/parent"))
	source, parent := getSubscription(t, ctx, "source"), getSubscription(t, ctx, "parent")
	pick := importGroup(t, ctx, memberTag(memberTag("parent", "child"), "pick"))
	var wg sync.WaitGroup
	failures := make(chan error, 2)
	generations := make(chan *snapshot, 40)
	for _, s := range []*Subscription{source, parent} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 16 {
				if s == source {
					if i%2 == 0 {
						body.Store(`{"outbounds":[]}`)
					} else {
						body.Store(present)
					}
				}
				old := s.activeSnapshot()
				generations <- old
				old.release()
				if err := s.Refresh(ctx); err != nil {
					failures <- err
					return
				}
			}
		}()
	}
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 60 {
				_ = pick.All()
				_ = pick.Selected("tcp")
				conn, err := pick.DialContext(ctx, "tcp", M.ParseSocksaddr(strings.TrimPrefix(target.URL, "http://")))
				if err == nil {
					conn.Close()
				}
			}
		}()
	}
	wg.Wait()
	close(failures)
	close(generations)
	for err := range failures {
		require.NoError(t, err)
	}
	body.Store(present)
	require.NoError(t, source.Refresh(ctx))
	require.NoError(t, parent.Refresh(ctx))
	require.Equal(t, []string{memberTag("source", "node")}, pick.All())
	require.NoError(t, instance.Close())
	for g := range generations {
		g.mu.Lock()
		refs, closed, done := g.refs, g.closed, g.closeDone
		g.mu.Unlock()
		require.True(t, closed)
		require.Zero(t, refs)
		select {
		case <-done:
		default:
			t.Fatal("retired resource cleanup survived shutdown")
		}
	}
}
