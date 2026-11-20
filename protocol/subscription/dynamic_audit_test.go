package subscription

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	box "github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/dialer"
	"github.com/sagernet/sing-box/common/interrupt"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
	"github.com/stretchr/testify/require"
)

func TestDynamicAuditImportedSelectedMetadata(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/empty" {
			fmt.Fprint(w, `{"outbounds":[]}`)
		} else {
			fmt.Fprint(w, `{"outbounds":[{"type":"test","tag":"node"}]}`)
		}
	}))
	defer server.Close()
	_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"subscription","tag":"aggregate","url":%q,"http_client":{},"import":[{"group":"source"}]},{"type":"subscription","tag":"source","url":%q,"http_client":{}}]}`, server.URL+"/empty", server.URL))
	selected := getSubscription(t, ctx, "aggregate").Selected("tcp")
	require.NotNil(t, selected)
	require.Equal(t, "test", selected.Type())
	require.True(t, selected.(adapter.ProvidedOutbound).Available())
	_, group := adapter.OutboundGroupFor(selected)
	require.False(t, group)
}

type auditGate struct {
	outbound.Adapter
	N.Dialer
	entered, resume chan struct{}
	once            sync.Once
}

func (g *auditGate) DialContext(ctx context.Context, network string, address M.Socksaddr) (net.Conn, error) {
	g.once.Do(func() { close(g.entered) })
	select {
	case <-g.resume:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return g.Dialer.DialContext(ctx, network, address)
}

func TestDynamicAuditPinnedGroupSelection(t *testing.T) {
	entered, resume := make(chan struct{}), make(chan struct{})
	ctx := testContext()
	outbound.Register[testOptions](service.FromContext[adapter.OutboundRegistry](ctx).(*outbound.Registry), "gate", func(ctx context.Context, _ adapter.Router, _ log.ContextLogger, tag string, options testOptions) (adapter.Outbound, error) {
		d, err := dialer.New(ctx, options.DialerOptions, true)
		return &auditGate{Adapter: outbound.NewAdapterWithDialerOptions("gate", tag, []string{"tcp", "udp"}, options.DialerOptions), Dialer: d, entered: entered, resume: resume}, err
	})
	var body atomic.Value
	body.Store(`{"outbounds":[{"type":"test","tag":"entry","detour":"gate"},{"type":"test","tag":"old"},{"type":"selector","tag":"inner","outbounds":["old"]}]}`)
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body.Load()) }))
	defer source.Close()
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	defer target.Close()
	_, ctx = startBoxWithContext(t, ctx, fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"subscription","tag":"sub","url":%q,"http_client":{},"outbounds":["inner"],"import":[{"group":"subscription/sub/inner"}],"default":"new"},{"type":"gate","tag":"gate","detour":"sub"}]}`, source.URL))
	s := getSubscription(t, ctx, "sub")
	require.True(t, s.SelectOutbound("subscription/sub/old"))
	entry, _ := s.ProviderOutbound("subscription/sub/entry")
	result := make(chan error, 1)
	go func() {
		conn, err := entry.DialContext(ctx, "tcp", M.ParseSocksaddr(strings.TrimPrefix(target.URL, "http://")))
		if conn != nil {
			conn.Close()
		}
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("dial did not reach gate")
	}
	body.Store(`{"outbounds":[{"type":"test","tag":"entry","detour":"gate"},{"type":"test","tag":"new"},{"type":"selector","tag":"inner","outbounds":["new"]}]}`)
	require.NoError(t, s.Refresh(ctx))
	close(resume)
	require.NoError(t, <-result, "in-flight chain must keep its generation's group selection")
	require.Equal(t, "subscription/sub/new", s.Selected("tcp").Tag())
}

func TestDynamicAuditRetiredResourcesReleased(t *testing.T) {
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"outbounds":[{"type":"test","tag":"node"}]}`)
	}))
	defer source.Close()
	_, ctx := startBox(t, configFor(source.URL, ""))
	s := getSubscription(t, ctx, "sub")
	old := s.current
	require.NoError(t, s.Refresh(ctx))
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, g := range s.retired {
			if g == old {
				return false
			}
		}
		return true
	}, time.Second, time.Millisecond, "closed generations must leave the retired list without another refresh")
}

func TestDynamicAuditRetiredChildCannotPublish(t *testing.T) {
	childSource := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"outbounds":[{"type":"test","tag":"node"}]}`)
	}))
	defer childSource.Close()
	parentSource := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"outbounds":[{"type":"subscription","tag":"child","url":%q,"http_client":{}}]}`, childSource.URL)
	}))
	defer parentSource.Close()
	_, ctx := startBox(t, configFor(parentSource.URL, ""))
	parent := getSubscription(t, ctx, "sub")
	oldParent := parent.current
	require.True(t, oldParent.acquire())
	defer oldParent.release()
	child := oldParent.children[0]
	candidate, err := child.build([]byte(`{"outbounds":[{"type":"test","tag":"replacement"}]}`))
	require.NoError(t, err)
	defer candidate.close()
	require.NoError(t, parent.Refresh(ctx))
	require.Error(t, child.publish(candidate, nil, "", time.Now()), "a superseded parent must not accept child publication")
}

func TestDynamicAuditRetiredURLTestStopsProbing(t *testing.T) {
	var oldCalls atomic.Int32
	health := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/old" {
			oldCalls.Add(1)
		}
		w.WriteHeader(204)
	}))
	defer health.Close()
	var body atomic.Value
	document := func(path string) string {
		return fmt.Sprintf(`{"outbounds":[{"type":"test","tag":"node"},{"type":"urltest","tag":"auto","outbounds":["node"],"url":%q,"interval":"10ms","idle_timeout":"1h"}]}`, health.URL+path)
	}
	body.Store(document("/old"))
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body.Load()) }))
	defer source.Close()
	_, ctx := startBox(t, configFor(source.URL, `"outbounds":["auto"],`))
	s := getSubscription(t, ctx, "sub")
	conn, err := s.DialContext(ctx, "tcp", M.ParseSocksaddr(strings.TrimPrefix(health.URL, "http://")))
	require.NoError(t, err)
	defer conn.Close()
	require.Eventually(t, func() bool { return oldCalls.Load() >= 2 }, time.Second, time.Millisecond)
	body.Store(document("/new"))
	require.NoError(t, s.Refresh(ctx))
	before := oldCalls.Load()
	time.Sleep(60 * time.Millisecond)
	require.Equal(t, before, oldCalls.Load(), "retired URL tests must not keep dialing obsolete nodes")
}

func TestDynamicAuditAcyclicCrossProviderReferences(t *testing.T) {
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		peer := "b"
		if r.URL.Path == "/b" {
			peer = "a"
		}
		fmt.Fprintf(w, `{"outbounds":[{"type":"test","tag":"entry","detour":"subscription/%s/relay"},{"type":"test","tag":"relay"}]}`, peer)
	}))
	defer source.Close()
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	defer target.Close()
	_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"subscription","tag":"a","url":%q,"http_client":{},"outbounds":["entry"]},{"type":"subscription","tag":"b","url":%q,"http_client":{},"outbounds":["entry"]},{"type":"selector","tag":"top","import":[{"group":"a"},{"group":"b"}]}]}`, source.URL+"/a", source.URL+"/b"))
	for _, tag := range []string{"a", "b"} {
		conn, err := getSubscription(t, ctx, tag).DialContext(ctx, "tcp", M.ParseSocksaddr(strings.TrimPrefix(target.URL, "http://")))
		require.NoError(t, err)
		require.NoError(t, conn.Close())
	}
}

func TestDynamicAuditForwardReferenceValidation(t *testing.T) {
	for _, cycle := range []bool{false, true} {
		t.Run(fmt.Sprint(cycle), func(t *testing.T) {
			source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/a" {
					ref := "missing"
					if cycle {
						ref = "entry"
					}
					fmt.Fprintf(w, `{"outbounds":[{"type":"test","tag":"entry","detour":"subscription/b/%s"}]}`, ref)
				} else {
					fmt.Fprint(w, `{"outbounds":[{"type":"test","tag":"entry","detour":"subscription/a/entry"}]}`)
				}
			}))
			defer source.Close()
			ctx := testContext()
			opts, err := json.UnmarshalExtendedContext[option.Options](ctx, []byte(fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"subscription","tag":"a","url":%q,"http_client":{}},{"type":"subscription","tag":"b","url":%q,"http_client":{}}]}`, source.URL+"/a", source.URL+"/b")))
			require.NoError(t, err)
			instance, err := box.New(box.Options{Options: opts, Context: ctx})
			require.NoError(t, err)
			defer instance.Close()
			err = instance.Start()
			if cycle {
				require.ErrorContains(t, err, "circular")
			} else {
				require.ErrorContains(t, err, "not found")
			}
		})
	}
}

func TestDynamicAuditNestedForwardReferencesRefresh(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	defer target.Close()
	childSource := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		peer := "b"
		if r.URL.Path == "/b" {
			peer = "a"
		}
		fmt.Fprintf(w, `{"outbounds":[{"type":"test","tag":"entry","detour":%q},{"type":"test","tag":"relay"}]}`, memberTag(memberTag("sub", peer), "relay"))
	}))
	defer childSource.Close()
	parentSource := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"outbounds":[{"type":"subscription","tag":"a","url":%q,"http_client":{},"outbounds":["entry"]},{"type":"subscription","tag":"b","url":%q,"http_client":{},"outbounds":["entry"]}]}`, childSource.URL+"/a", childSource.URL+"/b")
	}))
	defer parentSource.Close()
	_, ctx := startBox(t, configFor(parentSource.URL, ""))
	s := getSubscription(t, ctx, "sub")
	for range 3 {
		for _, tag := range []string{"a", "b"} {
			entry, ok := s.ProviderOutbound(memberTag(memberTag("sub", tag), "entry"))
			require.True(t, ok)
			conn, err := entry.DialContext(ctx, "tcp", M.ParseSocksaddr(strings.TrimPrefix(target.URL, "http://")))
			require.NoError(t, err)
			require.NoError(t, conn.Close())
		}
		require.NoError(t, s.Refresh(ctx))
	}
}

func auditHTTPProxy(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	count := new(atomic.Int32)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			w.WriteHeader(405)
			return
		}
		upstream, err := net.DialTimeout("tcp", r.Host, time.Second)
		if err != nil {
			w.WriteHeader(502)
			return
		}
		conn, reader, err := w.(http.Hijacker).Hijack()
		if err != nil {
			upstream.Close()
			return
		}
		defer upstream.Close()
		defer conn.Close()
		count.Add(1)
		fmt.Fprint(conn, "HTTP/1.1 200 Connection Established\r\n\r\n")
		done := make(chan struct{})
		go func() { defer close(done); io.Copy(upstream, reader); upstream.Close() }()
		io.Copy(conn, upstream)
		conn.Close()
		<-done
	}))
	t.Cleanup(server.Close)
	return server, count
}

func TestDynamicAuditLiveHTTPChainRefreshAndRemoval(t *testing.T) {
	relayOld, oldHits := auditHTTPProxy(t)
	relayNew, newHits := auditHTTPProxy(t)
	exit, exitHits := auditHTTPProxy(t)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "alive") }))
	defer target.Close()
	var aBody, bBody atomic.Value
	proxy := func(tag, address, detour string) string {
		addr := M.ParseSocksaddr(strings.TrimPrefix(address, "http://"))
		return fmt.Sprintf(`{"outbounds":[{"type":"http","tag":%q,"server":%q,"server_port":%d,"detour":%q}]}`, tag, addr.AddrString(), addr.Port, detour)
	}
	aBody.Store(proxy("exit", exit.URL, "subscription/b/relay"))
	bBody.Store(proxy("relay", relayOld.URL, ""))
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/a" {
			fmt.Fprint(w, aBody.Load())
		} else {
			fmt.Fprint(w, bBody.Load())
		}
	}))
	defer source.Close()
	_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"subscription","tag":"a","url":%q,"http_client":{}},{"type":"subscription","tag":"b","url":%q,"http_client":{}},{"type":"selector","tag":"top","import":[{"group":"a"}]}]}`, source.URL+"/a", source.URL+"/b"))
	a, b := getSubscription(t, ctx, "a"), getSubscription(t, ctx, "b")
	top := importGroup(t, ctx, "top")
	dial := func() net.Conn {
		conn, err := top.DialContext(interrupt.ContextWithIsExternalConnection(ctx, true), "tcp", M.ParseSocksaddr(strings.TrimPrefix(target.URL, "http://")))
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })
		return conn
	}
	request := func(conn net.Conn) {
		require.NoError(t, conn.SetDeadline(time.Now().Add(time.Second)))
		_, err := fmt.Fprintf(conn, "GET / HTTP/1.1\r\nHost: %s\r\n\r\n", strings.TrimPrefix(target.URL, "http://"))
		require.NoError(t, err)
		response, err := http.ReadResponse(bufio.NewReader(conn), nil)
		require.NoError(t, err)
		data, err := io.ReadAll(response.Body)
		require.NoError(t, err)
		response.Body.Close()
		require.Equal(t, "alive", string(data))
	}
	oldConn := dial()
	request(oldConn)
	oldB := b.current
	bBody.Store(proxy("relay", relayNew.URL, ""))
	require.NoError(t, b.Refresh(ctx))
	request(oldConn)
	newConn := dial()
	request(newConn)
	require.EqualValues(t, 1, oldHits.Load())
	require.EqualValues(t, 1, newHits.Load())
	require.EqualValues(t, 2, exitHits.Load())
	oldB.mu.Lock()
	closed := oldB.closed
	oldB.mu.Unlock()
	require.False(t, closed)
	require.NoError(t, oldConn.Close())
	require.Eventually(t, func() bool { oldB.mu.Lock(); defer oldB.mu.Unlock(); return oldB.closed }, time.Second, time.Millisecond)
	aBody.Store(`{"outbounds":[]}`)
	require.NoError(t, a.Refresh(ctx))
	require.Empty(t, top.All())
	request(newConn)
	_, err := top.DialContext(ctx, "tcp", M.ParseSocksaddr(strings.TrimPrefix(target.URL, "http://")))
	require.Error(t, err)
	require.NoError(t, newConn.Close())
	require.Eventually(t, func() bool { a.mu.Lock(); defer a.mu.Unlock(); return len(a.retired) == 0 }, time.Second, time.Millisecond)
}

func auditBackgroundWorkers() int {
	data := make([]byte, 2<<20)
	data = data[:runtime.Stack(data, true)]
	count := 0
	for _, stack := range strings.Split(string(data), "\n\n") {
		for _, name := range []string{"common/group.(*Members).Watch", "protocol/subscription.(*Subscription).loop", "protocol/subscription.(*snapshot).closeAsync", "protocol/group.(*URLTestGroup).PostStart", "protocol/group.(*URLTestGroup).loopCheck", "protocol/group.(*URLTestGroup).runBackground", "protocol/group.(*urlTestBatch).test"} {
			if strings.Contains(stack, name) {
				count++
				break
			}
		}
	}
	return count
}

func TestDynamicAuditConcurrentUpdatesAndResourceCleanup(t *testing.T) {
	baseline := auditBackgroundWorkers()
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	defer target.Close()
	var body atomic.Value
	document := func(node string) string { return fmt.Sprintf(`{"outbounds":[{"type":"test","tag":%q}]}`, node) }
	body.Store(document("node"))
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body.Load()) }))
	defer source.Close()
	for range 5 {
		instance, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"subscription","tag":"sub","url":%q,"http_client":{}},{"type":"urltest","tag":"auto","import":[{"group":"sub"}],"url":%q,"interval":"20ms","idle_timeout":"1h"},{"type":"selector","tag":"top","import":[{"group":"auto"}]}]}`, source.URL, target.URL))
		s := getSubscription(t, ctx, "sub")
		top := importGroup(t, ctx, "top")
		auto := importGroup(t, ctx, "auto")
		var generations []*snapshot
		var wg sync.WaitGroup
		for range 3 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range 35 {
					_ = top.All()
					_ = top.Selected("tcp")
					for _, node := range s.ProviderOutbounds() {
						_ = node.Type()
						_ = node.Network()
						_ = node.(adapter.ProvidedOutbound).Available()
						_, _ = adapter.OutboundGroupFor(node)
					}
					conn, err := top.DialContext(ctx, "tcp", M.ParseSocksaddr(strings.TrimPrefix(target.URL, "http://")))
					if err == nil {
						conn.Close()
					}
					auto.(adapter.InterfaceUpdateListener).InterfaceUpdated(ctx)
				}
			}()
		}
		for i := range 8 {
			generations = append(generations, s.current)
			if i%2 == 0 {
				body.Store(`{"outbounds":[]}`)
			} else {
				body.Store(document("node"))
			}
			require.NoError(t, s.Refresh(ctx))
		}
		wg.Wait()
		generations = append(generations, s.current)
		require.NoError(t, instance.Close())
		for _, g := range generations {
			g.mu.Lock()
			refs, closed, done := g.refs, g.closed, g.closeDone
			g.mu.Unlock()
			require.True(t, closed)
			require.Zero(t, refs)
			select {
			case <-done:
			default:
				t.Fatal("scope cleanup still running after close")
			}
			for _, node := range g.outbounds.Outbounds() {
				if raw, ok := node.(*testOutbound); ok {
					require.True(t, raw.closed.Load())
				}
			}
		}
	}
	require.Eventually(t, func() bool { return auditBackgroundWorkers() <= baseline }, 2*time.Second, 5*time.Millisecond, "subscription/group goroutines survived shutdown")
}

type auditNetworkOptions struct {
	option.DialerOptions
	Networks []string `json:"networks"`
}

func TestDynamicAuditPinnedNetworkCapabilities(t *testing.T) {
	entered, resume := make(chan struct{}), make(chan struct{})
	ctx := testContext()
	registry := service.FromContext[adapter.OutboundRegistry](ctx).(*outbound.Registry)
	outbound.Register[testOptions](registry, "gate", func(ctx context.Context, _ adapter.Router, _ log.ContextLogger, tag string, options testOptions) (adapter.Outbound, error) {
		d, err := dialer.New(ctx, options.DialerOptions, true)
		return &auditGate{Adapter: outbound.NewAdapterWithDialerOptions("gate", tag, []string{"tcp", "udp"}, options.DialerOptions), Dialer: d, entered: entered, resume: resume}, err
	})
	outbound.Register[auditNetworkOptions](registry, "audit-network", func(ctx context.Context, _ adapter.Router, _ log.ContextLogger, tag string, options auditNetworkOptions) (adapter.Outbound, error) {
		d, err := dialer.New(ctx, options.DialerOptions, true)
		return &testOutbound{Adapter: outbound.NewAdapterWithDialerOptions("audit-network", tag, options.Networks, options.DialerOptions), Dialer: d}, err
	})
	var body atomic.Value
	document := func(networks string) string {
		return `{"outbounds":[{"type":"test","tag":"entry","detour":"gate"},{"type":"audit-network","tag":"relay","networks":` + networks + `}]}`
	}
	body.Store(document(`["tcp","udp"]`))
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/a" {
			fmt.Fprint(w, `{"outbounds":[{"type":"selector","tag":"exit","import":[{"group":"b","include":"^relay$"}]}]}`)
		} else {
			fmt.Fprint(w, body.Load())
		}
	}))
	defer source.Close()
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	defer target.Close()
	_, ctx = startBoxWithContext(t, ctx, fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"subscription","tag":"a","url":%q,"http_client":{}},{"type":"subscription","tag":"b","url":%q,"http_client":{},"outbounds":["relay"]},{"type":"gate","tag":"gate","detour":"a"}]}`, source.URL+"/a", source.URL+"/b"))
	b := getSubscription(t, ctx, "b")
	entry, _ := b.ProviderOutbound("subscription/b/entry")
	result := make(chan error, 1)
	go func() {
		conn, err := entry.DialContext(ctx, "tcp", M.ParseSocksaddr(strings.TrimPrefix(target.URL, "http://")))
		if conn != nil {
			conn.Close()
		}
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("dial did not reach gate")
	}
	body.Store(document(`["udp"]`))
	require.NoError(t, b.Refresh(ctx))
	close(resume)
	require.NoError(t, <-result, "network checks must use the pinned relay generation")
	_, err := getSubscription(t, ctx, "a").DialContext(ctx, "tcp", M.ParseSocksaddr(strings.TrimPrefix(target.URL, "http://")))
	require.Error(t, err)
}

type auditPauseGate struct {
	*testOutbound
	entered, resume chan struct{}
	once            sync.Once
}

func (g *auditPauseGate) PauseUpdates() { g.once.Do(func() { close(g.entered) }); <-g.resume }

func TestDynamicAuditDialDuringPublication(t *testing.T) {
	entered, resume := make(chan struct{}), make(chan struct{})
	var resumeOnce sync.Once
	release := func() { resumeOnce.Do(func() { close(resume) }) }
	defer release()
	ctx := testContext()
	outbound.Register[testOptions](service.FromContext[adapter.OutboundRegistry](ctx).(*outbound.Registry), "pause-gate", func(ctx context.Context, _ adapter.Router, _ log.ContextLogger, tag string, options testOptions) (adapter.Outbound, error) {
		d, err := dialer.New(ctx, options.DialerOptions, true)
		return &auditPauseGate{testOutbound: &testOutbound{Adapter: outbound.NewAdapterWithDialerOptions("pause-gate", tag, []string{"tcp", "udp"}, options.DialerOptions), Dialer: d}, entered: entered, resume: resume}, err
	})
	var body atomic.Value
	document := func(node string) string {
		return fmt.Sprintf(`{"outbounds":[{"type":"pause-gate","tag":"pause"},{"type":"test","tag":"entry","detour":"sub"},{"type":"test","tag":%q},{"type":"selector","tag":"inner","outbounds":[%q]}]}`, node, node)
	}
	body.Store(document("old"))
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body.Load()) }))
	defer source.Close()
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	defer target.Close()
	_, ctx = startBoxWithContext(t, ctx, fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"subscription","tag":"sub","url":%q,"http_client":{},"outbounds":["inner"],"import":[{"group":"subscription/sub/inner"}],"default":"new"}]}`, source.URL))
	s := getSubscription(t, ctx, "sub")
	require.True(t, s.SelectOutbound("subscription/sub/old"))
	// Make the observable publication window deterministic, without relying on
	// the scheduling of the membership watcher to repair the selection first.
	s.stopWatch()
	body.Store(document("new"))
	refreshed := make(chan error, 1)
	go func() { refreshed <- s.Refresh(ctx) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("refresh did not enter retirement")
	}
	entry, found := s.ProviderOutbound("subscription/sub/entry")
	require.True(t, found)
	conn, err := entry.DialContext(ctx, "tcp", M.ParseSocksaddr(strings.TrimPrefix(target.URL, "http://")))
	if conn != nil {
		conn.Close()
	}
	release()
	require.NoError(t, <-refreshed)
	require.NoError(t, err, "a new generation must never be pinned with an old selection")
}
