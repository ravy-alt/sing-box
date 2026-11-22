package subscription

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	box "github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/service"
	"github.com/stretchr/testify/require"
)

func importGroup(t *testing.T, ctx context.Context, tag string) adapter.OutboundGroup {
	t.Helper()
	o, ok := service.FromContext[adapter.OutboundManager](ctx).Outbound(tag)
	require.True(t, ok, tag)
	g, ok := adapter.OutboundGroupFor(o)
	require.True(t, ok, tag)
	return g
}

func TestGroupImportStaticFiltersAndDeduplication(t *testing.T) {
	_, ctx := startBox(t, `{"log":{"disabled":true},"outbounds":[
 {"type":"test","tag":"HK-a"},{"type":"test","tag":"HK-expired"},{"type":"direct","tag":"HK-direct"},{"type":"test","tag":"JP-b"},
 {"type":"selector","tag":"source","outbounds":["HK-a","HK-expired","HK-direct","JP-b"]},
 {"type":"selector","tag":"filtered","outbounds":["JP-b"],"import":[{"group":"source","include":["^HK-"],"exclude":"expired$","types":"test"},{"group":"source","include":"^HK-a$"}],"default":"HK-a"},
 {"type":"selector","tag":"top","import":[{"group":"filtered"}]}]}`)
	require.Equal(t, []string{"JP-b", "HK-a"}, importGroup(t, ctx, "filtered").All())
	require.Equal(t, []string{"JP-b", "HK-a"}, importGroup(t, ctx, "top").All())
	require.Equal(t, "HK-a", importGroup(t, ctx, "filtered").Selected("tcp").Tag())
}

func TestGroupImportDynamicMultilevelAndSubscriptionConsumer(t *testing.T) {
	var body atomic.Value
	body.Store(`{"outbounds":[{"type":"test","tag":"HK-a"},{"type":"test","tag":"JP-b"}]}`)
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/empty" {
			fmt.Fprint(w, `{"outbounds":[]}`)
		} else {
			fmt.Fprint(w, body.Load())
		}
	}))
	defer remote.Close()
	health := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	defer health.Close()
	_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[
 {"type":"subscription","tag":"source","url":%q,"http_client":{}},
 {"type":"selector","tag":"filtered","import":[{"group":"source","include":"^HK-","types":"test"}]},
 {"type":"urltest","tag":"auto","import":[{"group":"filtered"}],"url":%q,"interval":"1h","idle_timeout":"2h"},
 {"type":"subscription","tag":"aggregate","url":%q,"http_client":{},"import":[{"group":"auto"}]},
 {"type":"selector","tag":"top","import":[{"group":"aggregate"}]}]}`, remote.URL, health.URL, remote.URL+"/empty"))
	source, aggregate := getSubscription(t, ctx, "source"), getSubscription(t, ctx, "aggregate")
	assertMembers := func(want []string) {
		for _, name := range []string{"filtered", "auto", "aggregate", "top"} {
			require.Equal(t, want, importGroup(t, ctx, name).All(), name)
		}
	}
	assertMembers([]string{"subscription/source/HK-a"})
	require.True(t, aggregate.SelectOutbound("subscription/source/HK-a"))
	require.NoError(t, aggregate.Refresh(ctx))
	require.Equal(t, "subscription/source/HK-a", aggregate.Selected("tcp").Tag())
	body.Store(`{"outbounds":[{"type":"test","tag":"HK-a"},{"type":"test","tag":"HK-c"},{"type":"test","tag":"JP-b"}]}`)
	require.NoError(t, source.Refresh(ctx))
	assertMembers([]string{"subscription/source/HK-a", "subscription/source/HK-c"})
	require.True(t, importGroup(t, ctx, "top").(adapter.SelectableOutboundGroup).SelectOutbound("subscription/source/HK-c"))
	body.Store(`{"outbounds":[{"type":"test","tag":"HK-c"}]}`)
	require.NoError(t, source.Refresh(ctx))
	assertMembers([]string{"subscription/source/HK-c"})
	require.Equal(t, "subscription/source/HK-c", aggregate.Selected("tcp").Tag())
	conn, err := importGroup(t, ctx, "top").DialContext(ctx, "tcp", M.ParseSocksaddr(strings.TrimPrefix(health.URL, "http://")))
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	body.Store(`{"outbounds":[]}`)
	require.NoError(t, source.Refresh(ctx))
	assertMembers(nil)
	for _, tag := range []string{"filtered", "auto", "aggregate", "top"} {
		g := importGroup(t, ctx, tag)
		require.Nil(t, g.Selected("tcp"), tag)
		_, err := g.DialContext(ctx, "tcp", M.ParseSocksaddr("127.0.0.1:1"))
		require.Error(t, err, tag)
	}
	body.Store(`{"outbounds":[{"type":"test","tag":"HK-c"}]}`)
	require.NoError(t, source.Refresh(ctx))
	assertMembers([]string{"subscription/source/HK-c"})
	// Same name, different type: all import layers reapply their filters.
	body.Store(`{"outbounds":[{"type":"direct","tag":"HK-c"}]}`)
	require.NoError(t, source.Refresh(ctx))
	assertMembers(nil)
}

func TestGroupImportNestedSubscriptionAndOriginalNames(t *testing.T) {
	var childBody atomic.Value
	childBody.Store(`{"outbounds":[{"type":"test","tag":"香港/01"}]}`)
	child := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, childBody.Load()) }))
	defer child.Close()
	parent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"outbounds":[{"type":"subscription","tag":"child","url":%q,"http_client":{}},{"type":"selector","tag":"local","import":[{"group":"child","include":"^香港/"}]}]}`, child.URL)
	}))
	defer parent.Close()
	_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"subscription","tag":"parent","url":%q,"http_client":{}},{"type":"selector","tag":"top","import":[{"group":"subscription/parent/local"}]}]}`, parent.URL))
	childTag := memberTag("parent", "child")
	require.Equal(t, []string{memberTag(childTag, "香港/01")}, importGroup(t, ctx, "top").All())
	childBody.Store(`{"outbounds":[{"type":"test","tag":"香港/02"},{"type":"test","tag":"日本"}]}`)
	handle, _ := service.FromContext[adapter.OutboundManager](ctx).Outbound(childTag)
	control, ok := adapter.SubscriptionFor(handle)
	require.True(t, ok)
	require.NoError(t, control.Refresh(ctx))
	require.Equal(t, []string{memberTag(childTag, "香港/02")}, importGroup(t, ctx, "top").All())
	require.NoError(t, getSubscription(t, ctx, "parent").Refresh(ctx))
	require.Equal(t, []string{memberTag(childTag, "香港/02")}, importGroup(t, ctx, "top").All())
}

func TestGroupImportRejectInvalidAndCyclicConfiguration(t *testing.T) {
	for _, tc := range []struct{ name, outbounds, message string }{
		{"invalid-regexp", `{"type":"selector","tag":"g","import":[{"group":"source","include":"["}]}`, "include"},
		{"self", `{"type":"selector","tag":"g","import":[{"group":"g"}]}`, "circular"},
		{"missing", `{"type":"selector","tag":"g","import":[{"group":"missing"}]}`, "not found"},
		{"not-group", `{"type":"test","tag":"node"},{"type":"selector","tag":"g","import":[{"group":"node"}]}`, "not a group"},
		{"mutual-import", `{"type":"selector","tag":"a","import":[{"group":"b"}]},{"type":"selector","tag":"b","import":[{"group":"a"}]}`, "circular"},
		{"mixed-cycle", `{"type":"selector","tag":"a","import":[{"group":"b"}]},{"type":"selector","tag":"b","outbounds":["a"]}`, "circular"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testContext()
			opts, err := json.UnmarshalExtendedContext[option.Options](ctx, []byte(`{"log":{"disabled":true},"outbounds":[`+tc.outbounds+`]}`))
			require.NoError(t, err)
			instance, err := box.New(box.Options{Options: opts, Context: ctx})
			if err == nil {
				defer instance.Close()
				err = instance.Start()
			}
			require.ErrorContains(t, err, tc.message)
		})
	}
}

func TestGroupImportRefreshCycleRollsBack(t *testing.T) {
	var body atomic.Value
	body.Store(`{"outbounds":[{"type":"test","tag":"node"}]}`)
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body.Load()) }))
	defer remote.Close()
	_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"subscription","tag":"sub","url":%q,"http_client":{}},{"type":"selector","tag":"top","import":[{"group":"sub"}]}]}`, remote.URL))
	s := getSubscription(t, ctx, "sub")
	generation := s.Status().Generation
	body.Store(`{"outbounds":[{"type":"selector","tag":"node","import":[{"group":"top"}]}]}`)
	require.ErrorContains(t, s.Refresh(ctx), "circular")
	require.Equal(t, generation, s.Status().Generation)
	require.Equal(t, []string{"subscription/sub/node"}, importGroup(t, ctx, "top").All())
}

func TestGroupImportURLTestRefreshAndStaleResults(t *testing.T) {
	var oldCalls, newCalls atomic.Int32
	oldTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { oldCalls.Add(1); <-r.Context().Done() }))
	defer oldTarget.Close()
	newTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { newCalls.Add(1); w.WriteHeader(204) }))
	defer newTarget.Close()
	ctx := testContext()
	outbound.Register[fixedDestinationOptions](service.FromContext[adapter.OutboundRegistry](ctx).(*outbound.Registry), "fixed", func(_ context.Context, _ adapter.Router, _ log.ContextLogger, tag string, options fixedDestinationOptions) (adapter.Outbound, error) {
		return &fixedDestinationOutbound{outbound.NewAdapter("fixed", tag, []string{"tcp"}, nil), options.Server}, nil
	})
	var body atomic.Value
	document := func(address string) string {
		return fmt.Sprintf(`{"outbounds":[{"type":"fixed","tag":"same","server":%q}]}`, strings.TrimPrefix(address, "http://"))
	}
	body.Store(document(oldTarget.URL))
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body.Load()) }))
	defer remote.Close()
	_, ctx = startBoxWithContext(t, ctx, fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"subscription","tag":"sub","url":%q,"http_client":{}},{"type":"urltest","tag":"auto","import":[{"group":"sub"}],"url":"http://probe.invalid","interval":"1h","idle_timeout":"2h"}]}`, remote.URL))
	require.Eventually(t, func() bool { return oldCalls.Load() > 0 }, time.Second, time.Millisecond)
	body.Store(document(newTarget.URL))
	require.NoError(t, getSubscription(t, ctx, "sub").Refresh(ctx))
	history := service.PtrFromContext[urltest.HistoryStorage](ctx)
	require.Eventually(t, func() bool {
		return newCalls.Load() > 0 && history.LoadURLTestHistory("subscription/sub/same", "http://probe.invalid") != nil
	}, 2*time.Second, time.Millisecond)
	require.Equal(t, "subscription/sub/same", importGroup(t, ctx, "auto").Selected("tcp").Tag())
}

func TestGroupImportConcurrentRefreshReadsAndSelection(t *testing.T) {
	var body atomic.Value
	body.Store(`{"outbounds":[{"type":"test","tag":"a"}]}`)
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body.Load()) }))
	defer remote.Close()
	_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"subscription","tag":"sub","url":%q,"http_client":{}},{"type":"selector","tag":"mid","import":[{"group":"sub"}]},{"type":"selector","tag":"top","import":[{"group":"mid"}]}]}`, remote.URL))
	s := getSubscription(t, ctx, "sub")
	top := importGroup(t, ctx, "top").(adapter.SelectableOutboundGroup)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				_ = top.All()
				_ = top.Selected("tcp")
				top.SelectOutbound("subscription/sub/a")
			}
		}()
	}
	for i := range 15 {
		if i%2 == 0 {
			body.Store(`{"outbounds":[]}`)
		} else {
			body.Store(`{"outbounds":[{"type":"test","tag":"a"}]}`)
		}
		require.NoError(t, s.Refresh(ctx))
	}
	wg.Wait()
	require.Empty(t, top.All())
}

func TestGroupImportHTTPDownloaderBootstrap(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"outbounds":[{"type":"test","tag":"node"}]}`)
	}))
	defer remote.Close()
	_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"selector","tag":"download","import":[{"group":"sub"}]},{"type":"subscription","tag":"sub","url":%q,"http_client":{"detour":"download"},"bootstrap_http_client":{}}]}`, remote.URL))
	require.NoError(t, getSubscription(t, ctx, "sub").Refresh(ctx))
	require.Equal(t, []string{"subscription/sub/node"}, importGroup(t, ctx, "download").All())
}

func TestGroupImportBootstrapSelectionPreference(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"outbounds":[{"type":"test","tag":"first"},{"type":"test","tag":"second"}]}`)
	}))
	defer remote.Close()
	for _, cached := range []bool{false, true} {
		t.Run(fmt.Sprint(cached), func(t *testing.T) {
			ctx := testContext()
			cache := newResourceCache(nil)
			if cached {
				require.NoError(t, cache.StoreSelected("top", "subscription/sub/second"))
			}
			service.MustRegister[adapter.SelectionCache](ctx, cache)
			_, ctx = startBoxWithContext(t, ctx, fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"test","tag":"fallback"},{"type":"selector","tag":"top","outbounds":["fallback"],"import":[{"group":"sub"}],"default":"subscription/sub/first"},{"type":"subscription","tag":"sub","url":%q,"http_client":{}}]}`, remote.URL))
			expected := "subscription/sub/first"
			if cached {
				expected = "subscription/sub/second"
			}
			require.Equal(t, expected, importGroup(t, ctx, "top").Selected("tcp").Tag())
		})
	}
}

func TestGroupImportDistinctProviderNamesAndFullTagFilter(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"outbounds":[{"type":"test","tag":"same"}]}`)
	}))
	defer remote.Close()
	_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"subscription","tag":"a","url":%q,"http_client":{}},{"type":"subscription","tag":"b","url":%q,"http_client":{}},{"type":"selector","tag":"both","import":[{"group":"a"},{"group":"b"}]},{"type":"selector","tag":"only-b","import":[{"group":"both","match":"tag","include":"^subscription/b/"}]}]}`, remote.URL, remote.URL))
	require.Equal(t, []string{"subscription/a/same", "subscription/b/same"}, importGroup(t, ctx, "both").All())
	require.Equal(t, []string{"subscription/b/same"}, importGroup(t, ctx, "only-b").All())
}

func TestGroupImportSourceGroupRemovalAndReturn(t *testing.T) {
	var body atomic.Value
	document := `{"outbounds":[{"type":"test","tag":"node"},{"type":"selector","tag":"inner","outbounds":["node"]}]}`
	body.Store(document)
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body.Load()) }))
	defer remote.Close()
	_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"subscription","tag":"sub","url":%q,"http_client":{}},{"type":"selector","tag":"top","import":[{"group":"subscription/sub/inner"}]}]}`, remote.URL))
	s := getSubscription(t, ctx, "sub")
	require.Equal(t, []string{"subscription/sub/node"}, importGroup(t, ctx, "top").All())
	body.Store(`{"outbounds":[{"type":"test","tag":"inner"}]}`)
	require.NoError(t, s.Refresh(ctx))
	require.Empty(t, importGroup(t, ctx, "top").All())
	body.Store(`{"outbounds":[]}`)
	require.NoError(t, s.Refresh(ctx))
	require.Empty(t, importGroup(t, ctx, "top").All())
	body.Store(document)
	require.NoError(t, s.Refresh(ctx))
	require.Equal(t, []string{"subscription/sub/node"}, importGroup(t, ctx, "top").All())
}

func TestGroupImportSubscriptionURLTestIncludesImportedMembers(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	defer target.Close()
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{"outbounds":[]}`) }))
	defer remote.Close()
	_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"test","tag":"node"},{"type":"selector","tag":"source","outbounds":["node"]},{"type":"subscription","tag":"sub","url":%q,"http_client":{},"import":[{"group":"source"}],"mode":"urltest","test_url":%q}]}`, remote.URL, target.URL))
	s := getSubscription(t, ctx, "sub")
	result, err := s.URLTest(ctx)
	require.NoError(t, err)
	require.Contains(t, result, "node")
	require.Equal(t, "node", s.Selected("tcp").Tag())
}

func TestGroupImportKeepsStaticGroupReferences(t *testing.T) {
	var body atomic.Value
	body.Store(`{"outbounds":[{"type":"test","tag":"node"}]}`)
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body.Load()) }))
	defer remote.Close()
	_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"subscription","tag":"sub","url":%q,"http_client":{}},{"type":"selector","tag":"explicit","outbounds":["sub"]},{"type":"selector","tag":"dynamic","import":[{"group":"sub"}]}]}`, remote.URL))
	body.Store(`{"outbounds":[]}`)
	require.NoError(t, getSubscription(t, ctx, "sub").Refresh(ctx))
	require.Equal(t, []string{"sub"}, importGroup(t, ctx, "explicit").All())
	require.Empty(t, importGroup(t, ctx, "dynamic").All())
	_, err := importGroup(t, ctx, "explicit").DialContext(ctx, "tcp", M.ParseSocksaddr("127.0.0.1:1"))
	require.Error(t, err)
}

func TestGroupImportNestedSubscriptionConsumer(t *testing.T) {
	empty := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{"outbounds":[]}`) }))
	defer empty.Close()
	parent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"outbounds":[{"type":"test","tag":"node"},{"type":"selector","tag":"source","outbounds":["node"]},{"type":"subscription","tag":"child","url":%q,"http_client":{},"import":[{"group":"source","types":"test"}]}]}`, empty.URL)
	}))
	defer parent.Close()
	_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"subscription","tag":"parent","url":%q,"http_client":{}},{"type":"selector","tag":"top","import":[{"group":"subscription/parent/child"}]}]}`, parent.URL))
	require.Equal(t, []string{"subscription/parent/node"}, importGroup(t, ctx, "top").All())
	require.NoError(t, getSubscription(t, ctx, "parent").Refresh(ctx))
	require.Equal(t, []string{"subscription/parent/node"}, importGroup(t, ctx, "top").All())
}

func TestGroupImportDNSCycleRollback(t *testing.T) {
	var body atomic.Value
	body.Store(`{"outbounds":[{"type":"http","tag":"node","server":"127.0.0.1","server_port":9}]}`)
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body.Load()) }))
	defer remote.Close()
	_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"dns":{"servers":[{"type":"udp","tag":"dns","server":"1.1.1.1","detour":"top"}]},"outbounds":[{"type":"subscription","tag":"sub","url":%q,"http_client":{}},{"type":"selector","tag":"mid","import":[{"group":"sub"}]},{"type":"selector","tag":"top","import":[{"group":"mid"}]}]}`, remote.URL))
	s := getSubscription(t, ctx, "sub")
	generation := s.Status().Generation
	body.Store(`{"outbounds":[{"type":"http","tag":"node","server":"proxy.invalid","server_port":8080,"domain_resolver":"dns"}]}`)
	require.ErrorContains(t, s.Refresh(ctx), "circular")
	require.Equal(t, generation, s.Status().Generation)
}

func TestGroupImportOwnDocumentGroup(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"outbounds":[{"type":"test","tag":"node"},{"type":"selector","tag":"inner","outbounds":["node"]}]}`)
	}))
	defer remote.Close()
	_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"subscription","tag":"sub","url":%q,"http_client":{},"outbounds":["inner"],"import":[{"group":"subscription/sub/inner"}]}]}`, remote.URL))
	require.Equal(t, []string{"subscription/sub/inner", "subscription/sub/node"}, getSubscription(t, ctx, "sub").All())
}

func TestGroupImportNestedOwnDocumentGroup(t *testing.T) {
	var childBody atomic.Value
	childBody.Store(`{"outbounds":[{"type":"test","tag":"node"},{"type":"selector","tag":"inner","outbounds":["node"]}]}`)
	child := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, childBody.Load())
	}))
	defer child.Close()
	parent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"outbounds":[{"type":"subscription","tag":"child","url":%q,"http_client":{},"outbounds":["inner"],"import":[{"group":"subscription/child/inner"}]}]}`, child.URL)
	}))
	defer parent.Close()
	_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"subscription","tag":"parent","url":%q,"http_client":{}},{"type":"selector","tag":"top","import":[{"group":"subscription/parent/child"}]}]}`, parent.URL))
	childTag := memberTag("parent", "child")
	require.Equal(t, []string{memberTag(childTag, "inner"), memberTag(childTag, "node")}, importGroup(t, ctx, "top").All())
	require.NoError(t, getSubscription(t, ctx, "parent").Refresh(ctx))
	require.Equal(t, []string{memberTag(childTag, "inner"), memberTag(childTag, "node")}, importGroup(t, ctx, "top").All())
	childBody.Store(`{"outbounds":[{"type":"selector","tag":"inner","import":[{"group":"subscription/parent/child"}]}]}`)
	handle, _ := service.FromContext[adapter.OutboundManager](ctx).Outbound(childTag)
	control, ok := adapter.SubscriptionFor(handle)
	require.True(t, ok)
	require.ErrorContains(t, control.Refresh(ctx), "circular")
	require.Equal(t, []string{memberTag(childTag, "inner"), memberTag(childTag, "node")}, importGroup(t, ctx, "top").All())

}
