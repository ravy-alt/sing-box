package subscription

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/service"
	"github.com/stretchr/testify/require"
)

func TestDashboardHistoryUpdatesSubscriptionURLTestSelection(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, goodDocument) }))
	defer remote.Close()
	_, ctx := startBox(t, configFor(remote.URL, `"mode":"urltest","test_url":"http://127.0.0.1:1","tolerance":20,`))
	s := getSubscription(t, ctx, "sub")
	// Stop scheduled probes so only the completed dashboard measurement drives
	// this test; the current generation remains alive and routable.
	s.pauseUpdates()
	first, second := memberTag("sub", "exit"), memberTag("sub", "relay")
	store := func(tag string, delay uint16) {
		s.history.StoreURLTestHistory(tag, &adapter.URLTestHistory{URL: s.options.TestURL, Time: time.Now(), Delay: delay})
	}
	store(first, 10)
	store(second, 100)
	s.PerformUpdateCheck()
	require.Equal(t, first, s.Selected("tcp").Tag())
	store(first, 100)
	store(second, 10)
	s.PerformUpdateCheck()
	require.Equal(t, second, s.Selected("tcp").Tag())
	require.Equal(t, second, s.Selected("udp").Tag())
	store(first, 5)
	s.PerformUpdateCheck()
	require.Equal(t, second, s.Selected("tcp").Tag(), "retain the current member within tolerance")
	s.history.DeleteURLTestHistory(second)
	s.PerformUpdateCheck()
	require.Equal(t, first, s.Selected("tcp").Tag(), "a failed member must not retain selection over a healthy one")
}

type networkSelectionTestGroup struct {
	outbound.Adapter
	manager adapter.OutboundManager
	tags    []string
}

func (g *networkSelectionTestGroup) All() []string { return g.tags }
func (g *networkSelectionTestGroup) Selected(network string) adapter.Outbound {
	index := 0
	if network == "udp" {
		index = 1
	}
	node, _ := g.manager.Outbound(g.tags[index])
	return node
}
func (g *networkSelectionTestGroup) AttachConnection(io.Closer) func() { return func() {} }
func (g *networkSelectionTestGroup) DialContext(ctx context.Context, network string, dst M.Socksaddr) (net.Conn, error) {
	return g.Selected(network).DialContext(ctx, network, dst)
}
func (g *networkSelectionTestGroup) ListenPacket(ctx context.Context, dst M.Socksaddr) (net.PacketConn, error) {
	return g.Selected("udp").ListenPacket(ctx, dst)
}

func TestSubscriptionURLTestUsesSeparateNestedNetworkLeaves(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/parent", r.URL.Path)
		w.WriteHeader(204)
	}))
	t.Cleanup(target.Close)
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"outbounds":[{"type":"test","tag":"tcp"},{"type":"test","tag":"udp"},{"type":"test","tag":"other"},{"type":"split-network-test","tag":"inner","outbounds":["tcp","udp"]},{"type":"selector","tag":"middle","outbounds":["inner"]}]}`)
	}))
	t.Cleanup(remote.Close)
	ctx := testContext()
	outbound.Register[option.SelectorOutboundOptions](service.FromContext[adapter.OutboundRegistry](ctx).(*outbound.Registry), "split-network-test", func(ctx context.Context, _ adapter.Router, _ log.ContextLogger, tag string, options option.SelectorOutboundOptions) (adapter.Outbound, error) {
		return &networkSelectionTestGroup{Adapter: outbound.NewAdapter("selector", tag, []string{"tcp", "udp"}, options.Outbounds), manager: service.FromContext[adapter.OutboundManager](ctx), tags: options.Outbounds}, nil
	})
	_, ctx = startBoxWithContext(t, ctx, configFor(remote.URL, fmt.Sprintf(`"test_url":%q,"outbounds":["middle","other"],`, target.URL+"/parent")))
	s := getSubscription(t, ctx, "sub")
	// Enable ranking without a background round racing the seeded observations.
	s.options.Mode = C.TypeURLTest
	result, err := s.URLTest(ctx)
	require.NoError(t, err)
	middle, other := memberTag("sub", "middle"), memberTag("sub", "other")
	require.Len(t, result, 2)
	for _, tag := range []string{"tcp", "udp", "other"} {
		require.NotNil(t, s.history.LoadURLTestHistory(memberTag("sub", tag), s.options.TestURL))
	}
	require.Equal(t, s.history.LoadURLTestHistory(memberTag("sub", "tcp"), s.options.TestURL).Delay, result[middle])
	require.Empty(t, s.history.URLs(middle))
	require.Empty(t, s.history.URLs(memberTag("sub", "inner")))
	for tag, delay := range map[string]uint16{"tcp": 10, "udp": 500, "other": 100} {
		s.history.StoreURLTestHistory(memberTag("sub", tag), &adapter.URLTestHistory{Time: time.Now(), URL: s.options.TestURL, Delay: delay})
	}
	s.PerformUpdateCheck()
	require.Equal(t, middle, s.Selected("tcp").Tag())
	require.Equal(t, other, s.Selected("udp").Tag())
	// The same UDP leaf measured at another URL must not change this ranking.
	s.history.StoreURLTestHistory(memberTag("sub", "udp"), &adapter.URLTestHistory{Time: time.Now(), URL: target.URL + "/other-url", Delay: 1})
	s.PerformUpdateCheck()
	require.Equal(t, other, s.Selected("udp").Tag())
}

func TestSubscriptionAutomaticHistoryUsesConfiguredURLAndFinalLeaf(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/subscription?scope=a", r.URL.String())
		w.WriteHeader(204)
	}))
	defer target.Close()
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"outbounds":[{"type":"test","tag":"leaf"},{"type":"selector","tag":"middle","outbounds":["leaf"]}]}`)
	}))
	defer remote.Close()
	_, ctx := startBox(t, configFor(remote.URL, fmt.Sprintf(`"mode":"urltest","test_url":%q,`, target.URL+"/subscription?scope=a")))
	s := getSubscription(t, ctx, "sub")
	result, err := s.URLTest(ctx)
	require.NoError(t, err)
	leaf, middle := memberTag("sub", "leaf"), memberTag("sub", "middle")
	require.Contains(t, result, leaf)
	require.Equal(t, result[leaf], result[middle])
	h := s.history.LoadURLTestHistory(leaf, s.options.TestURL)
	require.NotNil(t, h)
	require.Equal(t, s.options.TestURL, h.URL)
	require.Equal(t, leaf, h.Outbound)
	require.Empty(t, s.history.URLs(middle))
	require.Empty(t, s.history.URLs("sub"))
}
