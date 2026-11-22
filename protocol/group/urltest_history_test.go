package group

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/service"
	"github.com/stretchr/testify/require"
)

type historyTestGroup struct {
	*smartTestOutbound
	selected adapter.Outbound
	link     string
}

func (g *historyTestGroup) AttachConnection(io.Closer) func() { return func() {} }
func (g *historyTestGroup) All() []string                     { return []string{g.selected.Tag()} }
func (g *historyTestGroup) Selected(string) adapter.Outbound  { return g.selected }
func (g *historyTestGroup) TestURL() string                   { return g.link }

func TestAutomaticTestsUseRootURLAndStoreOnlyLeaves(t *testing.T) {
	var mu sync.Mutex
	hits := make(map[string]int)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits[r.URL.String()]++
		mu.Unlock()
		w.WriteHeader(204)
	}))
	defer target.Close()
	leaf := smartNode("leaf", func(ctx context.Context, network string, dst M.Socksaddr) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, dst.String())
	})
	c := &historyTestGroup{smartNode("c", nil), leaf, target.URL + "/c"}
	b := &historyTestGroup{smartNode("b", nil), c, target.URL + "/b"}
	smart, manager := smartFixture(t, option.SmartURLTestOutboundOptions{URL: target.URL + "/smart"}, b, c, leaf)
	result, err := smart.URLTest(context.Background())
	require.NoError(t, err)
	require.Len(t, result, 3)
	require.Equal(t, result["leaf"], result["b"])
	legacy, err := NewURLTestGroup(smart.ctx, manager, logger.NOP(), []adapter.Outbound{b, c, leaf}, target.URL+"/legacy", time.Hour, 50, time.Hour, false)
	require.NoError(t, err)
	defer legacy.Close()
	result, err = legacy.URLTest(context.Background())
	require.NoError(t, err)
	require.Len(t, result, 3)
	require.Equal(t, result["leaf"], result["b"])
	for _, name := range []string{"smart", "b", "c"} {
		require.Empty(t, smart.history.URLs(name))
	}
	for _, path := range []string{"/legacy", "/smart"} {
		h := smart.history.LoadURLTestHistory("leaf", target.URL+path)
		require.NotNil(t, h)
		require.Equal(t, target.URL+path, h.URL)
		require.Equal(t, "leaf", h.Outbound)
	}
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, map[string]int{"/legacy": 1, "/smart": 1}, hits)
}

func TestSmartRankingFollowsNestedSelectionAndOwnURL(t *testing.T) {
	x, y, z := smartNode("x", nil), smartNode("y", nil), smartNode("z", nil)
	child := &historyTestGroup{smartNode("child", nil), x, "https://child.example/"}
	s, _ := smartFixture(t, option.SmartURLTestOutboundOptions{URL: "https://parent.example/", Tolerance: new(uint16)}, child, z)
	for tag, delay := range map[string]uint16{"x": 10, "y": 100, "z": 50} {
		s.history.StoreURLTestHistory(tag, &adapter.URLTestHistory{URL: s.TestURL(), Time: time.Now(), Delay: delay})
	}
	s.history.StoreURLTestHistory("y", &adapter.URLTestHistory{URL: child.link, Time: time.Now(), Delay: 1})
	require.Equal(t, "child", s.Selected("tcp").Tag())
	child.selected = y
	require.Equal(t, "z", s.Selected("tcp").Tag(), "switching a nested selector must change ranking without a new probe")
	require.Nil(t, s.history.LoadURLTestHistory("child", s.TestURL()))
	require.EqualValues(t, 100, s.history.LoadOutboundHistory(child, s.TestURL()).Delay)
}

func TestStandaloneBatchRejectsResultsFromReplacedNodes(t *testing.T) {
	leaf := smartNode("leaf", func(ctx context.Context, network string, dst M.Socksaddr) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, dst.String())
	})
	s, manager := smartFixture(t, option.SmartURLTestOutboundOptions{}, leaf)
	updates := service.PtrFromContext[adapter.GroupUpdates](s.ctx)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		updates.Notify()
		w.WriteHeader(204)
	}))
	defer target.Close()
	result := URLTestOutbounds(s.ctx, manager, s.history, logger.NOP(), []adapter.Outbound{leaf}, target.URL, time.Hour, true)
	require.Empty(t, result)
	require.Empty(t, s.history.URLs("leaf"), "a daemon selector test must not revive replaced-node measurements")
}

func TestURLTestDetourProbeFailureDoesNotEraseOtherURLs(t *testing.T) {
	leaf := smartNode("transport", func(context.Context, string, M.Socksaddr) (net.Conn, error) { return nil, errors.New("unreachable") })
	s, manager := smartFixture(t, option.SmartURLTestOutboundOptions{}, leaf)
	g, err := NewURLTestGroup(s.ctx, manager, logger.NOP(), []adapter.Outbound{leaf}, "https://group.example/", time.Hour, 50, time.Hour, false)
	require.NoError(t, err)
	defer g.Close()
	dialer := &URLTest{group: g, logger: logger.NOP()}
	for _, link := range []string{"https://a.example/", "https://b.example/"} {
		s.history.StoreURLTestHistory(leaf.Tag(), &adapter.URLTestHistory{Time: time.Now(), URL: link, Delay: 42})
	}
	_, err = dialer.DialContext(adapter.ContextWithURLTest(s.ctx), "tcp", M.ParseSocksaddr("server.example:443"))
	require.Error(t, err)
	require.Len(t, s.history.URLs(leaf.Tag()), 2, "probe traffic must not invalidate the detour's independent histories")
}
