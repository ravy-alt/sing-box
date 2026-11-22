package clashapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/stretchr/testify/require"
)

func TestSmartGroupAPIUsesConfiguredRankingAndProvidesCacheClear(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
	defer target.Close()
	config := fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"http","tag":"bad","server":"127.0.0.1","server_port":1},{"type":"direct","tag":"node"},{"type":"smart-urltest","tag":"smart","outbounds":["bad","node"],"url":%q}]}`, target.URL)
	_, s, _ := startNamedSubscriptionBox(t, config)
	w := groupRequest(s, "smart", "", "3000")
	require.Equal(t, 200, w.Code, w.Body.String())
	var delays map[string]uint16
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &delays))
	require.Contains(t, delays, "node")
	require.NotContains(t, delays, "bad")
	outbound, _ := s.outbound.Outbound("smart")
	smart := outbound.(adapter.SmartURLTestGroup)
	require.Equal(t, "node", smart.Selected("tcp").Tag())
	// Optional panel IPv6/custom-URL probes don't overwrite primary rankings.
	w = groupRequest(s, "smart", "http://127.0.0.1:1/unreachable", "1000")
	require.Equal(t, 504, w.Code, w.Body.String())
	require.Equal(t, "node", smart.Selected("tcp").Tag())
	for name, status := range map[string]int{"smart": 204, "node": 400, "missing": 404} {
		w = httptest.NewRecorder()
		groupRouter(s).ServeHTTP(w, httptest.NewRequest(http.MethodDelete, "/"+name+"/cache", nil))
		require.Equal(t, status, w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	groupRouter(s).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/smart", nil))
	require.Equal(t, 200, w.Code)
	var info map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &info))
	require.Equal(t, "SmartURLTest", info["type"])
	require.NotContains(t, info, "x-sing-box-type")
	proxy := readProbeInfo(t, s, "smart", "")
	require.Equal(t, "SmartURLTest", proxy["type"])
	require.NotContains(t, proxy, "x-sing-box-type")
}

type dynamicProbeGroup struct {
	*testProvider
	best   adapter.Outbound
	actual adapter.Outbound
}

func (g *dynamicProbeGroup) DynamicSelection()                {}
func (g *dynamicProbeGroup) Selected(string) adapter.Outbound { return g.best }
func (g *dynamicProbeGroup) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return g.actual.DialContext(ctx, network, destination)
}

func TestDynamicGroupProbePinsSelectedLeafWithoutBusinessFallback(t *testing.T) {
	best := &delayedProbe{&namedTestOutbound{name: "best-but-unreachable"}, func(context.Context, string, M.Socksaddr) (net.Conn, error) { return nil, fmt.Errorf("unreachable") }}
	actual := &providerProbe{&namedTestOutbound{name: "working-fallback"}}
	dynamic := &dynamicProbeGroup{testProvider: &testProvider{name: "smart"}, best: best, actual: actual}
	s, _ := probeServer(t, []adapter.Outbound{dynamic, best, actual})
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
	defer target.Close()
	request := httptest.NewRequest(http.MethodGet, "/smart/delay?"+url.Values{"url": {target.URL}, "timeout": {"1000"}}.Encode(), nil)
	w := httptest.NewRecorder()
	proxyRouter(s, nil).ServeHTTP(w, request)
	require.Equal(t, 503, w.Code, w.Body.String())
	require.Empty(t, s.urlTestHistory.URLs("smart"), "groups must not own results")
	require.Nil(t, s.urlTestHistory.LoadURLTestHistory(best.Tag(), target.URL))
	require.Empty(t, s.urlTestHistory.URLs(actual.Tag()), "unselected fallback must not be probed")
	outer := &testProvider{all: []string{"smart"}}
	_, err := s.testGroup(context.Background(), outer, target.URL)
	require.Error(t, err)
}
