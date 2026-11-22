package clashapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/stretchr/testify/require"
)

func TestSmartGroupTCPDelayDoesNotWaitForAutomaticUDP(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
	defer target.Close()
	blackhole, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer blackhole.Close()
	config := fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"direct","tag":"node"},{"type":"smart-urltest","tag":"smart","outbounds":["node"],"url":%q,"udp_test":{"server":"127.0.0.1","server_port":%d,"timeout":"5s"}}]}`, target.URL, blackhole.LocalAddr().(*net.UDPAddr).Port)
	_, s, _ := startNamedSubscriptionBox(t, config)
	// Hold an actual automatic round in its UDP phase. A panel TCP request
	// must succeed before its short deadline, without joining that round.
	require.NoError(t, blackhole.SetReadDeadline(time.Now().Add(2*time.Second)))
	_, _, err = blackhole.ReadFrom(make([]byte, 4096))
	require.NoError(t, err)
	for _, link := range []string{"", target.URL} {
		w := groupRequest(s, "smart", link, "500")
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		var delays map[string]uint16
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &delays))
		require.Positive(t, delays["node"])
		require.NotNil(t, s.urlTestHistory.LoadURLTestHistory("node", target.URL))
		require.Empty(t, s.urlTestHistory.UDPHistories("node"), "TCP must not invent a UDP result")
	}
}

func TestSmartGroupAPIUsesConfiguredRankingAndProvidesCacheClear(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
	defer target.Close()
	config := fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"http","tag":"bad","server":"127.0.0.1","server_port":1},{"type":"direct","tag":"node"},{"type":"smart-urltest","tag":"smart","outbounds":["bad","node"],"url":%q},{"type":"selector","tag":"select","outbounds":["node","smart"]}]}`, target.URL)
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
	require.Equal(t, false, proxy["selectable"])
	require.Equal(t, false, info["selectable"])
	require.Equal(t, true, readProbeInfo(t, s, "select", "")["selectable"])
	selectNode := func(group, node string) int {
		t.Helper()
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPut, "/"+group, strings.NewReader(fmt.Sprintf(`{"name":%q}`, node)))
		request.Header.Set("Content-Type", "application/json")
		proxyRouter(s, nil).ServeHTTP(response, request)
		return response.Code
	}
	require.Equal(t, http.StatusNoContent, selectNode("select", "smart"))
	require.Equal(t, "smart", readProbeInfo(t, s, "select", "")["now"])
	require.Equal(t, http.StatusBadRequest, selectNode("smart", "bad"))
	require.Equal(t, "node", readProbeInfo(t, s, "smart", "")["now"])
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

func TestGroupMetadataKeepsEmptyGroupsAndGlobalSelectionConsistent(t *testing.T) {
	_, s, _ := startNamedSubscriptionBox(t, `{"log":{"disabled":true},"outbounds":[{"type":"direct","tag":"direct"}]}`)
	w := httptest.NewRecorder()
	proxyRouter(s, nil).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
	require.Equal(t, 200, w.Code)
	var info struct {
		Proxies map[string]map[string]any `json:"proxies"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &info))
	global := info.Proxies["GLOBAL"]
	require.Contains(t, global["all"], global["now"])
	require.Equal(t, false, global["selectable"])

	empty := &testProvider{name: "empty"}
	s, _ = probeServer(t, []adapter.Outbound{empty})
	proxy := readProbeInfo(t, s, "empty", "")
	require.Equal(t, []any{}, proxy["all"])
}
