package clashapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/stretchr/testify/require"
)

type configuredProbeGroup struct {
	*testProvider
	selected adapter.Outbound
	link     string
}

func (g *configuredProbeGroup) Selected(string) adapter.Outbound { return g.selected }
func (g *configuredProbeGroup) TestURL() string                  { return g.link }

func readProbeInfo(t *testing.T, s *Server, name, link string) map[string]any {
	t.Helper()
	w := httptest.NewRecorder()
	path := "/" + url.PathEscape(name)
	if link != "" {
		path += "?" + url.Values{"url": {link}}.Encode()
	}
	proxyRouter(s, nil).ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
	require.Equal(t, 200, w.Code, w.Body.String())
	var info map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &info))
	return info
}

func TestNestedQueriesUseRootURLAndCurrentLeafOnly(t *testing.T) {
	x, y := &providerProbe{&namedTestOutbound{name: "x"}}, &providerProbe{&namedTestOutbound{name: "y"}}
	c := &configuredProbeGroup{&testProvider{name: "c", all: []string{"x", "y"}}, x, "https://c.example/"}
	b := &configuredProbeGroup{&testProvider{name: "b", all: []string{"c", "y"}}, c, "https://b.example/"}
	a := &configuredProbeGroup{&testProvider{name: "a", all: []string{"b"}}, b, "https://a.example/"}
	s, _ := probeServer(t, []adapter.Outbound{a, b, c, x, y})
	for i, group := range []*configuredProbeGroup{a, b, c} {
		for j, node := range []string{"x", "y"} {
			s.urlTestHistory.StoreURLTestHistory(node, &adapter.URLTestHistory{URL: group.link, Time: time.Now(), Delay: uint16(10*(i+1) + j)})
		}
	}
	assertHistory := func(name, link, node string, delay int) {
		info := readProbeInfo(t, s, name, link)
		history := info["history"].([]any)
		require.Len(t, history, 1)
		record := history[0].(map[string]any)
		require.EqualValues(t, delay, record["delay"])
		require.Equal(t, node, record["outbound"])
	}
	assertHistory("a", "", "x", 10)
	assertHistory("b", "", "x", 20)
	assertHistory("c", "", "x", 30)
	assertHistory("a", b.link, "x", 20)
	b.selected = y
	assertHistory("a", "", "y", 11)
	assertHistory("b", "", "y", 21)
	assertHistory("c", "", "x", 30)
	for _, group := range []string{"a", "b", "c"} {
		require.Empty(t, s.urlTestHistory.URLs(group))
	}
	require.Empty(t, readProbeInfo(t, s, "a", "https://never-tested.example/")["history"])
}

func TestNestedProbeDeduplicatesLeavesAndPinsAtStart(t *testing.T) {
	var hits atomic.Int32
	entered, release := make(chan struct{}, 1), make(chan struct{})
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/probe?scope=a", r.URL.String())
		hits.Add(1)
		entered <- struct{}{}
		<-release
		w.WriteHeader(204)
	}))
	defer target.Close()
	leaf := &providerProbe{&namedTestOutbound{name: "leaf"}}
	other := &providerProbe{&namedTestOutbound{name: "other"}}
	b := &configuredProbeGroup{&testProvider{name: "b", all: []string{"leaf", "other"}}, leaf, target.URL + "/different"}
	a := &configuredProbeGroup{&testProvider{name: "a", all: []string{"b"}}, b, target.URL + "/probe?scope=a"}
	s, _ := probeServer(t, []adapter.Outbound{a, b, leaf, other})
	finished := make(chan *httptest.ResponseRecorder, 1)
	go func() { finished <- groupRequest(s, "auto", a.link, "1000") }()
	// The auto fixture also includes the unrelated leaf, so this batch makes
	// two real requests; a/b/leaf must share one of those requests.
	<-entered
	// All jobs have been pinned before any HTTP request starts.
	b.selected = other
	close(release)
	w := <-finished
	require.Equal(t, 200, w.Code, w.Body.String())
	var result map[string]uint16
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
	require.Equal(t, result["leaf"], result["a"])
	require.Equal(t, result["leaf"], result["b"])
	require.EqualValues(t, 2, hits.Load())
	require.Empty(t, s.urlTestHistory.URLs("a"))
	require.Empty(t, s.urlTestHistory.URLs("b"))
	// Selection changes need no copied-group-history invalidation.
	require.Equal(t, s.urlTestHistory.LoadURLTestHistory("other", a.link).Delay, s.urlTestHistory.LoadOutboundHistory(a, a.link).Delay)
}
