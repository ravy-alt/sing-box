package clashapi

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/endpoint"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/service"

	"github.com/stretchr/testify/require"
)

func groupRequest(s *Server, name, target, timeout string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	groupRouter(s).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/"+url.PathEscape(name)+"/delay?"+url.Values{"url": {target}, "timeout": {timeout}}.Encode(), nil))
	return w
}

func TestURLTestGroupUsesDashboardURL(t *testing.T) {
	var configuredHits, requestedHits atomic.Int32
	configured := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		configuredHits.Add(1)
		// Keep the automatic check in progress throughout the dashboard probe.
		<-r.Context().Done()
	}))
	t.Cleanup(configured.Close)
	requested := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/requested?ipv6=1", r.URL.String())
		require.Equal(t, http.MethodHead, r.Method)
		requestedHits.Add(1)
		w.WriteHeader(204)
	}))
	t.Cleanup(requested.Close)
	_, s, _ := startNamedSubscriptionBox(t, fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"http","tag":"bad","server":"127.0.0.1","server_port":1},{"type":"direct","tag":"node"},{"type":"urltest","tag":"auto","outbounds":["bad","node"],"url":%q},{"type":"selector","tag":"outer","outbounds":["auto"]}]}`, configured.URL))
	require.Eventually(t, func() bool { return configuredHits.Load() > 0 }, time.Second, time.Millisecond)
	auto, _ := s.outbound.Outbound("auto")
	control := auto.(adapter.OutboundGroup)
	require.Equal(t, "bad", control.Selected("tcp").Tag())
	for _, name := range []string{"auto"} {
		w := groupRequest(s, name, requested.URL+"/requested?ipv6=1", "1000")
		require.Equal(t, 200, w.Code, w.Body.String())
		var result map[string]uint16
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
		require.Len(t, result, 1)
		for _, delay := range result {
			require.Positive(t, delay, "successful probes cannot be encoded as unavailable")
		}
		require.Equal(t, "bad", control.Selected("tcp").Tag(), "a different test URL must not change the configured ranking")
	}
	require.EqualValues(t, 1, requestedHits.Load())
	// An unreachable requested target must not succeed using the configured URL
	// or a previous history entry, including through a nested group.
	for _, name := range []string{"auto", "outer"} {
		w := groupRequest(s, name, "https://127.0.0.1:1/unreachable", "1000")
		require.Equal(t, 504, w.Code, w.Body.String())
	}
	for _, timeout := range []string{"0", "-1", "60001", "invalid"} {
		require.Equal(t, 400, groupRequest(s, "auto", requested.URL, timeout).Code)
	}
}

type delayedProbe struct {
	*namedTestOutbound
	dial func(context.Context, string, M.Socksaddr) (net.Conn, error)
}

func (p *delayedProbe) DialContext(ctx context.Context, network string, dst M.Socksaddr) (net.Conn, error) {
	return p.dial(ctx, network, dst)
}

func probeServer(t *testing.T, nodes []adapter.Outbound) (*Server, *adapter.GroupUpdates) {
	t.Helper()
	ctx := service.ContextWithDefaultRegistry(context.Background())
	updates := adapter.NewGroupUpdates()
	service.MustRegisterPtr(ctx, updates)
	p := &testProvider{name: "auto"}
	objects := map[string]adapter.Outbound{"auto": p}
	for _, node := range nodes {
		p.all = append(p.all, node.Tag())
		objects[node.Tag()] = node
	}
	registry := outbound.NewRegistry()
	outbound.Register[option.StubOptions](registry, "test", func(_ context.Context, _ adapter.Router, _ log.ContextLogger, tag string, _ option.StubOptions) (adapter.Outbound, error) {
		return objects[tag], nil
	})
	manager := outbound.NewManager(registry, endpoint.NewManager(nil), "auto")
	for tag := range objects {
		require.NoError(t, manager.Create(ctx, nil, logger.NOP(), tag, "test", nil))
	}
	return &Server{ctx: ctx, outbound: manager, urlTestHistory: urltest.NewHistoryStorage()}, updates
}

func TestGroupProbeSlowMembersDoNotStarveHealthyNodes(t *testing.T) {
	var started atomic.Int32
	nodes := make([]adapter.Outbound, 0, 32)
	for i := range 31 {
		nodes = append(nodes, &delayedProbe{&namedTestOutbound{name: fmt.Sprint("slow-", i)}, func(ctx context.Context, _ string, _ M.Socksaddr) (net.Conn, error) {
			started.Add(1)
			<-ctx.Done()
			return nil, ctx.Err()
		}})
	}
	nodes = append(nodes, &providerProbe{&namedTestOutbound{name: "healthy"}})
	s, _ := probeServer(t, nodes)
	for _, node := range nodes {
		s.urlTestHistory.StoreURLTestHistory(node.Tag(), &adapter.URLTestHistory{Time: time.Now(), Delay: 99})
	}
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
	defer target.Close()
	start := time.Now()
	w := groupRequest(s, "auto", target.URL, "300")
	require.Equal(t, 200, w.Code, w.Body.String())
	require.Less(t, time.Since(start), time.Second)
	require.EqualValues(t, 31, started.Load())
	var result map[string]uint16
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
	require.Len(t, result, 1)
	require.Positive(t, result["healthy"])
	for _, node := range nodes[:31] {
		require.Nil(t, s.urlTestHistory.LoadURLTestHistory(node.Tag(), target.URL), "the queried URL must not retain a success")
		require.NotNil(t, s.urlTestHistory.LoadURLTestHistory(node.Tag()), "other URLs must remain intact")
	}
}

func TestGroupProbeRejectsReplacedGeneration(t *testing.T) {
	s, updates := probeServer(t, []adapter.Outbound{&providerProbe{&namedTestOutbound{name: "same-name"}}})
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		updates.Notify()
		w.WriteHeader(204)
	}))
	defer target.Close()
	w := groupRequest(s, "auto", target.URL, "1000")
	require.Equal(t, 504, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), "group changed")
	require.Nil(t, s.urlTestHistory.LoadURLTestHistory("same-name"))
}

type probeCertificateStore struct {
	adapter.CertificateStore
	pool *x509.CertPool
}

func (s *probeCertificateStore) Pool() *x509.CertPool { return s.pool }

func TestDashboardHTTPSProbeUsesCoreCertificateStore(t *testing.T) {
	s, _ := probeServer(t, []adapter.Outbound{&providerProbe{&namedTestOutbound{name: "node"}}})
	var hits atomic.Int32
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ipv6-probe" || r.Method != http.MethodHead {
			w.WriteHeader(400)
			return
		}
		hits.Add(1)
		w.WriteHeader(204)
	}))
	defer target.Close()
	// Reject untrusted TLS. Supporting a user-specified URL must not disable
	// certificate verification.
	require.Equal(t, 504, groupRequest(s, "auto", target.URL+"/ipv6-probe", "1000").Code)
	pool := x509.NewCertPool()
	pool.AddCert(target.Certificate())
	service.MustRegister[adapter.CertificateStore](s.ctx, &probeCertificateStore{pool: pool})
	w := groupRequest(s, "auto", target.URL+"/ipv6-probe", "1000")
	require.Equal(t, 200, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), `"node":`)
	require.EqualValues(t, 1, hits.Load())
}

type switchingProbeGroup struct {
	*testProvider
	selected adapter.Outbound
	dialed   atomic.Int32
}

func (s *switchingProbeGroup) Selected(string) adapter.Outbound { return s.selected }
func (s *switchingProbeGroup) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	// A selection change after the initial snapshot must not redirect this
	// measurement while leaving its history attributed to the previous member.
	s.dialed.Add(1)
	return nil, errors.New("group has switched to a different member")
}

func TestGroupProbePinsSelectedDialerToHistory(t *testing.T) {
	selected := &providerProbe{&namedTestOutbound{name: "selected"}}
	nested := &switchingProbeGroup{testProvider: &testProvider{name: "nested"}, selected: selected}
	s, _ := probeServer(t, []adapter.Outbound{nested})
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
	defer target.Close()
	w := groupRequest(s, "auto", target.URL, "1000")
	require.Equal(t, 200, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), `"nested":`)
	require.Zero(t, nested.dialed.Load())
	require.NotNil(t, s.urlTestHistory.LoadURLTestHistory("selected", target.URL))
}

func TestProxyRevisionChangesOnRefreshReaddAndRestart(t *testing.T) {
	var document atomic.Value
	document.Store(`{"outbounds":[{"type":"direct","tag":"same-name"}]}`)
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, document.Load()) }))
	defer source.Close()
	config := fmt.Sprintf(`{"log":{"disabled":true},"subscriptions":[{"tag":"source","url":%q,"http_client":{},"cache_path":%q}],"outbounds":[{"type":"direct","tag":"direct"}]}`, source.URL, filepath.Join(t.TempDir(), "source.json"))
	instance, s, router := startNamedSubscriptionBox(t, config)
	read := func() (int, string) {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/proxies/same-name", nil))
		var info map[string]any
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &info))
		version, _ := info["x-sing-box-revision"].(string)
		return w.Code, version
	}
	code, first := read()
	require.Equal(t, 200, code)
	require.NotEmpty(t, first)
	_, again := read()
	require.Equal(t, first, again)
	provider, _ := s.outbound.Outbound("source")
	refresh := func(data string) {
		document.Store(data)
		require.NoError(t, provider.(adapter.Subscription).Refresh(context.Background()))
	}
	refresh(`{"outbounds":[{"type":"direct","tag":"same-name","connect_timeout":"2s"}]}`)
	_, changed := read()
	require.NotEqual(t, first, changed)
	refresh(`{"dns":{"servers":[{"type":"hosts","tag":"resolver","predefined":{"proxy.example":"127.0.0.1"}}]},"outbounds":[{"type":"direct","tag":"same-name","connect_timeout":"2s"}]}`)
	_, dnsChanged := read()
	require.NotEqual(t, changed, dnsChanged, "a DNS-only graph change must invalidate the badge too")
	refresh(`{"outbounds":[]}`)
	code, _ = read()
	require.Equal(t, 404, code)
	refresh(`{"outbounds":[{"type":"direct","tag":"same-name"}]}`)
	_, readded := read()
	require.NotEqual(t, changed, readded)
	require.NotEqual(t, first, readded)
	require.NoError(t, instance.Close())
	_, _, router = startNamedSubscriptionBox(t, config)
	_, restarted := read()
	require.NotEqual(t, readded, restarted)
}
