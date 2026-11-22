package clashapi

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	boxDNS "github.com/sagernet/sing-box/dns"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/service"
	"github.com/sagernet/ws"
	"github.com/sagernet/ws/wsutil"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/require"
)

type diagnosticTransport struct{ boxDNS.TransportAdapter }

type diagnosticVisitor struct {
	adapter.Outbound
	router adapter.DNSRouter
}

func (v diagnosticVisitor) VisitDNSInspectors(visit func(string, adapter.DNSRouter) error) error {
	return visit("subscription", v.router)
}

type diagnosticOutbounds struct {
	adapter.OutboundManager
	entries []adapter.Outbound
}

func (m diagnosticOutbounds) Outbounds() []adapter.Outbound { return m.entries }

func (t *diagnosticTransport) Start(adapter.StartStage, *adapter.Scope) error { return nil }
func (t *diagnosticTransport) Reset()                                         {}
func (t *diagnosticTransport) Exchange(ctx context.Context, message *dns.Msg) (*dns.Msg, error) {
	return boxDNS.FixedResponse(message.Id, message.Question[0], []netip.Addr{netip.MustParseAddr("192.0.2.1"), netip.MustParseAddr("2001:db8::1")}, 300), nil
}
func (t *diagnosticTransport) ExchangeAsync(ctx context.Context, message *dns.Msg, cb func(*dns.Msg, error)) {
	cb(t.Exchange(ctx, message))
}

func diagnosticAPIServer(t *testing.T) *Server {
	t.Helper()
	ctx := service.ContextWithDefaultRegistry(context.Background())
	registry := boxDNS.NewTransportRegistry()
	boxDNS.RegisterTransport[struct{}](registry, "test", func(_ context.Context, _ log.ContextLogger, tag string, _ struct{}) (adapter.DNSTransport, error) {
		return &diagnosticTransport{boxDNS.NewTransportAdapter("test", tag, nil)}, nil
	})
	manager := boxDNS.NewTransportManager(registry, nil, "same")
	ctx = service.ContextWith[adapter.DNSTransportManager](ctx, manager)
	require.NoError(t, manager.Create(ctx, log.NewNOPFactory().Logger(), "same", "test", nil))
	router, err := boxDNS.NewRouter(ctx, log.NewNOPFactory(), option.DNSOptions{})
	require.NoError(t, err)
	scope := adapter.NewScope(ctx, log.NewNOPFactory().Logger())
	require.NoError(t, router.StartBootstrap(scope))
	t.Cleanup(func() { require.NoError(t, scope.Close()) })
	return &Server{ctx: ctx, dnsRouter: router}
}

func TestDNSManagementAPIQueryCacheAndDelete(t *testing.T) {
	s := diagnosticAPIServer(t)
	handler := dnsRouter(s)
	request := func(method, path string) *httptest.ResponseRecorder {
		r := httptest.NewRecorder()
		handler.ServeHTTP(r, httptest.NewRequest(method, path, nil))
		return r
	}
	response := request("GET", "/query?name=example.com&type=A&server=same")
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var result struct {
		Server string
		Trace  adapter.DNSQueryInfo
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
	require.Equal(t, "same", result.Server)
	require.Equal(t, "manual-server", result.Trace.Rule)
	require.Equal(t, "manual", result.Trace.Purpose)
	response = request("GET", "/query?name=example.com&type=A")
	require.Equal(t, http.StatusOK, response.Code)
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
	require.True(t, result.Trace.Cached)
	require.Equal(t, "default", result.Trace.Rule)
	response = request("GET", "/cache")
	var frame dnsCacheFrame
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &frame))
	require.Len(t, frame.Entries, 1)
	require.Equal(t, "default", frame.Entries[0].Info.Rule)
	require.Equal(t, http.StatusBadRequest, request("GET", "/query?name=&type=A").Code)
	require.Equal(t, http.StatusBadRequest, request("GET", "/query?name=example.com&type=INVALID").Code)
	require.NotEqual(t, http.StatusOK, request("GET", "/query?name=example.com&server=missing").Code)
	response = request("GET", "/servers")
	require.Contains(t, response.Body.String(), `"cacheCount":1`)
	response = request("DELETE", "/cache?id="+url.QueryEscape(frame.Entries[0].ID))
	require.Equal(t, http.StatusOK, response.Code)
	require.Contains(t, response.Body.String(), `"deleted":1`)
	response = request("GET", "/cache")
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &frame))
	require.Empty(t, frame.Entries)
}

func TestDNSManagementScopedServerNames(t *testing.T) {
	host, child := diagnosticAPIServer(t), diagnosticAPIServer(t)
	host.outbound = diagnosticOutbounds{entries: []adapter.Outbound{diagnosticVisitor{router: child.dnsRouter}}}
	handler := dnsRouter(host)
	for _, scope := range []string{"", "subscription"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest("GET", "/query?name=example.com&type=A&server=same&scope="+scope, nil))
		require.Equal(t, http.StatusOK, response.Code)
	}
	entries, err := host.dnsEntries()
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.NotEqual(t, entries[0].ID, entries[1].ID)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest("DELETE", "/cache?scope=subscription&server=same", nil))
	require.Equal(t, http.StatusOK, response.Code)
	entries, err = host.dnsEntries()
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Empty(t, entries[0].Scope, "scoped deletion must leave the host's same-name server intact")
}

func TestDNSCacheStreamDeltaAndClose(t *testing.T) {
	s := diagnosticAPIServer(t)
	returned := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dnsRouter(s).ServeHTTP(w, r)
		if r.Header.Get("Upgrade") == "websocket" {
			returned <- struct{}{}
		}
	}))
	defer server.Close()
	conn, buffered, _, err := ws.Dial(context.Background(), "ws"+strings.TrimPrefix(server.URL, "http")+"/cache")
	require.NoError(t, err)
	defer conn.Close()
	var reader io.Reader = conn
	if buffered != nil {
		reader = buffered
	}
	stream := struct {
		io.Reader
		io.Writer
	}{reader, conn}
	read := func() dnsCacheFrame {
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(3*time.Second)))
		data, _, err := wsutil.ReadServerData(stream)
		require.NoError(t, err)
		var frame dnsCacheFrame
		require.NoError(t, json.Unmarshal(data, &frame))
		return frame
	}
	first := read()
	require.True(t, first.Full)
	require.Empty(t, first.Entries)
	response, err := http.Get(server.URL + "/query?name=example.com")
	require.NoError(t, err)
	_, _ = io.Copy(io.Discard, response.Body)
	response.Body.Close()
	second := read()
	require.False(t, second.Full)
	require.Equal(t, first.Revision+1, second.Revision)
	require.Len(t, second.Entries, 1)
	request, err := http.NewRequest("DELETE", server.URL+"/cache", nil)
	require.NoError(t, err)
	response, err = http.DefaultClient.Do(request)
	require.NoError(t, err)
	_, _ = io.Copy(io.Discard, response.Body)
	response.Body.Close()
	third := read()
	require.Equal(t, []string{second.Entries[0].ID}, third.Removed)
	require.NoError(t, conn.Close())
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("DNS stream handler survived client disconnect")
	}
}
