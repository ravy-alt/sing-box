package clashapi

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	box "github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/certificate"
	"github.com/sagernet/sing-box/adapter/endpoint"
	"github.com/sagernet/sing-box/adapter/inbound"
	"github.com/sagernet/sing-box/adapter/outbound"
	boxservice "github.com/sagernet/sing-box/adapter/service"
	"github.com/sagernet/sing-box/common/interrupt"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/dns"
	"github.com/sagernet/sing-box/dns/transport"
	"github.com/sagernet/sing-box/dns/transport/hosts"
	"github.com/sagernet/sing-box/dns/transport/local"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/direct"
	"github.com/sagernet/sing-box/protocol/group"
	httpproxy "github.com/sagernet/sing-box/protocol/http"
	"github.com/sagernet/sing-box/protocol/subscription"
	"github.com/sagernet/sing/common/json"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/service"

	"github.com/go-chi/chi/v5"
	mDNS "github.com/miekg/dns"
	"github.com/stretchr/testify/require"
)

// Each document deliberately uses the same node, detour, resolver, bootstrap
// resolver and domain names. Traffic counters identify the actual resources.
func namedSubscriptionChain(t *testing.T) (string, func() [3]int32) {
	t.Helper()
	proxy := func() (uint16, *atomic.Int32) {
		hits := new(atomic.Int32)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodConnect {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			upstream, err := net.DialTimeout("tcp", r.Host, time.Second)
			if err != nil {
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			defer upstream.Close()
			conn, reader, err := w.(http.Hijacker).Hijack()
			if err != nil {
				return
			}
			defer conn.Close()
			hits.Add(1)
			fmt.Fprint(conn, "HTTP/1.1 200 Connection Established\r\n\r\n")
			done := make(chan struct{})
			go func() { defer close(done); io.Copy(upstream, reader); upstream.Close() }()
			io.Copy(conn, upstream)
			conn.Close()
			<-done
		}))
		t.Cleanup(server.Close)
		return M.ParseSocksaddr(strings.TrimPrefix(server.URL, "http://")).Port, hits
	}
	exitPort, exitHits := proxy()
	relayPort, relayHits := proxy()
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	queries := new(atomic.Int32)
	ready := make(chan struct{})
	resolver := &mDNS.Server{PacketConn: packet, NotifyStartedFunc: func() { close(ready) }, Handler: mDNS.HandlerFunc(func(w mDNS.ResponseWriter, request *mDNS.Msg) {
		response := new(mDNS.Msg).SetReply(request)
		if request.Question[0].Qtype == mDNS.TypeA {
			queries.Add(1)
			response.Answer = []mDNS.RR{&mDNS.A{Hdr: mDNS.RR_Header{Name: request.Question[0].Name, Rrtype: mDNS.TypeA, Class: mDNS.ClassINET, Ttl: 60}, A: net.ParseIP("127.0.0.1")}}
		}
		_ = w.WriteMsg(response)
	})}
	go func() { _ = resolver.ActivateAndServe() }()
	<-ready
	t.Cleanup(func() { require.NoError(t, resolver.Shutdown()) })
	document := fmt.Sprintf(`{"dns":{"servers":[{"type":"udp","tag":"解析","server":"dns.private.invalid","server_port":%d,"domain_resolver":"引导"},{"type":"hosts","tag":"引导","predefined":{"dns.private.invalid":"127.0.0.1"}}]},"outbounds":[{"type":"http","tag":"香港","server":"127.0.0.1","server_port":%d,"detour":"中转"},{"type":"http","tag":"中转","server":"relay.private.invalid","server_port":%d,"domain_resolver":{"server":"解析","strategy":"ipv4_only","disable_cache":true}}]}`, packet.LocalAddr().(*net.UDPAddr).Port, exitPort, relayPort)
	return document, func() [3]int32 { return [3]int32{exitHits.Load(), relayHits.Load(), queries.Load()} }
}

func startNamedSubscriptionBox(t *testing.T, config string) (*box.Box, *Server, http.Handler) {
	t.Helper()
	ctx := service.ContextWithDefaultRegistry(context.Background())
	outbounds := outbound.NewRegistry()
	subscription.RegisterOutbound(outbounds)
	group.RegisterSelector(outbounds)
	group.RegisterURLTest(outbounds)
	group.RegisterSmartURLTest(outbounds)
	direct.RegisterOutbound(outbounds)
	httpproxy.RegisterOutbound(outbounds)
	transports := dns.NewTransportRegistry()
	local.RegisterTransport(transports)
	hosts.RegisterTransport(transports)
	transport.RegisterUDP(transports)
	ctx = box.Context(ctx, inbound.NewRegistry(), outbounds, endpoint.NewRegistry(), transports, boxservice.NewRegistry(), certificate.NewRegistry())
	opts, err := json.UnmarshalExtendedContext[option.Options](ctx, []byte(config))
	require.NoError(t, err)
	instance, err := box.New(box.Options{Options: opts, Context: ctx})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, instance.Close()) })
	require.NoError(t, instance.Start())
	server := &Server{ctx: ctx, outbound: service.FromContext[adapter.OutboundManager](ctx), endpoint: service.FromContext[adapter.EndpointManager](ctx), urlTestHistory: service.PtrFromContext[urltest.HistoryStorage](ctx)}
	router := chi.NewRouter()
	router.Mount("/proxies", proxyRouter(server, nil))
	router.Mount("/providers/proxies", proxyProviderRouter(server))
	return instance, server, router
}

func TestSubscriptionNamesKeepRoutingIdentity(t *testing.T) {
	aDocument, aHits := namedSubscriptionChain(t)
	bDocument, bHits := namedSubscriptionChain(t)
	readdedDocument, readdedHits := namedSubscriptionChain(t)
	var a, b, c atomic.Value
	a.Store(aDocument)
	b.Store(bDocument)
	c.Store(`{"outbounds":[]}`)
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/a":
			fmt.Fprint(w, a.Load())
		case "/b":
			fmt.Fprint(w, b.Load())
		case "/c":
			fmt.Fprint(w, c.Load())
		}
	}))
	t.Cleanup(source.Close)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, "correct chain") }))
	t.Cleanup(target.Close)
	cache := t.TempDir()
	sources := make(map[string]string)
	for path, tag := range map[string]string{"a": "机场%41", "b": "机场A", "c": "第三个"} {
		sources[path] = fmt.Sprintf(`{"tag":%q,"url":%q,"http_client":{},"cache_path":%q}`, tag, source.URL+"/"+path, filepath.Join(cache, path+".json"))
	}
	config := func(order string) string {
		return fmt.Sprintf(`{"log":{"disabled":true},"dns":{"servers":[{"type":"hosts","tag":"解析"}]},"subscriptions":[%s],"outbounds":[{"type":"direct","tag":"香港-1"},{"type":"selector","tag":"选择%%41","import":[{"group":"机场%%41","include":"^香港$"},{"group":"机场A","include":"^香港$"},{"group":"第三个"}]},{"type":"selector","tag":"选择A","import":[{"group":"机场%%41","include":"^香港$"},{"group":"机场A","include":"^香港$"}]}]}`, order)
	}
	instance, server, router := startNamedSubscriptionBox(t, config(sources["a"]+","+sources["b"]+","+sources["c"]))
	request := func(method, path, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		return w
	}
	member := func(provider, name string) string {
		return "subscription/" + url.PathEscape(provider) + "/" + url.PathEscape(name)
	}
	aTag, bTag := member("机场%41", "香港"), member("机场A", "香港")
	assertName := func(tag, name string) {
		require.Equal(t, name, adapter.OutboundDisplayName(server.ctx, tag))
		require.Equal(t, tag, adapter.OutboundControlTag(server.ctx, name))
	}
	assertName(aTag, "香港")
	assertName(bTag, "香港-2")
	selectNode := func(name string, code int) {
		w := request(http.MethodPut, "/proxies/"+url.PathEscape("选择%41"), fmt.Sprintf(`{"name":%q}`, name))
		require.Equal(t, code, w.Code, w.Body.String())
	}
	refresh := func(tag string) {
		w := request(http.MethodPut, "/providers/proxies/"+url.PathEscape(tag), "")
		require.Equal(t, http.StatusNoContent, w.Code, w.Body.String())
	}
	dial := func() net.Conn {
		g, ok := server.outbound.Outbound("选择%41")
		require.True(t, ok)
		conn, err := g.DialContext(interrupt.ContextWithIsExternalConnection(server.ctx, true), "tcp", M.ParseSocksaddr(strings.TrimPrefix(target.URL, "http://")))
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })
		return conn
	}
	read := func(conn net.Conn) {
		require.NoError(t, conn.SetDeadline(time.Now().Add(3*time.Second)))
		_, err := fmt.Fprintf(conn, "GET / HTTP/1.1\r\nHost: %s\r\n\r\n", strings.TrimPrefix(target.URL, "http://"))
		require.NoError(t, err)
		response, err := http.ReadResponse(bufio.NewReader(conn), nil)
		require.NoError(t, err)
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		require.NoError(t, err)
		require.Equal(t, "correct chain", string(body))
	}
	selectNode("香港-2", http.StatusNoContent)
	other, _ := server.outbound.Outbound("选择A")
	require.Equal(t, aTag, other.(adapter.OutboundGroup).Selected("tcp").Tag(), "an encoded group name must not select the other group")
	conn := dial()
	read(conn)
	conn.Close()
	require.Equal(t, [3]int32{1, 1, 1}, bHits(), "both proxy hops and DNS must belong to B")
	require.Equal(t, [3]int32{}, aHits())
	selectNode("香港", http.StatusNoContent)
	old := dial()
	read(old)
	require.Equal(t, [3]int32{1, 1, 1}, aHits())
	a.Store(`{"outbounds":[]}`)
	refresh("机场%41")
	read(old)
	c.Store(`{"outbounds":[{"type":"direct","tag":"香港"}]}`)
	refresh("第三个")
	assertName(member("第三个", "香港"), "香港-3")
	selectNode("香港-2", http.StatusNoContent)
	selectNode("香港", http.StatusBadRequest)
	require.Equal(t, http.StatusNotFound, request(http.MethodGet, "/proxies/"+url.PathEscape("香港"), "").Code)
	// A stale name cannot select B or the newcomer, or pass provider membership checks.
	probePath := "/providers/proxies/" + url.PathEscape("机场A") + "/" + url.PathEscape("香港") + "/healthcheck?timeout=1000"
	require.Equal(t, http.StatusNotFound, request(http.MethodGet, probePath, "").Code)
	conn = dial()
	read(conn)
	conn.Close()
	require.Equal(t, [3]int32{2, 2, 2}, bHits())
	require.Equal(t, [3]int32{1, 1, 1}, aHits())
	old.Close()
	require.NoError(t, instance.Close())
	// Restore B and C before the empty A: its deleted node's reserved name
	// must survive in the cache even though no current member uses it.
	a.Store(`invalid`)
	b.Store(`invalid`)
	c.Store(`invalid`)
	_, server, router = startNamedSubscriptionBox(t, config(sources["c"]+","+sources["b"]+","+sources["a"]))
	assertName(aTag, "香港")
	assertName(bTag, "香港-2")
	assertName(member("第三个", "香港"), "香港-3")
	selectNode("香港", http.StatusBadRequest)
	a.Store(readdedDocument)
	refresh("机场%41")
	assertName(aTag, "香港")
	selectNode("香港", http.StatusNoContent)
	conn = dial()
	read(conn)
	conn.Close()
	require.Equal(t, [3]int32{1, 1, 1}, readdedHits(), "readded A must use its new proxy chain and DNS resources")
	require.Equal(t, [3]int32{1, 1, 1}, aHits(), "the old generation must not receive new connections")
	require.Equal(t, [3]int32{2, 2, 2}, bHits(), "A must never dial the identically named B")
}
