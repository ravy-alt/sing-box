package subscription

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
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
	"github.com/sagernet/sing-box/common/dialer"
	"github.com/sagernet/sing-box/dns"
	"github.com/sagernet/sing-box/dns/transport"
	"github.com/sagernet/sing-box/dns/transport/hosts"
	"github.com/sagernet/sing-box/dns/transport/local"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/direct"
	"github.com/sagernet/sing-box/protocol/group"
	httpproxy "github.com/sagernet/sing-box/protocol/http"
	"github.com/sagernet/sing/common/json"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"

	mDNS "github.com/miekg/dns"
	"github.com/stretchr/testify/require"
)

type testOptions struct {
	option.DialerOptions
	Fail bool `json:"fail,omitempty"`
}
type testOutbound struct {
	outbound.Adapter
	N.Dialer
	fail   bool
	closed atomic.Bool
}

func (o *testOutbound) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	if stage == adapter.StartStateInitialize {
		scope.Add(func() error { o.closed.Store(true); return nil })
	}
	if stage == adapter.StartStateStart && o.fail {
		return fmt.Errorf("injected start failure")
	}
	return nil
}
func testContext() context.Context {
	ctx := service.ContextWithDefaultRegistry(context.Background())
	outbounds := outbound.NewRegistry()
	RegisterOutbound(outbounds)
	group.RegisterSelector(outbounds)
	group.RegisterURLTest(outbounds)
	group.RegisterSmartURLTest(outbounds)
	direct.RegisterOutbound(outbounds)
	httpproxy.RegisterOutbound(outbounds)
	outbound.Register[testOptions](outbounds, "test", func(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options testOptions) (adapter.Outbound, error) {
		d, err := dialer.New(ctx, options.DialerOptions, true)
		if err != nil {
			return nil, err
		}
		return &testOutbound{Adapter: outbound.NewAdapterWithDialerOptions("test", tag, []string{"tcp", "udp"}, options.DialerOptions), Dialer: d, fail: options.Fail}, nil
	})
	dnsRegistry := dns.NewTransportRegistry()
	local.RegisterTransport(dnsRegistry)
	hosts.RegisterTransport(dnsRegistry)
	transport.RegisterUDP(dnsRegistry)
	transport.RegisterHTTPS(dnsRegistry)
	transport.RegisterRace(dnsRegistry)
	endpoints := endpoint.NewRegistry()
	endpoint.Register[testOptions](endpoints, "test", func(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options testOptions) (adapter.Endpoint, error) {
		d, err := dialer.New(ctx, options.DialerOptions, true)
		if err != nil {
			return nil, err
		}
		return &testOutbound{Adapter: outbound.NewAdapterWithDialerOptions("test", tag, []string{"tcp", "udp"}, options.DialerOptions), Dialer: d, fail: options.Fail}, nil
	})
	return box.Context(ctx, inbound.NewRegistry(), outbounds, endpoints, dnsRegistry, boxservice.NewRegistry(), certificate.NewRegistry())
}
func startBox(t *testing.T, config string) (*box.Box, context.Context) {
	t.Helper()
	return startBoxWithContext(t, testContext(), config)
}

func startBoxWithContext(t *testing.T, ctx context.Context, config string) (*box.Box, context.Context) {
	t.Helper()
	opts, err := json.UnmarshalExtendedContext[option.Options](ctx, []byte(config))
	require.NoError(t, err)
	instance, err := box.New(box.Options{Options: opts, Context: ctx})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, instance.Close()) })
	require.NoError(t, instance.Start())
	return instance, ctx
}
func getSubscription(t *testing.T, ctx context.Context, tag string) *Subscription {
	t.Helper()
	o, ok := service.FromContext[adapter.OutboundManager](ctx).Outbound(tag)
	require.True(t, ok)
	return o.(*Subscription)
}
func configFor(server, extra string) string {
	return fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"subscription","tag":"sub","url":%q,"http_client":{},%s "update_interval":"24h"}]}`, server, extra)
}

const goodDocument = `{"outbounds":[{"type":"test","tag":"exit","detour":"relay"},{"type":"test","tag":"relay"}]}`

func TestHTTPClientConfigurationRoundTrip(t *testing.T) {
	ctx := testContext()
	for _, clients := range []string{``, `"http_client":{},"bootstrap_http_client":{},`, `"http_client":"shared","bootstrap_http_client":{"detour":"bootstrap"},`} {
		options, err := json.UnmarshalExtendedContext[option.Options](ctx, []byte(configFor("https://subscription.invalid", clients)))
		require.NoError(t, err)
		encoded, err := json.MarshalContext(ctx, options)
		require.NoError(t, err)
		roundTrip, err := json.UnmarshalExtendedContext[option.Options](ctx, encoded)
		require.NoError(t, err)
		require.Equal(t, options.Outbounds[0].Options, roundTrip.Outbounds[0].Options)
	}
}

func TestGraphValidation(t *testing.T) {
	cases := []struct{ name, content, error string }{
		{"chain", goodDocument, ""},
		{"self", `{"outbounds":[{"type":"test","tag":"a","detour":"a"}]}`, "circular"},
		{"chain-cycle", `{"outbounds":[{"type":"test","tag":"a","detour":"b"},{"type":"test","tag":"b","detour":"a"}]}`, "circular"},
		{"group-cycle", `{"outbounds":[{"type":"test","tag":"a","detour":"g"},{"type":"selector","tag":"g","outbounds":["a"]}]}`, "circular"},
		{"missing", `{"outbounds":[{"type":"test","tag":"a","detour":"absent"}]}`, "missing outbound"},
		{"duplicate", `{"outbounds":[{"type":"test","tag":"a"},{"type":"test","tag":"a"}]}`, "duplicate"},
		{"dns-cycle", `{"dns":{"servers":[{"type":"udp","tag":"d","server":"1.1.1.1","detour":"a"}]},"outbounds":[{"type":"test","tag":"a","domain_resolver":"d"}]}`, "circular"},
		{"dns-missing", `{"outbounds":[{"type":"test","tag":"a","domain_resolver":"missing"}]}`, "missing dns_server"},
		{"dns-through-ip-proxy", `{"dns":{"servers":[{"type":"https","tag":"d","server":"1.1.1.1","detour":"a"}]},"outbounds":[{"type":"http","tag":"a","server":"127.0.0.1","server_port":8080}]}`, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, err := parseDocument(testContext(), "p", []byte(tc.content), nil)
			if tc.error != "" {
				require.ErrorContains(t, err, tc.error)
				return
			}
			require.NoError(t, err)
			require.NotEmpty(t, d.order)
		})
	}
	d, err := parseDocument(testContext(), "p", []byte(goodDocument), []string{"exit"})
	require.NoError(t, err)
	require.Len(t, d.members, 1)
	require.Len(t, d.options.Outbounds, 2)
	require.Equal(t, memberTag("p", "relay"), d.options.Outbounds[0].Options.(*testOptions).Detour)
}

func TestDownloadBootstrapDNSRules(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, goodDocument) }))
	defer server.Close()
	url := strings.Replace(server.URL, "127.0.0.1", "subscription.invalid", 1)
	config := fmt.Sprintf(`{"log":{"disabled":true},"dns":{"servers":[{"type":"hosts","tag":"empty"},{"type":"hosts","tag":"download","predefined":{"subscription.invalid":"127.0.0.1"}}],"rules":[{"domain":"subscription.invalid","server":"download"}],"final":"empty"},"outbounds":[{"type":"subscription","tag":"sub","url":%q,"http_client":{}}]}`, url)
	_, ctx := startBox(t, config)
	require.True(t, getSubscription(t, ctx, "sub").Status().Ready)
}

func TestImportedDNSResolverChain(t *testing.T) {
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	var queries atomic.Int32
	dnsServer := &mDNS.Server{PacketConn: packet, Handler: mDNS.HandlerFunc(func(w mDNS.ResponseWriter, request *mDNS.Msg) {
		queries.Add(1)
		response := new(mDNS.Msg).SetReply(request)
		if request.Question[0].Qtype == mDNS.TypeA {
			response.Answer = []mDNS.RR{&mDNS.A{Hdr: mDNS.RR_Header{Name: request.Question[0].Name, Rrtype: mDNS.TypeA, Class: mDNS.ClassINET, Ttl: 60}, A: net.ParseIP("127.0.0.1")}}
		}
		_ = w.WriteMsg(response)
	})}
	go func() { _ = dnsServer.ActivateAndServe() }()
	defer dnsServer.Shutdown()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"dns":{"servers":[{"type":"udp","tag":"remote","server":"resolver.invalid","server_port":%d,"domain_resolver":"bootstrap"},{"type":"hosts","tag":"bootstrap","predefined":{"resolver.invalid":"127.0.0.1"}}],"final":"remote"},"outbounds":[{"type":"test","tag":"a","domain_resolver":"remote"}]}`, packet.LocalAddr().(*net.UDPAddr).Port)
	}))
	defer server.Close()
	_, ctx := startBox(t, configFor(server.URL, ""))
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer target.Close()
	destination := M.ParseSocksaddr(strings.TrimPrefix(target.URL, "http://"))
	destination = M.ParseSocksaddrHostPort("target.invalid", destination.Port)
	s := getSubscription(t, ctx, "sub")
	conn, err := s.DialContext(ctx, "tcp", destination)
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	require.Positive(t, queries.Load())
	queries.Store(0)
	require.NoError(t, s.Refresh(ctx))
	conn, err = s.DialContext(ctx, "tcp", destination)
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	require.Positive(t, queries.Load(), "the new generation must own a new DNS cache")
}

func TestForcedDrainTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, goodDocument) }))
	defer server.Close()
	_, ctx := startBox(t, configFor(server.URL, `"drain_timeout":"20ms",`))
	s := getSubscription(t, ctx, "sub")
	target, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer target.Close()
	conn, err := s.DialContext(ctx, "tcp", M.SocksaddrFromNet(target.Addr()))
	require.NoError(t, err)
	defer conn.Close()
	peer, err := target.Accept()
	require.NoError(t, err)
	defer peer.Close()
	require.NoError(t, s.Refresh(ctx))
	require.NoError(t, peer.SetReadDeadline(time.Now().Add(3*time.Second)))
	_, err = peer.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF)
}

func TestRefreshRollbackAndDrain(t *testing.T) {
	var body atomic.Value
	body.Store(goodDocument)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body.Load().(string)) }))
	defer server.Close()
	_, ctx := startBox(t, configFor(server.URL, `"outbounds":["exit"],`))
	s := getSubscription(t, ctx, "sub")
	old := s.current
	require.True(t, s.SelectOutbound(memberTag("sub", "exit")))
	echo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "ok") }))
	defer echo.Close()
	dest := M.ParseSocksaddr(strings.TrimPrefix(echo.URL, "http://"))
	conn, err := s.DialContext(ctx, "tcp", dest)
	require.NoError(t, err)
	body.Store(`{"outbounds":[{"type":"test","tag":"exit","detour":"exit"}]}`)
	require.ErrorContains(t, s.Refresh(ctx), "circular")
	require.Same(t, old, s.current)
	body.Store(`{"outbounds":[{"type":"test","tag":"exit","fail":true}]}`)
	require.ErrorContains(t, s.Refresh(ctx), "injected start failure")
	require.Same(t, old, s.current)
	body.Store(`{"outbounds":[{"type":"test","tag":"exit"}]}`)
	require.NoError(t, s.Refresh(ctx))
	require.NotSame(t, old, s.current)
	require.False(t, old.closed)
	require.Equal(t, memberTag("sub", "exit"), s.Selected("tcp").Tag())
	_, err = fmt.Fprintf(conn, "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
	require.NoError(t, err)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(time.Second)))
	response, err := io.ReadAll(conn)
	require.NoError(t, err)
	require.Contains(t, string(response), "ok")
	require.NoError(t, conn.Close())
	require.True(t, old.closed)
	require.EqualValues(t, 2, s.Status().Generation)
}

func TestDNSIsolationAndRules(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"dns":{"servers":[{"type":"hosts","tag":"d","predefined":{"private.invalid":"127.0.0.1"}}],"rules":[{"domain":"private.invalid","server":"d"}]},"outbounds":[{"type":"test","tag":"a"}]}`)
	}))
	defer server.Close()
	_, ctx := startBox(t, configFor(server.URL, ""))
	s := getSubscription(t, ctx, "sub")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	conn, err := s.DialContext(ctx, "tcp", M.ParseSocksaddrHostPort("private.invalid", uint16(listener.Addr().(*net.TCPAddr).Port)))
	require.NoError(t, err)
	conn.Close()
	_, found := service.FromContext[adapter.DNSTransportManager](ctx).Transport("d")
	require.False(t, found)
}

func TestBootstrapCacheAndConditionalDownload(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Header.Get("If-None-Match") == `"v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"v1"`)
		fmt.Fprint(w, goodDocument)
	}))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "sub.json")
	config := fmt.Sprintf(`{"log":{"disabled":true},"http_clients":[{"tag":"download","detour":"g"}],"outbounds":[{"type":"selector","tag":"g","outbounds":["sub"]},{"type":"subscription","tag":"sub","url":%q,"http_client":"download","bootstrap_http_client":{},"cache_path":%q}]}`, server.URL, path)
	instance, ctx := startBox(t, config)
	s := getSubscription(t, ctx, "sub")
	require.NoError(t, s.Refresh(ctx))
	require.EqualValues(t, 1, s.Status().Generation)
	require.EqualValues(t, 2, requests.Load())
	require.NoError(t, instance.Close())
	server.Close()
	_, ctx2 := startBox(t, config)
	s2 := getSubscription(t, ctx2, "sub")
	require.True(t, s2.Status().Ready)
	require.Equal(t, []string{memberTag("sub", "exit"), memberTag("sub", "relay")}, s2.All())
}

func TestConcurrentRefreshAndDial(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, goodDocument) }))
	defer server.Close()
	_, ctx := startBox(t, configFor(server.URL, ""))
	s := getSubscription(t, ctx, "sub")
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 12 {
				o := s.Selected("tcp")
				require.NotNil(t, o)
				conn, err := o.DialContext(ctx, "tcp", M.ParseSocksaddr(strings.TrimPrefix(server.URL, "http://")))
				require.NoError(t, err)
				if conn != nil {
					conn.Close()
				}
				s.SelectOutbound(memberTag("sub", "exit"))
				_ = s.ProviderOutbounds()
				_ = s.Status()
			}
		}()
	}
	for range 6 {
		require.NoError(t, s.Refresh(ctx))
	}
	wg.Wait()
}

func TestColdStartWithoutBootstrapStartsEmpty(t *testing.T) {
	ctx := testContext()
	config := `{"log":{"disabled":true},"outbounds":[{"type":"subscription","tag":"sub","url":"http://127.0.0.1:1"}]}`
	opts, err := json.UnmarshalExtendedContext[option.Options](ctx, []byte(config))
	require.NoError(t, err)
	instance, err := box.New(box.Options{Options: opts, Context: ctx})
	require.NoError(t, err)
	defer instance.Close()
	require.NoError(t, instance.Start())
	s := getSubscription(t, ctx, "sub")
	require.False(t, s.Status().Ready)
	require.NotEmpty(t, s.Status().LastError)
	require.Empty(t, s.All())
}

func TestHostGroupReferencesImportedMembers(t *testing.T) {
	var document atomic.Value
	document.Store(goodDocument)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, document.Load()) }))
	defer server.Close()
	config := fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"selector","tag":"proxy","import":[{"group":"sub","include":"^exit$"}]},{"type":"subscription","tag":"sub","url":%q,"http_client":{"detour":"proxy"},"bootstrap_http_client":{}}],"route":{"final":"proxy"}}`, server.URL)
	_, ctx := startBox(t, config)
	s := getSubscription(t, ctx, "sub")
	manager := service.FromContext[adapter.OutboundManager](ctx)
	proxy, _ := manager.Outbound("proxy")
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer target.Close()
	destination := M.ParseSocksaddr(strings.TrimPrefix(target.URL, "http://"))
	conn, err := proxy.DialContext(ctx, "tcp", destination)
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	document.Store(`{"outbounds":[{"type":"test","tag":"exit"}]}`)
	require.NoError(t, s.Refresh(ctx))
	conn, err = proxy.DialContext(ctx, "tcp", destination)
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	require.EqualValues(t, 2, s.Status().Generation)
}

func TestMissingImportedMemberFailsConfigurationCheck(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, goodDocument) }))
	defer server.Close()
	ctx := testContext()
	config := fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"selector","tag":"proxy","outbounds":["subscription/sub/missing"]},{"type":"subscription","tag":"sub","url":%q,"http_client":{}}]}`, server.URL)
	options, err := json.UnmarshalExtendedContext[option.Options](ctx, []byte(config))
	require.NoError(t, err)
	instance, err := box.New(box.Options{Options: options, Context: ctx})
	require.ErrorContains(t, err, "is not declared in this configuration document")
	require.Nil(t, instance)
}

func TestUDPDrainAndDownloadLimit(t *testing.T) {
	var body atomic.Value
	body.Store(goodDocument)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body.Load().(string)) }))
	defer server.Close()
	_, ctx := startBox(t, configFor(server.URL, `"max_download_size":1024,`))
	s := getSubscription(t, ctx, "sub")
	udp, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer udp.Close()
	destination := M.ParseSocksaddr(udp.LocalAddr().String())
	packet, err := s.ListenPacket(ctx, destination)
	require.NoError(t, err)
	old := s.current
	body.Store(strings.Repeat("x", 1025))
	require.ErrorContains(t, s.Refresh(ctx), "max_download_size")
	require.Same(t, old, s.current)
	body.Store(goodDocument)
	require.NoError(t, s.Refresh(ctx))
	require.False(t, old.closed)
	_, err = packet.WriteTo([]byte("alive"), udp.LocalAddr())
	require.NoError(t, err)
	require.NoError(t, udp.SetReadDeadline(time.Now().Add(time.Second)))
	buffer := make([]byte, 16)
	n, _, err := udp.ReadFrom(buffer)
	require.NoError(t, err)
	require.Equal(t, "alive", string(buffer[:n]))
	require.NoError(t, packet.Close())
	require.True(t, old.closed)
}

func TestCrossSubscriptionBootstrapOrder(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); fmt.Fprint(w, goodDocument) }))
	defer server.Close()
	config := fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"subscription","tag":"first","url":%q,"http_client":{"detour":"second"}},{"type":"subscription","tag":"second","url":%q,"http_client":{}}]}`, server.URL, server.URL)
	_, ctx := startBox(t, config)
	require.True(t, getSubscription(t, ctx, "first").Status().Ready)
	require.True(t, getSubscription(t, ctx, "second").Status().Ready)
	require.EqualValues(t, 2, calls.Load())
	require.NotEqual(t, getSubscription(t, ctx, "first").All()[0], getSubscription(t, ctx, "second").All()[0])
}

func TestRefreshCancellationAndShutdown(t *testing.T) {
	var block atomic.Bool
	entered := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if block.Load() {
			entered <- struct{}{}
			<-r.Context().Done()
			return
		}
		fmt.Fprint(w, goodDocument)
	}))
	defer server.Close()
	instance, ctx := startBox(t, configFor(server.URL, ""))
	s := getSubscription(t, ctx, "sub")
	block.Store(true)
	done := make(chan error, 1)
	go func() { done <- s.Refresh(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("download did not start")
	}
	require.NoError(t, instance.Close())
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("refresh was not cancelled")
	}
	require.False(t, s.Status().Ready)
}

func TestURLTestAndSelection(t *testing.T) {
	probe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	defer probe.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, goodDocument) }))
	defer server.Close()
	_, ctx := startBox(t, configFor(server.URL, fmt.Sprintf(`"test_url":%q,`, probe.URL)))
	s := getSubscription(t, ctx, "sub")
	result, err := s.URLTest(ctx)
	require.NoError(t, err)
	require.Len(t, result, 2)
	require.True(t, s.SelectOutbound(memberTag("sub", "relay")))
	require.NoError(t, s.Refresh(ctx))
	require.Equal(t, memberTag("sub", "relay"), s.Selected("tcp").Tag())
	manager := service.FromContext[adapter.OutboundManager](ctx)
	handle, found := manager.Outbound(memberTag("sub", "exit"))
	require.True(t, found)
	require.NotNil(t, handle)
}

func TestRealHTTPProxyChain(t *testing.T) {
	newProxy := func() (*httptest.Server, *atomic.Int32) {
		count := new(atomic.Int32)
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
			client, buffer, err := w.(http.Hijacker).Hijack()
			if err != nil {
				upstream.Close()
				return
			}
			defer client.Close()
			defer upstream.Close()
			count.Add(1)
			fmt.Fprint(client, "HTTP/1.1 200 Connection Established\r\n\r\n")
			go func() { _, _ = io.Copy(upstream, buffer); upstream.Close() }()
			_, _ = io.Copy(client, upstream)
		}))
		return server, count
	}
	relay, relayCount := newProxy()
	defer relay.Close()
	exit, exitCount := newProxy()
	defer exit.Close()
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "through both proxies") }))
	defer target.Close()
	relayAddr := M.ParseSocksaddr(strings.TrimPrefix(relay.URL, "http://"))
	exitAddr := M.ParseSocksaddr(strings.TrimPrefix(exit.URL, "http://"))
	source := fmt.Sprintf(`{"outbounds":[{"type":"http","tag":"exit","server":%q,"server_port":%d,"detour":"relay"},{"type":"http","tag":"relay","server":%q,"server_port":%d}]}`, exitAddr.AddrString(), exitAddr.Port, relayAddr.AddrString(), relayAddr.Port)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, source) }))
	defer server.Close()
	_, ctx := startBox(t, configFor(server.URL, `"outbounds":["exit"],`))
	s := getSubscription(t, ctx, "sub")
	transport := &http.Transport{DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		return s.DialContext(ctx, network, M.ParseSocksaddr(addr))
	}}
	defer transport.CloseIdleConnections()
	response, err := (&http.Client{Transport: transport, Timeout: 3 * time.Second}).Get(target.URL)
	require.NoError(t, err)
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, "through both proxies", string(body))
	require.EqualValues(t, 1, relayCount.Load())
	require.EqualValues(t, 1, exitCount.Load())
}
