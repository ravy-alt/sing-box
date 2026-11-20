package subscription

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/transport/trojan"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
	"github.com/stretchr/testify/require"
)

func TestAuditDNSCycles(t *testing.T) {
	for _, servers := range []string{
		`[{"type":"udp","tag":"a","server":"a.invalid","domain_resolver":"a"}]`,
		`[{"type":"udp","tag":"a","server":"a.invalid","domain_resolver":"b"},{"type":"udp","tag":"b","server":"b.invalid","domain_resolver":"a"}]`,
		`[{"type":"udp","tag":"a","server":"a.invalid","domain_resolver":{}}]`,
	} {
		_, err := parseDocument(testContext(), "p", []byte(`{"dns":{"servers":`+servers+`},"outbounds":[{"type":"http","tag":"proxy","server":"127.0.0.1","server_port":9}]}`), nil)
		require.ErrorContains(t, err, "circular")
	}
}

func TestAuditCrossScopeCyclesRollback(t *testing.T) {
	initial := `{"outbounds":[{"type":"http","tag":"exit","server":"127.0.0.1","server_port":9}]}`
	for _, tc := range []struct{ name, host, source string }{
		{"self-provider", "", `{"outbounds":[{"type":"test","tag":"exit","detour":"sub"}]}`},
		{"host-group", `{"type":"selector","tag":"bridge","outbounds":["sub"]},`, `{"outbounds":[{"type":"selector","tag":"exit","import":[{"group":"bridge"}]}]}`},
		{"host-dns", ``, `{"outbounds":[{"type":"http","tag":"exit","server":"peer.invalid","server_port":9,"domain_resolver":"remote"}]}`},
		{"inherited-dns", ``, `{"outbounds":[{"type":"http","tag":"exit","server":"peer.invalid","server_port":9}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var body atomic.Value
			body.Store(initial)
			remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body.Load()) }))
			defer remote.Close()
			dns := ""
			if strings.Contains(tc.name, "dns") {
				dns = `"dns":{"servers":[{"type":"udp","tag":"remote","server":"1.1.1.1","detour":"sub"}]},`
			}
			_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},%s"outbounds":[%s{"type":"subscription","tag":"sub","url":%q,"http_client":{}}]}`, dns, tc.host, remote.URL))
			s := getSubscription(t, ctx, "sub")
			old := s.current
			body.Store(tc.source)
			require.ErrorContains(t, s.Refresh(ctx), "circular")
			require.Same(t, old, s.current)
		})
	}
}

func TestAuditConcurrentProviderCycle(t *testing.T) {
	var aBody, bBody atomic.Value
	initial := `{"outbounds":[{"type":"http","tag":"exit","server":"127.0.0.1","server_port":9}]}`
	aBody.Store(initial)
	bBody.Store(initial)
	a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, aBody.Load()) }))
	defer a.Close()
	b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, bBody.Load()) }))
	defer b.Close()
	_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"subscription","tag":"a","url":%q,"http_client":{}},{"type":"subscription","tag":"b","url":%q,"http_client":{}}]}`, a.URL, b.URL))
	aBody.Store(`{"outbounds":[{"type":"test","tag":"exit","detour":"b"}]}`)
	bBody.Store(`{"outbounds":[{"type":"test","tag":"exit","detour":"a"}]}`)
	results := make(chan error, 2)
	for _, tag := range []string{"a", "b"} {
		go func(tag string) { results <- getSubscription(t, ctx, tag).Refresh(ctx) }(tag)
	}
	successes := 0
	for range 2 {
		err := <-results
		if err == nil {
			successes++
		} else {
			require.ErrorContains(t, err, "circular")
		}
	}
	require.Equal(t, 1, successes)
}

func TestAuditAcyclicReentryThroughHost(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"outbounds":[{"type":"test","tag":"exit","detour":"bridge"},{"type":"test","tag":"relay"}]}`)
	}))
	defer remote.Close()
	_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"test","tag":"bridge","detour":"subscription/sub/relay"},{"type":"subscription","tag":"sub","url":%q,"http_client":{}}]}`, remote.URL))
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer target.Close()
	conn, err := getSubscription(t, ctx, "sub").DialContext(ctx, "tcp", M.ParseSocksaddr(strings.TrimPrefix(target.URL, "http://")))
	require.NoError(t, err)
	require.NoError(t, conn.Close())
}

type fixedDestinationOptions struct{ option.ServerOptions }
type fixedDestinationOutbound struct {
	outbound.Adapter
	address string
}

func (o *fixedDestinationOutbound) DialContext(ctx context.Context, network string, _ M.Socksaddr) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, network, o.address)
}
func (o *fixedDestinationOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, fmt.Errorf("TCP only")
}

func TestAuditAddModifyDeleteAndReAdd(t *testing.T) {
	endpoint := func(label string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, label) }))
	}
	first, second := endpoint("first"), endpoint("second")
	defer first.Close()
	defer second.Close()
	document := func(tag, address string) string {
		addr := M.ParseSocksaddr(strings.TrimPrefix(address, "http://"))
		return fmt.Sprintf(`{"outbounds":[{"type":"fixed","tag":%q,"server":%q,"server_port":%d}]}`, tag, addr.AddrString(), addr.Port)
	}
	var body atomic.Value
	body.Store(document("exit", first.URL))
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body.Load()) }))
	defer remote.Close()
	ctx := testContext()
	outbound.Register[fixedDestinationOptions](service.FromContext[adapter.OutboundRegistry](ctx).(*outbound.Registry), "fixed", func(_ context.Context, _ adapter.Router, _ log.ContextLogger, tag string, options fixedDestinationOptions) (adapter.Outbound, error) {
		return &fixedDestinationOutbound{outbound.NewAdapter("fixed", tag, []string{"tcp"}, nil), options.Build().String()}, nil
	})
	_, ctx = startBoxWithContext(t, ctx, configFor(remote.URL, ""))
	s := getSubscription(t, ctx, "sub")
	handle, found := s.ProviderOutbound(memberTag("sub", "exit"))
	require.True(t, found)
	oldConn, err := handle.DialContext(ctx, "tcp", M.Socksaddr{})
	require.NoError(t, err)
	defer oldConn.Close()
	read := func(conn net.Conn) string {
		require.NoError(t, conn.SetDeadline(time.Now().Add(time.Second)))
		_, err := fmt.Fprint(conn, "GET / HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
		require.NoError(t, err)
		body, err := io.ReadAll(conn)
		require.NoError(t, err)
		require.NoError(t, conn.Close())
		return string(body)
	}
	body.Store(document("exit", second.URL))
	require.NoError(t, s.Refresh(ctx))
	newConn, err := handle.DialContext(ctx, "tcp", M.Socksaddr{})
	require.NoError(t, err)
	require.Contains(t, read(newConn), "second")
	require.Contains(t, read(oldConn), "first")
	body.Store(document("replacement", second.URL))
	require.NoError(t, s.Refresh(ctx))
	require.Equal(t, memberTag("sub", "replacement"), s.Selected("tcp").Tag())
	_, err = handle.DialContext(ctx, "tcp", M.Socksaddr{})
	require.ErrorContains(t, err, "unavailable")
	body.Store(`{"outbounds":[]}`)
	require.NoError(t, s.Refresh(ctx))
	require.Empty(t, s.All())
	require.Nil(t, s.Selected("tcp"))
	_, err = s.DialContext(ctx, "tcp", M.Socksaddr{})
	require.Error(t, err)
	body.Store(document("exit", first.URL))
	require.NoError(t, s.Refresh(ctx))
	newConn, err = handle.DialContext(ctx, "tcp", M.Socksaddr{})
	require.NoError(t, err)
	require.Contains(t, read(newConn), "first")
}

func TestAuditCleanupDoesNotJoinReleasingUpdater(t *testing.T) {
	done := make(chan struct{})
	scope := adapter.NewScope(context.Background(), logger.NOP())
	scope.Add(func() error { <-done; return nil })
	g := &snapshot{scope: scope, refs: 1}
	g.retire(0)
	go func() { g.release(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cleanup joined the updater releasing its own final lease")
	}
	g.close()
}

func TestAuditQueuedRefreshCancellation(t *testing.T) {
	var block atomic.Bool
	entered := make(chan struct{}, 1)
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if block.Load() {
			entered <- struct{}{}
			<-r.Context().Done()
			return
		}
		fmt.Fprint(w, goodDocument)
	}))
	defer remote.Close()
	_, ctx := startBox(t, configFor(remote.URL, ""))
	s := getSubscription(t, ctx, "sub")
	block.Store(true)
	firstCtx, cancelFirst := context.WithCancel(ctx)
	defer cancelFirst()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _ = s.Refresh(firstCtx) }()
	<-entered
	queued, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Refresh(queued) }()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("queued refresh ignored cancellation")
	}
	cancelFirst()
	wg.Wait()
}

func TestAuditNestedProviderCycleRollback(t *testing.T) {
	var childBody atomic.Value
	childBody.Store(goodDocument)
	child := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, childBody.Load()) }))
	defer child.Close()
	parent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"outbounds":[{"type":"subscription","tag":"child","url":%q,"http_client":{}}]}`, child.URL)
	}))
	defer parent.Close()
	_, ctx := startBox(t, configFor(parent.URL, ""))
	root := getSubscription(t, ctx, "sub")
	childRaw, _ := root.current.outbounds.Outbound(memberTag("sub", "child"))
	nested := childRaw.(*Subscription)
	old := nested.current
	childBody.Store(`{"outbounds":[{"type":"test","tag":"exit","detour":"sub"}]}`)
	require.ErrorContains(t, nested.Refresh(ctx), "circular")
	require.Same(t, old, nested.current)
}

func TestAuditDNSChangeDoesNotReuseOldCache(t *testing.T) {
	var address atomic.Value
	address.Store("127.0.0.1")
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"dns":{"servers":[{"type":"hosts","tag":"hosts","predefined":{"node.invalid":%q}}]},"outbounds":[{"type":"test","tag":"exit"}]}`, address.Load())
	}))
	defer remote.Close()
	_, ctx := startBox(t, configFor(remote.URL, ""))
	s := getSubscription(t, ctx, "sub")
	old := s.current
	before, err := old.dnsRouter.Lookup(ctx, "node.invalid", adapter.DNSQueryOptions{})
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", before[0].String())
	require.True(t, old.acquire())
	defer old.release()
	address.Store("127.0.0.2")
	require.NoError(t, s.Refresh(ctx))
	after, err := s.current.dnsRouter.Lookup(ctx, "node.invalid", adapter.DNSQueryOptions{})
	require.NoError(t, err)
	require.Equal(t, "127.0.0.2", after[0].String())
	retained, err := old.dnsRouter.Lookup(ctx, "node.invalid", adapter.DNSQueryOptions{})
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", retained[0].String())
}

func TestAuditRetiredNestedSubscriptionStopsUpdating(t *testing.T) {
	child := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, goodDocument) }))
	defer child.Close()
	parent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"outbounds":[{"type":"subscription","tag":"child","url":%q,"http_client":{}}]}`, child.URL)
	}))
	defer parent.Close()
	_, ctx := startBox(t, configFor(parent.URL, ""))
	root := getSubscription(t, ctx, "sub")
	raw, _ := root.current.outbounds.Outbound(memberTag("sub", "child"))
	oldChild := raw.(*Subscription)
	conn, err := root.DialContext(ctx, "tcp", M.ParseSocksaddr(strings.TrimPrefix(child.URL, "http://")))
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, root.Refresh(ctx))
	require.ErrorIs(t, oldChild.updatesCtx.Err(), context.Canceled)
	require.NoError(t, oldChild.ctx.Err(), "connection resources must remain alive")
}

func TestAuditLeasedTCPHalfClose(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	done := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		_, err = io.ReadAll(conn)
		if err == nil {
			_, err = conn.Write([]byte("response-after-fin"))
		}
		done <- err
	}()
	raw, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	g := &snapshot{scope: adapter.NewScope(context.Background(), logger.NOP()), refs: 1}
	conn := &leasedConn{Conn: raw, release: g.release}
	defer conn.Close()
	g.retire(0)
	require.NoError(t, conn.SetDeadline(time.Now().Add(time.Second)))
	_, err = conn.Write([]byte("request"))
	require.NoError(t, err)
	require.NoError(t, conn.CloseWrite())
	require.False(t, g.closed)
	response, err := io.ReadAll(conn)
	require.NoError(t, err)
	require.Equal(t, "response-after-fin", string(response))
	require.NoError(t, <-done)
	require.NoError(t, conn.Close())
	g.close()
}

func TestAuditLeasedTrojanEarlyHandshake(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	raw := trojan.NewClientConn(client, [trojan.KeyLength]byte{}, M.ParseSocksaddr("127.0.0.1:443"))
	conn := &leasedConn{Conn: raw, release: func() {}}
	defer conn.Close()
	require.True(t, N.NeedHandshakeForWrite(conn))
	// IPv4 Trojan request: key, CRLF, command, address type/address/port, CRLF.
	done := make(chan error, 1)
	go func() {
		header := make([]byte, trojan.KeyLength+2+1+1+4+2+2)
		_, err := io.ReadFull(server, header)
		if err == nil {
			_, err = server.Write([]byte("server-first"))
		}
		done <- err
	}()
	require.NoError(t, conn.SetDeadline(time.Now().Add(time.Second)))
	_, err := conn.Write(nil)
	require.NoError(t, err)
	require.False(t, N.NeedHandshakeForWrite(conn))
	response := make([]byte, len("server-first"))
	_, err = io.ReadFull(conn, response)
	require.NoError(t, err)
	require.Equal(t, "server-first", string(response))
	require.NoError(t, <-done)
}

func (o *fixedDestinationOutbound) PreferredDomain(_ *adapter.InboundContext, domain string) bool {
	return M.ParseSocksaddr(o.address).AddrString() == domain
}
func (o *fixedDestinationOutbound) PreferredAddress(_ *adapter.InboundContext, address netip.Addr) bool {
	return M.ParseSocksaddr(o.address).Addr == address
}

func TestAuditPreferredRoutesFollowMemberRefresh(t *testing.T) {
	var address atomic.Value
	address.Store("127.0.0.1")
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"outbounds":[{"type":"fixed","tag":"exit","server":%q,"server_port":9}]}`, address.Load())
	}))
	defer remote.Close()
	ctx := testContext()
	outbound.Register[fixedDestinationOptions](service.FromContext[adapter.OutboundRegistry](ctx).(*outbound.Registry), "fixed", func(_ context.Context, _ adapter.Router, _ log.ContextLogger, tag string, options fixedDestinationOptions) (adapter.Outbound, error) {
		return &fixedDestinationOutbound{outbound.NewAdapter("fixed", tag, []string{"tcp"}, nil), options.Build().String()}, nil
	})
	_, ctx = startBoxWithContext(t, ctx, configFor(remote.URL, ""))
	s := getSubscription(t, ctx, "sub")
	handle, _ := s.ProviderOutbound(memberTag("sub", "exit"))
	routes := handle.(adapter.OutboundWithPreferredRoutes)
	require.True(t, routes.PreferredAddress(&adapter.InboundContext{}, netip.MustParseAddr("127.0.0.1")))
	address.Store("127.0.0.2")
	require.NoError(t, s.Refresh(ctx))
	require.False(t, routes.PreferredAddress(&adapter.InboundContext{}, netip.MustParseAddr("127.0.0.1")))
	require.True(t, routes.PreferredDomain(&adapter.InboundContext{}, "127.0.0.2"))
}

func TestAuditURLTestRunsAfterRefresh(t *testing.T) {
	var probes atomic.Int32
	probe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { probes.Add(1); w.WriteHeader(http.StatusNoContent) }))
	defer probe.Close()
	var body atomic.Value
	body.Store(goodDocument)
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body.Load()) }))
	defer remote.Close()
	_, ctx := startBox(t, configFor(remote.URL, fmt.Sprintf(`"mode":"urltest","test_url":%q,"test_interval":"24h",`, probe.URL)))
	require.Eventually(t, func() bool { return probes.Load() >= 2 }, time.Second, 10*time.Millisecond)
	previous := probes.Load()
	body.Store(`{"outbounds":[{"type":"test","tag":"new"}]}`)
	require.NoError(t, getSubscription(t, ctx, "sub").Refresh(ctx))
	require.Eventually(t, func() bool { return probes.Load() > previous }, time.Second, 10*time.Millisecond)
}

func TestAuditAuxiliaryDNSCycles(t *testing.T) {
	ctx := testContext()
	outbound.Register[option.Hysteria2OutboundOptions](service.FromContext[adapter.OutboundRegistry](ctx).(*outbound.Registry), "hysteria2", func(context.Context, adapter.Router, log.ContextLogger, string, option.Hysteria2OutboundOptions) (adapter.Outbound, error) {
		panic("parse only")
	})
	for _, node := range []string{
		`{"type":"http","tag":"a","server":"127.0.0.1","server_port":9,"tls":{"enabled":true,"server_name":"proxy.invalid","ech":{"enabled":true}}}`,
		`{"type":"hysteria2","tag":"a","server":"127.0.0.1","server_port":9,"realm":{"server_url":"https://realm.invalid","realm_id":"test","stun_servers":["127.0.0.1:3478"]}}`,
	} {
		_, err := parseDocument(ctx, "p", []byte(`{"dns":{"servers":[{"type":"udp","tag":"d","server":"1.1.1.1","detour":"a"}]},"outbounds":[`+node+`]}`), nil)
		require.ErrorContains(t, err, "circular")
	}
}

func TestAuditEmptyGroupCanLoadRemoteResources(t *testing.T) {
	var downloads atomic.Int32
	rules := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		downloads.Add(1)
		fmt.Fprint(w, `{"version":4,"rules":[{"domain":"example.invalid"}]}`)
	}))
	defer rules.Close()
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"outbounds":[],"route":{"rule_set":[{"type":"remote","tag":"set","format":"source","url":%q}]}}`, rules.URL)
	}))
	defer remote.Close()
	_, ctx := startBox(t, configFor(remote.URL, ""))
	s := getSubscription(t, ctx, "sub")
	require.True(t, s.Status().Ready)
	require.Empty(t, s.All())
	require.Positive(t, downloads.Load())
}

func TestAuditURLTestAcyclicReentryDoesNotDeadlock(t *testing.T) {
	probe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	defer probe.Close()
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"outbounds":[{"type":"selector","tag":"exit","outbounds":["entry"]},{"type":"test","tag":"entry","detour":"bridge"},{"type":"test","tag":"relay"}]}`)
	}))
	defer remote.Close()
	_, ctx := startBox(t, fmt.Sprintf(`{"log":{"disabled":true},"outbounds":[{"type":"selector","tag":"bridge","outbounds":["relay-proxy"]},{"type":"test","tag":"relay-proxy","detour":"subscription/sub/relay"},{"type":"subscription","tag":"sub","url":%q,"http_client":{},"mode":"urltest","test_url":%q}]}`, remote.URL, probe.URL))
	done := make(chan error, 1)
	go func() { _, err := getSubscription(t, ctx, "sub").URLTest(ctx); done <- err }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("URLTest recursively acquired its provider mutex")
	}
}
