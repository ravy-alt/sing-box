package subscription

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/dialer"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/protocol/anytls"
	"github.com/sagernet/sing-box/protocol/shadowsocks"
	"github.com/sagernet/sing-box/protocol/socks"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"

	"github.com/stretchr/testify/require"
)

func diagnosticSubscriptionContext() context.Context {
	ctx := testContext()
	registry := service.FromContext[adapter.OutboundRegistry](ctx).(*outbound.Registry)
	outbound.Register[auditNetworkOptions](registry, "diagnostic-network", func(ctx context.Context, _ adapter.Router, _ log.ContextLogger, tag string, options auditNetworkOptions) (adapter.Outbound, error) {
		d, err := dialer.New(ctx, options.DialerOptions, true)
		return &testOutbound{Adapter: outbound.NewAdapterWithDialerOptions("diagnostic-network", tag, options.Networks, options.DialerOptions), Dialer: d}, err
	})
	return ctx
}

func diagnosticServerChain(t *testing.T, ctx context.Context, tag string) []string {
	t.Helper()
	servers, err := service.FromContext[adapter.DNSRouter](ctx).(adapter.DNSInspector).DNSServers()
	require.NoError(t, err)
	for _, server := range servers {
		if server.Tag == tag {
			return server.DialerChain
		}
	}
	t.Fatalf("missing DNS server %s", tag)
	return nil
}

func TestDNSDialerChainNestedSubscriptionGroups(t *testing.T) {
	probe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	defer probe.Close()
	for _, kind := range []string{"urltest", "smart-urltest"} {
		t.Run(kind, func(t *testing.T) {
			child := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				fmt.Fprintf(w, `{"outbounds":[
				{"type":"selector","tag":"inner","outbounds":["auto"]},
				{"type":%q,"tag":"auto","outbounds":["tcp-entry","udp-entry"],"url":%q},
				{"type":"diagnostic-network","tag":"tcp-entry","networks":["tcp"],"detour":"relay"},
				{"type":"diagnostic-network","tag":"udp-entry","networks":["udp"],"detour":"relay"},
				{"type":"selector","tag":"relay","outbounds":["leaf","other"]},
				{"type":"test","tag":"leaf"},{"type":"test","tag":"other"}]}`, kind, probe.URL)
			}))
			defer child.Close()
			parent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				fmt.Fprintf(w, `{"outbounds":[{"type":"selector","tag":"outer","outbounds":["child"]},{"type":"subscription","tag":"child","url":%q,"http_client":{}}]}`, child.URL)
			}))
			defer parent.Close()
			config := fmt.Sprintf(`{"log":{"disabled":true},"dns":{"servers":[
			{"type":"hosts","tag":"bootstrap"},
			{"type":"udp","tag":"udp-dns","server":"192.0.2.53","detour":"pick"},
			{"type":"https","tag":"tcp-dns","server":"192.0.2.53","detour":"pick"}],"final":"bootstrap"},
			"route":{"default_domain_resolver":"bootstrap"},"outbounds":[
			{"type":"selector","tag":"pick","outbounds":["sub"]},
			{"type":"test","tag":"relay"},{"type":"test","tag":"leaf"},
			{"type":"subscription","tag":"sub","url":%q,"http_client":{}}]}`, parent.URL)
			_, ctx := startBoxWithContext(t, diagnosticSubscriptionContext(), config)
			childTag := memberTag("sub", "child")
			childMember := func(name string) string { return memberTag(childTag, name) }
			assertChains := func(leaf string) {
				for _, network := range []string{N.NetworkTCP, N.NetworkUDP} {
					want := []string{"pick", "sub", memberTag("sub", "outer"), childTag, childMember("inner"), childMember("auto"), childMember(network + "-entry"), childMember("relay"), childMember(leaf)}
					require.Equal(t, adapter.OutboundDisplayNames(ctx, want), diagnosticServerChain(t, ctx, network+"-dns"))
				}
			}
			assertChains("leaf")
			manager := service.FromContext[adapter.OutboundManager](ctx)
			raw, found := manager.Outbound(childMember("relay"))
			require.True(t, found)
			group, found := adapter.OutboundGroupFor(raw)
			require.True(t, found)
			require.True(t, group.(adapter.SelectableOutboundGroup).SelectOutbound(childMember("other")))
			assertChains("other")
		})
	}
}

func TestDialerChainSubscriptionUDPEncapsulation(t *testing.T) {
	probe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	defer probe.Close()
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, `{"outbounds":[
		{"type":"anytls","tag":"anytls","server":"192.0.2.1","server_port":443,"password":"test","tls":{"enabled":true},"detour":"hop"},
		{"type":"shadowsocks","tag":"ss","server":"192.0.2.1","server_port":443,"method":"aes-128-gcm","password":"test","detour":"hop"},
		{"type":"shadowsocks","tag":"ss-uot","server":"192.0.2.1","server_port":443,"method":"aes-128-gcm","password":"test","udp_over_tcp":true,"detour":"hop"},
		{"type":"shadowsocks","tag":"ss-mux","server":"192.0.2.1","server_port":443,"method":"aes-128-gcm","password":"test","multiplex":{"enabled":true},"detour":"hop"},
		{"type":"socks","tag":"socks","server":"192.0.2.1","server_port":1080,"detour":"hop"},
		{"type":"socks","tag":"socks-uot","server":"192.0.2.1","server_port":1080,"udp_over_tcp":true,"detour":"hop"},
		{"type":"smart-urltest","tag":"hop","outbounds":["tcp-hop","udp-hop"],"url":%q},
		{"type":"diagnostic-network","tag":"tcp-hop","networks":["tcp"]},
		{"type":"diagnostic-network","tag":"udp-hop","networks":["udp"]}]}`, probe.URL)
	}))
	defer remote.Close()
	ctx := diagnosticSubscriptionContext()
	registry := service.FromContext[adapter.OutboundRegistry](ctx).(*outbound.Registry)
	anytls.RegisterOutbound(registry)
	shadowsocks.RegisterOutbound(registry)
	socks.RegisterOutbound(registry)
	_, ctx = startBoxWithContext(t, ctx, configFor(remote.URL, ""))
	manager := service.FromContext[adapter.OutboundManager](ctx)
	for _, tc := range []struct{ node, udpHop string }{
		{"anytls", "tcp-hop"}, {"ss", "udp-hop"}, {"ss-uot", "tcp-hop"}, {"ss-mux", "tcp-hop"},
		{"socks", "udp-hop"}, {"socks-uot", "tcp-hop"},
	} {
		for _, network := range []string{N.NetworkTCP, N.NetworkUDP} {
			hop := "tcp-hop"
			if network == N.NetworkUDP {
				hop = tc.udpHop
			}
			want := adapter.OutboundDisplayNames(ctx, []string{memberTag("sub", tc.node), memberTag("sub", "hop"), memberTag("sub", hop)})
			require.Equal(t, want, adapter.InspectOutboundDialerChain(ctx, manager, memberTag("sub", tc.node), network), tc.node+"/"+network)
		}
	}
	sub := getSubscription(t, ctx, "sub")
	sub.mu.Lock()
	sub.selectedTCP, sub.selectedUDP = memberTag("sub", "ss"), memberTag("sub", "anytls")
	sub.mu.Unlock()
	require.Equal(t, []string{"sub", "ss", "hop", "tcp-hop"}, adapter.InspectOutboundDialerChain(ctx, manager, "sub", N.NetworkTCP))
	require.Equal(t, []string{"sub", "anytls", "hop", "tcp-hop"}, adapter.InspectOutboundDialerChain(ctx, manager, "sub", N.NetworkUDP))
}

type diagnosticInspectionGate struct {
	*testOutbound
	armed         *atomic.Bool
	once          *sync.Once
	enter, resume chan struct{}
}

func (o *diagnosticInspectionGate) Dependencies() []string {
	if o.armed.Load() {
		o.once.Do(func() { close(o.enter); <-o.resume })
	}
	return o.testOutbound.Dependencies()
}

func TestDialerChainPinsSubscriptionGeneration(t *testing.T) {
	var body atomic.Value
	body.Store(`{"outbounds":[{"type":"diagnostic-gate","tag":"exit","detour":"old"},{"type":"test","tag":"old"}]}`)
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body.Load()) }))
	defer remote.Close()
	ctx := testContext()
	armed, once := new(atomic.Bool), new(sync.Once)
	enter, resume := make(chan struct{}), make(chan struct{})
	var resumeOnce sync.Once
	unblock := func() { resumeOnce.Do(func() { close(resume) }) }
	defer unblock()
	outbound.Register[testOptions](service.FromContext[adapter.OutboundRegistry](ctx).(*outbound.Registry), "diagnostic-gate", func(ctx context.Context, _ adapter.Router, _ log.ContextLogger, tag string, options testOptions) (adapter.Outbound, error) {
		d, err := dialer.New(ctx, options.DialerOptions, true)
		return &diagnosticInspectionGate{
			testOutbound: &testOutbound{Adapter: outbound.NewAdapterWithDialerOptions("test", tag, []string{N.NetworkTCP, N.NetworkUDP}, options.DialerOptions), Dialer: d},
			armed:        armed, once: once, enter: enter, resume: resume,
		}, err
	})
	_, ctx = startBoxWithContext(t, ctx, configFor(remote.URL, ""))
	manager := service.FromContext[adapter.OutboundManager](ctx)
	sub := getSubscription(t, ctx, "sub")
	armed.Store(true)
	result := make(chan []string, 1)
	go func() { result <- adapter.InspectOutboundDialerChain(ctx, manager, "sub", N.NetworkUDP) }()
	select {
	case <-enter:
	case <-time.After(5 * time.Second):
		t.Fatal("inspection did not reach the old node")
	}
	armed.Store(false)
	body.Store(`{"outbounds":[{"type":"test","tag":"exit","detour":"new"},{"type":"test","tag":"new"}]}`)
	require.NoError(t, sub.Refresh(ctx))
	unblock()
	select {
	case chain := <-result:
		require.Equal(t, []string{"sub", "exit", "old"}, chain)
	case <-time.After(5 * time.Second):
		t.Fatal("inspection did not finish")
	}
	require.Equal(t, []string{"sub", "exit", "new"}, adapter.InspectOutboundDialerChain(ctx, manager, "sub", N.NetworkUDP))
	// A removed stable handle must not fall through to another node, and must
	// resolve the new chain when that same source node is reintroduced.
	body.Store(`{"outbounds":[]}`)
	require.NoError(t, sub.Refresh(ctx))
	require.Empty(t, adapter.InspectOutboundDialerChain(ctx, manager, memberTag("sub", "exit"), N.NetworkTCP))
	body.Store(`{"outbounds":[{"type":"test","tag":"exit","detour":"again"},{"type":"test","tag":"again"}]}`)
	require.NoError(t, sub.Refresh(ctx))
	require.Equal(t, []string{"exit", "again"}, adapter.InspectOutboundDialerChain(ctx, manager, memberTag("sub", "exit"), N.NetworkTCP))
}
