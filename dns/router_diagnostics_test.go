package dns

import (
	"context"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	C "github.com/sagernet/sing-box/constant"
	N "github.com/sagernet/sing/common/network"

	"github.com/stretchr/testify/require"
)

type diagnosticGroup struct {
	adapter.OutboundGroup
	tag      string
	tcp, udp adapter.Outbound
}

func (g *diagnosticGroup) Tag() string { return g.tag }
func (g *diagnosticGroup) Selected(network string) adapter.Outbound {
	if network == N.NetworkUDP {
		return g.udp
	}
	return g.tcp
}

type diagnosticNode struct {
	outbound.Adapter
	N.Dialer
}

type diagnosticOutboundManager struct {
	adapter.OutboundManager
	nodes map[string]adapter.Outbound
}

func (m *diagnosticOutboundManager) Outbound(tag string) (adapter.Outbound, bool) {
	node, found := m.nodes[tag]
	return node, found
}

type diagnosticRemoteTransport struct {
	*fakeDNSTransport
	kind string
}

func (t *diagnosticRemoteTransport) Type() string { return t.kind }
func (t *diagnosticRemoteTransport) DNSInfo() adapter.DNSServerInfo {
	return adapter.DNSServerInfo{Tag: t.Tag(), Type: t.Type(), Detour: "outer"}
}

type diagnosticTransportManager struct {
	*fakeDNSTransportManager
	nodes []adapter.DNSTransport
}

func (m *diagnosticTransportManager) Transports() []adapter.DNSTransport { return m.nodes }

func TestDNSServerDialerChainNetworks(t *testing.T) {
	tcp := &diagnosticNode{Adapter: outbound.NewAdapter("test", "tcp-node", []string{N.NetworkTCP}, nil)}
	udp := &diagnosticNode{Adapter: outbound.NewAdapter("test", "udp-node", []string{N.NetworkUDP}, nil)}
	inner := &diagnosticGroup{tag: "inner", tcp: tcp, udp: udp}
	outer := &diagnosticGroup{tag: "outer", tcp: inner, udp: inner}
	manager := &diagnosticOutboundManager{nodes: map[string]adapter.Outbound{"outer": outer}}
	for _, tc := range []struct{ kind, leaf string }{
		{C.DNSTypeTCP, "tcp-node"}, {C.DNSTypeTLS, "tcp-node"}, {C.DNSTypeHTTPS, "tcp-node"},
		{C.DNSTypeUDP, "udp-node"}, {C.DNSTypeQUIC, "udp-node"}, {C.DNSTypeHTTP3, "udp-node"},
		{C.DNSTypeLocal, "udp-node"}, {C.DNSTypeDHCP, "udp-node"},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			transport := &diagnosticRemoteTransport{&fakeDNSTransport{tag: "remote"}, tc.kind}
			router := &Router{
				ctx: context.Background(), client: diagnosticClient(), outbound: manager,
				transport: &diagnosticTransportManager{
					fakeDNSTransportManager: &fakeDNSTransportManager{defaultTransport: transport},
					nodes:                   []adapter.DNSTransport{transport},
				},
			}
			servers, err := router.DNSServers()
			require.NoError(t, err)
			require.Equal(t, []string{"outer", "inner", tc.leaf}, servers[0].DialerChain)
		})
	}
}

func TestDialerChainChangesNetworkAtProxy(t *testing.T) {
	tcp := &diagnosticNode{Adapter: outbound.NewAdapter("test", "tcp-hop", nil, nil)}
	udp := &diagnosticNode{Adapter: outbound.NewAdapter("test", "udp-hop", nil, nil)}
	group := &diagnosticGroup{tag: "hop", tcp: tcp, udp: udp}
	for _, tc := range []struct{ kind, network, leaf string }{
		{C.TypeAnyTLS, N.NetworkUDP, "tcp-hop"}, {C.TypeSnell, N.NetworkUDP, "tcp-hop"},
		{C.TypeHysteria2, N.NetworkTCP, "udp-hop"}, {C.TypeHysteria, N.NetworkTCP, "udp-hop"},
		{C.TypeTUIC, N.NetworkTCP, "udp-hop"}, {C.TypeWireGuard, N.NetworkTCP, "udp-hop"},
		{C.TypeDirect, N.NetworkUDP, "udp-hop"}, {C.TypeDirect, N.NetworkTCP, "tcp-hop"},
	} {
		t.Run(tc.kind+"/"+tc.network, func(t *testing.T) {
			node := &diagnosticNode{Adapter: outbound.NewAdapter(tc.kind, "proxy", nil, []string{"hop"})}
			manager := &diagnosticOutboundManager{nodes: map[string]adapter.Outbound{"proxy": node, "hop": group}}
			require.Equal(t, []string{"proxy", "hop", tc.leaf}, adapter.InspectOutboundDialerChain(context.Background(), manager, "proxy", tc.network))
		})
	}
}

func TestDialerChainEmptyAndCycles(t *testing.T) {
	group := &diagnosticGroup{tag: "empty"}
	manager := &diagnosticOutboundManager{nodes: map[string]adapter.Outbound{"empty": group}}
	require.Equal(t, []string{"empty"}, adapter.InspectOutboundDialerChain(context.Background(), manager, "empty", N.NetworkUDP))
	require.Empty(t, adapter.InspectOutboundDialerChain(context.Background(), manager, "missing", N.NetworkTCP))
	group.tcp = group
	require.Equal(t, []string{"empty"}, adapter.InspectOutboundDialerChain(context.Background(), manager, "empty", N.NetworkTCP))
}
