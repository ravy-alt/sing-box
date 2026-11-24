package adapter

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

type capabilityNode struct {
	Outbound
	tag, kind              string
	networks, dependencies []string
}

func (n *capabilityNode) Tag() string            { return n.tag }
func (n *capabilityNode) Type() string           { return n.kind }
func (n *capabilityNode) Network() []string      { return n.networks }
func (n *capabilityNode) Dependencies() []string { return n.dependencies }

type capabilityManager struct {
	OutboundManager
	nodes map[string]Outbound
}

func (m *capabilityManager) Outbound(tag string) (Outbound, bool) {
	node, ok := m.nodes[tag]
	return node, ok
}

type capabilityGroup struct {
	*capabilityNode
	selected map[string]Outbound
}

func (g *capabilityGroup) Selected(network string) Outbound  { return g.selected[network] }
func (g *capabilityGroup) All() []string                     { return nil }
func (g *capabilityGroup) AttachConnection(io.Closer) func() { return func() {} }

func TestCheckOutboundDialerNetworkTransport(t *testing.T) {
	for _, tc := range []struct {
		name, kind, network string
		relayNetworks       []string
		unsupported         string
	}{
		{"plain UDP needs UDP relay", "shadowsocks", "udp", []string{"tcp"}, "udp"},
		{"AnyTLS UDP uses TCP relay", "anytls", "udp", []string{"tcp"}, ""},
		{"AnyTLS cannot use UDP-only relay", "anytls", "udp", []string{"udp"}, "tcp"},
		{"QUIC TCP needs UDP relay", "hysteria2", "tcp", []string{"tcp"}, "udp"},
		{"QUIC TCP uses UDP relay", "hysteria2", "tcp", []string{"udp"}, ""},
		{"SOCKS UDP needs control TCP", "socks", "udp", []string{"udp"}, "tcp"},
		{"SOCKS UDP needs data UDP", "socks", "udp", []string{"tcp"}, "udp"},
		{"SOCKS UDP needs both", "socks", "udp", []string{"tcp", "udp"}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exit := &capabilityNode{tag: "exit", kind: tc.kind, networks: []string{"tcp", "udp"}, dependencies: []string{"relay"}}
			relay := &capabilityNode{tag: "relay", networks: tc.relayNetworks}
			manager := &capabilityManager{nodes: map[string]Outbound{"relay": relay}}
			err := CheckOutboundDialerNetwork(context.Background(), exit, manager, tc.network)
			if tc.unsupported == "" {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, ErrOutboundNetworkUnsupported)
				require.Contains(t, err.Error(), "[relay] requires "+tc.unsupported)
			}
		})
	}
}

func TestCheckOutboundDialerNetworkNestedSelectionAndInvalidPaths(t *testing.T) {
	tcp := &capabilityNode{tag: "tcp", networks: []string{"tcp"}}
	udp := &capabilityNode{tag: "udp", networks: []string{"udp"}}
	inner := &capabilityGroup{&capabilityNode{tag: "inner"}, map[string]Outbound{"tcp": tcp, "udp": udp}}
	outer := &capabilityGroup{&capabilityNode{tag: "outer"}, map[string]Outbound{"tcp": inner, "udp": inner}}
	exit := &capabilityNode{tag: "exit", kind: "socks", networks: []string{"udp"}, dependencies: []string{"outer"}}
	manager := &capabilityManager{nodes: map[string]Outbound{"outer": outer}}
	require.NoError(t, CheckOutboundDialerNetwork(context.Background(), exit, manager, "udp"), "control and packet paths must select different group members")
	inner.selected["udp"] = tcp
	require.ErrorIs(t, CheckOutboundDialerNetwork(context.Background(), exit, manager, "udp"), ErrOutboundNetworkUnsupported)
	inner.selected["udp"] = outer
	require.ErrorContains(t, CheckOutboundDialerNetwork(context.Background(), exit, manager, "udp"), "circular")
	delete(manager.nodes, "outer")
	require.ErrorContains(t, CheckOutboundDialerNetwork(context.Background(), exit, manager, "udp"), "unavailable")
}
