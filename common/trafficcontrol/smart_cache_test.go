package trafficcontrol

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/stretchr/testify/require"
)

type trackedTestOutbound struct {
	adapter.Outbound
	tag string
}

func (o *trackedTestOutbound) Tag() string  { return o.tag }
func (o *trackedTestOutbound) Type() string { return "test" }

type trackedSmartGroup struct {
	adapter.SmartURLTestGroup
	cleared atomic.Int32
}

func (g *trackedSmartGroup) ClearCache() { g.cleared.Add(1) }

func TestTrackerShowsActualChainAndAdministrativeCloseClearsCaches(t *testing.T) {
	manager := NewManager()
	manager.outboundCaches = new(adapter.OutboundCacheRegistry)
	unused := new(trackedSmartGroup)
	unregister := manager.outboundCaches.Register(unused)
	defer unregister()
	outer, smart, winner, detour := &trackedTestOutbound{tag: "outer"}, &trackedTestOutbound{tag: "smart"}, &trackedTestOutbound{tag: "winner"}, &trackedTestOutbound{tag: "detour"}
	trace := adapter.NewOutboundTrace([]adapter.Outbound{outer, smart})
	var cleared atomic.Int32
	trace.BindCache(func() { cleared.Add(1) })
	client, peer := net.Pipe()
	defer peer.Close()
	conn := manager.RoutedConnection(context.Background(), client, adapter.InboundContext{OutboundChain: []adapter.Outbound{outer, smart}, OutboundTrace: trace}, nil, outer)
	trace.Append(winner)
	trace.Append(detour)
	record := manager.Connections()[0]
	require.Equal(t, []string{"detour", "winner", "smart", "outer"}, record.Chain)
	require.Equal(t, "detour", record.Outbound)
	record.Chain[0] = "mutated copy"
	require.Equal(t, "detour", manager.Connections()[0].Chain[0])
	manager.CloseConnection(record.ID)
	require.EqualValues(t, 1, cleared.Load())
	require.Zero(t, manager.ConnectionsLen())
	require.Equal(t, []string{"detour", "winner", "smart", "outer"}, manager.ClosedConnections()[0].Chain)
	conn.Close()
	manager.CloseAllConnections()
	require.EqualValues(t, 1, unused.cleared.Load(), "clear all includes groups without active connections")
}

func TestOrdinaryCloseKeepsAffinity(t *testing.T) {
	manager := NewManager()
	node := &trackedTestOutbound{tag: "smart"}
	trace := adapter.NewOutboundTrace([]adapter.Outbound{node})
	var cleared atomic.Int32
	trace.BindCache(func() { cleared.Add(1) })
	client, peer := net.Pipe()
	defer peer.Close()
	conn := manager.RoutedConnection(context.Background(), client, adapter.InboundContext{OutboundChain: []adapter.Outbound{node}, OutboundTrace: trace}, nil, node)
	conn.Close()
	require.Zero(t, cleared.Load())
}
