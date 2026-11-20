package daemon

import (
	"context"
	"io"
	"slices"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/endpoint"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/logger"
	"github.com/sagernet/sing/service"
	"github.com/stretchr/testify/require"
)

type nameTestNode struct {
	adapter.Outbound
	tag string
}

func (n *nameTestNode) Tag() string       { return n.tag }
func (n *nameTestNode) Type() string      { return "socks" }
func (n *nameTestNode) Network() []string { return []string{"tcp", "udp"} }

type nameTestGroup struct {
	nameTestNode
	nodes    []adapter.Outbound
	selected adapter.Outbound
}

func (g *nameTestGroup) Type() string { return "selector" }
func (g *nameTestGroup) All() []string {
	var tags []string
	for _, node := range g.nodes {
		tags = append(tags, node.Tag())
	}
	return tags
}
func (g *nameTestGroup) Selected(string) adapter.Outbound  { return g.selected }
func (g *nameTestGroup) AttachConnection(io.Closer) func() { return func() {} }
func (g *nameTestGroup) SelectOutbound(tag string) bool {
	i := slices.IndexFunc(g.nodes, func(node adapter.Outbound) bool { return node.Tag() == tag })
	if i < 0 {
		return false
	}
	g.selected = g.nodes[i]
	return true
}

func TestNativeControlPublicNames(t *testing.T) {
	ctx := service.ContextWithDefaultRegistry(context.Background())
	names := adapter.NewOutboundNames(nil)
	service.MustRegisterPtr(ctx, names)
	a := &nameTestNode{tag: "subscription/a/node"}
	b := &nameTestNode{tag: "subscription/b/node"}
	g := &nameTestGroup{nameTestNode: nameTestNode{tag: "subscription/a/group"}, nodes: []adapter.Outbound{a, b}, selected: a}
	names.Register([]adapter.OutboundName{{Tag: a.Tag(), Name: "香港"}, {Tag: b.Tag(), Name: "香港"}, {Tag: g.Tag(), Name: "策略"}}, nil)
	objects := map[string]adapter.Outbound{a.Tag(): a, b.Tag(): b, g.Tag(): g}
	registry := outbound.NewRegistry()
	outbound.Register[option.StubOptions](registry, "test", func(_ context.Context, _ adapter.Router, _ log.ContextLogger, tag string, _ option.StubOptions) (adapter.Outbound, error) {
		return objects[tag], nil
	})
	manager := outbound.NewManager(registry, endpoint.NewManager(nil), g.Tag())
	for _, tag := range []string{a.Tag(), b.Tag(), g.Tag()} {
		require.NoError(t, manager.Create(ctx, nil, logger.NOP(), tag, "test", nil))
	}
	instance := &Instance{ctx: ctx, outboundManager: manager, urlTestHistoryStorage: urltest.NewHistoryStorage()}
	server := &StartedService{instance: instance}
	groups := server.readGroups()
	require.Len(t, groups.Group, 1)
	require.Equal(t, "策略", groups.Group[0].Tag)
	require.Equal(t, "香港", groups.Group[0].Selected)
	require.Equal(t, "香港-1", groups.Group[0].Items[1].Tag)
	_, err := server.SelectOutbound(ctx, &SelectOutboundRequest{GroupTag: "策略", OutboundTag: "香港-1"})
	require.NoError(t, err)
	require.Same(t, b, g.selected)
	require.Equal(t, "香港-1", server.readGroups().Group[0].Selected)
	node, err := resolveOutbound(instance, "香港-1")
	require.NoError(t, err)
	require.Same(t, b, node)
	events := displayConnectionEvents(ctx, []*ConnectionEvent{{Connection: &Connection{Outbound: b.Tag(), FromOutbound: g.Tag(), ChainList: []string{b.Tag(), g.Tag()}}}})
	require.Equal(t, "香港-1", events[0].Connection.Outbound)
	require.Equal(t, "策略", events[0].Connection.FromOutbound)
	require.Equal(t, []string{"香港-1", "策略"}, events[0].Connection.ChainList)
}
