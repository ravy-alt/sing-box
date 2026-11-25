package adapter

import (
	"context"

	C "github.com/sagernet/sing-box/constant"
	N "github.com/sagernet/sing/common/network"
)

// Inspection callbacks must not retain or dial the supplied outbound. Providers
// keep its generation alive only for the duration of the callback.
type OutboundInspectionFunc func(context.Context, Outbound, OutboundManager)

type OutboundWrapperInspector interface {
	WithOutboundForInspection(context.Context, OutboundInspectionFunc)
}

type OutboundGroupInspector interface {
	WithSelectedForInspection(context.Context, string, OutboundInspectionFunc)
}

// OutboundDialerNetwork reports the network used to reach the next detour, which
// may differ from the payload network (for example UDP over AnyTLS uses TCP).
type OutboundDialerNetwork interface {
	DialerNetwork(network string) string
}

// OutboundDialerNetworkAlternatives lists independently usable underlying
// transports. Capability validation accepts any viable alternative, whereas
// DialerNetwork reports the active/preferred transport for diagnostics.
type OutboundDialerNetworkAlternatives interface {
	DialerNetworkAlternatives(network string) []string
}

// DialerNetworkFor also accepts optional transports/plugins. Their default is
// supplied by the caller; only transports which change it need to implement it.
func DialerNetworkFor(dialer any, network string) string {
	if dialer, ok := dialer.(OutboundDialerNetwork); ok {
		return dialer.DialerNetwork(network)
	}
	if outbound, ok := dialer.(Outbound); ok {
		switch outbound.Type() {
		case C.TypeAnyTLS, C.TypeHTTP, C.TypeSSH, C.TypeShadowTLS, C.TypeTor, C.TypeSnell:
			return N.NetworkTCP
		case C.TypeHysteria, C.TypeHysteria2, C.TypeTUIC, C.TypeWireGuard:
			return N.NetworkUDP
		}
	}
	return network
}

func InspectOutboundDialerChain(ctx context.Context, manager OutboundManager, tag, network string) []string {
	chain := []string{}
	if manager == nil || tag == "" {
		return chain
	}
	seen := make(map[string]bool)
	var walk func(context.Context, Outbound, OutboundManager, string, int)
	walk = func(ctx context.Context, outbound Outbound, manager OutboundManager, network string, depth int) {
		if outbound == nil || depth >= 128 {
			return
		}
		if wrapper, ok := outbound.(OutboundWrapperInspector); ok {
			wrapper.WithOutboundForInspection(ctx, func(ctx context.Context, raw Outbound, manager OutboundManager) {
				walk(ctx, raw, manager, network, depth+1)
			})
			return
		}
		if seen[outbound.Tag()] {
			return
		}
		seen[outbound.Tag()] = true
		chain = append(chain, OutboundDisplayName(ctx, outbound.Tag()))
		if group, ok := outbound.(OutboundGroupInspector); ok {
			group.WithSelectedForInspection(ctx, network, func(ctx context.Context, selected Outbound, manager OutboundManager) {
				walk(ctx, selected, manager, network, depth+1)
			})
		} else if group, ok := OutboundGroupFor(outbound); ok {
			// Keep the selected object and its declaring manager. Resolving its tag
			// again through the host manager would lose private subscription state.
			walk(ctx, group.Selected(network), manager, network, depth+1)
		} else if dependencies := outbound.Dependencies(); len(dependencies) == 1 && manager != nil {
			network = DialerNetworkFor(outbound, network)
			next, _ := manager.Outbound(dependencies[0])
			walk(ctx, next, manager, network, depth+1)
		}
	}
	outbound, _ := manager.Outbound(tag)
	walk(ctx, outbound, manager, network, 0)
	return chain
}
