package adapter

import (
	"context"
	"errors"
	"fmt"
	"slices"

	C "github.com/sagernet/sing-box/constant"
	N "github.com/sagernet/sing/common/network"
)

var ErrOutboundNetworkUnsupported = errors.New("outbound network is disabled or unsupported")

// CheckOutboundDialerNetwork checks a selected path without dialing. Inspection
// callbacks keep private subscription managers and their generations together;
// their raw objects must never escape this traversal for use as dialers.
func CheckOutboundDialerNetwork(ctx context.Context, outbound Outbound, manager OutboundManager, network string) error {
	type hop struct{ tag, network string }
	path := make(map[hop]bool)
	var walk func(context.Context, Outbound, OutboundManager, string, int) error
	walk = func(ctx context.Context, outbound Outbound, manager OutboundManager, network string, depth int) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if outbound == nil {
			return errors.New("probe path has no selected outbound")
		}
		if depth >= 128 {
			return errors.New("probe dialer chain is too deep")
		}
		if wrapper, ok := outbound.(OutboundWrapperInspector); ok {
			err := fmt.Errorf("probe outbound [%s] is unavailable", OutboundDisplayName(ctx, outbound.Tag()))
			wrapper.WithOutboundForInspection(ctx, func(ctx context.Context, raw Outbound, manager OutboundManager) {
				err = walk(ctx, raw, manager, network, depth+1)
			})
			return err
		}
		key := hop{outbound.Tag(), network}
		if path[key] {
			return errors.New("circular probe dialer chain")
		}
		path[key] = true
		defer delete(path, key)
		if group, ok := outbound.(OutboundGroupInspector); ok {
			err := fmt.Errorf("probe group [%s] has no selected %s outbound", OutboundDisplayName(ctx, outbound.Tag()), network)
			group.WithSelectedForInspection(ctx, network, func(ctx context.Context, selected Outbound, manager OutboundManager) {
				err = walk(ctx, selected, manager, network, depth+1)
			})
			return err
		}
		if group, ok := OutboundGroupFor(outbound); ok {
			return walk(ctx, group.Selected(network), manager, network, depth+1)
		}
		if !slices.Contains(outbound.Network(), network) {
			return fmt.Errorf("%w: outbound [%s] requires %s", ErrOutboundNetworkUnsupported, OutboundDisplayName(ctx, outbound.Tag()), network)
		}
		dependencies := outbound.Dependencies()
		if len(dependencies) == 0 {
			return nil
		}
		if manager == nil || len(dependencies) != 1 {
			return fmt.Errorf("cannot resolve probe detour for [%s]", OutboundDisplayName(ctx, outbound.Tag()))
		}
		next, found := manager.Outbound(dependencies[0])
		if !found {
			return fmt.Errorf("probe detour [%s] is unavailable", OutboundDisplayName(ctx, dependencies[0]))
		}
		dialerNetwork := DialerNetworkFor(outbound, network)
		// SOCKS UDP also opens a TCP control connection. UoT only needs TCP.
		if outbound.Type() == C.TypeSOCKS && network == N.NetworkUDP && dialerNetwork == N.NetworkUDP {
			if err := walk(ctx, next, manager, N.NetworkTCP, depth+1); err != nil {
				return err
			}
		}
		return walk(ctx, next, manager, dialerNetwork, depth+1)
	}
	return walk(ctx, outbound, manager, network, 0)
}
