package subscription

import (
	"context"
	"slices"

	"github.com/sagernet/sing-box/adapter"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

type inspectionGeneration struct {
	snapshot *snapshot
	tcp, udp string
}

type inspectionGenerationsKey struct{}

// Reuse a retained generation when a chain reenters an enclosing provider.
// Never consult its replacement midway through an inspection.
func (s *Subscription) withInspectionGeneration(ctx context.Context, inspect func(context.Context, inspectionGeneration)) {
	path, _ := ctx.Value(inspectionGenerationsKey{}).([]inspectionGeneration)
	for _, entry := range path {
		if entry.snapshot.owner == s {
			inspect(ctx, entry)
			return
		}
	}
	s.reconcileMembers()
	s.mu.Lock()
	g := s.current
	if enclosing := service.PtrFromContext[snapshot](ctx); enclosing != nil && enclosing.owner == s {
		g = enclosing
	}
	if s.closed || g == nil || !g.retain() {
		s.mu.Unlock()
		return
	}
	entry := inspectionGeneration{g, s.selectedTCP, s.selectedUDP}
	s.mu.Unlock()
	defer g.release()
	ctx = context.WithValue(ctx, inspectionGenerationsKey{}, append(slices.Clone(path), entry))
	inspect(ctx, entry)
}

func (m *member) WithOutboundForInspection(ctx context.Context, inspect adapter.OutboundInspectionFunc) {
	m.owner.withInspectionGeneration(ctx, func(ctx context.Context, entry inspectionGeneration) {
		if raw, found := entry.snapshot.outbounds.Outbound(m.tag); found {
			inspect(ctx, raw, entry.snapshot.outbounds)
		}
	})
}

func (s *Subscription) WithSelectedForInspection(ctx context.Context, network string, inspect adapter.OutboundInspectionFunc) {
	s.withInspectionGeneration(ctx, func(ctx context.Context, entry inspectionGeneration) {
		tag := entry.tcp
		if network == N.NetworkUDP {
			tag = entry.udp
		}
		if selected, found := entry.snapshot.outbounds.Outbound(tag); found {
			inspect(ctx, selected, entry.snapshot.outbounds)
		}
	})
}
