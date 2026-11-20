package subscription

import (
	"context"
	"slices"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/dependency"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/service"
)

// Publication is serialized across providers so simultaneous individually valid
// updates cannot jointly introduce a cycle. Candidate construction remains parallel.
type dependencyCoordinator struct {
	sync.Mutex
	ctx context.Context
}

func (s *Subscription) validateCandidate(g *snapshot) error {
	s.dependencies.Lock()
	defer s.dependencies.Unlock()
	return s.dependencies.validate(s, g)
}

func (c *dependencyCoordinator) validate(target *Subscription, candidate *snapshot) error {
	for parent := target.parent; parent != nil; parent = parent.owner.parent {
		parent.mu.Lock()
		obsolete := parent.retired || parent.closed
		parent.mu.Unlock()
		if obsolete {
			return E.New("subscription parent generation is retired: ", parent.owner.Tag())
		}
	}
	graph := dependency.Host(c.ctx)
	overrides := map[*Subscription]*snapshot{target: candidate}
	for parent := target.parent; parent != nil; parent = parent.owner.parent {
		overrides[parent.owner] = parent
	}
	var collect func(*Subscription)
	collect = func(s *Subscription) {
		g, overridden := overrides[s]
		if !overridden {
			s.mu.Lock()
			g = s.current
			s.mu.Unlock()
		}
		if g == nil {
			return
		}
		for node, edges := range g.dependencies {
			graph[node] = slices.Clone(edges)
		}
		node := "outbound:" + s.Tag()
		graph[node] = nil
		if s.options.EmptyFallback != "" {
			graph[node] = append(graph[node], "outbound:"+s.options.EmptyFallback)
		}
		for _, spec := range s.options.Import {
			graph[node] = append(graph[node], "outbound:"+spec.Group)
		}
		for _, member := range g.members {
			graph[node] = append(graph[node], "outbound:"+member)
		}
		dependency.AddIPGroup(s.Tag(), graph)
		for _, child := range g.children {
			collect(child)
		}
	}
	if manager := service.FromContext[adapter.OutboundManager](c.ctx); manager != nil {
		for _, outbound := range manager.Outbounds() {
			if s, ok := outbound.(*Subscription); ok {
				collect(s)
			}
		}
	}
	// Directly constructed providers (and children still being constructed) may
	// not yet be present in the root manager.
	if _, exists := graph["outbound:"+target.Tag()]; !exists {
		collect(target)
	}
	dependency.CompleteExternalReferences(graph)
	_, err := dependency.Order(graph)
	return err
}
