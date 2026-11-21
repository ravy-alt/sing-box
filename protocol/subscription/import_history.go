package subscription

import (
	groupmembers "github.com/sagernet/sing-box/common/group"
	"github.com/sagernet/sing/service"
)

func (s *Subscription) importHistory() *groupmembers.ImportHistory {
	history := &groupmembers.ImportHistory{References: make(map[string]map[string]string), Aliases: make(map[string]map[string]string)}
	if inherited := service.PtrFromContext[groupmembers.ImportHistory](s.ctx); inherited != nil {
		for owner, references := range inherited.References {
			history.References[owner] = references
		}
		for owner, references := range inherited.Aliases {
			history.Aliases[owner] = references
		}
	}
	s.mu.Lock()
	for owner, references := range s.importAliases {
		history.Aliases[owner] = references
	}
	ready := s.status.Ready
	s.mu.Unlock()
	// The unloaded connection scope has no document identities of its own.
	// Preserve inherited and cached aliases until a real snapshot is accepted.
	if !ready {
		return history
	}
	g := s.activeSnapshot()
	if g == nil {
		return history
	}
	defer g.release()
	for owner := range history.References {
		if ownsMember(s.Tag(), owner) {
			delete(history.References, owner)
		}
	}
	for owner := range history.Aliases {
		if ownsMember(s.Tag(), owner) {
			delete(history.Aliases, owner)
		}
	}
	var collect func(*snapshot)
	collect = func(g *snapshot) {
		for owner, references := range g.imports {
			history.References[owner] = references
		}
		for _, child := range g.children {
			if current := child.activeSnapshot(); current != nil {
				collect(current)
				current.release()
			}
		}
	}
	collect(g)
	return history
}
