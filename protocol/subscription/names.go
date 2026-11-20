package subscription

import (
	"net/url"
	"strings"

	"github.com/sagernet/sing-box/adapter"
)

func originalMemberName(tag string) string {
	_, name, found := strings.Cut(strings.TrimPrefix(tag, "subscription/"), "/")
	if !found {
		return tag
	}
	decoded, err := url.PathUnescape(name)
	if err != nil {
		return tag
	}
	return decoded
}

// Caller holds the publication coordinator and the provider lock. A candidate
// child's names become visible only when its enclosing resource tree commits.
func (s *Subscription) publishNames(g *snapshot) {
	for parent := s.parent; parent != nil; parent = parent.owner.parent {
		parent.mu.Lock()
		published := parent.published
		parent.mu.Unlock()
		if !published {
			return
		}
	}
	var entries []adapter.OutboundName
	for _, outbound := range g.allOutbounds() {
		tag := outbound.Tag()
		entries = append(entries, adapter.OutboundName{Tag: tag, Name: originalMemberName(tag)})
	}
	saved := make(map[string]string)
	var collect func(*snapshot)
	collect = func(current *snapshot) {
		for tag, name := range current.savedNames {
			if ownsMember(current.owner.Tag(), tag) && adapter.ValidOutboundName(originalMemberName(tag), name) {
				saved[tag] = name
			}
		}
		for _, child := range current.children {
			child.mu.Lock()
			childSnapshot := child.current
			child.mu.Unlock()
			if childSnapshot != nil {
				collect(childSnapshot)
			}
		}
	}
	collect(g)
	adapter.OutboundNamesFromContext(s.ctx).Register(entries, saved)
}
