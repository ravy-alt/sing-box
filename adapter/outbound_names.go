package adapter

import (
	"context"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/sagernet/sing/service"
)

// OutboundNames separates public control names from stable routing identities.
// Retired names stay reserved so a stale selection cannot target another node.
type OutboundNames struct {
	mu    sync.RWMutex
	names map[string]string
	tags  map[string]string
}

type OutboundName struct {
	Tag  string
	Name string
}

func NewOutboundNames(staticTags []string) *OutboundNames {
	n := &OutboundNames{names: make(map[string]string), tags: make(map[string]string)}
	for _, tag := range append(slices.Clone(staticTags), "GLOBAL") {
		n.names[tag], n.tags[tag] = tag, tag
	}
	return n
}

func (n *OutboundNames) Register(entries []OutboundName, saved map[string]string) {
	if n == nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	assign := func(tag, name string) bool {
		if name == "" {
			return false
		}
		if _, exists := n.names[tag]; exists {
			return true
		}
		if _, taken := n.tags[name]; taken {
			return false
		}
		n.names[tag], n.tags[name] = name, tag
		return true
	}
	// Reserve persisted choices, including removed nodes, before new names.
	for _, tag := range slices.Sorted(maps.Keys(saved)) {
		assign(tag, saved[tag])
	}
	// Give each unclaimed original name priority over generated suffixes.
	for _, entry := range entries {
		assign(entry.Tag, entry.Name)
	}
	for _, entry := range entries {
		if _, exists := n.names[entry.Tag]; exists {
			continue
		}
		for suffix := 1; ; suffix++ {
			if assign(entry.Tag, entry.Name+"-"+strconv.Itoa(suffix)) {
				break
			}
		}
	}
}

func (n *OutboundNames) Name(tag string) string {
	if n != nil {
		n.mu.RLock()
		name, found := n.names[tag]
		n.mu.RUnlock()
		if found {
			return name
		}
	}
	return tag
}

func (n *OutboundNames) Tag(name string) string {
	if n != nil {
		n.mu.RLock()
		tag, found := n.tags[name]
		n.mu.RUnlock()
		if found {
			return tag
		}
	}
	return name
}

func (n *OutboundNames) Snapshot() map[string]string {
	if n == nil {
		return nil
	}
	n.mu.RLock()
	defer n.mu.RUnlock()
	return maps.Clone(n.names)
}

func ValidOutboundName(original, name string) bool {
	if name == original {
		return true
	}
	if suffix, found := strings.CutPrefix(name, original+"-"); found {
		value, err := strconv.Atoi(suffix)
		return err == nil && value > 0 && strconv.Itoa(value) == suffix
	}
	return false
}

func OutboundNamesFromContext(ctx context.Context) *OutboundNames {
	if ctx == nil {
		return nil
	}
	return service.PtrFromContext[OutboundNames](ctx)
}

func OutboundDisplayName(ctx context.Context, tag string) string {
	return OutboundNamesFromContext(ctx).Name(tag)
}

func OutboundControlTag(ctx context.Context, name string) string {
	return OutboundNamesFromContext(ctx).Tag(name)
}

func OutboundDisplayNames(ctx context.Context, tags []string) []string {
	result := slices.Clone(tags)
	for i, tag := range result {
		result[i] = OutboundDisplayName(ctx, tag)
	}
	return result
}
