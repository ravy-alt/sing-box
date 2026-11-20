package subscription

import (
	"maps"
	"slices"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
)

// Candidate generations cannot change the committed cache. The notifier is
// attached only after publication, including for nested subscriptions.
type savedResources struct {
	Selections    map[string]string               `json:"selections,omitempty"`
	Subscriptions map[string]savedSubscription    `json:"subscriptions,omitempty"`
	RuleSets      map[string]*adapter.SavedBinary `json:"rule_sets,omitempty"`
}

type resourceCache struct {
	mu       sync.Mutex
	data     savedResources
	onChange func()
}

func newResourceCache(saved *savedResources) *resourceCache {
	c := &resourceCache{data: savedResources{Selections: make(map[string]string), Subscriptions: make(map[string]savedSubscription), RuleSets: make(map[string]*adapter.SavedBinary)}}
	if saved != nil {
		maps.Copy(c.data.Selections, saved.Selections)
		maps.Copy(c.data.Subscriptions, saved.Subscriptions)
		maps.Copy(c.data.RuleSets, saved.RuleSets)
	}
	return c
}

func (c *resourceCache) snapshot() *savedResources {
	c.mu.Lock()
	defer c.mu.Unlock()
	return &savedResources{Selections: maps.Clone(c.data.Selections), Subscriptions: maps.Clone(c.data.Subscriptions), RuleSets: maps.Clone(c.data.RuleSets)}
}

func (c *resourceCache) setNotifier(notify func()) {
	c.mu.Lock()
	c.onChange = notify
	c.mu.Unlock()
}

func (c *resourceCache) loadSubscription(tag string) (savedSubscription, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	saved, found := c.data.Subscriptions[tag]
	return saved, found
}

func (c *resourceCache) saveSubscription(tag string, saved savedSubscription) {
	c.mu.Lock()
	c.data.Subscriptions[tag] = saved
	notify := c.onChange
	c.mu.Unlock()
	if notify != nil {
		notify()
	}
}

func cloneSavedBinary(saved *adapter.SavedBinary) *adapter.SavedBinary {
	if saved == nil {
		return nil
	}
	clone := *saved
	clone.Content = slices.Clone(saved.Content)
	clone.URLHash = slices.Clone(saved.URLHash)
	return &clone
}

func (c *resourceCache) LoadRuleSet(tag string) *adapter.SavedBinary {
	c.mu.Lock()
	defer c.mu.Unlock()
	return cloneSavedBinary(c.data.RuleSets[tag])
}

func (c *resourceCache) SaveRuleSet(tag string, saved *adapter.SavedBinary) error {
	c.mu.Lock()
	c.data.RuleSets[tag] = cloneSavedBinary(saved)
	notify := c.onChange
	c.mu.Unlock()
	if notify != nil {
		notify()
	}
	return nil
}

func (c *resourceCache) retain(doc *document) {
	children := make(map[string]bool)
	for _, outbound := range doc.options.Outbounds {
		if outbound.Type == C.TypeSubscription {
			children[outbound.Tag] = true
		}
	}
	ruleSets := make(map[string]bool)
	for _, tag := range doc.resources["rule_set"] {
		ruleSets[tag] = true
	}
	c.mu.Lock()
	maps.DeleteFunc(c.data.Selections, func(tag string, _ string) bool {
		for _, mapped := range doc.outboundTags {
			if tag == mapped {
				return false
			}
		}
		return true
	})
	maps.DeleteFunc(c.data.Subscriptions, func(tag string, _ savedSubscription) bool { return !children[tag] })
	maps.DeleteFunc(c.data.RuleSets, func(tag string, _ *adapter.SavedBinary) bool { return !ruleSets[tag] })
	c.mu.Unlock()
}

func (c *resourceCache) LoadSelected(group string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.data.Selections[group]
}
func (c *resourceCache) StoreSelected(group, selected string) error {
	c.mu.Lock()
	c.data.Selections[group] = selected
	notify := c.onChange
	c.mu.Unlock()
	if notify != nil {
		notify()
	}
	return nil
}
