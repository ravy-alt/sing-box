package group

import (
	"container/list"
	"net/netip"
	"strings"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
)

type smartCacheKey struct{ target, network string }

type smartCacheEntry struct {
	key     smartCacheKey
	node    string
	expires time.Time
	serial  uint64
}

// The generation prevents invalidated in-flight dials from repopulating the
// cache. The serial prevents an old connection's failure from deleting a newer
// successful binding, even when its node has the same tag.
type smartDestinationCache struct {
	mu         sync.Mutex
	entries    map[smartCacheKey]*list.Element
	order      list.List
	generation uint64
	serial     uint64
	capacity   int
	ttl        time.Duration
}

func smartTarget(metadata adapter.InboundContext) string {
	target := metadata.Domain
	if target == "" {
		target = metadata.Destination.Fqdn
	}
	if target == "" && metadata.Destination.Addr.IsValid() {
		return metadata.Destination.Addr.Unmap().String()
	}
	target = strings.TrimSuffix(strings.ToLower(target), ".")
	if address, err := netip.ParseAddr(target); err == nil {
		return address.Unmap().String()
	}
	return target
}

func (c *smartDestinationCache) remove(element *list.Element) {
	delete(c.entries, element.Value.(smartCacheEntry).key)
	c.order.Remove(element)
}

func (c *smartDestinationCache) prune(now time.Time) {
	for element := c.order.Front(); element != nil; element = c.order.Front() {
		if now.Before(element.Value.(smartCacheEntry).expires) {
			break
		}
		c.remove(element)
	}
}

func (c *smartDestinationCache) get(key smartCacheKey, now time.Time) (node string, generation, serial uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.prune(now)
	if element := c.entries[key]; element != nil {
		entry := element.Value.(smartCacheEntry)
		if now.Before(entry.expires) {
			return entry.node, c.generation, entry.serial
		}
		c.remove(element)
	}
	return "", c.generation, 0
}

func (c *smartDestinationCache) put(key smartCacheKey, node string, generation uint64, now time.Time) uint64 {
	if key.target == "" || node == "" {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if generation != c.generation || c.capacity <= 0 {
		return 0
	}
	c.prune(now)
	if element := c.entries[key]; element != nil {
		entry := element.Value.(smartCacheEntry)
		if entry.node == node && now.Before(entry.expires) {
			return entry.serial // Neither reads nor successes extend the fixed TTL.
		}
		c.remove(element)
	}
	if c.entries == nil {
		c.entries = make(map[smartCacheKey]*list.Element)
	}
	if len(c.entries) >= c.capacity {
		c.remove(c.order.Front())
	}
	c.serial++
	c.entries[key] = c.order.PushBack(smartCacheEntry{key, node, now.Add(c.ttl), c.serial})
	return c.serial
}

func (c *smartDestinationCache) forget(key smartCacheKey, node string, serial uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if element := c.entries[key]; element != nil {
		entry := element.Value.(smartCacheEntry)
		if entry.node == node && entry.serial == serial {
			c.remove(element)
		}
	}
}

func (c *smartDestinationCache) clearTarget(target string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.generation++
	for key, element := range c.entries {
		if key.target == target {
			c.remove(element)
		}
	}
}

func (c *smartDestinationCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.generation++
	clear(c.entries)
	c.order.Init()
}
