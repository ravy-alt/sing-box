package group

import (
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"

	"github.com/stretchr/testify/require"
)

func TestSmartCacheFixedTTLAndInvalidation(t *testing.T) {
	c := smartDestinationCache{capacity: 2, ttl: 5 * time.Minute}
	now := time.Now()
	key := smartCacheKey{"example.com", "tcp"}
	_, generation, _ := c.get(key, now)
	first := c.put(key, "a", generation, now)
	require.NotZero(t, first)
	require.Equal(t, first, c.put(key, "a", generation, now.Add(4*time.Minute)))
	node, _, _ := c.get(key, now.Add(5*time.Minute))
	require.Empty(t, node, "success and reads must not extend the TTL")
	second := c.put(key, "a", generation, now.Add(5*time.Minute))
	c.forget(key, "a", first)
	node, _, serial := c.get(key, now.Add(6*time.Minute))
	require.Equal(t, "a", node)
	require.Equal(t, second, serial, "an older connection must not evict the replacement")
	c.clearTarget(key.target)
	require.Zero(t, c.put(key, "b", generation, now.Add(6*time.Minute)), "in-flight dial must not undo an API clear")
	_, generation, _ = c.get(key, now.Add(6*time.Minute))
	require.NotZero(t, c.put(key, "b", generation, now.Add(6*time.Minute)))
	c.clear()
	require.Zero(t, c.put(key, "a", generation, now.Add(7*time.Minute)))
}

func TestSmartCacheCapacityNetworksAndNormalization(t *testing.T) {
	c := smartDestinationCache{capacity: 2, ttl: time.Minute}
	now := time.Now()
	tcp, udp := smartCacheKey{"example.com", "tcp"}, smartCacheKey{"example.com", "udp"}
	c.put(tcp, "a", 0, now)
	c.put(udp, "b", 0, now)
	got, _, _ := c.get(tcp, now)
	require.Equal(t, "a", got)
	got, _, _ = c.get(udp, now)
	require.Equal(t, "b", got)
	c.put(smartCacheKey{"other.test", "tcp"}, "c", 0, now)
	got, _, _ = c.get(tcp, now)
	require.Empty(t, got, "capacity evicts the oldest binding")
	c.clearTarget("example.com")
	got, _, _ = c.get(udp, now)
	require.Empty(t, got)
	got, _, _ = c.get(smartCacheKey{"other.test", "tcp"}, now)
	require.Equal(t, "c", got)
	require.Equal(t, "example.com", smartTarget(adapter.InboundContext{Domain: "EXAMPLE.COM.", Destination: M.ParseSocksaddr("192.0.2.1:443")}))
	require.Equal(t, "192.0.2.1", smartTarget(adapter.InboundContext{Destination: M.ParseSocksaddr("[::ffff:192.0.2.1]:443")}))
}

func TestSmartCacheConcurrentClear(t *testing.T) {
	c := smartDestinationCache{capacity: 32, ttl: time.Minute}
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Go(func() {
			key := smartCacheKey{target: "example.com", network: "tcp"}
			for range 100 {
				_, generation, _ := c.get(key, time.Now())
				serial := c.put(key, "a", generation, time.Now())
				if i%2 == 0 {
					c.clearTarget(key.target)
				} else {
					c.forget(key, "a", serial)
				}
			}
		})
	}
	wg.Wait()
	c.clear()
	require.Empty(t, c.entries)
}
