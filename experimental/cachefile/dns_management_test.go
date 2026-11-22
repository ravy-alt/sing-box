package cachefile

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/bbolt"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/require"
)

func diagnosticStore(t *testing.T) *CacheFile {
	t.Helper()
	c := New(context.Background(), log.NewNOPFactory().Logger(), option.CacheFileOptions{Path: filepath.Join(t.TempDir(), "cache.db"), StoreDNS: true})
	require.NoError(t, c.start())
	t.Cleanup(func() { waitDNSSaves(t, c); require.NoError(t, c.database().Close()) })
	return c
}

func waitDNSSaves(t *testing.T, c *CacheFile) {
	t.Helper()
	require.Eventually(t, func() bool {
		c.saveDNSCacheAccess.RLock()
		defer c.saveDNSCacheAccess.RUnlock()
		return len(c.saveDNSCache) == 0
	}, 5*time.Second, time.Millisecond)
}

func TestDNSManagementPersistentMetadataAndDelete(t *testing.T) {
	c := diagnosticStore(t)
	raw, err := new(dns.Msg).SetQuestion("example.com.", dns.TypeA).Pack()
	require.NoError(t, err)
	c.SaveDNSCacheAsync("server", "example.com.", dns.TypeA, raw, time.Now().Add(time.Hour), c.logger)
	info := adapter.DNSQueryInfo{Source: "192.0.2.2", SourcePort: 54321, Rule: "latest rule", Purpose: "client", LastUsed: time.Now(), Hits: 3}
	require.NoError(t, c.UpdateDNSCacheInfo("server", "example.com.", dns.TypeA, info))
	waitDNSSaves(t, c)
	entries, err := c.ListDNSCache()
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, uint16(54321), entries[0].Info.SourcePort)
	require.Equal(t, uint64(3), entries[0].Info.Hits)
	require.NoError(t, c.DeleteDNSCache("server", "example.com.", dns.TypeA))
	_, _, exists := c.LoadDNSCache("server", "example.com.", dns.TypeA)
	require.False(t, exists)
	require.Empty(t, c.DNSCacheInfo("server", "example.com.", dns.TypeA).Rule)
}

func TestDNSManagementClearWhileAsyncSaveQueued(t *testing.T) {
	c := diagnosticStore(t)
	for range 30 {
		c.SaveDNSCacheAsync("server", "example.com.", dns.TypeA, []byte{1, 2, 3}, time.Now().Add(time.Hour), c.logger)
		require.NoError(t, c.ClearDNSCache())
		waitDNSSaves(t, c)
		entries, err := c.ListDNSCache()
		require.NoError(t, err)
		require.Empty(t, entries)
	}
}

func TestDNSManagementConcurrentListingAndUpdates(t *testing.T) {
	c := diagnosticStore(t)
	var workers sync.WaitGroup
	for worker := range 5 {
		workers.Go(func() {
			for i := range 25 {
				name := fmt.Sprintf("%d.example.", i%3)
				switch worker {
				case 0:
					_, _ = c.ListDNSCache()
				case 1:
					_ = c.DeleteDNSCache("server", name, dns.TypeA)
				case 2:
					_ = c.UpdateDNSCacheInfo("server", name, dns.TypeA, adapter.DNSQueryInfo{LastUsed: time.Now()})
				default:
					c.SaveDNSCacheAsync("server", name, dns.TypeA, []byte{byte(i)}, time.Now().Add(time.Hour), c.logger)
				}
			}
		})
	}
	finished := make(chan struct{})
	go func() { workers.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("cache operations deadlocked")
	}
	waitDNSSaves(t, c)
	require.NoError(t, c.ClearDNSCache())
	entries, err := c.ListDNSCache()
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestDNSManagementExpiryCleansMetadata(t *testing.T) {
	c := diagnosticStore(t)
	c.SaveDNSCacheAsync("server", "expired.", dns.TypeA, []byte{1}, time.Now().Add(-time.Hour), c.logger)
	require.NoError(t, c.UpdateDNSCacheInfo("server", "expired.", dns.TypeA, adapter.DNSQueryInfo{Rule: "expired rule"}))
	waitDNSSaves(t, c)
	c.cleanupDNSCache()
	entries, err := c.ListDNSCache()
	require.NoError(t, err)
	require.Empty(t, entries)
	require.NoError(t, c.view(func(tx *bbolt.Tx) error {
		infos := c.bucket(tx, bucketDNSCacheInfo)
		if infos != nil {
			require.Nil(t, infos.Bucket([]byte("server")))
		}
		return nil
	}))
}

func TestDNSManagementCloseFlushesLatestRequest(t *testing.T) {
	ctx := context.Background()
	logger := log.NewNOPFactory().Logger()
	options := option.CacheFileOptions{Path: filepath.Join(t.TempDir(), "cache.db"), StoreDNS: true}
	c := New(ctx, logger, options)
	scope := adapter.NewScope(ctx, logger)
	require.NoError(t, c.Start(adapter.StartStateInitialize, scope))
	c.SaveDNSCacheAsync("server", "example.com.", dns.TypeA, []byte{1, 2, 3}, time.Now().Add(time.Hour), logger)
	require.NoError(t, c.UpdateDNSCacheInfo("server", "example.com.", dns.TypeA, adapter.DNSQueryInfo{SourcePort: 54321, Rule: "last request"}))
	// No waiting for the background writer: orderly shutdown must drain it.
	require.NoError(t, scope.Close())
	reopened := New(ctx, logger, options)
	reopenedScope := adapter.NewScope(ctx, logger)
	require.NoError(t, reopened.Start(adapter.StartStateInitialize, reopenedScope))
	defer reopenedScope.Close()
	entries, err := reopened.ListDNSCache()
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, uint16(54321), entries[0].Info.SourcePort)
	require.Equal(t, "last request", entries[0].Info.Rule)
}
