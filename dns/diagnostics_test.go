package dns

import (
	"context"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/require"
)

func diagnosticClient() *Client {
	return NewClient(ClientOptions{Context: context.Background(), Logger: log.NewNOPFactory().Logger()})
}

func TestDNSCacheLastRequestAndDeletion(t *testing.T) {
	client := diagnosticClient()
	server := &fakeDNSTransport{tag: "one", address: netip.MustParseAddr("192.0.2.1")}
	other := &fakeDNSTransport{tag: "two", address: netip.MustParseAddr("192.0.2.2")}
	message := new(dns.Msg).SetQuestion("example.com.", dns.TypeA)
	ctx := adapter.ContextWithDNSQueryInfo(context.Background(), adapter.DNSQueryInfo{Purpose: "proxy-server"})
	_, err := client.Exchange(ctx, server, message, adapter.DNSQueryOptions{DiagnosticRule: "first"}, nil)
	require.NoError(t, err)
	ctx, trace := adapter.ContextWithDNSQueryTrace(adapter.ContextWithDNSQueryInfo(context.Background(), adapter.DNSQueryInfo{Purpose: "client", Source: "192.0.2.55", SourcePort: 12345, Destination: "8.8.8.8", DestinationPort: 53}))
	response, err := client.Exchange(ctx, server, message, adapter.DNSQueryOptions{DiagnosticRule: "second"}, nil)
	require.NoError(t, err)
	require.True(t, trace.Result(response).Cached)
	entries, err := client.DNSCacheEntries()
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, uint16(12345), entries[0].Info.SourcePort)
	require.Equal(t, "second", entries[0].Info.Rule)
	require.Equal(t, uint64(1), entries[0].Info.Hits)
	require.Equal(t, int32(1), server.queryCount.Load())
	_, err = client.Exchange(ctx, other, message, adapter.DNSQueryOptions{}, nil)
	require.NoError(t, err)
	count, err := client.DeleteDNSCacheEntries(entries[0].ID, "")
	require.NoError(t, err)
	require.Equal(t, 1, count)
	entries, err = client.DNSCacheEntries()
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "two", entries[0].Server)
	count, err = client.DeleteDNSCacheEntries("", "two")
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestDNSClearPreventsInflightResurrection(t *testing.T) {
	client := diagnosticClient()
	server := &fakeDNSTransport{tag: "one", address: netip.MustParseAddr("192.0.2.1"), delay: 100 * time.Millisecond}
	message := new(dns.Msg).SetQuestion("example.com.", dns.TypeA)
	done := make(chan error, 1)
	go func() {
		_, err := client.Exchange(context.Background(), server, message, adapter.DNSQueryOptions{}, nil)
		done <- err
	}()
	require.Eventually(t, func() bool { return server.queryCount.Load() == 1 }, time.Second, time.Millisecond)
	_, err := client.DeleteDNSCacheEntries("", "")
	require.NoError(t, err)
	require.NoError(t, <-done)
	entries, err := client.DNSCacheEntries()
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestDNSCacheOptimisticExpiry(t *testing.T) {
	client := diagnosticClient()
	client.optimisticTimeout = time.Hour
	key := dnsCacheKey{Question: dns.Question{Name: "expired.", Qtype: dns.TypeA, Qclass: dns.ClassINET}, transportTag: "one"}
	response := FixedResponse(0, key.Question, []netip.Addr{netip.MustParseAddr("192.0.2.1")}, 5)
	client.cache.Add(key, &dnsCacheValue{message: response, expires: time.Now().Add(-time.Minute)})
	entries, err := client.DNSCacheEntries()
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.True(t, entries[0].Stale)
	_, _, stale := client.loadResponse(key)
	require.True(t, stale, "inspection must not purge usable optimistic entries")
	client.optimisticTimeout = 0
	entries, err = client.DNSCacheEntries()
	require.NoError(t, err)
	require.Empty(t, entries)
	client.disableExpire = true
	entries, err = client.DNSCacheEntries()
	require.NoError(t, err)
	require.Nil(t, entries[0].ExpiresAt)
}

func TestDNSRuleTraceEvaluatedWinner(t *testing.T) {
	fast := &fakeDNSTransport{tag: "fast", address: netip.MustParseAddr("192.0.2.1")}
	slow := &fakeDNSTransport{tag: "slow", address: netip.MustParseAddr("192.0.2.2"), delay: time.Second}
	router := raceTestRouter(t, fast, slow)
	router.client = diagnosticClient()
	router.rules = raceTestRules(t, []option.DNSRule{evaluateRule("slow", "slow-answer", true), evaluateRule("fast", "fast-answer", true), respondRule("fast-answer", true, true), respondRule("slow-answer", true, true)})
	ctx, trace := adapter.ContextWithDNSQueryTrace(context.Background())
	response, err := router.Exchange(ctx, new(dns.Msg).SetQuestion("example.com.", dns.TypeA), adapter.DNSQueryOptions{})
	require.NoError(t, err)
	info := trace.Result(response)
	require.Equal(t, "fast", info.Server)
	require.Contains(t, info.Rule, "respond")
	require.Contains(t, info.Rule, "evaluate")
	entries, err := router.DNSCacheEntries()
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, info.Rule, entries[0].Info.Rule)
}

func TestDNSCacheKeyScopeAndSubnet(t *testing.T) {
	key := dnsCacheKey{Question: dns.Question{Name: "example.com.", Qtype: dns.TypeAAAA, Qclass: dns.ClassINET}, transportTag: "same", scope: "订阅 / 一", clientSubnet: netip.MustParsePrefix("192.0.2.0/24"), environment: 123}
	decoded := cacheRecordKey(adapter.DNSCacheRecord{Transport: key.persistentName(), Name: key.Name, Type: key.Qtype})
	require.Equal(t, key, decoded)
	other := key
	other.scope = "订阅 / 二"
	require.NotEqual(t, key.id(), other.id())
}

func TestDNSRuleTraceAndOriginalDestination(t *testing.T) {
	server := &fakeDNSTransport{tag: "final", address: netip.MustParseAddr("192.0.2.1")}
	router := raceTestRouter(t, server)
	router.client = diagnosticClient()
	router.rules = raceTestRules(t, []option.DNSRule{routeRule("final", false)})
	ctx, metadata := adapter.ExtendContext(context.Background())
	metadata.Source = M.ParseSocksaddr("192.0.2.50:12345")
	metadata.Destination = M.ParseSocksaddr("8.8.8.8:53")
	ctx, trace := adapter.ContextWithDNSQueryTrace(ctx)
	message := new(dns.Msg).SetQuestion("example.com.", dns.TypeA)
	response, err := router.Exchange(ctx, message, adapter.DNSQueryOptions{})
	require.NoError(t, err)
	info := trace.Result(response)
	require.Equal(t, uint16(53), info.DestinationPort)
	require.Equal(t, "8.8.8.8", info.Destination)
	require.Contains(t, info.Rule, "0:")
	require.Equal(t, "final", info.Server)
}

func TestDNSCacheConcurrentInspection(t *testing.T) {
	client := diagnosticClient()
	server := &fakeDNSTransport{tag: "one", address: netip.MustParseAddr("192.0.2.1")}
	var group sync.WaitGroup
	for worker := range 4 {
		group.Go(func() {
			for range 50 {
				if worker == 0 {
					_, _ = client.DeleteDNSCacheEntries("", "one")
				} else if worker == 1 {
					_, _ = client.DNSCacheEntries()
				} else {
					_, _ = client.Exchange(context.Background(), server, new(dns.Msg).SetQuestion("example.com.", dns.TypeA), adapter.DNSQueryOptions{}, nil)
				}
			}
		})
	}
	group.Wait()
}
