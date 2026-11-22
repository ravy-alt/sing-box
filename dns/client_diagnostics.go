package dns

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/netip"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/sagernet/sing-box/adapter"

	"github.com/miekg/dns"
)

type dnsCacheValue struct {
	message *dns.Msg
	expires time.Time
	info    adapter.DNSQueryInfo
}

func (c *Client) cacheEpoch() uint64 {
	c.cacheAccess.Lock()
	defer c.cacheAccess.Unlock()
	return c.cacheGeneration
}

func (c *Client) queryInfo(ctx context.Context, server string, options adapter.DNSQueryOptions, cached bool) adapter.DNSQueryInfo {
	info := adapter.DNSQueryInfoFromContext(adapter.CaptureDNSQueryInfo(ctx))
	info.Server = adapter.DNSQueryTraceFromContext(ctx).Server(server)
	info.Rule = options.DiagnosticRule
	if info.Rule == "" {
		if options.Transport != nil {
			info.Rule = "specified-server"
		} else {
			info.Rule = "default"
		}
	}
	info.Cached, info.LastUsed = cached, time.Now()
	return info
}

func (c *Client) cacheInfoLocked(key dnsCacheKey) adapter.DNSQueryInfo {
	if store, ok := c.dnsCache.(adapter.DNSCacheStoreAdmin); ok {
		return store.DNSCacheInfo(key.persistentName(), key.Name, key.Qtype)
	}
	if c.cache != nil {
		if value, ok := c.cache.Peek(key); ok {
			return value.info
		}
	}
	return adapter.DNSQueryInfo{}
}

func (c *Client) recordCacheUse(ctx context.Context, key dnsCacheKey, response *dns.Msg, options adapter.DNSQueryOptions, generation uint64) {
	c.cacheAccess.Lock()
	defer c.cacheAccess.Unlock()
	previous := c.cacheInfoLocked(key)
	info := c.queryInfo(ctx, key.transportTag, options, true)
	if previous.Server != "" {
		info.Server = previous.Server
	}
	info.Hits = previous.Hits + 1
	adapter.DNSQueryTraceFromContext(ctx).RecordWithUpdate(response, info, c.cacheInfoUpdate(key, info.LastUsed, generation))
	if generation != c.cacheGeneration {
		return
	}
	if store, ok := c.dnsCache.(adapter.DNSCacheStoreAdmin); ok {
		if err := store.UpdateDNSCacheInfo(key.persistentName(), key.Name, key.Qtype, info); err != nil && c.logger != nil {
			c.logger.Warn("update DNS cache metadata: ", err)
		}
	} else if c.cache != nil {
		if value, ok := c.cache.Peek(key); ok {
			value.info = info
		}
	}
}

func (k dnsCacheKey) id() string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%d\x00%s", k.persistentName(), k.Qclass, k.Qtype, k.Name)))
	return hex.EncodeToString(digest[:])
}

func cacheRecordKey(record adapter.DNSCacheRecord) dnsCacheKey {
	name := record.Transport
	key := dnsCacheKey{Question: dns.Question{Name: record.Name, Qtype: record.Type, Qclass: dns.ClassINET}}
	if i := strings.LastIndexByte(name, 2); i >= 0 {
		key.scope = name[i+1:]
		name = name[:i]
	}
	if i := strings.LastIndexByte(name, 1); i >= 0 {
		key.environment, _ = strconv.ParseUint(name[i+1:], 36, 64)
		name = name[:i]
	}
	if i := strings.IndexByte(name, 0); i >= 0 {
		key.clientSubnet, _ = netip.ParsePrefix(name[i+1:])
		name = name[:i]
	}
	key.transportTag = name
	return key
}

func (c *Client) cacheValuesLocked() (map[dnsCacheKey]*dnsCacheValue, error) {
	values := make(map[dnsCacheKey]*dnsCacheValue)
	if c.dnsCache != nil {
		store, ok := c.dnsCache.(adapter.DNSCacheStoreAdmin)
		if !ok {
			return nil, fmt.Errorf("DNS cache store does not support inspection")
		}
		records, err := store.ListDNSCache()
		if err != nil {
			return nil, err
		}
		for _, record := range records {
			key := cacheRecordKey(record)
			if key.scope != c.cacheScope {
				continue
			}
			message := new(dns.Msg)
			if message.Unpack(record.Message) != nil {
				continue
			}
			values[key] = &dnsCacheValue{message, record.ExpiresAt, record.Info}
		}
	} else if c.cache != nil {
		for _, key := range c.cache.Keys() {
			if value, ok := c.cache.Peek(key); ok {
				values[key] = value
			}
		}
	}
	return values, nil
}

func (c *Client) DNSCacheEntries() ([]adapter.DNSCacheEntry, error) {
	c.cacheAccess.Lock()
	defer c.cacheAccess.Unlock()
	values, err := c.cacheValuesLocked()
	if err != nil {
		return nil, err
	}
	entries := make([]adapter.DNSCacheEntry, 0, len(values))
	now := time.Now()
	for key, value := range values {
		if !c.disableExpire && !now.Before(value.expires.Add(c.optimisticTimeout)) {
			continue
		}
		entry := adapter.DNSCacheEntry{
			ID: key.id(), Server: key.transportTag, Domain: FqdnToDomain(key.Name),
			Type: dns.TypeToString[key.Qtype], Class: key.Qclass, Rcode: dns.RcodeToString[value.message.Rcode],
			Answers: []string{}, AnswerData: []string{}, Addresses: []string{}, Info: value.info,
		}
		// The root name consists only of the final dot. Keep it visible in the
		// diagnostics API without changing domain normalization used by routing.
		if key.Name == "." {
			entry.Domain = "."
		}
		if entry.Type == "" {
			entry.Type = "TYPE" + strconv.Itoa(int(key.Qtype))
		}
		if key.clientSubnet.IsValid() {
			entry.ClientSubnet = key.clientSubnet.String()
		}
		if !c.disableExpire {
			expires := value.expires
			entry.ExpiresAt, entry.Stale = &expires, !now.Before(expires)
		}
		for _, rr := range value.message.Answer {
			entry.Answers = append(entry.Answers, rr.String())
			entry.AnswerData = append(entry.AnswerData, strings.TrimPrefix(rr.String(), rr.Header().String()))
			switch address := rr.(type) {
			case *dns.A:
				entry.Addresses = append(entry.Addresses, address.A.String())
			case *dns.AAAA:
				entry.Addresses = append(entry.Addresses, address.AAAA.String())
			}
		}
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].ID < entries[j].ID })
	return entries, nil
}

func (c *Client) DeleteDNSCacheEntries(id, server string) (int, error) {
	c.cacheAccess.Lock()
	defer c.cacheAccess.Unlock()
	values, err := c.cacheValuesLocked()
	if err != nil {
		return 0, err
	}
	// A single epoch prevents in-flight queries resurrecting cleared answers
	// without retaining an unbounded collection of tombstones.
	c.cacheGeneration++
	records := []adapter.DNSCacheRecord{}
	for key := range values {
		if id != "" && key.id() != id || server != "" && key.transportTag != server {
			continue
		}
		records = append(records, adapter.DNSCacheRecord{Transport: key.persistentName(), Name: key.Name, Type: key.Qtype})
		if c.cache != nil {
			c.cache.Remove(key)
		}
	}
	if store, ok := c.dnsCache.(adapter.DNSCacheStoreAdmin); ok {
		if err := store.DeleteDNSCacheRecords(records); err != nil {
			return 0, err
		}
	}
	return len(records), nil
}

func (c *Client) cacheInfoUpdate(key dnsCacheKey, used time.Time, generation uint64) func(adapter.DNSQueryInfo) {
	return func(info adapter.DNSQueryInfo) {
		c.cacheAccess.Lock()
		defer c.cacheAccess.Unlock()
		if generation != c.cacheGeneration || !c.cacheInfoLocked(key).LastUsed.Equal(used) {
			return
		}
		if store, ok := c.dnsCache.(adapter.DNSCacheStoreAdmin); ok {
			_ = store.UpdateDNSCacheInfo(key.persistentName(), key.Name, key.Qtype, info)
		} else if c.cache != nil {
			if value, ok := c.cache.Peek(key); ok {
				value.info = info
			}
		}
	}
}
