package dns

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	N "github.com/sagernet/sing/common/network"
)

func dnsRuleDescription(index int, rule adapter.DNSRule) string {
	if rule == nil {
		return "default"
	}
	return fmt.Sprintf("%d: %s => %s", index, rule.String(), rule.Action())
}

func defaultDNSDiagnosticOptions(options adapter.DNSQueryOptions) adapter.DNSQueryOptions {
	options.DiagnosticRule = "default"
	return options
}

func (r *Router) DNSCacheEntries() ([]adapter.DNSCacheEntry, error) {
	if client, ok := r.client.(adapter.DNSCacheInspector); ok {
		return client.DNSCacheEntries()
	}
	return nil, fmt.Errorf("DNS cache inspection unavailable")
}

func (r *Router) DeleteDNSCacheEntries(id, server string) (int, error) {
	if client, ok := r.client.(adapter.DNSCacheInspector); ok {
		return client.DeleteDNSCacheEntries(id, server)
	}
	return 0, fmt.Errorf("DNS cache inspection unavailable")
}

func (r *Router) DNSRules() []adapter.DNSRuleInfo {
	r.rulesAccess.RLock()
	defer r.rulesAccess.RUnlock()
	result := make([]adapter.DNSRuleInfo, 0, len(r.rules)+1)
	for index, rule := range r.rules {
		result = append(result, adapter.DNSRuleInfo{Index: index, Match: rule.String(), Action: rule.Action().String()})
	}
	if server := r.transport.Default(); server != nil {
		result = append(result, adapter.DNSRuleInfo{Index: -1, Match: "default", Action: "route(" + server.Tag() + ")"})
	}
	return result
}

func (r *Router) DNSServers() ([]adapter.DNSServerInfo, error) {
	entries, err := r.DNSCacheEntries()
	if err != nil {
		return nil, err
	}
	counts := make(map[string]int)
	for _, entry := range entries {
		counts[entry.Server]++
	}
	result := []adapter.DNSServerInfo{}
	transports := slices.Clone(r.transport.Transports())
	seenServers := make(map[string]bool)
	for _, transport := range transports {
		seenServers[transport.Tag()] = true
	}
	// A subscription may explicitly borrow a host resolver; its cache still
	// belongs to that subscription's DNS client and needs its own card/count.
	for _, entry := range entries {
		if !seenServers[entry.Server] {
			if transport, found := r.transport.Transport(entry.Server); found {
				transports = append(transports, transport)
				seenServers[entry.Server] = true
			}
		}
	}
	for _, server := range transports {
		info := adapter.DNSServerInfo{Tag: server.Tag(), Type: server.Type(), Dependencies: slices.Clone(server.Dependencies())}
		if inspector, ok := server.(interface{ DNSInfo() adapter.DNSServerInfo }); ok {
			info = inspector.DNSInfo()
		}
		info.CacheCount = counts[server.Tag()]
		info.Default = server == r.transport.Default()
		network := N.NetworkTCP
		switch info.Type {
		case C.DNSTypeUDP, C.DNSTypeQUIC, C.DNSTypeHTTP3, C.DNSTypeLocal, C.DNSTypeDHCP:
			network = N.NetworkUDP
		}
		info.DialerChain = adapter.InspectOutboundDialerChain(r.ctx, r.outbound, info.Detour, network)
		result = append(result, info)
	}
	return result, nil
}

func (r *Router) DNSTransport(tag string) (adapter.DNSTransport, bool) {
	return r.transport.Transport(tag)
}

func (r *Router) completeDNSRule(ctx context.Context, result exchangeWithRulesResult, rule string) {
	info := adapter.DNSQueryInfoFromContext(ctx)
	info.LastUsed = time.Now()
	if result.transport != nil {
		info.Server = result.transport.Tag()
	}
	adapter.DNSQueryTraceFromContext(ctx).Complete(result.response, info, rule)
}
