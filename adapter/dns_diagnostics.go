package adapter

import (
	"context"
	"sync"
	"time"

	"github.com/miekg/dns"
)

// DNSQueryInfo describes the last request using an answer, not an upstream socket.
// Internal lookups intentionally leave the client address/ports empty.
type DNSQueryInfo struct {
	Source          string    `json:"source,omitempty"`
	SourcePort      uint16    `json:"sourcePort,omitempty"`
	Destination     string    `json:"destination,omitempty"`
	DestinationPort uint16    `json:"destinationPort,omitempty"`
	Purpose         string    `json:"purpose,omitempty"`
	Rule            string    `json:"rule,omitempty"`
	Server          string    `json:"server,omitempty"`
	Cached          bool      `json:"cached"`
	LastUsed        time.Time `json:"lastUsed"`
	Hits            uint64    `json:"hits"`
}

type DNSCacheRecord struct {
	Transport string
	Name      string
	Type      uint16
	Message   []byte
	ExpiresAt time.Time
	Info      DNSQueryInfo
}

// DNSCacheStoreAdmin is optional so third-party stores keep their existing API.
type DNSCacheStoreAdmin interface {
	ListDNSCache() ([]DNSCacheRecord, error)
	DNSCacheInfo(transport, name string, qType uint16) DNSQueryInfo
	DeleteDNSCache(transport, name string, qType uint16) error
	DeleteDNSCacheRecords(records []DNSCacheRecord) error
	UpdateDNSCacheInfo(transport, name string, qType uint16, info DNSQueryInfo) error
}

type DNSCacheEntry struct {
	Scope        string       `json:"scope,omitempty"`
	ID           string       `json:"id"`
	Server       string       `json:"server"`
	Domain       string       `json:"domain"`
	Type         string       `json:"type"`
	Class        uint16       `json:"class"`
	ClientSubnet string       `json:"clientSubnet,omitempty"`
	Answers      []string     `json:"answers"`
	AnswerData   []string     `json:"answerData"`
	Addresses    []string     `json:"addresses"`
	Rcode        string       `json:"rcode"`
	ExpiresAt    *time.Time   `json:"expiresAt"`
	Stale        bool         `json:"stale"`
	Info         DNSQueryInfo `json:"request"`
}

type DNSCacheInspector interface {
	DNSCacheEntries() ([]DNSCacheEntry, error)
	DeleteDNSCacheEntries(id, server string) (int, error)
}

type DNSServerInfo struct {
	Scope        string   `json:"scope,omitempty"`
	Tag          string   `json:"tag"`
	Type         string   `json:"type"`
	Address      string   `json:"address"`
	Detour       string   `json:"detour,omitempty"`
	DialerChain  []string `json:"dialerChain"`
	Resolver     string   `json:"resolver,omitempty"`
	Dependencies []string `json:"dependencies"`
	CacheCount   int      `json:"cacheCount"`
	Default      bool     `json:"default"`
}

type DNSRuleInfo struct {
	Scope  string `json:"scope,omitempty"`
	Index  int    `json:"index"`
	Match  string `json:"match"`
	Action string `json:"action"`
}

type DNSInspector interface {
	DNSCacheInspector
	DNSServers() ([]DNSServerInfo, error)
	DNSRules() []DNSRuleInfo
}

type dnsQueryInfoKey struct{}

func ContextWithDNSQueryInfo(ctx context.Context, info DNSQueryInfo) context.Context {
	return context.WithValue(ctx, dnsQueryInfoKey{}, info)
}

func DNSQueryInfoFromContext(ctx context.Context) DNSQueryInfo {
	info, _ := ctx.Value(dnsQueryInfoKey{}).(DNSQueryInfo)
	return info
}

// CaptureDNSQueryInfo must run before DNS rule matching clears Destination.
func CaptureDNSQueryInfo(ctx context.Context) context.Context {
	if _, exists := ctx.Value(dnsQueryInfoKey{}).(DNSQueryInfo); exists {
		return ctx
	}
	info := DNSQueryInfo{Purpose: "client"}
	if metadata := ContextFrom(ctx); metadata != nil {
		info.Source = metadata.Source.AddrString()
		info.SourcePort = metadata.Source.Port
		info.Destination = metadata.Destination.AddrString()
		info.DestinationPort = metadata.Destination.Port
	} else {
		info.Purpose = "internal"
	}
	return ContextWithDNSQueryInfo(ctx, info)
}

// A trace belongs to one DNS request. Responses are keyed by identity so a late
// speculative response cannot overwrite the trace of the accepted response.
type DNSQueryTrace struct {
	access    sync.Mutex
	responses map[*dns.Msg]DNSQueryInfo
	updates   map[*dns.Msg]func(DNSQueryInfo)
	server    string
}

type dnsQueryTraceKey struct{}

func ContextWithDNSQueryTrace(ctx context.Context) (context.Context, *DNSQueryTrace) {
	trace := &DNSQueryTrace{responses: make(map[*dns.Msg]DNSQueryInfo), updates: make(map[*dns.Msg]func(DNSQueryInfo))}
	return context.WithValue(ctx, dnsQueryTraceKey{}, trace), trace
}

func DNSQueryTraceFromContext(ctx context.Context) *DNSQueryTrace {
	trace, _ := ctx.Value(dnsQueryTraceKey{}).(*DNSQueryTrace)
	return trace
}

func (t *DNSQueryTrace) Record(response *dns.Msg, info DNSQueryInfo) {
	if t == nil || response == nil {
		return
	}
	t.access.Lock()
	t.responses[response] = info
	t.access.Unlock()
}

func (t *DNSQueryTrace) Result(response *dns.Msg) DNSQueryInfo {
	if t == nil {
		return DNSQueryInfo{}
	}
	t.access.Lock()
	defer t.access.Unlock()
	return t.responses[response]
}

// Server is set only by the winning branch of a composite DNS transport.
func (t *DNSQueryTrace) SetServer(server string) {
	if t == nil {
		return
	}
	t.access.Lock()
	t.server = server
	t.access.Unlock()
}

func (t *DNSQueryTrace) Server(fallback string) string {
	if t == nil {
		return fallback
	}
	t.access.Lock()
	defer t.access.Unlock()
	if t.server != "" {
		return t.server
	}
	return fallback
}

// DNSInspectorVisitor retains subscription generations for the callback lifetime.
type DNSInspectorVisitor interface {
	VisitDNSInspectors(func(scope string, router DNSRouter) error) error
}

func (t *DNSQueryTrace) RecordWithUpdate(response *dns.Msg, info DNSQueryInfo, update func(DNSQueryInfo)) {
	if t == nil || response == nil {
		return
	}
	t.access.Lock()
	t.responses[response], t.updates[response] = info, update
	t.access.Unlock()
}

func (t *DNSQueryTrace) Complete(response *dns.Msg, fallback DNSQueryInfo, rule string) {
	if t == nil || response == nil {
		return
	}
	t.access.Lock()
	info, found := t.responses[response]
	if !found {
		info = fallback
	}
	if info.Rule != "" && info.Rule != rule {
		info.Rule += " → " + rule
	} else {
		info.Rule = rule
	}
	t.responses[response] = info
	update := t.updates[response]
	delete(t.updates, response)
	t.access.Unlock()
	if update != nil {
		update(info)
	}
}
