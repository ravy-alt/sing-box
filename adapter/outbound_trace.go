package adapter

import (
	"context"
	"crypto/tls"
	"maps"
	"net"
	"slices"
	"sync"

	"github.com/sagernet/sing/common"
)

// DynamicOutboundGroup must be dialed to select its per-destination member.
// Selected is only a presentation of its current default, not a routing decision.
type DynamicOutboundGroup interface {
	OutboundGroup
	DynamicSelection()
}

type SmartURLTestGroup interface {
	URLTestGroup
	DynamicSelection()
	TestURL() string
	ClearCache()
}

type OutboundCacheRegistry struct {
	mu     sync.Mutex
	caches map[SmartURLTestGroup]struct{}
}

func (r *OutboundCacheRegistry) Register(cache SmartURLTestGroup) func() {
	r.mu.Lock()
	if r.caches == nil {
		r.caches = make(map[SmartURLTestGroup]struct{})
	}
	r.caches[cache] = struct{}{}
	r.mu.Unlock()
	return func() { r.mu.Lock(); delete(r.caches, cache); r.mu.Unlock() }
}

func (r *OutboundCacheRegistry) Clear() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for cache := range r.caches {
		cache.ClearCache()
	}
}

// OutboundTrace is shared with the connection tracker, but speculative attempts
// fork it. Only the successful attempt commits its path and cache bindings.
type OutboundTrace struct {
	mu          sync.Mutex
	parts       []outboundTracePart
	bindings    []func()
	invalidated bool
	selected    *OutboundTrace
}

type outboundTracePart struct {
	outbound  Outbound
	transport *OutboundTrace
	key       any
}

func NewOutboundTrace(chain []Outbound) *OutboundTrace {
	trace := new(OutboundTrace)
	for _, outbound := range chain {
		trace.parts = append(trace.parts, outboundTracePart{outbound: outbound})
	}
	return trace
}

func (t *OutboundTrace) Append(outbound Outbound) {
	if t == nil || outbound == nil {
		return
	}
	t.mu.Lock()
	if selected := t.selected; selected != nil {
		t.mu.Unlock()
		selected.Append(outbound)
		return
	}
	defer t.mu.Unlock()
	t.parts = append(t.parts, outboundTracePart{outbound: outbound})
}

func TraceOutbound(ctx context.Context, outbound Outbound) {
	if metadata := ContextFrom(ctx); metadata != nil {
		metadata.OutboundTrace.Append(outbound)
	}
}

func (t *OutboundTrace) Chain() []Outbound {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	if selected := t.selected; selected != nil {
		t.mu.Unlock()
		return selected.Chain()
	}
	parts := slices.Clone(t.parts)
	t.mu.Unlock()
	var chain []Outbound
	appendOutbound := func(outbound Outbound) {
		if outbound != nil && (len(chain) == 0 || chain[len(chain)-1].Tag() != outbound.Tag()) {
			chain = append(chain, outbound)
		}
	}
	for _, part := range parts {
		if part.transport != nil {
			for _, outbound := range part.transport.Chain() {
				appendOutbound(outbound)
			}
		} else {
			appendOutbound(part.outbound)
		}
	}
	return chain
}

func (t *OutboundTrace) Fork() *OutboundTrace {
	if t == nil {
		return new(OutboundTrace)
	}
	t.mu.Lock()
	if selected := t.selected; selected != nil {
		invalidated := t.invalidated
		t.mu.Unlock()
		fork := selected.Fork()
		fork.invalidated = fork.invalidated || invalidated
		return fork
	}
	defer t.mu.Unlock()
	return &OutboundTrace{parts: slices.Clone(t.parts), bindings: slices.Clone(t.bindings), invalidated: t.invalidated}
}

func (t *OutboundTrace) Commit(child *OutboundTrace) {
	if t == nil || child == nil || child == t {
		return
	}
	t.mu.Lock()
	// Keep following this winner: a lazy handshake can discover further
	// detours after DialContext returns. Other speculative traces stay isolated.
	t.selected = child
	t.parts, t.bindings = nil, nil
	invalidated := t.invalidated
	t.mu.Unlock()
	if invalidated {
		child.InvalidateCaches()
	}
}

func (t *OutboundTrace) BindCache(clear func()) {
	if t == nil {
		return
	}
	t.mu.Lock()
	if selected := t.selected; selected != nil {
		t.mu.Unlock()
		selected.BindCache(clear)
		return
	}
	t.bindings = append(t.bindings, clear)
	invalidated := t.invalidated
	t.mu.Unlock()
	if invalidated {
		clear()
	}
}

// Invalidation remains sticky so an API close during dialing also invalidates
// a cache entry installed by the eventual winner.
func (t *OutboundTrace) InvalidateCaches() {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.invalidated = true
	if selected := t.selected; selected != nil {
		t.mu.Unlock()
		selected.InvalidateCaches()
		return
	}
	bindings := slices.Clone(t.bindings)
	parts := slices.Clone(t.parts)
	t.mu.Unlock()
	for _, part := range parts {
		if part.transport != nil {
			part.transport.InvalidateCaches()
		}
	}
	for _, clear := range bindings {
		clear()
	}
}

// MergeTransport attaches the route retained by a reused transport session.
// Its prefix belongs to that transport, not to the session's first application.
func (t *OutboundTrace) MergeTransport(transport *OutboundTrace) {
	t.SetTransport(transport, transport)
}

// A transport slot can be replaced when an HTTP transport retries its request
// on a new session before sending application data. Other path segments retain
// their order, and late updates of the selected transport remain visible.
func (t *OutboundTrace) SetTransport(key any, transport *OutboundTrace) {
	if t == nil || transport == nil || transport == t {
		return
	}
	t.mu.Lock()
	if selected := t.selected; selected != nil {
		t.mu.Unlock()
		selected.SetTransport(key, transport)
		return
	}
	replaced := false
	for i, part := range t.parts {
		if part.transport != nil && part.key == key {
			t.parts[i].transport = transport
			replaced = true
			break
		}
	}
	if !replaced {
		t.parts = append(t.parts, outboundTracePart{transport: transport, key: key})
	}
	invalidated := t.invalidated
	t.mu.Unlock()
	if invalidated {
		transport.InvalidateCaches()
	}
}

type OutboundTraceCarrier interface{ OutboundTrace() *OutboundTrace }

type tracedTransportConn struct {
	net.Conn
	trace *OutboundTrace
}

func WithTransportTrace(conn net.Conn, trace *OutboundTrace) net.Conn {
	wrapped := &tracedTransportConn{conn, trace}
	if state, ok := conn.(interface{ ConnectionState() tls.ConnectionState }); ok {
		return &tracedTLSConn{wrapped, state}
	}
	return wrapped
}

type tracedTLSConn struct {
	*tracedTransportConn
	state interface{ ConnectionState() tls.ConnectionState }
}

func (c *tracedTLSConn) ConnectionState() tls.ConnectionState { return c.state.ConnectionState() }

func TraceTransportConn(ctx context.Context, conn net.Conn, slot ...any) {
	if carrier, ok := common.Cast[OutboundTraceCarrier](conn); ok {
		if len(slot) > 0 {
			if metadata := ContextFrom(ctx); metadata != nil {
				metadata.OutboundTrace.SetTransport(slot[0], carrier.OutboundTrace())
			}
		} else {
			TraceSessionSelection(ctx, carrier.OutboundTrace())
		}
	}
}
func (c *tracedTransportConn) OutboundTrace() *OutboundTrace { return c.trace }
func (c *tracedTransportConn) Upstream() any                 { return c.Conn }
func (c *tracedTransportConn) ReaderReplaceable() bool       { return true }
func (c *tracedTransportConn) WriterReplaceable() bool       { return true }

// NewTracedSession keeps a shared transport owned by the client's lifetime,
// while retaining whether the request that started it was a health probe.
func NewTracedSession(lifetime, request context.Context) (context.Context, any) {
	if lifetime == nil {
		lifetime = context.Background()
	}
	ctx := ContextForMultiplexSession(lifetime)
	if request != nil && IsURLTest(request) {
		ctx = ContextWithURLTest(ctx)
	}
	return ctx, ContextFrom(ctx).OutboundTrace
}

func TraceSessionSelection(ctx context.Context, sessionMetadata any) {
	if trace, ok := sessionMetadata.(*OutboundTrace); ok {
		if metadata := ContextFrom(ctx); metadata != nil {
			metadata.OutboundTrace.MergeTransport(trace)
		}
	}
}

// Port hopping may replace the underlying route of an otherwise live QUIC
// session. Keep a single live transport slot so existing streams follow it.
func TraceSessionTransport(ctx context.Context, sessionMetadata any) {
	if trace, ok := sessionMetadata.(*OutboundTrace); ok {
		if metadata := ContextFrom(ctx); metadata != nil {
			metadata.OutboundTrace.SetTransport("session-transport", trace)
		}
	}
}

// ForkOutboundContext isolates metadata mutations as well as the trace. Normal
// dialers may update DNS metadata while concurrent candidates are connecting.
func ForkOutboundContext(ctx context.Context) (context.Context, *OutboundTrace) {
	var metadata InboundContext
	if original := ContextFrom(ctx); original != nil {
		metadata = *original
		metadata.OutboundChain = slices.Clone(original.OutboundChain)
		metadata.SnifferNames = slices.Clone(original.SnifferNames)
		metadata.NetworkType = slices.Clone(original.NetworkType)
		metadata.FallbackNetworkType = slices.Clone(original.FallbackNetworkType)
		metadata.DestinationAddresses = slices.Clone(original.DestinationAddresses)
		metadata.SourceMACAddress = slices.Clone(original.SourceMACAddress)
		if original.DNSResponse != nil {
			metadata.DNSResponse = original.DNSResponse.Copy()
		}
		metadata.NamedDNSResponses = maps.Clone(original.NamedDNSResponses)
		for name, response := range metadata.NamedDNSResponses {
			if response != nil {
				metadata.NamedDNSResponses[name] = response.Copy()
			}
		}
	}
	metadata.OutboundTrace = metadata.OutboundTrace.Fork()
	return WithContext(ctx, &metadata), metadata.OutboundTrace
}
