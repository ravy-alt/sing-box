package dns

import (
	"context"
	"net"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"

	mDNS "github.com/miekg/dns"
)

// A pooled DNS connection can hold a subscription generation indefinitely.
// On membership changes, detach the pool from new queries and drain exchanges
// already using it. This applies equally to UDP, TCP, TLS, HTTP/2 and QUIC.
type generationTransport struct {
	adapter.DNSTransport // immutable metadata of the original transport
	ctx                  context.Context
	logger               log.ContextLogger
	updates              *adapter.GroupUpdates
	factory              func() (adapter.DNSTransport, error)
	mu                   sync.Mutex
	stages               []adapter.StartStage
	current              *dnsGeneration
	live                 map[*dnsGeneration]bool
	closed               bool
	keep                 bool
	stop                 chan struct{}
}

type dnsGeneration struct {
	transport adapter.DNSTransport
	scope     *adapter.Scope
	revision  uint64
	users     int
}

func newGenerationTransport(ctx context.Context, logger log.ContextLogger, updates *adapter.GroupUpdates, initial adapter.DNSTransport, factory func() (adapter.DNSTransport, error)) *generationTransport {
	revision, _ := updates.State()
	g := &dnsGeneration{transport: initial, scope: adapter.NewScope(ctx, logger), revision: revision}
	return &generationTransport{DNSTransport: initial, ctx: ctx, logger: logger, updates: updates, factory: factory, current: g, live: map[*dnsGeneration]bool{g: true}, keep: true, stop: make(chan struct{})}
}

func closeDNSGeneration(g *dnsGeneration) {
	if g != nil {
		_ = g.scope.Close()
	}
}

// Called with mu held. Never close a scope while holding mu: completion of an
// asynchronous exchange may need to acquire it to release the generation.
func (t *generationTransport) retireLocked() *dnsGeneration {
	g := t.current
	t.current = nil
	if g != nil && g.users == 0 {
		delete(t.live, g)
		return g
	}
	return nil
}

func (t *generationTransport) createLocked(revision uint64) (*dnsGeneration, error) {
	native, err := t.factory()
	if err != nil {
		return nil, err
	}
	g := &dnsGeneration{transport: native, scope: adapter.NewScope(t.ctx, t.logger), revision: revision}
	for _, stage := range t.stages {
		if err = native.Start(stage, g.scope); err != nil {
			closeDNSGeneration(g)
			return nil, err
		}
	}
	if keeper, ok := native.(adapter.IdleConnectionKeeper); ok {
		keeper.SetKeepIdleConnections(t.keep)
	}
	t.current = g
	t.live[g] = true
	return g, nil
}

func (t *generationTransport) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	revision, _ := t.updates.State()
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return net.ErrClosed
	}
	if t.current == nil {
		if _, err := t.createLocked(revision); err != nil {
			t.mu.Unlock()
			return err
		}
	}
	err := t.current.transport.Start(stage, t.current.scope)
	if err == nil {
		t.stages = append(t.stages, stage)
	}
	t.mu.Unlock()
	if stage == adapter.StartStateInitialize {
		scope.Add(t.Close)
	}
	if err == nil && stage == adapter.StartStateStarted {
		go t.watch(scope.Context())
	}
	return err
}

func (t *generationTransport) watch(ctx context.Context) {
	for {
		revision, changed := t.updates.State()
		t.mu.Lock()
		var stale *dnsGeneration
		if t.current != nil && t.current.revision < revision {
			stale = t.retireLocked()
		}
		t.mu.Unlock()
		closeDNSGeneration(stale)
		select {
		case <-ctx.Done():
			return
		case <-t.stop:
			return
		case <-changed:
		}
	}
}

func (t *generationTransport) acquire() (*dnsGeneration, error) {
	revision, _ := t.updates.State()
	t.mu.Lock()
	var stale *dnsGeneration
	defer func() { t.mu.Unlock(); closeDNSGeneration(stale) }()
	if t.closed {
		return nil, net.ErrClosed
	}
	if t.current != nil && t.current.revision < revision {
		stale = t.retireLocked()
	}
	g := t.current
	if g == nil {
		var err error
		g, err = t.createLocked(revision)
		if err != nil {
			return nil, err
		}
	}
	g.users++
	return g, nil
}

func (t *generationTransport) release(g *dnsGeneration) {
	t.mu.Lock()
	g.users--
	closeNow := g.users == 0 && g != t.current && t.live[g]
	if closeNow {
		delete(t.live, g)
	}
	t.mu.Unlock()
	if closeNow {
		closeDNSGeneration(g)
	}
}

func (t *generationTransport) Exchange(ctx context.Context, message *mDNS.Msg) (*mDNS.Msg, error) {
	g, err := t.acquire()
	if err != nil {
		return nil, err
	}
	defer t.release(g)
	return g.transport.Exchange(ctx, message)
}

func (t *generationTransport) ExchangeAsync(ctx context.Context, message *mDNS.Msg, callback func(*mDNS.Msg, error)) {
	g, err := t.acquire()
	if err != nil {
		callback(nil, err)
		return
	}
	g.transport.ExchangeAsync(ctx, message, func(response *mDNS.Msg, err error) {
		t.release(g)
		callback(response, err)
	})
}

func (t *generationTransport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	close(t.stop)
	t.current = nil
	live := t.live
	t.live = make(map[*dnsGeneration]bool)
	t.mu.Unlock()
	for g := range live {
		closeDNSGeneration(g)
	}
	return nil
}

func (t *generationTransport) Reset() {
	t.mu.Lock()
	transports := make([]adapter.DNSTransport, 0, len(t.live))
	for g := range t.live {
		transports = append(transports, g.transport)
	}
	t.mu.Unlock()
	for _, transport := range transports {
		transport.Reset()
	}
}

func (t *generationTransport) SetKeepIdleConnections(keep bool) {
	t.mu.Lock()
	t.keep = keep
	for g := range t.live {
		if keeper, ok := g.transport.(adapter.IdleConnectionKeeper); ok {
			keeper.SetKeepIdleConnections(keep)
		}
	}
	t.mu.Unlock()
}

func (t *generationTransport) CloseIdleConnections() {
	t.mu.Lock()
	stale := t.retireLocked()
	t.mu.Unlock()
	closeDNSGeneration(stale)
}

func (t *generationTransport) References() []string {
	return t.DNSTransport.(adapter.Referrer).References()
}

func (t *generationTransport) DNSInfo() adapter.DNSServerInfo {
	if provider, ok := t.DNSTransport.(interface{ DNSInfo() adapter.DNSServerInfo }); ok {
		return provider.DNSInfo()
	}
	return adapter.DNSServerInfo{Tag: t.Tag(), Type: t.Type()}
}
