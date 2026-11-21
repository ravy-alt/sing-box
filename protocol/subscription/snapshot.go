package subscription

import (
	"context"
	"io"
	"net"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/certificate"
	"github.com/sagernet/sing-box/adapter/endpoint"
	"github.com/sagernet/sing-box/adapter/outbound"
	certificateStore "github.com/sagernet/sing-box/common/certificate"
	"github.com/sagernet/sing-box/common/httpclient"
	"github.com/sagernet/sing-box/common/netns"
	"github.com/sagernet/sing-box/common/urltest"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/dns"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	R "github.com/sagernet/sing-box/route/rule"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/bufio/deadline"
	E "github.com/sagernet/sing/common/exceptions"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"

	mDNS "github.com/miekg/dns"
)

type snapshot struct {
	owner          *Subscription
	children       []*Subscription
	dependencies   map[string][]string
	imports        map[string]map[string]string
	savedNames     map[string]string
	scope          *adapter.Scope
	outbounds      adapter.OutboundManager
	endpoints      adapter.EndpointManager
	ruleSetUpdater *R.RuleSetUpdater
	dnsRouter      adapter.DNSRouter
	httpManager    *httpclient.Manager
	members        []string
	mu             sync.Mutex
	refs           int
	published      bool
	retired        bool
	closed         bool
	closeDone      chan struct{}
	timer          *time.Timer
	connections    map[io.Closer]struct{}
	resources      *resourceCache
}

func (g *snapshot) acquire() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed || g.retired {
		return false
	}
	g.refs++
	return true
}

func (g *snapshot) release() {
	g.mu.Lock()
	g.refs--
	closeNow := g.retired && g.refs == 0 && !g.closed
	g.mu.Unlock()
	if closeNow {
		g.closeAsync()
	}
}

func (g *snapshot) pauseUpdates() {
	for _, child := range g.children {
		child.pauseUpdates()
	}
	if g.outbounds != nil {
		for _, outbound := range g.outbounds.Outbounds() {
			if pauser, ok := outbound.(interface{ PauseUpdates() }); ok {
				pauser.PauseUpdates()
			}
		}
	}
	if g.ruleSetUpdater != nil {
		_ = g.ruleSetUpdater.Close()
	}
}

func (g *snapshot) retire(timeout time.Duration) {
	g.pauseUpdates()
	g.mu.Lock()
	g.retired = true
	closeNow := g.refs == 0
	if !closeNow && timeout > 0 && !g.closed {
		g.timer = time.AfterFunc(timeout, g.closeAsync)
	}
	g.mu.Unlock()
	if closeNow {
		g.closeAsync()
	}
}

func (g *snapshot) retain() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return false
	}
	g.refs++
	return true
}

func (g *snapshot) close() {
	g.closeAsync()
	g.mu.Lock()
	done := g.closeDone
	g.mu.Unlock()
	<-done
}

// Cleanup must not run in the goroutine releasing the final connection: that
// goroutine can be a child provider's own updater, which cleanup must join.
func (g *snapshot) closeAsync() {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return
	}
	g.closed = true
	g.closeDone = make(chan struct{})
	if g.timer != nil {
		g.timer.Stop()
	}
	var connections []io.Closer
	for conn := range g.connections {
		connections = append(connections, conn)
	}
	g.mu.Unlock()
	go func() {
		defer close(g.closeDone)
		for _, conn := range connections {
			_ = conn.Close()
		}
		_ = g.scope.Close()
		if g.owner != nil {
			g.owner.mu.Lock()
			g.owner.retired = slices.DeleteFunc(g.owner.retired, func(retired *snapshot) bool { return retired == g })
			g.owner.mu.Unlock()
		}
	}()
}

func (g *snapshot) track(conn io.Closer) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return false
	}
	if g.connections == nil {
		g.connections = make(map[io.Closer]struct{})
	}
	g.connections[conn] = struct{}{}
	return true
}

func (g *snapshot) untrack(conn io.Closer) {
	g.mu.Lock()
	delete(g.connections, conn)
	g.mu.Unlock()
	g.release()
}

type leasedConn struct {
	net.Conn
	once     sync.Once
	release  func()
	closeErr error
}

func (c *leasedConn) Close() error {
	c.once.Do(func() {
		c.closeErr = c.Conn.Close()
		release := c.release
		c.release = nil
		release()
	})
	return c.closeErr
}

func (c *leasedConn) NeedHandshakeForRead() bool  { return N.NeedHandshakeForRead(c.Conn) }
func (c *leasedConn) NeedHandshakeForWrite() bool { return N.NeedHandshakeForWrite(c.Conn) }
func (c *leasedConn) NeedAdditionalReadDeadline() bool {
	return deadline.NeedAdditionalReadDeadline(c.Conn)
}
func (c *leasedConn) CloseRead() error { return N.CloseRead(c.Conn) }
func (c *leasedConn) CloseWrite() error {
	writer, _ := N.UnwrapCountWriter(c.Conn, nil)
	if duplex, ok := N.UnwrapWriter(writer).(N.WriteCloser); ok {
		return duplex.CloseWrite()
	}
	// Match the connection manager's fallback for protocols without half-close.
	return c.Close()
}

// Do not expose Upstream: bypassing this wrapper would skip the snapshot lease.
type leasedPacketConn struct {
	// Keep packet methods so domain addresses survive DNS NAT on UDP replies.
	N.NetPacketConn
	once     sync.Once
	release  func()
	closeErr error
}

func (c *leasedPacketConn) Close() error {
	c.once.Do(func() {
		c.closeErr = c.NetPacketConn.Close()
		release := c.release
		c.release = nil
		release()
	})
	return c.closeErr
}

func (c *leasedPacketConn) NeedHandshakeForRead() bool {
	return N.NeedHandshakeForReadAny(c.NetPacketConn)
}

func (c *leasedPacketConn) NeedHandshakeForWrite() bool {
	return N.NeedHandshakeForWriteAny(c.NetPacketConn)
}

func (c *leasedPacketConn) NeedAdditionalReadDeadline() bool {
	return deadline.NeedAdditionalReadDeadline(c.NetPacketConn)
}

// Packet methods pass buffers straight to the protocol. Preserve its allocation
// requirements even though the connection must remain opaque to unwrapping:
// exposing Upstream would let callers bypass Close and leak the snapshot lease.
// Use the calculated requirements so nested dialers/wrappers are included.
func (c *leasedPacketConn) FrontHeadroom() int {
	return N.CalculateFrontHeadroom(c.NetPacketConn)
}

func (c *leasedPacketConn) RearHeadroom() int {
	return N.CalculateRearHeadroom(c.NetPacketConn)
}

func (c *leasedPacketConn) ReaderOverhead() int {
	return N.CalculateReaderOverhead(c.NetPacketConn)
}

func (c *leasedPacketConn) ReaderMTU() int {
	return N.CalculateMTU(c.NetPacketConn, nil)
}

func (c *leasedPacketConn) WriterMTU() int {
	return N.CalculateMTU(nil, c.NetPacketConn)
}

type networkView struct {
	adapter.NetworkManager
	defaults adapter.NetworkOptions
}

func (n *networkView) DefaultOptions() adapter.NetworkOptions { return n.defaults }

type dnsView struct {
	*dns.TransportManager
	resolver adapter.DNSTransport
	parent   adapter.DNSTransportManager
	final    string
}

func (m *dnsView) Transport(tag string) (adapter.DNSTransport, bool) {
	if m.resolver != nil && tag == m.resolver.Tag() {
		return m.resolver, true
	}
	if result, found := m.TransportManager.Transport(tag); found {
		return result, true
	}
	if m.parent != nil {
		return m.parent.Transport(tag)
	}
	return nil, false
}

func (m *dnsView) Default() adapter.DNSTransport {
	if result, found := m.Transport(m.final); found {
		return result
	}
	if m.parent != nil {
		return m.parent.Default()
	}
	return m.TransportManager.Default()
}

// routerTransport preserves source DNS rules for nodes with an implicit resolver.
type routerTransport struct {
	dns.TransportAdapter
	router adapter.DNSRouter
}

// Resolve the synthetic transport through rules directly, avoiding recursive
// entry into the same router (and its rule lock/cache).
type scopedDNSRouter struct {
	adapter.DNSRouter
	resolver adapter.DNSTransport
}

func (r *scopedDNSRouter) options(options adapter.DNSQueryOptions) adapter.DNSQueryOptions {
	if options.Transport == r.resolver {
		options.Transport = nil
	}
	return options
}

func (r *scopedDNSRouter) Lookup(ctx context.Context, domain string, options adapter.DNSQueryOptions) ([]netip.Addr, error) {
	return r.DNSRouter.Lookup(ctx, domain, r.options(options))
}

func (r *scopedDNSRouter) Exchange(ctx context.Context, message *mDNS.Msg, options adapter.DNSQueryOptions) (*mDNS.Msg, error) {
	return r.DNSRouter.Exchange(ctx, message, r.options(options))
}

func (r *scopedDNSRouter) ExchangeAsync(ctx context.Context, message *mDNS.Msg, options adapter.DNSQueryOptions, callback func(*mDNS.Msg, error)) {
	r.DNSRouter.ExchangeAsync(ctx, message, r.options(options), callback)
}

func (t *routerTransport) Start(adapter.StartStage, *adapter.Scope) error { return nil }
func (t *routerTransport) Reset()                                         {}
func (t *routerTransport) Exchange(ctx context.Context, message *mDNS.Msg) (*mDNS.Msg, error) {
	return t.router.Exchange(ctx, message, adapter.DNSQueryOptions{})
}
func (t *routerTransport) ExchangeAsync(ctx context.Context, message *mDNS.Msg, callback func(*mDNS.Msg, error)) {
	go func() { response, err := t.Exchange(ctx, message); callback(response, err) }()
}

func (s *Subscription) build(content []byte) (_ *snapshot, err error) {
	return s.buildContext(s.ctx, content)
}

func (s *Subscription) buildContext(buildCtx context.Context, content []byte) (_ *snapshot, err error) {
	return s.buildResources(buildCtx, content, nil)
}

func (s *Subscription) buildResources(buildCtx context.Context, content []byte, saved *savedResources) (_ *snapshot, err error) {
	ctx := service.ExtendContext(s.ctx)
	service.MustRegisterPtr(ctx, s.importHistory())
	doc, err := parseDocument(ctx, s.Tag(), content, s.options.Outbounds, s.options.OverrideDialer)
	if err != nil {
		return nil, err
	}
	if manager := service.FromContext[adapter.OutboundManager](s.ctx); manager != nil {
		for _, tag := range doc.outboundTags {
			if existing, found := manager.Outbound(tag); found {
				owned, ok := existing.(*member)
				if !ok || owned.owner != s {
					return nil, E.New("subscription tag conflicts with host outbound: ", tag)
				}
			}
		}
	}
	ancestors, _ := s.ctx.Value(subscriptionAncestorsKey{}).([]string)
	ctx = context.WithValue(ctx, subscriptionAncestorsKey{}, append(append([]string(nil), ancestors...), s.options.URL))
	g := &snapshot{scope: adapter.NewScope(ctx, s.logger), members: doc.members, owner: s, dependencies: doc.dependencies, imports: doc.imports}
	defer func() {
		if err != nil {
			g.close()
		}
	}()
	if err = s.validateCandidate(g); err != nil {
		return nil, err
	}
	ctx = g.scope.Context()
	service.MustRegisterPtr(ctx, g)
	service.MustRegisterPtr(ctx, &adapter.SubscriptionScopeOptions{Options: doc.options})
	startupCtx, cancelStartup := context.WithCancel(ctx)
	stopStartup := context.AfterFunc(buildCtx, cancelStartup)
	defer stopStartup()
	defer cancelStartup()
	if saved == nil {
		s.mu.Lock()
		if s.current != nil {
			saved = s.current.resources.snapshot()
		}
		s.mu.Unlock()
	}
	g.resources = newResourceCache(saved)
	service.MustRegisterPtr(ctx, g.resources)
	service.MustRegister[adapter.RuleSetCache](ctx, g.resources)
	service.MustRegister[adapter.SelectionCache](ctx, g.resources)
	// Private registries keep cached dialer references inside this generation.
	registry := service.FromContext[adapter.OutboundRegistry](ctx)
	if registry == nil {
		return nil, E.New("missing outbound registry")
	}
	endpoints := &endpointView{endpoint.NewManager(service.FromContext[adapter.EndpointRegistry](ctx)), service.FromContext[adapter.EndpointManager](ctx)}
	g.endpoints = endpoints
	service.MustRegister[adapter.EndpointManager](ctx, endpoints)
	defaultTag := ""
	for _, entry := range doc.options.Outbounds {
		if source, ok := entry.Options.(*option.SubscriptionOutboundOptions); ok && source.SourceOnly {
			continue
		}
		defaultTag = entry.Tag
		break
	}
	if defaultTag == "" && len(doc.options.Endpoints) > 0 {
		defaultTag = doc.options.Endpoints[0].Tag
	}
	if doc.options.Route != nil && doc.options.Route.Final != "" {
		defaultTag = doc.outboundTags[doc.options.Route.Final]
		if defaultTag == "" {
			defaultTag = doc.options.Route.Final
			if !externalReferenceExists(s.ctx, "outbound", defaultTag) {
				return nil, E.New("subscription default outbound not found: ", defaultTag)
			}
		}
	}
	outbounds := &outboundView{outbound.NewManager(registry, endpoints, defaultTag), service.FromContext[adapter.OutboundManager](ctx), s.Tag(), defaultTag}
	g.outbounds = outbounds
	service.MustRegister[adapter.OutboundManager](ctx, outbounds)
	service.MustRegister[adapter.CacheFile](ctx, nil)
	service.MustRegisterPtr[urltest.HistoryStorage](ctx, urltest.NewHistoryStorage())
	privateRouter := &routerView{Router: s.router, ruleSets: make(map[string]adapter.RuleSet)}
	service.MustRegister[adapter.Router](ctx, privateRouter)
	service.MustRegister[adapter.DNSRuleSetUpdateValidator](ctx, nil)
	var extraLifecycles []adapter.Lifecycle
	if doc.options.Certificate != nil {
		store, storeErr := certificateStore.NewStore(ctx, s.logger, *doc.options.Certificate)
		if storeErr != nil {
			return nil, storeErr
		}
		service.MustRegister[adapter.CertificateStore](ctx, store)
		if err = g.scope.Start("subscription certificates", store, adapter.StartStateInitialize); err != nil {
			return nil, err
		}
		extraLifecycles = append(extraLifecycles, store)
	}
	if len(doc.options.NetworkNamespaces) > 0 {
		parent := service.FromContext[adapter.NetworkNamespaceManager](ctx)
		var holderArgs []string
		if factory, ok := parent.(interface{ NamespaceHolderArgs() []string }); ok {
			holderArgs = factory.NamespaceHolderArgs()
		}
		namespaces, createErr := netns.NewManager(s.logger, doc.options.NetworkNamespaces, holderArgs)
		if createErr != nil {
			return nil, createErr
		}
		tags := make(map[string]bool)
		for _, namespace := range doc.options.NetworkNamespaces {
			tags[namespace.Tag] = true
		}
		service.MustRegister[adapter.NetworkNamespaceManager](ctx, &namespaceView{namespaces, parent, tags})
		if err = g.scope.Start("subscription namespaces", namespaces, adapter.StartStateInitialize); err != nil {
			return nil, err
		}
	}
	certificates := &certificateView{certificate.NewManager(service.FromContext[adapter.CertificateProviderRegistry](ctx)), service.FromContext[adapter.CertificateProviderManager](ctx)}
	service.MustRegister[adapter.CertificateProviderManager](ctx, certificates)
	defaultClient := ""
	if doc.options.Route != nil {
		defaultClient = doc.options.Route.DefaultHTTPClient
	}
	externalDefault := ""
	if defaultClient != "" {
		local := false
		for _, client := range doc.options.HTTPClients {
			if client.Tag == defaultClient {
				local = true
				break
			}
		}
		if !local {
			externalDefault, defaultClient = defaultClient, ""
		}
	}
	httpManager := httpclient.NewManager(ctx, s.logger, doc.options.HTTPClients, defaultClient)
	httpManager.Initialize(func() (*httpclient.ManagedTransport, error) {
		return httpclient.NewTransport(ctx, s.logger, "", option.HTTPClientOptions{DefaultOutbound: defaultTag != ""})
	})
	g.httpManager = httpManager
	service.MustRegister[adapter.HTTPClientManager](ctx, &httpView{Manager: httpManager, parent: service.FromContext[adapter.HTTPClientManager](ctx), hasDefault: true, externalDefault: externalDefault, ctx: ctx, logger: s.logger})
	// Start before constructors so partial construction also cleans up HTTP clients.
	if err = g.scope.Start("subscription HTTP clients", httpManager, adapter.StartStateInitialize); err != nil {
		return nil, err
	}
	parentNetwork := service.FromContext[adapter.NetworkManager](ctx)
	defaults := parentNetwork.DefaultOptions()
	if doc.options.DNS != nil {
		defaults.DomainResolver = doc.resolverTag
		defaults.DomainResolveOptions = adapter.DNSQueryOptions{}
	}
	if route := doc.options.Route; route != nil {
		if route.DefaultInterface != "" {
			defaults.BindInterface = route.DefaultInterface
		}
		if route.DefaultMark != 0 {
			defaults.RoutingMark = uint32(route.DefaultMark)
		}
		if route.DefaultNetworkStrategy != nil {
			defaults.NetworkStrategy = (*C.NetworkStrategy)(route.DefaultNetworkStrategy)
		}
		if route.DefaultNetworkType != nil {
			defaults.NetworkType = common.Map(route.DefaultNetworkType, option.InterfaceType.Build)
		}
		if route.DefaultFallbackNetworkType != nil {
			defaults.FallbackNetworkType = common.Map(route.DefaultFallbackNetworkType, option.InterfaceType.Build)
		}
		if route.DefaultFallbackDelay != 0 {
			defaults.FallbackDelay = time.Duration(route.DefaultFallbackDelay)
		}
		if resolver := route.DefaultDomainResolver; resolver != nil {
			defaults.DomainResolver = resolver.Server
			if mapped := doc.dnsTags[resolver.Server]; mapped != "" {
				defaults.DomainResolver = mapped
			}
			defaults.DomainResolveOptions = adapter.DNSQueryOptions{
				Strategy: C.DomainStrategy(resolver.Strategy), Timeout: time.Duration(resolver.Timeout), DisableCache: resolver.DisableCache,
				DisableOptimisticCache: resolver.DisableOptimisticCache, RewriteTTL: resolver.RewriteTTL, ClientSubnet: resolver.ClientSubnet.Build(netip.Prefix{}),
			}
		}
	}
	service.MustRegister[adapter.NetworkManager](ctx, &networkView{parentNetwork, defaults})
	var dnsManager *dnsView
	var dnsRouter *dns.Router
	if doc.options.DNS != nil {
		dnsRegistry := service.FromContext[adapter.DNSTransportRegistry](ctx)
		if dnsRegistry == nil {
			return nil, E.New("missing DNS registry")
		}
		dnsManager = &dnsView{TransportManager: dns.NewTransportManager(dnsRegistry, outbounds, doc.options.DNS.Final), parent: service.FromContext[adapter.DNSTransportManager](ctx), final: doc.options.DNS.Final}
		service.MustRegister[adapter.DNSTransportManager](ctx, dnsManager)
		dnsRouter, err = dns.NewRouter(ctx, service.FromContext[log.Factory](ctx), *doc.options.DNS)
		if err != nil {
			return nil, err
		}
		g.dnsRouter = dnsRouter
		service.MustRegister[adapter.DNSRuleSetUpdateValidator](ctx, dnsRouter)
		dnsManager.resolver = &routerTransport{dns.NewTransportAdapter("subscription", doc.resolverTag, nil), dnsRouter}
		service.MustRegister[adapter.DNSRouter](ctx, &scopedDNSRouter{dnsRouter, dnsManager.resolver})
	}
	ruleSetTasks, err := s.createRuleSets(ctx, g, doc, privateRouter)
	if err != nil {
		return nil, err
	}
	// Outbound constructors may look up DNS transports immediately.
	optionsByTag := make(map[string]option.Outbound, len(doc.options.Outbounds))
	for _, options := range doc.options.Outbounds {
		optionsByTag[options.Tag] = options
	}
	endpointOptions := make(map[string]option.Endpoint, len(doc.options.Endpoints))
	for _, options := range doc.options.Endpoints {
		endpointOptions[options.Tag] = options
	}
	servers := make(map[string]option.DNSServerOptions)
	if doc.options.DNS != nil {
		for _, server := range doc.options.DNS.Servers {
			servers[server.Tag] = server
		}
	}
	providers := make(map[string]option.CertificateProvider)
	for _, provider := range doc.options.CertificateProviders {
		providers[provider.Tag] = provider
	}
	for _, node := range doc.order {
		var lifecycle adapter.Lifecycle
		switch {
		case strings.HasPrefix(node, "outbound:"):
			tag := strings.TrimPrefix(node, "outbound:")
			nodeCtx := adapter.WithContext(ctx, &adapter.InboundContext{Outbound: tag})
			if options, exists := endpointOptions[tag]; exists {
				err = endpoints.Create(nodeCtx, privateRouter, s.logger, tag, options.Type, options.Options)
			} else {
				options, found := optionsByTag[tag]
				if !found {
					continue
				} // provider member or external dependency
				err = outbounds.Create(nodeCtx, privateRouter, s.logger, tag, options.Type, options.Options)
			}
			created, _ := outbounds.Outbound(tag)
			if child, ok := created.(*Subscription); ok {
				g.children = append(g.children, child)
			}
			lifecycle, _ = created.(adapter.Lifecycle)
		case strings.HasPrefix(node, "dns_server:"):
			if options, found := servers[strings.TrimPrefix(node, "dns_server:")]; found {
				err = dnsManager.Create(ctx, s.logger, options.Tag, options.Type, options.Options)
				lifecycle, _ = dnsManager.Transport(options.Tag)
			}
		case strings.HasPrefix(node, "certificate_provider:"):
			options, found := providers[strings.TrimPrefix(node, "certificate_provider:")]
			if !found {
				continue
			}
			err = certificates.Create(ctx, s.logger, options.Tag, options.Type, options.Options)
			lifecycle, _ = certificates.Get(options.Tag)
		}
		if err != nil {
			return nil, E.Cause(err, "create ", node)
		}
		if lifecycle != nil {
			if err = g.scope.Start(node, lifecycle, adapter.StartStateInitialize); err != nil {
				return nil, err
			}
		}
	}
	if dnsRouter != nil {
		if err = dnsRouter.Initialize(doc.options.DNS.Rules); err != nil {
			return nil, err
		}
	}
	if err = g.scope.Start("subscription HTTP clients", httpManager, adapter.StartStateStart); err != nil {
		return nil, err
	}
	for _, stage := range adapter.ListStartStages {
		if stage == adapter.StartStateInitialize {
			continue
		}
		for _, lifecycle := range extraLifecycles {
			if err = g.scope.Start("subscription resource", lifecycle, stage); err != nil {
				return nil, err
			}
		}
		for _, node := range doc.order {
			if _, own := doc.dependencies[node]; !own {
				continue
			}
			var lifecycle adapter.Lifecycle
			switch {
			case strings.HasPrefix(node, "outbound:"):
				tag := strings.TrimPrefix(node, "outbound:")
				if _, exists := optionsByTag[tag]; !exists {
					if _, exists := endpointOptions[tag]; !exists {
						continue
					}
				}
				outbound, _ := outbounds.Outbound(strings.TrimPrefix(node, "outbound:"))
				lifecycle, _ = outbound.(adapter.Lifecycle)
			case strings.HasPrefix(node, "certificate_provider:"):
				lifecycle, _ = certificates.Get(strings.TrimPrefix(node, "certificate_provider:"))
			case strings.HasPrefix(node, "dns_server:") && dnsManager != nil:
				lifecycle, _ = dnsManager.Transport(strings.TrimPrefix(node, "dns_server:"))
			}
			if lifecycle != nil {
				if err = g.scope.Start(node, lifecycle, stage); err != nil {
					return nil, err
				}
			}
		}
		if dnsRouter != nil {
			if stage == adapter.StartStateStart {
				if err = dnsRouter.StartBootstrap(g.scope); err != nil {
					return nil, err
				}
			}
		}
		if stage == adapter.StartStateStart {
			if err = adapter.InitializeSubscriptionScope(startupCtx, outbounds, ruleSetTasks...); err != nil {
				return nil, err
			}
		}
		if dnsRouter != nil {
			if err = g.scope.Start("subscription DNS router", dnsRouter, stage); err != nil {
				return nil, err
			}
		}
	}
	if g.ruleSetUpdater != nil {
		g.ruleSetUpdater.Start()
	}
	if dnsRouter != nil {
		if parent, ok := service.FromContext[adapter.DNSRuleSetUpdateValidator](s.ctx).(interface {
			AddRuleSetUpdateValidator(adapter.DNSRuleSetUpdateValidator) func() error
		}); ok {
			g.scope.Add(parent.AddRuleSetUpdateValidator(dnsRouter))
		}
	}
	g.resources.retain(doc)
	return g, nil
}

var _ io.Closer = (*leasedConn)(nil)
