package subscription

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	groupmembers "github.com/sagernet/sing-box/common/group"
	"github.com/sagernet/sing-box/common/interrupt"
	"github.com/sagernet/sing-box/common/urltest"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/bufio"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/json/badoption"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

func RegisterOutbound(registry *outbound.Registry) {
	outbound.Register[option.SubscriptionOutboundOptions](registry, C.TypeSubscription, New)
}

var (
	_ adapter.Subscription            = (*Subscription)(nil)
	_ adapter.URLTestGroup            = (*Subscription)(nil)
	_ adapter.SelectableOutboundGroup = (*Subscription)(nil)
	_ adapter.InterfaceUpdateListener = (*Subscription)(nil)
)

type Subscription struct {
	stopWatch    func()
	members      *groupmembers.Members
	testWake     chan struct{}
	dependencies *dependencyCoordinator
	parent       *snapshot
	outbound.Adapter
	ctx                   context.Context
	cancel                context.CancelFunc
	updatesCtx            context.Context
	cancelUpdates         context.CancelFunc
	router                adapter.Router
	logger                log.ContextLogger
	options               option.SubscriptionOutboundOptions
	refreshGate           chan struct{}
	cacheMu               sync.Mutex
	mu                    sync.Mutex
	current               *snapshot
	retired               []*snapshot
	selectedTCP           string
	selectedUDP           string
	initialSelected       string
	selectionInitialized  bool
	selectionBootstrapped bool
	status                adapter.SubscriptionStatus
	etag                  string
	content               []byte
	importAliases         map[string]map[string]string
	closed                bool
	wg                    sync.WaitGroup
	testCalls             sync.WaitGroup
	interrupt             *interrupt.Group
	history               *urltest.HistoryStorage
}

func New(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.SubscriptionOutboundOptions) (adapter.Outbound, error) {
	u, err := url.Parse(options.URL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, E.New("subscription requires an HTTP(S) url")
	}
	// Fragments are not sent in HTTP requests and cannot distinguish sources.
	u.Fragment, u.RawFragment = "", ""
	options.URL = u.String()
	ancestors, _ := ctx.Value(subscriptionAncestorsKey{}).([]string)
	if slices.Contains(ancestors, options.URL) {
		return nil, E.New("circular nested subscription URL: ", options.URL)
	}
	if options.Mode == "" {
		options.Mode = C.TypeSelector
	}
	if options.Mode != C.TypeSelector && options.Mode != C.TypeURLTest {
		return nil, E.New("unknown subscription mode: ", options.Mode)
	}
	if options.UpdateInterval < 0 || options.DownloadTimeout < 0 || options.TestInterval < 0 || options.DrainTimeout < 0 || options.MaxDownloadSize < 0 || options.MaxCacheSize < 0 {
		return nil, E.New("subscription limits and intervals cannot be negative")
	}
	if options.UpdateInterval == 0 {
		options.UpdateInterval = badoption.Duration(24 * time.Hour)
	}
	if options.DownloadTimeout == 0 {
		options.DownloadTimeout = badoption.Duration(30 * time.Second)
	}
	if options.TestInterval == 0 {
		options.TestInterval = badoption.Duration(3 * time.Minute)
	}
	if options.MaxDownloadSize == 0 {
		options.MaxDownloadSize = 16 << 20
	}
	if options.MaxDownloadSize > 1<<30 {
		return nil, E.New("max_download_size cannot exceed 1 GiB")
	}
	if options.MaxCacheSize == 0 {
		options.MaxCacheSize = max(64<<20, options.MaxDownloadSize*2)
	}
	if options.MaxCacheSize > 2<<30 {
		return nil, E.New("max_cache_size cannot exceed 2 GiB")
	}
	if options.Tolerance == 0 {
		options.Tolerance = 50
	}
	coordinator := service.PtrFromContext[dependencyCoordinator](ctx)
	if coordinator == nil {
		coordinator = &dependencyCoordinator{ctx: ctx}
		service.MustRegisterPtr(ctx, coordinator)
	}
	parent := service.PtrFromContext[snapshot](ctx)
	ctx, cancel := context.WithCancel(ctx)
	updatesCtx, cancelUpdates := context.WithCancel(ctx)
	importDependencies := slices.DeleteFunc(groupmembers.Dependencies(nil, options.Import), func(dependency string) bool { return ownsMember(tag, dependency) })
	s := &Subscription{updatesCtx: updatesCtx, cancelUpdates: cancelUpdates, dependencies: coordinator, parent: parent,
		Adapter: outbound.NewAdapter(C.TypeSubscription, tag, []string{N.NetworkTCP, N.NetworkUDP}, importDependencies),
		ctx:     ctx, cancel: cancel, router: router, logger: logger, options: options, refreshGate: make(chan struct{}, 1), testWake: make(chan struct{}, 1),
		status: adapter.SubscriptionStatus{Name: tag}, interrupt: interrupt.NewGroup(),
		history: service.PtrFromContext[urltest.HistoryStorage](ctx),
	}
	s.members, err = groupmembers.New(ctx, tag, func() []string {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.current == nil || s.closed {
			return nil
		}
		return slices.Clone(s.current.members)
	}, options.Import)
	if err != nil {
		cancelUpdates()
		cancel()
		return nil, err
	}
	return s, nil
}

func (s *Subscription) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	switch stage {
	case adapter.StartStateInitialize:
		scope.Add(s.close)
	case adapter.StartStateStart:
		return s.members.Validate(true)
	case adapter.StartStateStarted:
		s.stopWatch = s.members.Watch(scope, func() {
			s.reconcileMembers()
			if s.options.Mode == C.TypeURLTest {
				select {
				case s.testWake <- struct{}{}:
				default:
				}
			}
		})
		s.wg.Add(1)
		go s.loop()
	}
	return nil
}

func (s *Subscription) Status() adapter.SubscriptionStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status
}

func (s *Subscription) ValidateImports(allowPending bool) error {
	if err := s.members.Validate(allowPending); err != nil {
		return err
	}
	g := s.activeSnapshot()
	if g != nil {
		defer g.release()
		if err := adapter.ValidateSubscriptionDependencies(g.scope.Context(), g.outbounds, allowPending); err != nil {
			return err
		}
	}
	if !allowPending {
		s.mu.Lock()
		s.selectionBootstrapped = true
		s.mu.Unlock()
		s.reconcileMembers()
	}
	return nil
}

func (s *Subscription) OptionalImportDependency(tag string) bool {
	return s.members.OptionalImportDependency(tag)
}

func (s *Subscription) reconcileMembers() *groupmembers.Snapshot {
	members := s.members.Resolve()
	s.mu.Lock()
	defer s.mu.Unlock()
	current, _ := s.members.Updates().State()
	if current != members.Revision {
		return members
	}
	for _, selected := range []*string{&s.selectedTCP, &s.selectedUDP} {
		preferred := s.options.Default
		if preferred != "" {
			if slices.Contains(members.Tags, memberTag(s.Tag(), preferred)) {
				preferred = memberTag(s.Tag(), preferred)
			}
		}
		if !s.selectionInitialized {
			if slices.Contains(members.Tags, s.initialSelected) {
				*selected = s.initialSelected
			} else if slices.Contains(members.Tags, preferred) {
				*selected = preferred
			}
		}
		if slices.Contains(members.Tags, *selected) {
			continue
		}
		*selected = ""
		if slices.Contains(members.Tags, preferred) {
			*selected = preferred
		}
		if *selected == "" && len(members.Tags) > 0 {
			*selected = members.Tags[0]
		}
	}
	if s.selectionBootstrapped && s.selectedTCP != "" {
		s.selectionInitialized = true
	}
	return members
}

func (s *Subscription) All() []string {
	return slices.Clone(s.reconcileMembers().Tags)
}

func (s *Subscription) Selected(network string) adapter.Outbound {
	s.reconcileMembers()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current == nil || s.closed {
		return nil
	}
	tag := s.selectedTCP
	if network == N.NetworkUDP {
		tag = s.selectedUDP
	}
	if tag == "" {
		return nil
	}
	return &member{owner: s, tag: tag}
}

func (s *Subscription) AttachConnection(conn io.Closer) func() { return s.interrupt.Add(conn, true) }

func (s *Subscription) SelectOutbound(tag string) bool {
	members := s.reconcileMembers()
	s.mu.Lock()
	if s.closed || s.current == nil || !slices.Contains(members.Tags, tag) {
		s.mu.Unlock()
		return false
	}
	s.selectedTCP, s.selectedUDP = tag, tag
	s.selectionInitialized = true
	s.mu.Unlock()
	if cache := adapter.SelectionCacheFromContext(s.ctx); cache != nil {
		if err := cache.StoreSelected(s.Tag(), tag); err != nil {
			s.logger.Error("store subscription selection: ", err)
		}
	}
	s.notify()
	return true
}

// Pin the active generation before consulting scoped managers or other groups.
// Calling them with s.mu held can reenter this provider through an external ref.
func (s *Subscription) activeSnapshot() *snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	g := s.current
	if s.closed || g == nil || !g.acquire() {
		return nil
	}
	return g
}

func (s *Subscription) ProviderOutbounds() []adapter.Outbound {
	g := s.activeSnapshot()
	if g == nil {
		return nil
	}
	defer g.release()
	var result []adapter.Outbound
	for _, outbound := range g.allOutbounds() {
		result = append(result, &member{owner: s, tag: outbound.Tag()})
	}
	return result
}

func (s *Subscription) ProviderOutbound(tag string) (adapter.Outbound, bool) {
	if !ownsMember(s.Tag(), tag) {
		return nil, false
	}
	g := s.activeSnapshot()
	if g == nil {
		s.mu.Lock()
		pending := !s.closed && s.current == nil
		s.mu.Unlock()
		if pending {
			return &member{owner: s, tag: tag}, true
		}
		return nil, false
	}
	defer g.release()
	if _, found := g.outbounds.Outbound(tag); !found {
		return nil, false
	}
	return &member{owner: s, tag: tag}, true
}

type dialLease struct {
	owner                    *Subscription
	snapshot                 *snapshot
	member                   string
	selectedTCP, selectedUDP string
}
type dialPathKey struct{}

func (s *Subscription) acquire(ctx context.Context, tag, network string) (*snapshot, adapter.Outbound, context.Context, error) {
	path, _ := ctx.Value(dialPathKey{}).([]dialLease)
	var pinned *dialLease
	for i := range path {
		if path[i].owner == s {
			pinned = &path[i]
			break
		}
	}
	for {
		var members *groupmembers.Snapshot
		if pinned == nil {
			members = s.reconcileMembers()
		}
		s.mu.Lock()
		if members != nil {
			revision, _ := s.members.Updates().State()
			if revision != members.Revision {
				s.mu.Unlock()
				continue
			}
		}
		break
	}
	g := s.current
	selectedTCP, selectedUDP := s.selectedTCP, s.selectedUDP
	if pinned != nil {
		g = pinned.snapshot
		selectedTCP, selectedUDP = pinned.selectedTCP, pinned.selectedUDP
	}
	if s.closed || g == nil {
		s.mu.Unlock()
		return nil, nil, ctx, E.New("subscription[", s.Tag(), "] is not ready")
	}
	if tag == "" {
		tag = selectedTCP
		if network == N.NetworkUDP {
			tag = selectedUDP
		}
	}
	for _, entry := range path {
		if entry.owner == s && entry.member == tag {
			s.mu.Unlock()
			return nil, nil, ctx, E.New("circular subscription dial path: ", s.Tag(), "/", tag)
		}
	}
	// A reentry can retain an already retired generation held by its caller.
	retained := g.acquire()
	if !retained {
		for _, entry := range path {
			if entry.owner == s && entry.snapshot == g {
				retained = g.retain()
				break
			}
		}
	}
	s.mu.Unlock()
	if !retained {
		return nil, nil, ctx, net.ErrClosed
	}
	outbound, found := g.outbounds.Outbound(tag)
	path = append(slices.Clone(path), dialLease{owner: s, snapshot: g, member: tag, selectedTCP: selectedTCP, selectedUDP: selectedUDP})
	if !found || !slices.Contains(outboundNetworksFor(outbound, make(map[string]bool), path), network) {
		g.release()
		return nil, nil, ctx, E.New("subscription member unavailable for ", network, ": ", tag)
	}
	ctx = context.WithValue(ctx, dialPathKey{}, path)
	return g, outbound, ctx, nil
}

func (s *Subscription) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return s.dial(ctx, "", network, destination)
}

func (s *Subscription) dial(ctx context.Context, tag, network string, destination M.Socksaddr) (net.Conn, error) {
	g, outbound, ctx, err := s.acquire(ctx, tag, N.NetworkName(network))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(g.scope.Context(), cancel)
	conn, err := outbound.DialContext(ctx, network, destination)
	if err != nil {
		stop()
		cancel()
		g.release()
		return nil, err
	}
	leased := &leasedConn{Conn: conn}
	leased.release = func() { stop(); cancel(); g.untrack(leased) }
	if !g.track(leased) {
		_ = leased.Close()
		return nil, net.ErrClosed
	}
	return leased, nil
}

func (s *Subscription) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return s.listen(ctx, "", destination)
}

func (s *Subscription) listen(ctx context.Context, tag string, destination M.Socksaddr) (net.PacketConn, error) {
	g, outbound, ctx, err := s.acquire(ctx, tag, N.NetworkUDP)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(g.scope.Context(), cancel)
	conn, err := outbound.ListenPacket(ctx, destination)
	if err != nil {
		stop()
		cancel()
		g.release()
		return nil, err
	}
	leased := &leasedPacketConn{NetPacketConn: bufio.NewPacketConn(conn)}
	leased.release = func() { stop(); cancel(); g.untrack(leased) }
	if !g.track(leased) {
		_ = leased.Close()
		return nil, net.ErrClosed
	}
	return leased, nil
}

// A member intentionally does not implement OutboundGroup: routing must pass
// through its Dial/Listen methods to acquire a generation lease.
type member struct {
	owner *Subscription
	tag   string
}

func (m *member) Tag() string         { return m.tag }
func (m *member) ProviderTag() string { return m.owner.Tag() }
func (m *member) Available() bool {
	return m.control(func(adapter.Outbound) error { return nil }) == nil
}
func (m *member) Type() string {
	kind := C.TypeSubscription
	_ = m.control(func(o adapter.Outbound) error { kind = o.Type(); return nil })
	return kind
}
func (m *member) Network() []string { return outboundNetworks(m, make(map[string]bool)) }

// Never hold a provider mutex while invoking another group's Network method.
// A defensive visited set also makes metadata requests safe on stale handles.
func outboundNetworks(outbound adapter.Outbound, visited map[string]bool) []string {
	return outboundNetworksFor(outbound, visited, nil)
}

func outboundNetworksFor(outbound adapter.Outbound, visited map[string]bool, path []dialLease) []string {
	if outbound == nil || visited[outbound.Tag()] {
		return nil
	}
	visited[outbound.Tag()] = true
	if m, ok := outbound.(*member); ok {
		var g *snapshot
		for _, entry := range path {
			if entry.owner == m.owner {
				g = entry.snapshot
				break
			}
		}
		if g != nil {
			if !g.retain() {
				return nil
			}
		} else {
			g = m.owner.activeSnapshot()
		}
		if g == nil {
			return nil
		}
		defer g.release()
		raw, _ := g.outbounds.Outbound(m.tag)
		// The raw object and its stable handle deliberately share their tag.
		delete(visited, m.tag)
		if nested, ok := raw.(*member); ok && nested.owner == m.owner {
			return nil
		}
		return outboundNetworksFor(raw, visited, path)
	}
	if group, ok := outbound.(adapter.OutboundGroup); ok {
		var networks []string
		for _, network := range []string{N.NetworkTCP, N.NetworkUDP} {
			branch := make(map[string]bool, len(visited))
			for tag, present := range visited {
				branch[tag] = present
			}
			var selected adapter.Outbound
			if provider, ok := group.(*Subscription); ok {
				for _, entry := range path {
					if entry.owner == provider {
						tag := entry.selectedTCP
						if network == N.NetworkUDP {
							tag = entry.selectedUDP
						}
						selected = &member{owner: provider, tag: tag}
						break
					}
				}
			}
			if selected == nil {
				selected = group.Selected(network)
			}
			if slices.Contains(outboundNetworksFor(selected, branch, path), network) {
				networks = append(networks, network)
			}
		}
		return networks
	}
	return slices.DeleteFunc(slices.Clone(outbound.Network()), func(network string) bool { return network != N.NetworkTCP && network != N.NetworkUDP })
}
func (m *member) Dependencies() []string { return nil }
func (m *member) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return m.owner.dial(ctx, m.tag, network, destination)
}
func (m *member) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return m.owner.listen(ctx, m.tag, destination)
}

func (s *Subscription) notify() {
	if s.history != nil {
		s.history.NotifyUpdated()
	}
}

func (s *Subscription) InterfaceUpdated(ctx context.Context) {
	s.mu.Lock()
	g := s.current
	if g == nil || !g.acquire() {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	defer g.release()
	for _, outbound := range g.allOutbounds() {
		if listener, ok := outbound.(adapter.InterfaceUpdateListener); ok {
			listener.InterfaceUpdated(ctx)
		}
	}
	if g.dnsRouter != nil {
		g.dnsRouter.ResetNetwork()
	}
	g.httpManager.ResetNetwork()
}

func (s *Subscription) publish(g *snapshot, content []byte, etag string, updated time.Time) error {
	s.dependencies.Lock()
	if err := s.dependencies.validate(s, g); err != nil {
		s.dependencies.Unlock()
		g.close()
		return err
	}
	s.mu.Lock()
	allowPending := s.current == nil && s.parent == nil
	s.mu.Unlock()
	for parent := s.parent; parent != nil; parent = parent.owner.parent {
		parent.mu.Lock()
		allowPending = allowPending || !parent.published
		parent.mu.Unlock()
	}
	if err := adapter.ValidateSubscriptionDependencies(g.scope.Context(), g.outbounds, allowPending); err != nil {
		s.dependencies.Unlock()
		g.close()
		return err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.dependencies.Unlock()
		g.close()
		return net.ErrClosed
	}
	old := s.current
	if old == nil {
		if cache := adapter.SelectionCacheFromContext(s.ctx); cache != nil {
			if selected := cache.LoadSelected(s.Tag()); selected != "" {
				s.initialSelected = selected
				s.selectedTCP, s.selectedUDP = selected, selected
			}
		}
	}
	g.mu.Lock()
	g.published = true
	g.mu.Unlock()
	s.current = g
	s.importAliases = nil
	if updates := service.PtrFromContext[adapter.GroupUpdates](s.ctx); updates != nil {
		updates.Notify()
	}
	if old != nil && s.history != nil {
		for _, tag := range old.members {
			s.history.DeleteURLTestHistory(tag)
		}
	}
	s.content, s.etag = slices.Clone(content), etag
	s.status.Ready, s.status.LastError, s.status.UpdatedAt = true, "", updated
	s.status.Generation++
	// Keep only live retired scopes so shutdown can force them closed.
	s.retired = slices.DeleteFunc(s.retired, func(g *snapshot) bool {
		g.mu.Lock()
		defer g.mu.Unlock()
		if g.closeDone == nil {
			return false
		}
		select {
		case <-g.closeDone:
			return true
		default:
			return false
		}
	})
	if old != nil {
		s.retired = append(s.retired, old)
		// Mark retirement under the shared publication lock, before a child
		// updater can validate against this obsolete parent.
		old.mu.Lock()
		old.retired = true
		old.mu.Unlock()
	}
	s.mu.Unlock()
	s.dependencies.Unlock()
	g.resources.setNotifier(func() {
		s.mu.Lock()
		active := s.current == g && !s.closed
		s.mu.Unlock()
		if active {
			if err := s.saveCache(); err != nil {
				s.logger.Error("save subscription resource cache: ", err)
			}
		}
	})
	if old != nil {
		old.retire(time.Duration(s.options.DrainTimeout))
	}
	if s.options.Mode == C.TypeURLTest {
		select {
		case s.testWake <- struct{}{}:
		default:
		}
	}
	s.reconcileMembers()
	s.notify()
	return nil
}

func (s *Subscription) lockRefresh(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.ctx.Err(); err != nil {
		return err
	}
	select {
	case s.refreshGate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}
func (s *Subscription) unlockRefresh() { <-s.refreshGate }

func (s *Subscription) Refresh(ctx context.Context) error {
	if err := s.lockRefresh(ctx); err != nil {
		return err
	}
	defer s.unlockRefresh()
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()
	defer cancel()
	err := s.fetch(ctx, s.options.HTTPClient)
	if err != nil && s.options.BootstrapHTTPClient != nil && ctx.Err() == nil {
		s.logger.Debug("subscription download failed; trying configured bootstrap client: ", err)
		err = s.fetch(ctx, s.options.BootstrapHTTPClient)
	}
	if err != nil {
		s.mu.Lock()
		s.status.LastError = err.Error()
		s.mu.Unlock()
	}
	return err
}

func (s *Subscription) fetch(ctx context.Context, options *option.HTTPClientOptions) error {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.options.DownloadTimeout))
	defer cancel()
	manager := service.FromContext[adapter.HTTPClientManager](s.ctx)
	if manager == nil {
		return E.New("missing HTTP client manager")
	}
	transport, closeTransport, err := manager.NewDownloadTransport(s.ctx, s.logger, options)
	if err != nil {
		return err
	}
	defer closeTransport()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, s.options.URL, nil)
	if err != nil {
		return err
	}
	if s.etag != "" {
		request.Header.Set("If-None-Match", s.etag)
	}
	response, err := (&http.Client{Transport: transport}).Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotModified {
		if !s.Status().Ready {
			return E.New("subscription returned 304 without a cached snapshot")
		}
		s.mu.Lock()
		s.status.UpdatedAt = time.Now()
		s.status.LastError = ""
		s.mu.Unlock()
		if err = s.saveCache(); err != nil {
			s.logger.Error("save subscription cache: ", err)
		}
		return nil
	}
	if response.StatusCode != http.StatusOK {
		return E.New("subscription HTTP status: ", response.StatusCode)
	}
	if response.ContentLength > s.options.MaxDownloadSize {
		return E.New("subscription exceeds max_download_size")
	}
	content, err := io.ReadAll(io.LimitReader(response.Body, s.options.MaxDownloadSize+1))
	if err != nil {
		return err
	}
	if int64(len(content)) > s.options.MaxDownloadSize {
		return E.New("subscription exceeds max_download_size")
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	g, err := s.buildContext(ctx, content)
	if err != nil {
		return E.Cause(err, "validate subscription")
	}
	if ctx.Err() != nil {
		g.close()
		return ctx.Err()
	}
	if err = s.publish(g, content, response.Header.Get("ETag"), time.Now()); err != nil {
		return err
	}
	if err = s.saveCache(); err != nil {
		s.logger.Error("save subscription cache: ", err)
	}
	return nil
}

func (s *Subscription) loop() {
	defer s.wg.Done()
	update := time.NewTicker(time.Duration(s.options.UpdateInterval))
	defer update.Stop()
	test := time.NewTicker(time.Duration(s.options.TestInterval))
	defer test.Stop()
	if time.Since(s.Status().UpdatedAt) >= time.Duration(s.options.UpdateInterval) {
		if err := s.Refresh(s.updatesCtx); err != nil {
			s.logger.Error("refresh subscription: ", err)
		}
	}
	for {
		select {
		case <-s.updatesCtx.Done():
			return
		case <-update.C:
			if err := s.Refresh(s.updatesCtx); err != nil {
				s.logger.Error("refresh subscription: ", err)
			}
		case <-s.testWake:
			_, _ = s.URLTest(s.updatesCtx)
		case <-test.C:
			if s.options.Mode == C.TypeURLTest {
				_, _ = s.URLTest(s.updatesCtx)
			}
		}
	}
}

// Retired parent generations keep their existing connections, but their child
// providers must stop downloading fresh generations into an obsolete resource tree.
func (s *Subscription) pauseUpdates() {
	s.cancelUpdates()
	if s.stopWatch != nil {
		s.stopWatch()
	}
	s.mu.Lock()
	generations := append(slices.Clone(s.retired), s.current)
	s.mu.Unlock()
	// Cancellation plus the admission mutex prevents new probes from racing
	// this wait. Retired children must not keep manual API probes alive either.
	s.testCalls.Wait()
	for _, g := range generations {
		if g != nil {
			g.pauseUpdates()
		}
	}
}

func (s *Subscription) close() error {
	s.cancel()
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.wg.Wait()
	s.testCalls.Wait()
	s.refreshGate <- struct{}{}
	defer s.unlockRefresh()
	s.mu.Lock()
	all := append(slices.Clone(s.retired), s.current)
	s.current = nil
	s.status.Ready = false
	s.mu.Unlock()
	s.interrupt.Interrupt(true)
	for _, g := range all {
		if g != nil {
			g.close()
		}
	}
	return nil
}
