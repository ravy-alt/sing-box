package group

import (
	"context"
	"io"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	groupmembers "github.com/sagernet/sing-box/common/group"
	"github.com/sagernet/sing-box/common/interrupt"
	"github.com/sagernet/sing-box/common/urltest"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/batch"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/x/list"
	"github.com/sagernet/sing/service"
	"github.com/sagernet/sing/service/pause"
)

func RegisterURLTest(registry *outbound.Registry) {
	outbound.Register[option.URLTestOutboundOptions](registry, C.TypeURLTest, NewURLTest)
}

var (
	_ adapter.OutboundGroup           = (*URLTest)(nil)
	_ adapter.InterfaceUpdateListener = (*URLTest)(nil)
	_ adapter.Referrer                = (*URLTest)(nil)
)

type URLTest struct {
	stopWatch func()
	outbound.Adapter
	ctx                          context.Context
	outbound                     adapter.OutboundManager
	logger                       log.ContextLogger
	members                      *groupmembers.Members
	tags                         []string
	link                         string
	interval                     time.Duration
	tolerance                    uint16
	idleTimeout                  time.Duration
	group                        *URLTestGroup
	checkAccess                  sync.Mutex
	interruptExternalConnections bool
}

func NewURLTest(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.URLTestOutboundOptions) (adapter.Outbound, error) {
	outbound := &URLTest{
		Adapter:                      outbound.NewAdapter(C.TypeURLTest, tag, []string{N.NetworkTCP, N.NetworkUDP}, groupmembers.Dependencies(options.Outbounds, options.Import, options.EmptyFallback)),
		ctx:                          ctx,
		outbound:                     service.FromContext[adapter.OutboundManager](ctx),
		logger:                       logger,
		tags:                         options.Outbounds,
		link:                         options.URL,
		interval:                     time.Duration(options.Interval),
		tolerance:                    options.Tolerance,
		idleTimeout:                  time.Duration(options.IdleTimeout),
		interruptExternalConnections: options.InterruptExistConnections,
	}
	if err := groupmembers.ValidateBase(options.Outbounds, len(options.Import) > 0, options.EmptyFallback); err != nil {
		return nil, err
	}
	members, err := groupmembers.New(ctx, tag, func() []string { return options.Outbounds }, options.Import, options.EmptyFallback)
	if err != nil {
		return nil, err
	}
	outbound.members = members

	return outbound, nil
}

func (s *URLTest) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	switch stage {
	case adapter.StartStateStart:
		outbounds := make([]adapter.Outbound, 0, len(s.tags))
		for i, tag := range s.tags {
			detour, loaded := s.outbound.Outbound(tag)
			if !loaded {
				return E.New("outbound ", i, " not found: ", tag)
			}
			outbounds = append(outbounds, detour)
		}
		if err := s.members.Validate(true); err != nil {
			return err
		}
		group, err := NewURLTestGroup(s.ctx, s.outbound, s.logger, outbounds, s.link, s.interval, s.tolerance, s.idleTimeout, s.interruptExternalConnections)
		if err != nil {
			return err
		}
		group.members = s.members
		s.group = group
		scope.Add(s.group.Close)
	case adapter.StartStateStarted:
		s.group.PostStart()
		s.stopWatch = s.members.Watch(scope, func() {
			s.group.refreshMembers()
			s.group.requestTest()
		})
	}
	return nil
}

// Retiring a subscription stops probes while established traffic keeps its
// generation alive. Closing the test worker does not close traffic connections.
func (s *URLTest) PauseUpdates() {
	if s.stopWatch != nil {
		s.stopWatch()
	}
	if s.group != nil {
		_ = s.group.Close()
	}
}

func (s *URLTest) ValidateImports(allowPending bool) error { return s.members.Validate(allowPending) }

func (s *URLTest) OptionalImportDependency(tag string) bool {
	return s.members.OptionalImportDependency(tag)
}

func (s *URLTest) ImportsSource(tag string) bool { return s.members.ImportsSource(tag) }

func (s *URLTest) All() []string { return slices.Clone(s.members.Resolve().Tags) }

func (s *URLTest) Selected(network string) adapter.Outbound {
	if s.group == nil {
		return nil
	}
	return s.group.Selected(network)
}

func (s *URLTest) AttachConnection(closer io.Closer) func() {
	s.group.Touch()
	return s.group.interruptGroup.Add(closer, true)
}

func (s *URLTest) References() []string {
	group := s.group
	if group == nil {
		return nil
	}
	var references []string
	tcp, udp := group.Selected(N.NetworkTCP), group.Selected(N.NetworkUDP)
	if tcp != nil {
		references = append(references, tcp.Tag())
	}
	if udp != nil && (tcp == nil || udp.Tag() != tcp.Tag()) {
		references = append(references, udp.Tag())
	}
	return references
}

func (s *URLTest) URLTest(ctx context.Context) (map[string]uint16, error) {
	return s.group.URLTest(ctx)
}

func (s *URLTest) TestURL() string { return s.link }

func (s *URLTest) CheckOutbounds() {
	s.group.CheckOutbounds(s.ctx, true)
}

func (s *URLTest) PerformUpdateCheck() {
	s.group.performUpdateCheck()
}

func (s *URLTest) InterfaceUpdated(ctx context.Context) {
	group := s.group
	if group == nil {
		return
	}
	if group.pause.IsDevicePaused() || group.pause.IsNetworkPaused() {
		return
	}
	group.runBackground(func() {
		s.checkAccess.Lock()
		defer s.checkAccess.Unlock()
		if ctx.Err() != nil {
			return
		}
		group.CheckOutbounds(ctx, true)
	})
}

func (s *URLTest) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	s.group.Touch()
	networkName := N.NetworkName(network)
	if networkName != N.NetworkTCP && networkName != N.NetworkUDP {
		return nil, E.Extend(N.ErrUnknownNetwork, network)
	}
	outbound := s.group.Selected(networkName)
	if outbound == nil {
		return nil, E.New("missing supported outbound")
	}
	adapter.TraceOutbound(ctx, outbound)
	conn, err := outbound.DialContext(ctx, network, destination)
	if err == nil {
		return s.group.interruptGroup.NewConn(conn, interrupt.IsExternalConnectionFromContext(ctx)), nil
	}
	s.logger.ErrorContext(ctx, err)
	if !adapter.IsURLTest(ctx) && ctx.Err() == nil {
		s.group.history.DeleteURLTestHistory(outbound.Tag())
	}
	return nil, err
}

func (s *URLTest) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	s.group.Touch()
	outbound := s.group.Selected(N.NetworkUDP)
	if outbound == nil {
		return nil, E.New("missing supported outbound")
	}
	adapter.TraceOutbound(ctx, outbound)
	conn, err := outbound.ListenPacket(ctx, destination)
	if err == nil {
		return s.group.interruptGroup.NewPacketConn(conn, interrupt.IsExternalConnectionFromContext(ctx)), nil
	}
	s.logger.ErrorContext(ctx, err)
	if !adapter.IsURLTest(ctx) && ctx.Err() == nil {
		s.group.history.DeleteURLTestHistory(outbound.Tag())
	}
	return nil, err
}

type URLTestGroup struct {
	background                   sync.WaitGroup
	testCalls                    sync.WaitGroup
	members                      *groupmembers.Members
	stateMu                      sync.Mutex
	revision                     uint64
	membershipReady              bool
	testWake                     chan struct{}
	cancel                       context.CancelFunc
	closed                       bool
	ctx                          context.Context
	outbound                     adapter.OutboundManager
	pause                        pause.Manager
	pauseCallback                *list.Element[pause.Callback]
	logger                       log.Logger
	outbounds                    []adapter.Outbound
	link                         string
	interval                     time.Duration
	tolerance                    uint16
	idleTimeout                  time.Duration
	history                      *urltest.HistoryStorage
	checking                     atomic.Bool
	selectedOutboundTCP          adapter.Outbound
	selectedOutboundUDP          adapter.Outbound
	interruptGroup               *interrupt.Group
	interruptExternalConnections bool
	access                       sync.Mutex
	updateAccess                 sync.Mutex
	ticker                       *time.Ticker
	close                        chan struct{}
	started                      bool
	lastActive                   common.TypedValue[time.Time]
}

func NewURLTestGroup(ctx context.Context, outboundManager adapter.OutboundManager, logger log.Logger, outbounds []adapter.Outbound, link string, interval time.Duration, tolerance uint16, idleTimeout time.Duration, interruptExternalConnections bool) (*URLTestGroup, error) {
	if interval == 0 {
		interval = C.DefaultURLTestInterval
	}
	if tolerance == 0 {
		tolerance = 50
	}
	if idleTimeout == 0 {
		idleTimeout = C.DefaultURLTestIdleTimeout
	}
	if interval > idleTimeout {
		return nil, E.New("interval must be less or equal than idle_timeout")
	}
	history := service.PtrFromContext[urltest.HistoryStorage](ctx)
	if history == nil {
		return nil, E.New("missing URL test history storage")
	}
	ctx, cancel := context.WithCancel(ctx)
	return &URLTestGroup{
		cancel: cancel, testWake: make(chan struct{}, 1),
		ctx:                          ctx,
		outbound:                     outboundManager,
		logger:                       logger,
		outbounds:                    outbounds,
		link:                         link,
		interval:                     interval,
		tolerance:                    tolerance,
		idleTimeout:                  idleTimeout,
		history:                      history,
		close:                        make(chan struct{}),
		pause:                        service.FromContext[pause.Manager](ctx),
		interruptGroup:               interrupt.NewGroup(),
		interruptExternalConnections: interruptExternalConnections,
	}, nil
}

func (g *URLTestGroup) PostStart() {
	g.access.Lock()
	defer g.access.Unlock()
	if g.closed || g.started {
		return
	}
	g.started = true
	g.lastActive.Store(time.Now())
	g.background.Add(1)
	go func() {
		defer g.background.Done()
		for {
			select {
			case <-g.ctx.Done():
				return
			case <-g.testWake:
			}
			g.CheckOutbounds(g.ctx, true)
		}
	}()
	g.requestTest()
}

func (g *URLTestGroup) Touch() {
	g.access.Lock()
	defer g.access.Unlock()
	if !g.started || g.closed {
		return
	}
	if g.ticker != nil {
		g.lastActive.Store(time.Now())
		return
	}
	ticker := time.NewTicker(g.interval)
	g.ticker = ticker
	g.pauseCallback = pause.RegisterTicker(g.pause, ticker, g.interval, nil)
	g.background.Add(1)
	go func() { defer g.background.Done(); g.loopCheck(ticker, g.close) }()
}

func (g *URLTestGroup) Close() error {
	g.cancel()
	g.access.Lock()
	if !g.closed {
		g.closed = true
		if g.ticker != nil {
			g.ticker.Stop()
			g.ticker = nil
			g.pause.UnregisterCallback(g.pauseCallback)
			g.pauseCallback = nil
		}
		close(g.close)
	}
	g.access.Unlock()
	g.background.Wait()
	g.testCalls.Wait()
	return nil
}

func (g *URLTestGroup) runBackground(run func()) {
	g.access.Lock()
	defer g.access.Unlock()
	if g.closed {
		return
	}
	g.background.Add(1)
	go func() { defer g.background.Done(); run() }()
}

func (g *URLTestGroup) requestTest() {
	select {
	case g.testWake <- struct{}{}:
	default:
	}
}

func (g *URLTestGroup) refreshMembers() {
	if g.members == nil {
		return
	}
	members := g.members.Resolve()
	g.stateMu.Lock()
	if g.membershipReady && g.revision >= members.Revision {
		g.stateMu.Unlock()
		return
	}
	g.membershipReady, g.revision = true, members.Revision
	g.outbounds = members.Outbounds
	changed := false
	for _, selected := range []*adapter.Outbound{&g.selectedOutboundTCP, &g.selectedOutboundUDP} {
		if *selected == nil {
			continue
		}
		if !slices.Contains(members.Tags, (*selected).Tag()) {
			*selected = nil
			changed = true
		}
	}
	g.stateMu.Unlock()
	if changed {
		g.interruptGroup.Interrupt(g.interruptExternalConnections)
		g.history.NotifyUpdated()
	}
}

func (g *URLTestGroup) Selected(network string) adapter.Outbound {
	g.refreshMembers()
	g.stateMu.Lock()
	selected := g.selectedOutboundTCP
	if network == N.NetworkUDP {
		selected = g.selectedOutboundUDP
	}
	g.stateMu.Unlock()
	if selected != nil && common.Contains(selected.Network(), network) {
		return selected
	}
	selected, _ = g.Select(network)
	return selected
}

func (g *URLTestGroup) Select(network string) (adapter.Outbound, bool) {
	g.refreshMembers()
	g.stateMu.Lock()
	outbounds := g.outbounds
	previous := g.selectedOutboundTCP
	if network == N.NetworkUDP {
		previous = g.selectedOutboundUDP
	}
	g.stateMu.Unlock()
	var minDelay uint16
	var best adapter.Outbound
	if previous != nil && common.Contains(previous.Network(), network) {
		if history := g.history.LoadOutboundNetworkHistory(previous, g.link, network); history != nil && history.Delay > 0 {
			best, minDelay = previous, history.Delay
		}
	}
	for _, o := range outbounds {
		if !common.Contains(o.Network(), network) {
			continue
		}
		history := g.history.LoadOutboundNetworkHistory(o, g.link, network)
		if history != nil && history.Delay > 0 && (best == nil || int(minDelay) > int(history.Delay)+int(g.tolerance)) {
			best, minDelay = o, history.Delay
		}
	}
	if best != nil {
		return best, true
	}
	for _, o := range outbounds {
		if common.Contains(o.Network(), network) {
			return o, false
		}
	}
	return nil, false
}

func (g *URLTestGroup) loopCheck(ticker *time.Ticker, closeChan <-chan struct{}) {
	if time.Since(g.lastActive.Load()) > g.interval {
		g.lastActive.Store(time.Now())
		g.CheckOutbounds(g.ctx, false)
	}
	for {
		select {
		case <-closeChan:
			return
		case <-ticker.C:
		}
		if time.Since(g.lastActive.Load()) > g.idleTimeout {
			g.access.Lock()
			if g.ticker == ticker {
				g.ticker.Stop()
				g.ticker = nil
				g.pause.UnregisterCallback(g.pauseCallback)
				g.pauseCallback = nil
			}
			g.access.Unlock()
			return
		}
		g.CheckOutbounds(g.ctx, false)
	}
}

func (g *URLTestGroup) CheckOutbounds(ctx context.Context, force bool) {
	_, _ = g.urlTest(ctx, force)
}

func (g *URLTestGroup) URLTest(ctx context.Context) (map[string]uint16, error) {
	return g.urlTest(ctx, true)
}

func (g *URLTestGroup) urlTest(ctx context.Context, force bool) (map[string]uint16, error) {
	g.access.Lock()
	if g.closed {
		g.access.Unlock()
		return nil, net.ErrClosed
	}
	g.testCalls.Add(1)
	g.access.Unlock()
	defer g.testCalls.Done()
	if g.checking.Swap(true) {
		return make(map[string]uint16), nil
	}
	defer g.checking.Store(false)
	g.refreshMembers()
	g.stateMu.Lock()
	outbounds, revision := slices.Clone(g.outbounds), g.revision
	g.stateMu.Unlock()
	// Stage measurements separately: a deleted or replaced node must not write
	// stale latency back into the shared history after a subscription refresh.
	history := urltest.NewHistoryStorage()
	for _, o := range outbounds {
		for _, network := range []string{N.NetworkTCP, N.NetworkUDP} {
			if h := g.history.LoadOutboundNetworkHistory(o, g.link, network); h != nil {
				history.StoreURLTestHistory(h.Outbound, h)
			}
		}
	}
	testCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(g.ctx, cancel)
	defer stop()
	defer cancel()
	if g.members != nil {
		current, changed := g.members.Updates().State()
		if current != revision {
			g.requestTest()
			return map[string]uint16{}, nil
		}
		go func() {
			select {
			case <-testCtx.Done():
			case <-changed:
				cancel()
			}
		}()
	}
	result, measured := testOutbounds(testCtx, g.outbound, history, g.logger, outbounds, g.link, g.interval, force)
	if testCtx.Err() != nil {
		if ctx.Err() == nil && g.ctx.Err() == nil {
			g.requestTest()
		}
		return result, testCtx.Err()
	}
	commit := func() {
		for tag, h := range measured {
			if h != nil {
				g.history.StoreURLTestHistory(tag, h)
			} else {
				g.history.DeleteURLTestHistory(tag, g.link)
			}
		}
	}
	if g.members != nil {
		if !g.members.Updates().Commit(revision, commit) {
			g.requestTest()
			return map[string]uint16{}, nil
		}
	} else {
		commit()
	}
	g.performUpdateCheck()
	return result, nil
}

func URLTestOutbounds(ctx context.Context, outboundManager adapter.OutboundManager, history *urltest.HistoryStorage, logger log.Logger, outbounds []adapter.Outbound, link string, interval time.Duration, force bool) map[string]uint16 {
	updates := service.PtrFromContext[adapter.GroupUpdates](ctx)
	var revision uint64
	if updates != nil {
		revision, _ = updates.State()
	}
	result, measured := testOutbounds(ctx, outboundManager, history, logger, outbounds, link, interval, force)
	if ctx.Err() != nil {
		return result
	}
	commit := func() {
		for tag, h := range measured {
			history.StoreURLTestHistory(tag, h)
		}
	}
	if updates != nil {
		if !updates.Commit(revision, commit) {
			return map[string]uint16{}
		}
	} else {
		commit()
	}
	return result
}

func testOutbounds(ctx context.Context, _ adapter.OutboundManager, history *urltest.HistoryStorage, logger log.Logger, outbounds []adapter.Outbound, link string, interval time.Duration, force bool) (map[string]uint16, map[string]*adapter.URLTestHistory) {
	link = urltest.NormalizeURL(link)
	type job struct {
		outbound adapter.Outbound
		aliases  []string
	}
	jobs := make(map[string]*job)
	for _, outbound := range outbounds {
		for _, network := range []string{N.NetworkTCP, N.NetworkUDP} {
			resolved, err := urltest.ResolveNetwork(outbound, network)
			if err != nil || !slices.Contains(resolved.Network(), N.NetworkTCP) {
				continue
			}
			work := jobs[resolved.Tag()]
			if work == nil {
				work = &job{outbound: resolved}
				jobs[resolved.Tag()] = work
			}
			// API delays continue to describe the TCP-selected leaf. The UDP
			// leaf's HTTP latency is stored only under that leaf for ranking.
			if network == N.NetworkTCP {
				work.aliases = append(work.aliases, outbound.Tag())
			}
		}
	}
	result := make(map[string]uint16)
	measured := make(map[string]*adapter.URLTestHistory)
	var mu sync.Mutex
	b, _ := batch.New(ctx, batch.WithConcurrencyNum[any](10))
	for key, work := range jobs {
		if h := history.LoadOutboundHistory(work.outbound, link); !force && h != nil && time.Since(h.Time) < interval {
			mu.Lock()
			for _, tag := range work.aliases {
				result[tag] = h.Delay
			}
			mu.Unlock()
			continue
		}
		b.Go(key, func() (any, error) {
			probeCtx, cancel := context.WithTimeout(ctx, C.TCPTimeout)
			defer cancel()
			completed := make(chan urltest.Result, 1)
			started := time.Now()
			go func() { completed <- urltest.URLTestWithResult(probeCtx, link, work.outbound) }()
			var observation urltest.Result
			select {
			case observation = <-completed:
			case <-probeCtx.Done():
				observation = urltest.Result{URL: link, Started: started, Via: key, Err: probeCtx.Err()}
				if _, group := adapter.OutboundGroupFor(work.outbound); !group {
					observation.Outbound = key
				}
			}
			if ctx.Err() != nil {
				return nil, nil
			}
			mu.Lock()
			defer mu.Unlock()
			if observation.Outbound != "" {
				delay := observation.Delay
				if observation.Err != nil {
					delay = 0
				}
				value := &adapter.URLTestHistory{URL: link, Time: observation.Started, Delay: delay, Outbound: observation.Outbound}
				if old := measured[observation.Outbound]; old == nil || !value.Time.Before(old.Time) {
					measured[observation.Outbound] = value
				}
			}
			if observation.Err == nil {
				for _, tag := range work.aliases {
					result[tag] = observation.Delay
				}
			} else {
				logger.Debug("outbound ", key, " unavailable: ", observation.Err)
			}
			return nil, nil
		})
	}
	b.Wait()
	return result, measured
}

func (g *URLTestGroup) performUpdateCheck() {
	g.updateAccess.Lock()
	defer g.updateAccess.Unlock()
	g.refreshMembers()
	g.stateMu.Lock()
	revision := g.revision
	g.stateMu.Unlock()
	tcp, _ := g.Select(N.NetworkTCP)
	udp, _ := g.Select(N.NetworkUDP)
	g.stateMu.Lock()
	if g.revision != revision {
		g.stateMu.Unlock()
		return
	}
	oldTCP, oldUDP := g.selectedOutboundTCP, g.selectedOutboundUDP
	g.selectedOutboundTCP, g.selectedOutboundUDP = tcp, udp
	g.stateMu.Unlock()
	changed := func(a, b adapter.Outbound) bool { return a != nil && (b == nil || a.Tag() != b.Tag()) }
	if changed(oldTCP, tcp) || changed(oldUDP, udp) {
		g.interruptGroup.Interrupt(g.interruptExternalConnections)
	}
	g.history.NotifyUpdated()
}
