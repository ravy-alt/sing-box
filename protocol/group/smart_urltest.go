package group

import (
	"context"
	"errors"
	"io"
	"maps"
	"net"
	"net/url"
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
	"github.com/sagernet/sing/common/json/badoption"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

func RegisterSmartURLTest(registry *outbound.Registry) {
	outbound.Register[option.SmartURLTestOutboundOptions](registry, C.TypeSmartURLTest, NewSmartURLTest)
}

var _ adapter.SmartURLTestGroup = (*SmartURLTest)(nil)

var errSmartMembersChanged = errors.New("smart-urltest: membership changed during test")

type smartTestRound struct {
	finalized       bool
	published       int
	snapshot        *groupmembers.Snapshot
	partial         map[string]uint16
	measurements    map[string]urltest.Result
	udpMeasurements map[string]urltest.UDPResult
	done            chan struct{}
	result          map[string]uint16
	err             error
}

type SmartURLTest struct {
	outbound.Adapter
	ctx                   context.Context
	cancel                context.CancelFunc
	probeCtx              context.Context
	stopProbes            context.CancelFunc
	stopWatch             func()
	members               *groupmembers.Members
	logger                log.ContextLogger
	history               *urltest.HistoryStorage
	options               option.SmartURLTestOutboundOptions
	tolerance             uint16
	cache                 smartDestinationCache
	dialSlots, probeSlots chan struct{}
	interruptGroup        *interrupt.Group
	wake                  chan struct{}
	mu                    sync.Mutex
	snapshot              *groupmembers.Snapshot
	best                  map[string]string
	round                 *smartTestRound
	lastTest              time.Time
	health                smartHealth
}

func NewSmartURLTest(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.SmartURLTestOutboundOptions) (adapter.Outbound, error) {
	if err := groupmembers.ValidateBase(options.Outbounds, len(options.Import) > 0, options.EmptyFallback); err != nil {
		return nil, err
	}
	if options.URL == "" {
		options.URL = "https://www.gstatic.com/generate_204"
	}
	udpOptions := option.UDPTestOptions{}
	if options.UDPTest != nil {
		udpOptions = *options.UDPTest
	}
	udpOptions, udpErr := urltest.NormalizeUDPTest(udpOptions)
	if udpErr != nil {
		return nil, udpErr
	}
	options.UDPTest = &udpOptions
	link, err := url.Parse(options.URL)
	if err != nil || link.Hostname() == "" || (link.Scheme != "http" && link.Scheme != "https") {
		return nil, errors.New("smart-urltest: url must be an absolute HTTP(S) URL")
	}
	if options.Interval == 0 {
		options.Interval = badoption.Duration(5 * time.Minute)
	}
	if options.CacheTTL == 0 {
		options.CacheTTL = badoption.Duration(5 * time.Minute)
	}
	if options.DialTimeout == 0 {
		options.DialTimeout = badoption.Duration(5 * time.Second)
	}
	if options.FailureWindow == 0 {
		options.FailureWindow = badoption.Duration(time.Minute)
	}
	if options.UDPResponseTimeout == 0 {
		options.UDPResponseTimeout = badoption.Duration(5 * time.Second)
	}
	if options.UDPFailureWindow == 0 {
		options.UDPFailureWindow = badoption.Duration(time.Minute)
	}
	if options.MinTestInterval == 0 {
		options.MinTestInterval = badoption.Duration(30 * time.Second)
	}
	if options.CacheCapacity == 0 {
		options.CacheCapacity = 10000
	}
	if options.FailureThreshold == 0 {
		options.FailureThreshold = 5
	}
	if options.UDPMinSessions == 0 {
		options.UDPMinSessions = 10
	}
	if options.UDPFailureRatio == 0 {
		options.UDPFailureRatio = 0.8
	}
	if options.TestConcurrency == 0 {
		options.TestConcurrency = 10
	}
	if options.MaxConcurrentDials == 0 {
		options.MaxConcurrentDials = 64
	}
	for _, duration := range []time.Duration{time.Duration(options.Interval), time.Duration(options.CacheTTL), time.Duration(options.DialTimeout), time.Duration(options.FailureWindow), time.Duration(options.UDPResponseTimeout), time.Duration(options.UDPFailureWindow), time.Duration(options.MinTestInterval)} {
		if duration < time.Millisecond {
			return nil, errors.New("smart-urltest: durations must be at least 1ms")
		}
	}
	if options.CacheCapacity < 1 || options.CacheCapacity > 1000000 || options.FailureThreshold < 1 || options.FailureThreshold > 100000 || options.UDPMinSessions < 1 || options.UDPMinSessions > 100000 || !(options.UDPFailureRatio > 0 && options.UDPFailureRatio <= 1) || options.TestConcurrency < 1 || options.TestConcurrency > 256 || options.MaxConcurrentDials < 1 || options.MaxConcurrentDials > 4096 {
		return nil, errors.New("smart-urltest: invalid capacity, concurrency or failure threshold")
	}
	members, err := groupmembers.New(ctx, tag, func() []string { return options.Outbounds }, options.Import, options.EmptyFallback)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	probeCtx, stopProbes := context.WithCancel(ctx)
	s := &SmartURLTest{
		Adapter: outbound.NewAdapter(C.TypeSmartURLTest, tag, []string{N.NetworkTCP, N.NetworkUDP}, groupmembers.Dependencies(options.Outbounds, options.Import, options.EmptyFallback)),
		ctx:     ctx, cancel: cancel, probeCtx: probeCtx, stopProbes: stopProbes, members: members, logger: logger, options: options,
		history: service.PtrFromContext[urltest.HistoryStorage](ctx), tolerance: 50,
		cache:     smartDestinationCache{capacity: options.CacheCapacity, ttl: time.Duration(options.CacheTTL)},
		dialSlots: make(chan struct{}, options.MaxConcurrentDials), probeSlots: make(chan struct{}, options.TestConcurrency),
		interruptGroup: interrupt.NewGroup(), wake: make(chan struct{}, 1), best: make(map[string]string),
	}
	if options.Tolerance != nil {
		s.tolerance = *options.Tolerance
	}
	s.health.init(s)
	return s, nil
}

func (s *SmartURLTest) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	switch stage {
	case adapter.StartStateStart:
		manager := service.FromContext[adapter.OutboundManager](s.ctx)
		for _, tag := range s.options.Outbounds {
			if _, found := manager.Outbound(tag); !found {
				return errors.New("smart-urltest: outbound not found: " + tag)
			}
		}
		if err := s.members.Validate(true); err != nil {
			return err
		}
		scope.Add(s.Close)
		if registry := service.PtrFromContext[adapter.OutboundCacheRegistry](s.ctx); registry != nil {
			unregister := registry.Register(s)
			scope.Add(func() error { unregister(); return nil })
		}
	case adapter.StartStateStarted:
		s.refresh()
		s.stopWatch = s.members.Watch(scope, func() { s.refresh(); s.requestTest() })
		go s.schedule()
		go s.health.run(s.ctx)
		s.requestTest()
	}
	return nil
}

func (s *SmartURLTest) Close() error {
	s.cancel()
	s.stopProbes()
	s.interruptGroup.Interrupt(true)
	s.cache.clear()
	return nil
}

// Retirement stops automatic work without terminating leased connections.
func (s *SmartURLTest) PauseUpdates() {
	s.stopProbes()
	if s.stopWatch != nil {
		s.stopWatch()
	}
}

func (s *SmartURLTest) ValidateImports(pending bool) error { return s.members.Validate(pending) }
func (s *SmartURLTest) OptionalImportDependency(tag string) bool {
	return s.members.OptionalImportDependency(tag)
}
func (s *SmartURLTest) ImportsSource(tag string) bool         { return s.members.ImportsSource(tag) }
func (s *SmartURLTest) All() []string                         { return slices.Clone(s.refresh().Tags) }
func (s *SmartURLTest) DynamicSelection()                     {}
func (s *SmartURLTest) TestURL() string                       { return s.options.URL }
func (s *SmartURLTest) UDPTestOptions() option.UDPTestOptions { return *s.options.UDPTest }
func (s *SmartURLTest) ClearCache()                           { s.cache.clear() }
func (s *SmartURLTest) AttachConnection(conn io.Closer) func() {
	return s.interruptGroup.Add(conn, true)
}
func (s *SmartURLTest) PerformUpdateCheck() {
	s.ranked(N.NetworkTCP)
	s.ranked(N.NetworkUDP)
}
func (s *SmartURLTest) InterfaceUpdated(ctx context.Context) {
	if ctx.Err() == nil {
		s.ClearCache()
		s.requestTest()
	}
}
func (s *SmartURLTest) References() []string {
	var result []string
	for _, network := range []string{N.NetworkTCP, N.NetworkUDP} {
		if selected := s.Selected(network); selected != nil && !slices.Contains(result, selected.Tag()) {
			result = append(result, selected.Tag())
		}
	}
	return result
}

func (s *SmartURLTest) refresh() *groupmembers.Snapshot {
	for {
		snapshot := s.members.Resolve()
		if s.members.Updates().Commit(snapshot.Revision, func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.snapshot != snapshot {
				s.snapshot = snapshot
				clear(s.best)
				s.cache.clear()
			}
		}) {
			return snapshot
		}
	}
}

// Keep calls into other groups outside mu and the provider publication lock.
func (s *SmartURLTest) ranked(network string) (*groupmembers.Snapshot, []adapter.Outbound, string) {
	snapshot := s.refresh()
	// Resolve current leaves on every read. A child selector can switch without
	// changing membership; cached delays under child group tags would be stale.
	health := make(map[string]smartProbeHealth)
	var candidates []adapter.Outbound
	var tags []string
	for _, outbound := range snapshot.Outbounds {
		if slices.Contains(outbound.Network(), network) {
			observation := s.probeHealth(outbound, network)
			if observation.tcpFailed {
				continue
			}
			candidates = append(candidates, outbound)
			tags = append(tags, outbound.Tag())
			health[outbound.Tag()] = observation
		}
	}
	s.mu.Lock()
	best := smartChooseHealthy(tags, health, s.best[network], s.tolerance)
	if s.snapshot == snapshot {
		s.best[network] = best
	}
	s.mu.Unlock()
	slices.SortStableFunc(candidates, func(a, b adapter.Outbound) int {
		return compareSmartHealth(health[a.Tag()], health[b.Tag()])
	})
	if best == "" && len(candidates) > 0 {
		best = candidates[0].Tag()
	}
	return snapshot, candidates, best
}

func (s *SmartURLTest) Selected(network string) adapter.Outbound {
	_, candidates, best := s.ranked(network)
	for _, candidate := range candidates {
		if candidate.Tag() == best {
			return candidate
		}
	}
	if len(candidates) > 0 {
		return candidates[0]
	}
	return nil
}

func (s *SmartURLTest) requestTest() {
	if s.probeCtx.Err() != nil {
		return
	}
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *SmartURLTest) schedule() {
	ticker := time.NewTicker(time.Duration(s.options.Interval))
	defer ticker.Stop()
	for {
		select {
		case <-s.probeCtx.Done():
			return
		case <-ticker.C:
		case <-s.wake:
		}
		for {
			round, wait, err := s.startTest(true)
			if err != nil {
				return
			}
			if wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <-s.probeCtx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
				continue
			}
			select {
			case <-s.probeCtx.Done():
				return
			case <-round.done:
			}
			if round.err != nil && s.probeCtx.Err() == nil {
				s.logger.DebugContext(s.probeCtx, "automatic URL test: ", round.err)
			}
			break
		}
	}
}

// Checking the automatic cooldown and creating a round share the same lock.
// A simultaneous API request cannot slip between those operations.
func (s *SmartURLTest) startTest(automatic bool) (*smartTestRound, time.Duration, error) {
	snapshot := s.refresh()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.probeCtx.Err() != nil {
		return nil, 0, net.ErrClosed
	}
	if s.round != nil {
		return s.round, 0, nil
	}
	if automatic {
		if wait := time.Until(s.lastTest.Add(time.Duration(s.options.MinTestInterval))); wait > 0 {
			return nil, wait, nil
		}
		select {
		case <-s.wake:
		default:
		}
	}
	round := &smartTestRound{done: make(chan struct{}), snapshot: snapshot, partial: make(map[string]uint16), measurements: make(map[string]urltest.Result), udpMeasurements: make(map[string]urltest.UDPResult)}
	s.round = round
	s.lastTest = time.Now()
	go s.runTest(snapshot, round)
	return round, 0, nil
}

func (s *SmartURLTest) URLTest(ctx context.Context) (map[string]uint16, error) {
	// A newly published subscription may still have its pre-publication startup
	// round running. Retry that obsolete round once against current membership.
	for attempt := 0; attempt < 2; attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		round, _, err := s.startTest(false)
		if err != nil {
			return nil, err
		}
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				s.mu.Lock()
				partial := maps.Clone(round.partial)
				s.mu.Unlock()
				if len(partial) > 0 && s.publishTest(round, partial, false) {
					return partial, nil
				}
			}
			return nil, ctx.Err()
		case <-round.done:
			if errors.Is(round.err, errSmartMembersChanged) && attempt == 0 {
				continue
			}
			return maps.Clone(round.result), round.err
		}
	}
	return nil, errSmartMembersChanged
}

// Publish only measured leaves. Ranking is derived from current selections and
// this group's URL when read, rather than storing a second set of group delays.
func (s *SmartURLTest) publishTest(round *smartTestRound, results map[string]uint16, final bool) bool {
	snapshot := round.snapshot
	return s.members.Updates().Commit(snapshot.Revision, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.probeCtx.Err() != nil || s.round != round || round.finalized || (!final && len(results) < round.published) {
			return
		}
		round.finalized = final
		round.published = len(results)
		for _, observation := range round.measurements {
			observation.Store(s.history)
		}
		for _, observation := range round.udpMeasurements {
			observation.Store(s.history)
		}

	})
}

func (s *SmartURLTest) runTest(snapshot *groupmembers.Snapshot, round *smartTestRound) {
	type job struct {
		outbound adapter.Outbound
		aliases  []string // TCP result names, separate from the probed leaf.
	}
	jobs := make([]job, 0, len(snapshot.Outbounds))
	indices := make(map[string]int)
	add := func(leaf adapter.Outbound) int {
		if index, found := indices[leaf.Tag()]; found {
			return index
		}
		index := len(jobs)
		indices[leaf.Tag()] = index
		jobs = append(jobs, job{outbound: leaf})
		return index
	}
	// Panel/group delays still describe the TCP-selected leaf at this group's URL.
	for _, outbound := range snapshot.Outbounds {
		if leaf, err := urltest.Resolve(outbound); err == nil {
			index := add(leaf)
			jobs[index].aliases = append(jobs[index].aliases, outbound.Tag())
		}
	}
	// Probe every reachable candidate, including unselected nested members and
	// UDP-only leaves. Do not confuse an HTTP test failure with UDP failure.
	manager := service.FromContext[adapter.OutboundManager](s.ctx)
	seen := make(map[string]bool)
	var walk func(adapter.Outbound)
	walk = func(outbound adapter.Outbound) {
		if outbound == nil || seen[outbound.Tag()] {
			return
		}
		seen[outbound.Tag()] = true
		if group, ok := adapter.OutboundGroupFor(outbound); ok {
			// Selected objects also cover private/test group implementations
			// whose members are not directly addressable by this manager.
			walk(group.Selected(N.NetworkTCP))
			walk(group.Selected(N.NetworkUDP))
			if manager != nil {
				for _, tag := range group.All() {
					if member, found := manager.Outbound(tag); found {
						walk(member)
					}
				}
			}
			return
		}
		if slices.Contains(outbound.Network(), N.NetworkUDP) {
			add(outbound)
		}
	}
	for _, outbound := range snapshot.Outbounds {
		walk(outbound)
	}
	type measurement struct {
		index  int
		result urltest.Result
		udp    urltest.UDPResult
	}
	completed := make(chan measurement, len(jobs))
	var next atomic.Int64
	var workers sync.WaitGroup
	for range min(len(jobs), s.options.TestConcurrency) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				index := int(next.Add(1)) - 1
				if index >= len(jobs) || s.probeCtx.Err() != nil {
					return
				}
				var result urltest.Result
				if slices.Contains(jobs[index].outbound.Network(), N.NetworkTCP) {
					result = s.probe(jobs[index].outbound)
				}
				var udp urltest.UDPResult
				if slices.Contains(jobs[index].outbound.Network(), N.NetworkUDP) {
					udp = urltest.UDPTestWithResult(s.probeCtx, *s.options.UDPTest, jobs[index].outbound)
				}
				completed <- measurement{index: index, result: result, udp: udp}
			}
		}()
	}
	go func() { workers.Wait(); close(completed) }()
	results := make(map[string]uint16)
	for item := range completed {
		s.mu.Lock()
		leafTag := jobs[item.index].outbound.Tag()
		round.measurements[leafTag] = item.result
		round.udpMeasurements[leafTag] = item.udp
		for _, tag := range jobs[item.index].aliases {
			if item.result.Outbound != "" && item.result.Err == nil {
				results[tag] = item.result.Delay
				round.partial[tag] = item.result.Delay
			}
		}
		s.mu.Unlock()
	}
	committed := s.publishTest(round, results, true)
	if !committed {
		round.err = errSmartMembersChanged
		s.requestTest()
	} else if s.probeCtx.Err() != nil {
		round.err = net.ErrClosed
	} else if len(results) == 0 {
		round.err = errors.New("smart-urltest: all TCP probes failed")
	} else {
		round.result = results
	}
	s.mu.Lock()
	s.round = nil
	close(round.done)
	s.mu.Unlock()
}

func (s *SmartURLTest) probe(outbound adapter.Outbound) urltest.Result {
	ctx, cancel := context.WithTimeout(adapter.ContextWithURLTest(s.probeCtx), C.TCPTimeout)
	defer cancel()
	failure := urltest.Result{URL: urltest.NormalizeURL(s.options.URL), Started: time.Now(), Via: outbound.Tag()}
	if _, group := adapter.OutboundGroupFor(outbound); !group {
		failure.Outbound = outbound.Tag()
	}
	// Keep capacity until the actual operation exits, even if it ignores cancellation.
	select {
	case s.probeSlots <- struct{}{}:
	default:
		// Local capacity exhaustion is not an observation about this node.
		failure.Outbound = ""
		failure.Err = errSmartDialCapacity
		return failure
	}
	done := make(chan urltest.Result, 1)
	go func() {
		result := urltest.URLTestWithResult(ctx, s.options.URL, outbound)
		<-s.probeSlots
		done <- result
	}()
	select {
	case result := <-done:
		return result
	case <-ctx.Done():
		failure.Err = ctx.Err()
		return failure
	}
}
