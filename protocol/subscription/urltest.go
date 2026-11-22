package subscription

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	C "github.com/sagernet/sing-box/constant"
	E "github.com/sagernet/sing/common/exceptions"
	N "github.com/sagernet/sing/common/network"
)

func (s *Subscription) TestURL() string { return s.options.TestURL }

func (s *Subscription) PerformUpdateCheck() {
	if s.options.Mode != C.TypeURLTest || s.history == nil {
		return
	}
	members := s.reconcileMembers()
	s.mu.Lock()
	g := s.current
	if s.closed || g == nil || !g.acquire() {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	defer g.release()
	result := make(map[string]uint16)
	networks := make(map[string][]string)
	for _, tag := range members.Tags {
		member, _ := g.outbounds.Outbound(tag)
		networks[tag] = outboundNetworks(member, make(map[string]bool))
		if h := s.history.LoadOutboundHistory(member, s.options.TestURL); h != nil && h.Delay > 0 {
			result[tag] = h.Delay
		}
	}
	s.mu.Lock()
	if !s.closed && s.current == g {
		s.members.Updates().Commit(members.Revision, func() {
			s.selectMeasured(members.Tags, networks, result)
		})
	}
	s.mu.Unlock()
	s.notify()
}

// Called with s.mu held and the membership revision validated.
func (s *Subscription) selectMeasured(tags []string, networks map[string][]string, result map[string]uint16) {
	if s.options.Mode != C.TypeURLTest {
		return
	}
	for network, selected := range map[string]*string{N.NetworkTCP: &s.selectedTCP, N.NetworkUDP: &s.selectedUDP} {
		best, found := result[*selected]
		found = found && slices.Contains(networks[*selected], network)
		for _, tag := range tags {
			if !slices.Contains(networks[tag], network) {
				continue
			}
			if delay, ok := result[tag]; ok && (!found || int(delay)+int(s.options.Tolerance) < int(best)) {
				*selected, best, found = tag, delay, true
			}
		}
	}
}

func (s *Subscription) URLTest(ctx context.Context) (map[string]uint16, error) {
	members := s.reconcileMembers()
	s.mu.Lock()
	g := s.current
	if s.closed || s.updatesCtx.Err() != nil || g == nil || !g.acquire() {
		s.mu.Unlock()
		return nil, E.New("subscription is not ready")
	}
	s.testCalls.Add(1)
	s.mu.Unlock()
	defer s.testCalls.Done()
	defer g.release()
	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.options.DownloadTimeout))
	stop := context.AfterFunc(s.updatesCtx, cancel)
	stopGeneration := context.AfterFunc(g.scope.Context(), cancel)
	defer stopGeneration()
	defer stop()
	defer cancel()
	revision, changed := s.members.Updates().State()
	if revision != members.Revision {
		return nil, context.Canceled
	}
	go func() {
		select {
		case <-ctx.Done():
		case <-changed:
			cancel()
		}
	}()
	result := make(map[string]uint16)
	measurements := make(map[string]urltest.Result)
	networks := make(map[string][]string, len(members.Tags))
	type job struct {
		outbound adapter.Outbound
		aliases  []string
	}
	jobs := make(map[string]*job)
	for _, tag := range members.Tags {
		outbound, _ := g.outbounds.Outbound(tag)
		networks[tag] = outboundNetworks(outbound, make(map[string]bool))
		if !slices.Contains(networks[tag], N.NetworkTCP) {
			continue
		}
		leaf, err := urltest.Resolve(outbound)
		if err != nil {
			continue
		}
		if work := jobs[leaf.Tag()]; work != nil {
			work.aliases = append(work.aliases, tag)
		} else {
			jobs[leaf.Tag()] = &job{leaf, []string{tag}}
		}
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 10)
loop:
	for key, work := range jobs {
		select {
		case semaphore <- struct{}{}:
		case <-ctx.Done():
			break loop
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-semaphore }()
			probeCtx, cancelProbe := context.WithTimeout(ctx, C.TCPTimeout)
			defer cancelProbe()
			observation := urltest.URLTestWithResult(probeCtx, s.options.TestURL, work.outbound)
			mu.Lock()
			measurements[key] = observation
			if observation.Err == nil {
				for _, tag := range work.aliases {
					result[tag] = observation.Delay
				}
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	if ctx.Err() != nil {
		return result, ctx.Err()
	}
	s.mu.Lock()
	if s.current == g && !s.closed {
		s.members.Updates().Commit(members.Revision, func() {
			for _, observation := range measurements {
				observation.Store(s.history)
			}
		})
	}
	s.mu.Unlock()
	// A nested selector may have switched while probes were running. Derive
	// selection from the current leaves, not the aliases captured at test start.
	s.PerformUpdateCheck()
	s.notify()
	return result, nil
}
