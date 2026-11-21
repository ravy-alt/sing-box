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
		if h := s.history.LoadURLTestHistory(tag); h != nil && h.Delay > 0 {
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
	networks := make(map[string][]string, len(members.Tags))
	outbounds := make(map[string]adapter.Outbound, len(members.Tags))
	for _, tag := range members.Tags {
		outbound, _ := g.outbounds.Outbound(tag)
		outbounds[tag] = outbound
		networks[tag] = outboundNetworks(outbound, make(map[string]bool))
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 10)
loop:
	for _, tag := range members.Tags {
		outbound := outbounds[tag]
		if !slices.Contains(networks[tag], N.NetworkTCP) {
			continue
		}
		select {
		case semaphore <- struct{}{}:
		case <-ctx.Done():
			break loop
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-semaphore }()
			delay, err := urltest.URLTest(ctx, s.options.TestURL, outbound)
			if err == nil {
				mu.Lock()
				result[tag] = delay
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if ctx.Err() != nil {
		return result, ctx.Err()
	}
	s.mu.Lock()
	if s.current == g && !s.closed {
		s.members.Updates().Commit(members.Revision, func() {
			if s.history != nil {
				for _, tag := range members.Tags {
					if delay, ok := result[tag]; ok {
						s.history.StoreURLTestHistory(tag, &adapter.URLTestHistory{Time: time.Now(), Delay: delay})
					} else {
						s.history.DeleteURLTestHistory(tag)
					}
				}
			}
			s.selectMeasured(members.Tags, networks, result)
		})
	}
	s.mu.Unlock()
	s.notify()
	return result, nil
}
