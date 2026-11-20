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

func (s *Subscription) PerformUpdateCheck() { /* Selection is updated with a complete generation's test results. */
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
			if s.options.Mode == C.TypeURLTest {
				for network, selected := range map[string]*string{N.NetworkTCP: &s.selectedTCP, N.NetworkUDP: &s.selectedUDP} {
					best, found := result[*selected]
					found = found && slices.Contains(networks[*selected], network)
					for _, tag := range members.Tags {
						if !slices.Contains(networks[tag], network) {
							continue
						}
						if delay, ok := result[tag]; ok && (!found || int(delay)+int(s.options.Tolerance) < int(best)) {
							*selected, best, found = tag, delay, true
						}
					}
				}
			}
		})
	}
	s.mu.Unlock()
	s.notify()
	return result, nil
}
