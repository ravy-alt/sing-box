package clashapi

import (
	"context"
	"errors"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

// Dashboard probes are independent of a group's automatic health check. In
// particular, an IPv6 probe must not join a check of the configured IPv4 URL.
// Like the Clash API, test each immediate member through its actual dialer;
// a nested group is tested through its selected path, not recursively expanded.
func (s *Server) testGroup(ctx context.Context, outboundGroup adapter.OutboundGroup, link string) (map[string]uint16, error) {
	ctx = s.probeContext(ctx)
	var updates *adapter.GroupUpdates
	var revision uint64
	if s.ctx != nil {
		updates = service.PtrFromContext[adapter.GroupUpdates](s.ctx)
		if updates != nil {
			revision, _ = updates.State()
		}
	}
	type measurement struct {
		tag, realTag string
		delay        uint16
		err          error
	}
	members := outboundGroup.All()
	completed := make(chan measurement, len(members))
	pending := 0
	seen := make(map[string]bool)
	history := make(map[string]*adapter.URLTestHistory)
	for _, tag := range members {
		if seen[tag] {
			continue
		}
		seen[tag] = true
		member, loaded := s.outbound.Outbound(tag)
		if !loaded {
			continue
		}
		// Freeze native groups' selected dialer along with its history key.
		// Merely reading RealTag and then dialing the group can measure a different
		// member if selection changes between those two operations. Provided
		// handles remain intact so their generation leases are still respected.
		for {
			nested, isGroup := member.(adapter.OutboundGroup)
			if !isGroup {
				break
			}
			selected := nested.Selected(N.NetworkTCP)
			if selected == nil {
				break
			}
			member = selected
		}
		realTag := member.Tag()
		history[realTag] = nil
		pending++
		go func() {
			delay, err := urltest.URLTest(ctx, link, member)
			completed <- measurement{tag, realTag, delay, err}
		}()
	}
	result := make(map[string]uint16)
	collect := func(m measurement) {
		if m.err == nil {
			result[m.tag] = m.delay
			history[m.realTag] = &adapter.URLTestHistory{Time: time.Now(), Delay: m.delay}
		} else if history[m.realTag] == nil {
			history[m.realTag] = nil
		}
	}
wait:
	for pending > 0 {
		select {
		case m := <-completed:
			pending--
			collect(m)
		case <-ctx.Done():
			// Keep successful results even when another member times out. Also
			// drain measurements already delivered at the deadline.
			for {
				select {
				case m := <-completed:
					collect(m)
				default:
					break wait
				}
			}
		}
	}
	commit := func() {
		for tag, value := range history {
			if value == nil {
				s.urlTestHistory.DeleteURLTestHistory(tag)
			} else {
				s.urlTestHistory.StoreURLTestHistory(tag, value)
			}
		}
	}
	if updates != nil {
		if !updates.Commit(revision, commit) {
			return nil, errors.New("group changed during delay test; retry with current members")
		}
	} else {
		commit()
	}
	if urlTestGroup, ok := outboundGroup.(adapter.URLTestGroup); ok {
		urlTestGroup.PerformUpdateCheck()
	}
	if len(result) == 0 {
		return nil, errors.New("get delay: all proxies failed or timed out")
	}
	return result, nil
}

// Preserve the HTTP deadline/cancellation while using the core's certificate
// store, clock and other services, just like an automatic URL test.
func (s *Server) probeContext(ctx context.Context) context.Context {
	if s.ctx != nil {
		if registry := service.RegistryFromContext(s.ctx); registry != nil {
			return service.ContextWithRegistry(ctx, registry)
		}
	}
	return ctx
}
