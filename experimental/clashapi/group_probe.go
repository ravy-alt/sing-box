package clashapi

import (
	"context"
	"errors"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing/service"
)

// Nested groups are pinned recursively and aliases of the same leaf share a
// single measurement. Only final nodes own measurements, never group aliases.
func (s *Server) testGroup(ctx context.Context, outboundGroup adapter.OutboundGroup, link string) (map[string]uint16, error) {
	ctx = s.probeContext(ctx)
	if link == "" {
		link = urltest.OutboundTestURL(outboundGroup)
	}
	if smart, ok := outboundGroup.(adapter.SmartURLTestGroup); ok && (link == "" || urltest.NormalizeURL(link) == urltest.NormalizeURL(smart.TestURL())) {
		return smart.URLTest(ctx)
	}
	link = urltest.NormalizeURL(link)
	var updates *adapter.GroupUpdates
	var revision uint64
	if s.ctx != nil {
		updates = service.PtrFromContext[adapter.GroupUpdates](s.ctx)
		if updates != nil {
			revision, _ = updates.State()
		}
	}
	type job struct {
		outbound adapter.Outbound
		aliases  []string
		result   urltest.Result
	}
	jobs := make(map[string]*job)
	for _, tag := range outboundGroup.All() {
		member, loaded := s.outbound.Outbound(tag)
		if !loaded {
			continue
		}
		resolved, err := urltest.Resolve(member)
		if err != nil {
			continue
		}
		key := resolved.Tag()
		if existing := jobs[key]; existing != nil {
			existing.aliases = append(existing.aliases, tag)
			continue
		}
		jobs[key] = &job{outbound: resolved, aliases: []string{tag}, result: urltest.Result{}}
	}
	type measurement struct {
		key    string
		result urltest.Result
	}
	completed := make(chan measurement, len(jobs))
	for key, work := range jobs {
		go func() { completed <- measurement{key, probeWithDeadline(ctx, link, work.outbound)} }()
	}
	// Each worker has a bounded wait, including admission to the probe pool.
	// Collect its actual outcome so queued jobs are not mistaken for timed-out
	// network attempts when the request deadline expires.
	for range jobs {
		m := <-completed
		jobs[m.key].result = m.result
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		return nil, ctx.Err()
	}
	commit := func() {
		for _, work := range jobs {
			work.result.Store(s.urlTestHistory)
		}
	}
	if updates != nil {
		if !updates.Commit(revision, commit) {
			return nil, errors.New("group changed during delay test; retry with current members")
		}
	} else {
		commit()
	}
	result := make(map[string]uint16)
	for _, work := range jobs {
		if work.result.Err != nil {
			continue
		}
		// Read the committed observation so concurrent probes cannot make
		// aliases disagree with that leaf's latest URL result.
		if history := s.urlTestHistory.LoadURLTestHistory(work.result.Outbound, link); history != nil {
			for _, tag := range work.aliases {
				result[tag] = history.Delay
			}
		}
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
