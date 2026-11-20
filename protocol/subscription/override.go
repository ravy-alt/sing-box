package subscription

import (
	"context"

	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
)

func prepareImportedDialer(ctx context.Context, options any, node string, defaults *option.DomainResolveOptions, resolver string, graph map[string][]string, rewrite func(string) func(string, string) (string, error), overrides []*option.SubscriptionDialerOverride) error {
	var override *option.SubscriptionDialerOverride
	if len(overrides) > 0 {
		override = overrides[0]
	}
	wrapper, supported := options.(option.DialerOptionsWrapper)
	if !supported || override == nil {
		return prepareDialer(options, node, defaults, resolver, graph, rewrite)
	}
	dial, err := override.Apply(wrapper.TakeDialerOptions())
	if err != nil {
		return E.Cause(err, node, ": override_dialer")
	}
	var explicit *option.DomainResolveOptions
	if override.DomainResolver != nil {
		explicit = dial.DomainResolver
		if explicit == nil || explicit.Server == "" || !externalReferenceExists(ctx, "dns_server", explicit.Server) {
			return E.New(node, ": override_dialer.domain_resolver not found in the declaring scope: ", override.DomainResolver.Server)
		}
		// The override belongs to the declaring scope, not the downloaded
		// document. A same-named source DNS server must not shadow it.
		dial.DomainResolver = nil
		defaults, resolver = nil, ""
		// An explicit resolver also replaces the deprecated per-node strategy.
		dial.DomainStrategy = 0
	}
	wrapper.ReplaceDialerOptions(dial)
	if err := prepareDialer(options, node, defaults, resolver, graph, rewrite); err != nil {
		return err
	}
	if explicit != nil {
		dial = wrapper.TakeDialerOptions()
		dial.DomainResolver = explicit
		wrapper.ReplaceDialerOptions(dial)
		graph[node] = append(graph[node], "dns_server:"+explicit.Server)
	}
	return nil
}
