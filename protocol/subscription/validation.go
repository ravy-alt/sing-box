package subscription

import (
	"context"
	"reflect"
	"slices"
	"strconv"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/service"
)

// Publication is serialized across providers so simultaneous individually valid
// updates cannot jointly introduce a cycle. Candidate construction remains parallel.
type dependencyCoordinator struct {
	sync.Mutex
	ctx context.Context
}

func (s *Subscription) validateCandidate(g *snapshot) error {
	s.dependencies.Lock()
	defer s.dependencies.Unlock()
	return s.dependencies.validate(s, g)
}

func (c *dependencyCoordinator) validate(target *Subscription, candidate *snapshot) error {
	for parent := target.parent; parent != nil; parent = parent.owner.parent {
		parent.mu.Lock()
		obsolete := parent.retired || parent.closed
		parent.mu.Unlock()
		if obsolete {
			return E.New("subscription parent generation is retired: ", parent.owner.Tag())
		}
	}
	graph := hostDependencies(c.ctx)
	overrides := map[*Subscription]*snapshot{target: candidate}
	for parent := target.parent; parent != nil; parent = parent.owner.parent {
		overrides[parent.owner] = parent
	}
	var collect func(*Subscription)
	collect = func(s *Subscription) {
		g, overridden := overrides[s]
		if !overridden {
			s.mu.Lock()
			g = s.current
			s.mu.Unlock()
		}
		if g == nil {
			return
		}
		for node, edges := range g.dependencies {
			graph[node] = slices.Clone(edges)
		}
		node := "outbound:" + s.Tag()
		graph[node] = nil
		for _, spec := range s.options.Import {
			graph[node] = append(graph[node], "outbound:"+spec.Group)
		}
		for _, member := range g.members {
			graph[node] = append(graph[node], "outbound:"+member)
		}
		for _, child := range g.children {
			collect(child)
		}
	}
	if manager := service.FromContext[adapter.OutboundManager](c.ctx); manager != nil {
		for _, outbound := range manager.Outbounds() {
			if s, ok := outbound.(*Subscription); ok {
				collect(s)
			}
		}
	}
	// Directly constructed providers (and children still being constructed) may
	// not yet be present in the root manager.
	if _, exists := graph["outbound:"+target.Tag()]; !exists {
		collect(target)
	}
	for _, edges := range graph {
		for _, edge := range edges {
			if _, exists := graph[edge]; !exists {
				graph[edge] = nil
			}
		}
	}
	_, err := dependencyOrder(graph)
	return err
}

func parentResolverDependency(ctx context.Context) string {
	if network := service.FromContext[adapter.NetworkManager](ctx); network != nil {
		if tag := network.DefaultOptions().DomainResolver; tag != "" {
			return "dns_server:" + tag
		}
	}
	if manager := service.FromContext[adapter.DNSTransportManager](ctx); manager != nil && len(manager.Transports()) < 2 {
		if transport := manager.Default(); transport != nil {
			return "dns_server:" + transport.Tag()
		}
	}
	return "dns_router:host"
}

func implicitResolverDependency(options any, node, resolver string, graph map[string][]string) {
	if wrapper, ok := options.(option.DialerOptionsWrapper); ok && needsDomainResolver(options) {
		dial := wrapper.TakeDialerOptions()
		if (dial.DomainResolver == nil || dial.DomainResolver.Server == "") && (dial.Detour == "" || resolvesOnDetour(options)) {
			graph[node] = append(graph[node], resolver)
		}
	}
}

func hostDependencies(ctx context.Context) map[string][]string {
	graph := make(map[string][]string)
	host := service.PtrFromContext[adapter.SubscriptionHostOptions](ctx)
	if host == nil {
		return graph
	}
	options := host.Options
	tagAt := func(tag string, index int) string {
		if tag != "" {
			return tag
		}
		return strconv.Itoa(index)
	}
	collect := func(node string, value any) {
		if _, exists := graph[node]; !exists {
			graph[node] = nil
		}
		_ = visitReferences(reflect.ValueOf(value), func(kind, tag string) (string, error) {
			graph[node] = append(graph[node], kind+":"+tag)
			return tag, nil
		})
	}
	resolver := parentResolverDependency(ctx)
	for i, outbound := range options.Outbounds {
		node := "outbound:" + tagAt(outbound.Tag, i)
		if subscription, ok := outbound.Options.(*option.SubscriptionOutboundOptions); ok {
			collect(node, subscription.Import)
			continue
		}
		if selector, ok := outbound.Options.(*option.SelectorOutboundOptions); ok {
			members := *selector
			members.Default = "" // A selection preference does not add members.
			collect(node, &members)
			continue
		}
		collect(node, outbound.Options)
		implicitResolverDependency(outbound.Options, node, resolver, graph)
		auxiliaryResolverDependencies(outbound.Options, node, resolver, graph)
	}
	for i, endpoint := range options.Endpoints {
		node := "outbound:" + tagAt(endpoint.Tag, i)
		collect(node, endpoint.Options)
		implicitResolverDependency(endpoint.Options, node, resolver, graph)
		auxiliaryResolverDependencies(endpoint.Options, node, resolver, graph)
	}
	if options.DNS != nil {
		final := options.DNS.Final
		if final == "" && len(options.DNS.Servers) > 0 {
			final = tagAt(options.DNS.Servers[0].Tag, 0)
		}
		if final != "" {
			graph["dns_router:host"] = []string{"dns_server:" + final}
		}
		_ = visitReferences(reflect.ValueOf(&options.DNS.Rules), func(kind, tag string) (string, error) {
			if kind == "dns_server" {
				graph["dns_router:host"] = append(graph["dns_router:host"], kind+":"+tag)
			}
			return tag, nil
		})
		for i, server := range options.DNS.Servers {
			node := "dns_server:" + tagAt(server.Tag, i)
			collect(node, server.Options)
			auxiliaryResolverDependencies(server.Options, node, resolver, graph)
			if _, remote := server.Options.(option.ServerOptionsWrapper); remote {
				implicitResolverDependency(server.Options, node, resolver, graph)
			}
		}
	}
	for i := range options.HTTPClients {
		collect("http_client:"+options.HTTPClients[i].Tag, &options.HTTPClients[i])
		implicitResolverDependency(&options.HTTPClients[i], "http_client:"+options.HTTPClients[i].Tag, resolver, graph)
		auxiliaryResolverDependencies(&options.HTTPClients[i], "http_client:"+options.HTTPClients[i].Tag, resolver, graph)
	}
	for i, provider := range options.CertificateProviders {
		collect("certificate_provider:"+tagAt(provider.Tag, i), provider.Options)
		auxiliaryResolverDependencies(provider.Options, "certificate_provider:"+tagAt(provider.Tag, i), resolver, graph)
	}
	return graph
}

// Protocol side channels (for example Realm HTTP requests and ECH DNS queries)
// may resolve domains even when the proxy's own server address is an IP.
func auxiliaryResolverDependencies(options any, node, resolver string, graph map[string][]string) {
	if wrapper, ok := options.(option.OutboundTLSOptionsWrapper); ok {
		tls := wrapper.TakeOutboundTLSOptions()
		if tls != nil && tls.Enabled && tls.ECH != nil && tls.ECH.Enabled && len(tls.ECH.Config) == 0 && tls.ECH.ConfigPath == "" {
			explicit := false
			if dialer, ok := options.(option.DialerOptionsWrapper); ok {
				dial := dialer.TakeDialerOptions()
				explicit = dial.DomainResolver != nil && dial.DomainResolver.Server != ""
			}
			if !explicit {
				graph[node] = append(graph[node], resolver)
			}
		}
	}
	var visit func(reflect.Value)
	visit = func(value reflect.Value) {
		if !value.IsValid() {
			return
		}
		if value.Type() == reflect.TypeFor[*option.HTTPClientOptions]() {
			options := &option.HTTPClientOptions{}
			if !value.IsNil() {
				options = value.Interface().(*option.HTTPClientOptions)
			}
			if options.Tag == "" {
				implicitResolverDependency(options, node, resolver, graph)
				auxiliaryResolverDependencies(options, node, resolver, graph)
			}
			return
		}
		switch value.Kind() {
		case reflect.Pointer, reflect.Interface:
			if !value.IsNil() {
				visit(value.Elem())
			}
		case reflect.Struct:
			for i := 0; i < value.NumField(); i++ {
				if value.Type().Field(i).IsExported() {
					visit(value.Field(i))
				}
			}
		case reflect.Slice:
			for i := 0; i < value.Len(); i++ {
				visit(value.Index(i))
			}
		}
	}
	// An HTTP client is already the leaf; only inspect protocol option containers.
	if _, client := options.(*option.HTTPClientOptions); !client {
		visit(reflect.ValueOf(options))
	}
}
