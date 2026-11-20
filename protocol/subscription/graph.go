package subscription

import (
	"context"
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/dependency"
	groupmembers "github.com/sagernet/sing-box/common/group"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/json"
	"github.com/sagernet/sing/service"
)

type document struct {
	options      option.Options
	outboundTags map[string]string
	dnsTags      map[string]string
	resources    map[string]map[string]string
	order        []string
	members      []string
	resolverTag  string
	dependencies map[string][]string
	imports      map[string]map[string]string
}

func memberTag(provider, tag string) string {
	return "subscription/" + url.PathEscape(provider) + "/" + url.PathEscape(tag)
}

func ownsMember(provider, tag string) bool {
	for strings.HasPrefix(tag, "subscription/") {
		parts := strings.SplitN(strings.TrimPrefix(tag, "subscription/"), "/", 2)
		if len(parts) != 2 || parts[1] == "" {
			return false
		}
		parent, err := url.PathUnescape(parts[0])
		if err != nil || parent == tag {
			return false
		}
		if parent == provider {
			return true
		}
		tag = parent
	}
	return false
}

// Resolve a source document's reference to a child provider's stable member.
// The graph depends on the provider; its member is checked after bootstrap.
func mapProvidedMember(tag string, providers map[string]string) (mapped, owner string) {
	parts := strings.SplitN(strings.TrimPrefix(tag, "subscription/"), "/", 2)
	if !strings.HasPrefix(tag, "subscription/") || len(parts) != 2 {
		return "", ""
	}
	parent, err := url.PathUnescape(parts[0])
	if err != nil {
		return "", ""
	}
	name, err := url.PathUnescape(parts[1])
	if err != nil || name == "" {
		return "", ""
	}
	if owner = providers[parent]; owner != "" {
		return memberTag(owner, name), owner
	}
	if mapped, owner = mapProvidedMember(parent, providers); mapped != "" {
		return memberTag(mapped, name), owner
	}
	return "", ""
}

func externalReferenceExists(ctx context.Context, kind, tag string) bool {
	switch kind {
	case "outbound":
		if manager := service.FromContext[adapter.OutboundManager](ctx); manager != nil {
			outbound, found := manager.Outbound(tag)
			return found && !adapter.IsSubscriptionSource(outbound)
		}
	case "dns_server":
		if manager := service.FromContext[adapter.DNSTransportManager](ctx); manager != nil {
			_, found := manager.Transport(tag)
			return found
		}
	case "rule_set":
		if router := service.FromContext[adapter.Router](ctx); router != nil {
			_, found := router.RuleSet(tag)
			return found
		}
	case "certificate_provider":
		if manager := service.FromContext[adapter.CertificateProviderManager](ctx); manager != nil {
			_, found := manager.Get(tag)
			return found
		}
	case "http_client":
		if manager, ok := service.FromContext[adapter.HTTPClientManager](ctx).(interface{ HasClient(string) bool }); ok {
			return manager.HasClient(tag)
		}
	case "network_namespace":
		return service.FromContext[adapter.NetworkNamespaceManager](ctx) != nil
	case "inbound":
		if manager := service.FromContext[adapter.InboundManager](ctx); manager != nil {
			_, found := manager.Get(tag)
			return found
		}
	}
	return false
}

func parseDocument(ctx context.Context, provider string, content []byte, members []string, overrides ...*option.SubscriptionDialerOverride) (*document, error) {
	options, err := json.UnmarshalExtendedContext[option.Options](ctx, content)
	if err != nil {
		return nil, err
	}
	options, err = option.ExpandSubscriptions(options)
	if err != nil {
		return nil, err
	}
	if options.Outbounds == nil && options.Endpoints == nil {
		return nil, E.New("subscription contains no outbounds")
	}
	if err := option.ValidateGroupOutbounds(options); err != nil {
		return nil, err
	}
	// Match the native box's default tags for resources whose tags are optional.
	for i := range options.Outbounds {
		if options.Outbounds[i].Tag == "" {
			options.Outbounds[i].Tag = strconv.Itoa(i)
		}
	}
	for i := range options.Endpoints {
		if options.Endpoints[i].Tag == "" {
			options.Endpoints[i].Tag = strconv.Itoa(i)
		}
	}
	for i := range options.CertificateProviders {
		if options.CertificateProviders[i].Tag == "" {
			options.CertificateProviders[i].Tag = strconv.Itoa(i)
		}
	}
	if options.DNS != nil {
		for i := range options.DNS.Servers {
			if options.DNS.Servers[i].Tag == "" {
				options.DNS.Servers[i].Tag = strconv.Itoa(i)
			}
		}
	}

	d := &document{options: options, outboundTags: map[string]string{}, dnsTags: map[string]string{}, resources: map[string]map[string]string{}, imports: map[string]map[string]string{}}
	d.resources["outbound"], d.resources["dns_server"] = d.outboundTags, d.dnsTags
	graph := make(map[string][]string)
	addResource := func(kind, name string) error {
		if name == "" {
			return E.New("subscription ", kind, " must have an explicit tag")
		}
		if d.resources[kind] == nil {
			d.resources[kind] = make(map[string]string)
		}
		if _, found := d.resources[kind][name]; found {
			return E.New("duplicate ", kind, " tag: ", name)
		}
		tag := "subscription-" + kind + "/" + url.PathEscape(provider) + "/" + url.PathEscape(name)
		if kind == "outbound" {
			tag = memberTag(provider, name)
		}
		if kind == "dns_server" {
			tag = "subscription-dns/" + url.PathEscape(provider) + "/" + url.PathEscape(name)
		}
		d.resources[kind][name] = tag
		graph[kind+":"+tag] = nil
		return nil
	}
	for _, outbound := range options.Outbounds {
		if err := addResource("outbound", outbound.Tag); err != nil {
			return nil, err
		}
	}
	for _, endpoint := range options.Endpoints {
		if err := addResource("outbound", endpoint.Tag); err != nil {
			return nil, err
		}
	}
	for _, client := range options.HTTPClients {
		if err := addResource("http_client", client.Tag); err != nil {
			return nil, err
		}
	}
	for _, namespace := range options.NetworkNamespaces {
		if err := addResource("network_namespace", namespace.Tag); err != nil {
			return nil, err
		}
	}
	for _, certificate := range options.CertificateProviders {
		if err := addResource("certificate_provider", certificate.Tag); err != nil {
			return nil, err
		}
	}
	if options.Route != nil {
		for _, ruleSet := range options.Route.RuleSet {
			for _, tag := range ruleSet.Tag {
				if err := addResource("rule_set", tag); err != nil {
					return nil, err
				}
			}
		}
	}
	if options.DNS != nil {
		for _, server := range options.DNS.Servers {
			if err := addResource("dns_server", server.Tag); err != nil {
				return nil, err
			}
		}
		if options.DNS.Final == "" {
			if len(options.DNS.Servers) > 0 {
				options.DNS.Final = options.DNS.Servers[0].Tag
			} else if manager := service.FromContext[adapter.DNSTransportManager](ctx); manager != nil && manager.Default() != nil {
				options.DNS.Final = manager.Default().Tag()
			}
		}
	}
	if len(members) == 0 {
		for _, outbound := range options.Outbounds {
			if source, ok := outbound.Options.(*option.SubscriptionOutboundOptions); ok && source.SourceOnly {
				continue
			}
			members = append(members, outbound.Tag)
		}
		for _, endpoint := range options.Endpoints {
			members = append(members, endpoint.Tag)
		}
	}
	for _, name := range members {
		for _, outbound := range options.Outbounds {
			if source, ok := outbound.Options.(*option.SubscriptionOutboundOptions); ok && source.SourceOnly && outbound.Tag == name {
				return nil, E.New("subscription source cannot be a member: ", name, "; use import")
			}
		}
		tag, exists := d.outboundTags[name]
		if !exists {
			return nil, E.New("subscription member not found: ", name)
		}
		if slices.Contains(d.members, tag) {
			return nil, E.New("duplicate subscription member: ", name)
		}
		d.members = append(d.members, tag)
	}
	providers := make(map[string]string)
	for _, outbound := range options.Outbounds {
		if _, ok := outbound.Options.(*option.SubscriptionOutboundOptions); ok {
			providers[outbound.Tag] = d.outboundTags[outbound.Tag]
		}
	}
	rewrite := func(node string) func(string, string) (string, error) {
		return func(kind, name string) (string, error) {
			mapped := d.resources[kind][name]
			if mapped == "" {
				if kind == "outbound" {
					if member, owner := mapProvidedMember(name, providers); member != "" {
						graph[node] = append(graph[node], "outbound:"+member)
						graph["outbound:"+member] = []string{"outbound:" + owner}
						return member, nil
					}
				}
				if externalReferenceExists(ctx, kind, name) {
					graph[node] = append(graph[node], kind+":"+name)
					return name, nil
				}
				return "", E.New(node, ": missing ", kind, " reference: ", name)
			}
			graph[node] = append(graph[node], kind+":"+mapped)
			return mapped, nil
		}
	}
	var defaultResolver *option.DomainResolveOptions
	history := service.PtrFromContext[groupmembers.ImportHistory](ctx)
	rewriteImports := func(owner string, imports []option.GroupImport) error {
		if len(imports) == 0 {
			return nil
		}
		node := "outbound:" + owner
		d.imports[owner] = make(map[string]string)
		for i := range imports {
			original := imports[i].Group
			previous := history.Resolve(owner, original)
			var mapped string
			var err error
			if previous != "" && ownsMember(provider, previous) && d.outboundTags[original] == "" {
				// Preserve a removed local group's identity instead of falling
				// through to an unrelated parent resource with the same name.
				mapped = previous
				graph[node] = append(graph[node], "outbound:"+mapped)
			} else {
				// Source-only providers can be consumed by import, never by a
				// detour, resolver, routing action or fixed group membership.
				if manager := service.FromContext[adapter.OutboundManager](ctx); manager != nil && d.outboundTags[original] == "" {
					if source, found := manager.Outbound(original); found && adapter.IsSubscriptionSource(source) {
						mapped = original
						graph[node] = append(graph[node], "outbound:"+mapped)
					}
				}
				if mapped == "" {
					mapped, err = rewrite(node)("outbound", original)
				}
				if err != nil && previous != "" {
					mapped, err = previous, nil
					graph[node] = append(graph[node], "outbound:"+mapped)
				}
			}
			if err != nil {
				return err
			}
			imports[i].Group = mapped
			d.imports[owner][original] = mapped
		}
		return nil
	}
	if options.Route != nil {
		defaultResolver = options.Route.DefaultDomainResolver
	}
	if options.DNS != nil {
		d.resolverTag = "subscription-dns-router/" + url.PathEscape(provider)
		resolverNode := "dns_server:" + d.resolverTag
		graph[resolverNode] = nil
		mapped, err := rewrite(resolverNode)("dns_server", options.DNS.Final)
		if err != nil {
			return nil, err
		}
		options.DNS.Final = mapped
		if err := dependency.VisitReferences(reflect.ValueOf(&options.DNS.Rules), func(kind, name string) (string, error) {
			if kind == "outbound" {
				// Matching an outbound tag does not dial through that outbound.
				if name == "any" {
					return name, nil
				}
				if tag, found := d.outboundTags[name]; found {
					return tag, nil
				}
				if tag, _ := mapProvidedMember(name, providers); tag != "" {
					return tag, nil
				}
				if externalReferenceExists(ctx, kind, name) {
					return name, nil
				}
				return "", E.New("DNS rule: missing outbound reference: ", name)
			}
			return rewrite(resolverNode)(kind, name)
		}); err != nil {
			return nil, err
		}
		for i := range options.DNS.Servers {
			server := &options.DNS.Servers[i]
			server.Tag = d.dnsTags[server.Tag]
			if _, remote := server.Options.(option.ServerOptionsWrapper); remote {
				err = prepareDialer(server.Options, "dns_server:"+server.Tag, defaultResolver, d.resolverTag, graph, rewrite)
			} else {
				err = dependency.VisitReferences(reflect.ValueOf(server.Options), rewrite("dns_server:"+server.Tag))
			}
			if err != nil {
				return nil, err
			}
		}
	}
	for i := range options.Outbounds {
		outbound := &options.Outbounds[i]
		outbound.Tag = d.outboundTags[outbound.Tag]
		// A default only chooses among the effective members. An imported
		// preference can be temporarily absent and does not add a dial edge.
		if group, ok := outbound.Options.(*option.SelectorOutboundOptions); ok {
			if group.EmptyFallback != "" {
				group.EmptyFallback, err = rewrite("outbound:"+outbound.Tag)("outbound", group.EmptyFallback)
				if err != nil {
					return nil, err
				}
			}
			if err := rewriteImports(outbound.Tag, group.Import); err != nil {
				return nil, err
			}
			for j, tag := range group.Outbounds {
				group.Outbounds[j], err = rewrite("outbound:"+outbound.Tag)("outbound", tag)
				if err != nil {
					return nil, err
				}
			}
			if group.Default != "" {
				if len(group.Import) == 0 {
					group.Default, err = rewrite("outbound:"+outbound.Tag)("outbound", group.Default)
					if err != nil {
						return nil, err
					}
				} else if mapped := d.outboundTags[group.Default]; mapped != "" {
					group.Default = mapped
				} else if mapped, _ := mapProvidedMember(group.Default, providers); mapped != "" {
					group.Default = mapped
				}
			}
			continue
		}
		if group, ok := outbound.Options.(*option.URLTestOutboundOptions); ok {
			if group.EmptyFallback != "" {
				group.EmptyFallback, err = rewrite("outbound:"+outbound.Tag)("outbound", group.EmptyFallback)
				if err != nil {
					return nil, err
				}
			}
			if err := rewriteImports(outbound.Tag, group.Import); err != nil {
				return nil, err
			}
			for j, tag := range group.Outbounds {
				group.Outbounds[j], err = rewrite("outbound:"+outbound.Tag)("outbound", tag)
				if err != nil {
					return nil, err
				}
			}
			continue
		}
		if nested, ok := outbound.Options.(*option.SubscriptionOutboundOptions); ok {
			if nested.EmptyFallback != "" {
				nested.EmptyFallback, err = rewrite("outbound:"+outbound.Tag)("outbound", nested.EmptyFallback)
				if err != nil {
					return nil, err
				}
			}
			// Imports are live membership dependencies, unlike download edges.
			if err := rewriteImports(outbound.Tag, nested.Import); err != nil {
				return nil, err
			}
			// A child can import a group in its own downloaded document. Its
			// virtual member depends on constructing the child, so this edge
			// cannot order construction of the child itself. The child's real
			// graph and imports are checked together before it is published.
			node := "outbound:" + outbound.Tag
			graph[node] = slices.DeleteFunc(graph[node], func(edge string) bool {
				return strings.HasPrefix(edge, "outbound:") && ownsMember(outbound.Tag, strings.TrimPrefix(edge, "outbound:"))
			})
			// Download edges are bootstrap dependencies, not data-plane cycles.
			download := *nested
			download.Import = nil
			download.EmptyFallback = ""
			if err := dependency.VisitReferences(reflect.ValueOf(&download), rewrite("bootstrap:"+outbound.Tag)); err != nil {
				return nil, err
			}
			nested.HTTPClient, nested.BootstrapHTTPClient = download.HTTPClient, download.BootstrapHTTPClient
			continue
		}
		if err := prepareImportedDialer(ctx, outbound.Options, "outbound:"+outbound.Tag, defaultResolver, d.resolverTag, graph, rewrite, overrides); err != nil {
			return nil, err
		}
	}
	for i := range options.Endpoints {
		endpoint := &options.Endpoints[i]
		endpoint.Tag = d.outboundTags[endpoint.Tag]
		if err := prepareImportedDialer(ctx, endpoint.Options, "outbound:"+endpoint.Tag, defaultResolver, d.resolverTag, graph, rewrite, overrides); err != nil {
			return nil, err
		}
	}
	// HTTP definitions used by source DNS transports have their own scope too.
	for i := range options.HTTPClients {
		options.HTTPClients[i].Tag = d.resources["http_client"][options.HTTPClients[i].Tag]
		if err := prepareDialer(&options.HTTPClients[i], "http_client:"+options.HTTPClients[i].Tag, defaultResolver, d.resolverTag, graph, rewrite); err != nil {
			return nil, err
		}
	}
	for i := range options.NetworkNamespaces {
		options.NetworkNamespaces[i].Tag = d.resources["network_namespace"][options.NetworkNamespaces[i].Tag]
	}
	for i := range options.CertificateProviders {
		certificate := &options.CertificateProviders[i]
		certificate.Tag = d.resources["certificate_provider"][certificate.Tag]
		if err := dependency.VisitReferences(reflect.ValueOf(certificate.Options), rewrite("certificate_provider:"+certificate.Tag)); err != nil {
			return nil, err
		}
	}
	if options.Route != nil {
		for i := range options.Route.RuleSet {
			ruleSet := &options.Route.RuleSet[i]
			// Expand the source tags before namespacing; {tag} is a URL/path template.
			if err := dependency.VisitReferences(reflect.ValueOf(ruleSet), rewrite("bootstrap:rule_set:"+strings.Join(ruleSet.Tag, ","))); err != nil {
				return nil, err
			}
		}
		if name := options.Route.DefaultHTTPClient; name != "" {
			options.Route.DefaultHTTPClient, err = rewrite("bootstrap:http_default")("http_client", name)
			if err != nil {
				return nil, err
			}
		}
	}
	if options.DNS == nil {
		resolver := dependency.ParentResolver(ctx)
		for _, outbound := range options.Outbounds {
			dependency.ImplicitResolver(outbound.Options, "outbound:"+outbound.Tag, resolver, graph)
		}
		for _, endpoint := range options.Endpoints {
			dependency.ImplicitResolver(endpoint.Options, "outbound:"+endpoint.Tag, resolver, graph)
		}
	}
	resolver := dependency.ParentResolver(ctx)
	if d.resolverTag != "" {
		resolver = "dns_server:" + d.resolverTag
	}
	if defaultResolver != nil && defaultResolver.Server != "" {
		tag := defaultResolver.Server
		if mapped := d.dnsTags[tag]; mapped != "" {
			tag = mapped
		}
		resolver = "dns_server:" + tag
	}
	for _, outbound := range options.Outbounds {
		if _, nested := outbound.Options.(*option.SubscriptionOutboundOptions); !nested {
			dependency.AuxiliaryResolvers(outbound.Options, "outbound:"+outbound.Tag, resolver, graph)
		}
	}
	for _, endpoint := range options.Endpoints {
		dependency.AuxiliaryResolvers(endpoint.Options, "outbound:"+endpoint.Tag, resolver, graph)
	}
	for _, provider := range options.CertificateProviders {
		dependency.AuxiliaryResolvers(provider.Options, "certificate_provider:"+provider.Tag, resolver, graph)
	}
	for i := range options.HTTPClients {
		dependency.AuxiliaryResolvers(&options.HTTPClients[i], "http_client:"+options.HTTPClients[i].Tag, resolver, graph)
	}
	if options.DNS != nil {
		for _, server := range options.DNS.Servers {
			dependency.AuxiliaryResolvers(server.Options, "dns_server:"+server.Tag, resolver, graph)
		}
	}
	d.options = options
	for _, outbound := range options.Outbounds {
		dependency.ResolveIPDetour(outbound.Options, "outbound:"+outbound.Tag, graph)
		dependency.AddIPNode(outbound.Options, outbound.Tag, graph)
	}
	for _, endpoint := range options.Endpoints {
		dependency.ResolveIPDetour(endpoint.Options, "outbound:"+endpoint.Tag, graph)
		dependency.AddIPNode(endpoint.Options, endpoint.Tag, graph)
	}
	if options.DNS != nil {
		for _, server := range options.DNS.Servers {
			dependency.ResolveIPDetour(server.Options, "dns_server:"+server.Tag, graph)
		}
	}
	d.dependencies = graph
	localGraph := make(map[string][]string, len(graph))
	for node, edges := range graph {
		localGraph[node] = slices.Clone(edges)
	}
	dependency.CompleteExternalReferences(localGraph)
	d.order, err = dependency.Order(localGraph)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func prepareDialer(options any, node string, defaults *option.DomainResolveOptions, resolver string, graph map[string][]string, rewrite func(string) func(string, string) (string, error)) error {
	wrapper, hasDialer := options.(option.DialerOptionsWrapper)
	needsResolver := dependency.NeedsDomainResolver(options)
	if hasDialer && needsResolver {
		dial := wrapper.TakeDialerOptions()
		if (dial.DomainResolver == nil || dial.DomainResolver.Server == "") && (dial.Detour == "" || dependency.ResolvesOnDetour(options)) && defaults != nil {
			copy := *defaults
			if dial.DomainResolver != nil {
				copy = *dial.DomainResolver
				copy.Server = defaults.Server
			}
			dial.DomainResolver = &copy
			wrapper.ReplaceDialerOptions(dial)
		}
	}
	if err := dependency.VisitReferences(reflect.ValueOf(options), rewrite(node)); err != nil {
		return err
	}
	if hasDialer && needsResolver && resolver != "" {
		dial := wrapper.TakeDialerOptions()
		if (dial.DomainResolver == nil || dial.DomainResolver.Server == "") && (dial.Detour == "" || dependency.ResolvesOnDetour(options)) {
			if dial.DomainResolver == nil {
				dial.DomainResolver = &option.DomainResolveOptions{}
			}
			dial.DomainResolver.Server = resolver
			wrapper.ReplaceDialerOptions(dial)
			graph[node] = append(graph[node], "dns_server:"+resolver)
		}
	}
	return nil
}
