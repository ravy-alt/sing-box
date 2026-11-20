package dependency

import (
	"context"
	"fmt"
	"net/netip"
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/service"
)

// visitReferences uses the same typed reference metadata as the configuration
// schema. It never rewrites arbitrary strings (passwords, URLs or certificates).
func visitReferences(value reflect.Value, visit func(string, string) (string, error)) error {
	if !value.IsValid() {
		return nil
	}
	if value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface {
		if value.IsNil() {
			return nil
		}
		return visitReferences(value.Elem(), visit)
	}
	var specialKind string
	switch value.Type() {
	case reflect.TypeFor[option.HTTPClientOptions]():
		specialKind = "http_client"
	case reflect.TypeFor[option.CertificateProviderOptions]():
		specialKind = "certificate_provider"
	}
	if specialKind != "" {
		tag := value.FieldByName("Tag")
		if tag.String() != "" {
			mapped, err := visit(specialKind, tag.String())
			if err != nil {
				return err
			}
			if tag.String() != mapped {
				tag.SetString(mapped)
			}
			return nil
		}
	}
	switch value.Kind() {
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			field := value.Type().Field(i)
			if !field.IsExported() {
				continue
			}
			v := value.Field(i)
			if kind := field.Tag.Get("reference"); kind != "" {
				var rewrite func(reflect.Value) error
				rewrite = func(v reflect.Value) error {
					if v.Kind() == reflect.Slice {
						for j := 0; j < v.Len(); j++ {
							if err := rewrite(v.Index(j)); err != nil {
								return err
							}
						}
						return nil
					}
					if v.Kind() != reflect.String {
						return E.New("unsupported reference field: ", field.Name)
					}
					if v.String() == "" {
						return nil
					}
					mapped, err := visit(kind, v.String())
					if err != nil {
						return err
					}
					if v.String() != mapped {
						v.SetString(mapped)
					}
					return nil
				}
				if err := rewrite(v); err != nil {
					return err
				}
			} else if err := visitReferences(v, visit); err != nil {
				return err
			}
		}
	case reflect.Slice:
		if value.Type().Elem().Kind() == reflect.Uint8 {
			return nil
		}
		for i := 0; i < value.Len(); i++ {
			if err := visitReferences(value.Index(i), visit); err != nil {
				return err
			}
		}
	}
	return nil
}

// Only add an implicit resolver when dialing can actually resolve a name.
// In particular, an IP-only tunnel may itself carry the DNS server's traffic.
func needsDomainResolver(options any) bool {
	switch options := options.(type) {
	case *option.NaiveOutboundOptions:
		return true
	case *option.Hysteria2OutboundOptions:
		return options.ServerIsDomain() || options.Realm != nil && options.Realm.STUNServersIsDomain()
	case *option.WireGuardEndpointOptions:
		for _, peer := range options.Peers {
			if peer.Address != "" {
				if _, err := netip.ParseAddr(peer.Address); err != nil {
					return true
				}
			}
		}
		return false
	case *option.OpenVPNClientEndpointOptions:
		if options.ServerIsDomain() {
			return true
		}
		for _, remote := range options.Servers {
			if remote.ServerIsDomain() {
				return true
			}
		}
		return false
	case *option.OpenConnectEndpointOptions:
		server := options.Server
		if !strings.Contains(server, "://") {
			server = "https://" + server
		}
		parsed, err := url.Parse(server)
		if err != nil {
			return true
		}
		_, err = netip.ParseAddr(parsed.Hostname())
		return err != nil
	}
	if server, ok := options.(option.ServerOptionsWrapper); ok {
		return server.TakeServerOptions().ServerIsDomain()
	}
	return true
}

func resolvesOnDetour(options any) bool {
	switch options := options.(type) {
	case *option.WireGuardEndpointOptions, *option.TailscaleEndpointOptions, *option.OpenVPNClientEndpointOptions, *option.OpenConnectEndpointOptions, *option.NaiveOutboundOptions:
		return true
	case *option.Hysteria2OutboundOptions:
		return options.Realm != nil && options.Realm.STUNServersIsDomain()
	}
	return false
}

func dependencyOrder(graph map[string][]string) ([]string, error) {
	state := make(map[string]uint8)
	var order, path []string
	var visit func(string) error
	visit = func(node string) error {
		if state[node] == 2 {
			return nil
		}
		if state[node] == 1 {
			return fmt.Errorf("circular subscription dependency: %s", strings.Join(append(path, node), " -> "))
		}
		if _, exists := graph[node]; !exists {
			return E.New("missing subscription dependency: ", node)
		}
		state[node] = 1
		path = append(path, node)
		for _, dependency := range graph[node] {
			if err := visit(dependency); err != nil {
				return err
			}
		}
		path = path[:len(path)-1]
		state[node] = 2
		order = append(order, node)
		return nil
	}
	var nodes []string
	for node := range graph {
		nodes = append(nodes, node)
	}
	slices.Sort(nodes)
	for _, node := range nodes {
		if err := visit(node); err != nil {
			return nil, err
		}
	}
	return order, nil
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
			if subscription.EmptyFallback != "" {
				graph[node] = append(graph[node], "outbound:"+subscription.EmptyFallback)
			}
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
	for i, outbound := range options.Outbounds {
		tag := tagAt(outbound.Tag, i)
		ResolveIPDetour(outbound.Options, "outbound:"+tag, graph)
		AddIPNode(outbound.Options, tag, graph)
	}
	for i, endpoint := range options.Endpoints {
		tag := tagAt(endpoint.Tag, i)
		ResolveIPDetour(endpoint.Options, "outbound:"+tag, graph)
		AddIPNode(endpoint.Options, tag, graph)
	}
	if options.DNS != nil {
		for i, server := range options.DNS.Servers {
			ResolveIPDetour(server.Options, "dns_server:"+tagAt(server.Tag, i), graph)
		}
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

// Host returns the static dependency graph used both at startup and when a
// candidate subscription generation is checked against all active providers.
func Host(ctx context.Context) map[string][]string      { return hostDependencies(ctx) }
func Order(graph map[string][]string) ([]string, error) { return dependencyOrder(graph) }
func VisitReferences(value reflect.Value, visit func(string, string) (string, error)) error {
	return visitReferences(value, visit)
}
func NeedsDomainResolver(options any) bool      { return needsDomainResolver(options) }
func ResolvesOnDetour(options any) bool         { return resolvesOnDetour(options) }
func ParentResolver(ctx context.Context) string { return parentResolverDependency(ctx) }
func AuxiliaryResolvers(options any, node, resolver string, graph map[string][]string) {
	auxiliaryResolverDependencies(options, node, resolver, graph)
}
func ImplicitResolver(options any, node, resolver string, graph map[string][]string) {
	implicitResolverDependency(options, node, resolver, graph)
}

// ValidateHost includes cross-manager DNS/outbound cycles even when there are no
// subscriptions. Dynamic members and separately validated resources are leaves
// until their candidate graphs are available.
func ValidateHost(ctx context.Context) error {
	graph := hostDependencies(ctx)
	CompleteExternalReferences(graph)
	_, err := dependencyOrder(graph)
	return err
}
