package dependency

import (
	"strings"

	"github.com/sagernet/sing-box/option"
)

// A resolved destination passes through direct/selection groups without another
// DNS lookup. Fixed proxy peers still need their own resolution regardless of
// the application destination. Model both paths to avoid rejecting DNS over an
// IP-addressed direct route while retaining proxy-peer cycles.
func AddIPNode(options any, tag string, graph map[string][]string) {
	node := "outbound:" + tag
	ip := "outbound-ip:" + tag
	switch options.(type) {
	case *option.DirectOutboundOptions:
		graph[ip] = nil
		for _, edge := range graph[node] {
			if !strings.HasPrefix(edge, "dns_server:") && !strings.HasPrefix(edge, "dns_router:") {
				graph[ip] = append(graph[ip], edge)
			}
		}
	case *option.SelectorOutboundOptions, *option.URLTestOutboundOptions, *option.SmartURLTestOutboundOptions, *option.SubscriptionOutboundOptions:
		AddIPGroup(tag, graph)
	default:
		graph[ip] = []string{node}
	}
}

func AddIPGroup(tag string, graph map[string][]string) {
	ip := "outbound-ip:" + tag
	graph[ip] = nil
	for _, edge := range graph["outbound:"+tag] {
		if strings.HasPrefix(edge, "outbound:") {
			edge = "outbound-ip:" + strings.TrimPrefix(edge, "outbound:")
		}
		graph[ip] = append(graph[ip], edge)
	}
}

func ResolveIPDetour(options any, node string, graph map[string][]string) {
	wrapper, ok := options.(option.DialerOptionsWrapper)
	if !ok {
		return
	}
	dial := wrapper.TakeDialerOptions()
	if dial.Detour == "" {
		return
	}
	resolved := dial.DomainResolver != nil && dial.DomainResolver.Server != ""
	if server, ok := options.(option.ServerOptionsWrapper); ok {
		resolved = resolved || !server.TakeServerOptions().ServerIsDomain()
	}
	if !resolved {
		return
	}
	for i, edge := range graph[node] {
		if edge == "outbound:"+dial.Detour {
			graph[node][i] = "outbound-ip:" + dial.Detour
		}
	}
}

// CompleteExternalReferences runs on the combined graph or an ordering copy.
// Unloaded external members remain leaves; their conservative IP view aliases
// the normal node until a provider supplies a concrete implementation.
func CompleteExternalReferences(graph map[string][]string) {
	var missing []string
	for _, edges := range graph {
		for _, edge := range edges {
			if _, ok := graph[edge]; !ok {
				missing = append(missing, edge)
			}
		}
	}
	for _, edge := range missing {
		if _, ok := graph[edge]; ok {
			continue
		}
		if strings.HasPrefix(edge, "outbound-ip:") {
			normal := "outbound:" + strings.TrimPrefix(edge, "outbound-ip:")
			graph[edge] = []string{normal}
			if _, ok := graph[normal]; !ok {
				graph[normal] = nil
			}
		} else {
			graph[edge] = nil
		}
	}
}
