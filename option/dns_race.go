package option

import "github.com/sagernet/sing/common/json/badoption"

// RaceDNSServerOptions exposes a DNS race as a transport, so node-specific
// domain_resolver settings can use it without going through DNS routing rules.
type RaceDNSServerOptions struct {
	Servers []string           `json:"servers" reference:"dns_server"`
	Timeout badoption.Duration `json:"timeout,omitempty"`
}
