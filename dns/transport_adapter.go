package dns

import (
	"net"
	"strconv"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/option"
)

type TransportAdapter struct {
	transportType string
	transportTag  string
	dependencies  []string
	references    []string
	address       string
}

func NewTransportAdapter(transportType string, transportTag string, dependencies []string) TransportAdapter {
	return TransportAdapter{
		transportType: transportType,
		transportTag:  transportTag,
		dependencies:  dependencies,
	}
}

func NewTransportAdapterWithLocalOptions(transportType string, transportTag string, localOptions option.LocalDNSServerOptions) TransportAdapter {
	var dependencies []string
	if localOptions.DomainResolver != nil && localOptions.DomainResolver.Server != "" {
		dependencies = append(dependencies, localOptions.DomainResolver.Server)
	}
	var references []string
	if localOptions.Detour != "" {
		references = []string{localOptions.Detour}
	}
	return TransportAdapter{
		transportType: transportType,
		transportTag:  transportTag,
		dependencies:  dependencies,
		references:    references,
	}
}

func NewTransportAdapterWithRemoteOptions(transportType string, transportTag string, remoteOptions option.RemoteDNSServerOptions) TransportAdapter {
	var dependencies []string
	if remoteOptions.DomainResolver != nil && remoteOptions.DomainResolver.Server != "" {
		dependencies = append(dependencies, remoteOptions.DomainResolver.Server)
	}
	var references []string
	if remoteOptions.Detour != "" {
		references = []string{remoteOptions.Detour}
	}
	port := remoteOptions.ServerPort
	if port == 0 {
		switch transportType {
		case "https", "h3":
			port = 443
		case "tls", "quic":
			port = 853
		default:
			port = 53
		}
	}
	return TransportAdapter{
		address:       net.JoinHostPort(remoteOptions.Server, strconv.Itoa(int(port))),
		transportType: transportType,
		transportTag:  transportTag,
		dependencies:  dependencies,
		references:    references,
	}
}

func (a *TransportAdapter) Type() string {
	return a.transportType
}

func (a *TransportAdapter) Tag() string {
	return a.transportTag
}

func (a *TransportAdapter) Dependencies() []string {
	return a.dependencies
}

func (a *TransportAdapter) References() []string {
	return a.references
}

func (a *TransportAdapter) DNSInfo() adapter.DNSServerInfo {
	info := adapter.DNSServerInfo{Tag: a.transportTag, Type: a.transportType, Address: a.address, Dependencies: append([]string{}, a.dependencies...)}
	if len(a.references) > 0 {
		info.Detour = a.references[0]
	}
	if len(a.dependencies) > 0 && a.transportType != "race" {
		info.Resolver = a.dependencies[0]
	}
	return info
}
