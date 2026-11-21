package option

import (
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/json"
	"github.com/sagernet/sing/common/json/badoption"
)

func (o *SubscriptionDialerOverride) Validate() error {
	if o == nil {
		return nil
	}
	for name, duration := range map[string]*badoption.Duration{
		"connect_timeout": o.ConnectTimeout, "tcp_keep_alive": o.TCPKeepAlive,
		"tcp_keep_alive_interval": o.TCPKeepAliveInterval, "fallback_delay": o.FallbackDelay,
	} {
		if duration != nil && *duration < 0 {
			return E.New("override_dialer.", name, " cannot be negative")
		}
	}
	if o.DomainResolver != nil {
		if o.DomainResolver.Server == "" {
			return E.New("override_dialer.domain_resolver.server cannot be empty")
		}
		if o.DomainResolver.Timeout < 0 {
			return E.New("override_dialer.domain_resolver.timeout cannot be negative")
		}
	}
	return nil
}

// Every optional scalar is a pointer so false, zero and the empty string are
// intentional overrides. Unspecified fields retain the source's values.
type SubscriptionDialerOverride struct {
	Detour               *string                            `json:"detour,omitempty" reference:"outbound"`
	DomainResolver       *DomainResolveOptions              `json:"domain_resolver,omitempty"`
	BindInterface        *string                            `json:"bind_interface,omitempty"`
	Inet4BindAddress     *badoption.Addr                    `json:"inet4_bind_address,omitempty"`
	Inet6BindAddress     *badoption.Addr                    `json:"inet6_bind_address,omitempty"`
	BindAddressNoPort    *bool                              `json:"bind_address_no_port,omitempty"`
	RoutingMark          *FwMark                            `json:"routing_mark,omitempty"`
	ReuseAddr            *bool                              `json:"reuse_addr,omitempty"`
	ConnectTimeout       *badoption.Duration                `json:"connect_timeout,omitempty"`
	TCPFastOpen          *bool                              `json:"tcp_fast_open,omitempty"`
	TCPMultiPath         *bool                              `json:"tcp_multi_path,omitempty"`
	DisableTCPKeepAlive  *bool                              `json:"disable_tcp_keep_alive,omitempty"`
	TCPKeepAlive         *badoption.Duration                `json:"tcp_keep_alive,omitempty"`
	TCPKeepAliveInterval *badoption.Duration                `json:"tcp_keep_alive_interval,omitempty"`
	UDPFragment          *bool                              `json:"udp_fragment,omitempty"`
	NetworkStrategy      *NetworkStrategy                   `json:"network_strategy,omitempty"`
	NetworkType          *badoption.Listable[InterfaceType] `json:"network_type,omitempty"`
	FallbackNetworkType  *badoption.Listable[InterfaceType] `json:"fallback_network_type,omitempty"`
	FallbackDelay        *badoption.Duration                `json:"fallback_delay,omitempty"`
}

// Merge uses the native dialer decoder and allocates independent nested values
// for each node. Only keys explicitly present in the override replace the source.
func (o *SubscriptionDialerOverride) Apply(dial DialerOptions) (DialerOptions, error) {
	if o == nil {
		return dial, nil
	}
	base, err := json.Marshal(dial)
	if err != nil {
		return dial, err
	}
	patch, err := json.Marshal(o)
	if err != nil {
		return dial, err
	}
	var fields, overrides map[string]json.RawMessage
	if err = json.Unmarshal(base, &fields); err != nil {
		return dial, err
	}
	if err = json.Unmarshal(patch, &overrides); err != nil {
		return dial, err
	}
	for key, value := range overrides {
		fields[key] = value
	}
	merged, err := json.Marshal(fields)
	if err != nil {
		return dial, err
	}
	// Retain internal defaults which are intentionally excluded from JSON.
	result := DialerOptions{}
	result.TCPKeepAliveSystemDefaults = dial.TCPKeepAliveSystemDefaults
	result.UDPBindPort = dial.UDPBindPort
	result.UDPFragmentDefault = dial.UDPFragmentDefault
	err = json.Unmarshal(merged, &result)
	return result, err
}
