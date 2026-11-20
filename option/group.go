package option

import "github.com/sagernet/sing/common/json/badoption"

type SelectorOutboundOptions struct {
	Outbounds                 []string      `json:"outbounds,omitempty" reference:"outbound"`
	Import                    []GroupImport `json:"import,omitempty"`
	Default                   string        `json:"default,omitempty" reference:"outbound"`
	InterruptExistConnections bool          `json:"interrupt_exist_connections,omitempty"`
}

type URLTestOutboundOptions struct {
	Outbounds                 []string           `json:"outbounds,omitempty" reference:"outbound"`
	Import                    []GroupImport      `json:"import,omitempty"`
	URL                       string             `json:"url,omitempty"`
	Interval                  badoption.Duration `json:"interval,omitempty"`
	Tolerance                 uint16             `json:"tolerance,omitempty"`
	IdleTimeout               badoption.Duration `json:"idle_timeout,omitempty"`
	InterruptExistConnections bool               `json:"interrupt_exist_connections,omitempty"`
}

// GroupImport copies membership, retaining the original outbound identities.
type GroupImport struct {
	Group   string                     `json:"group" reference:"outbound"`
	Include badoption.Listable[string] `json:"include,omitempty"`
	Exclude badoption.Listable[string] `json:"exclude,omitempty"`
	Types   badoption.Listable[string] `json:"types,omitempty"`
	Match   string                     `json:"match,omitempty" enum:"name,tag"`
}
