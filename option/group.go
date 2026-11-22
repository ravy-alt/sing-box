package option

import "github.com/sagernet/sing/common/json/badoption"

type SelectorOutboundOptions struct {
	Outbounds                 []string      `json:"outbounds,omitempty" reference:"outbound"`
	Import                    []GroupImport `json:"import,omitempty"`
	EmptyFallback             string        `json:"empty_fallback,omitempty" reference:"outbound"`
	Default                   string        `json:"default,omitempty" reference:"outbound"`
	InterruptExistConnections bool          `json:"interrupt_exist_connections,omitempty"`
}

type URLTestOutboundOptions struct {
	Outbounds                 []string           `json:"outbounds,omitempty" reference:"outbound"`
	Import                    []GroupImport      `json:"import,omitempty"`
	EmptyFallback             string             `json:"empty_fallback,omitempty" reference:"outbound"`
	URL                       string             `json:"url,omitempty"`
	Interval                  badoption.Duration `json:"interval,omitempty"`
	Tolerance                 uint16             `json:"tolerance,omitempty"`
	IdleTimeout               badoption.Duration `json:"idle_timeout,omitempty"`
	InterruptExistConnections bool               `json:"interrupt_exist_connections,omitempty"`
}

// SmartURLTestOutboundOptions keeps destination affinity separate from the
// latency measurement used to rank the group's live members.
type SmartURLTestOutboundOptions struct {
	Outbounds          []string           `json:"outbounds,omitempty" reference:"outbound"`
	Import             []GroupImport      `json:"import,omitempty"`
	EmptyFallback      string             `json:"empty_fallback,omitempty" reference:"outbound"`
	URL                string             `json:"url,omitempty"`
	Interval           badoption.Duration `json:"interval,omitempty"`
	Tolerance          *uint16            `json:"tolerance,omitempty"`
	CacheTTL           badoption.Duration `json:"cache_ttl,omitempty"`
	CacheCapacity      int                `json:"cache_capacity,omitempty"`
	DialTimeout        badoption.Duration `json:"dial_timeout,omitempty"`
	FailureWindow      badoption.Duration `json:"failure_window,omitempty"`
	FailureThreshold   int                `json:"failure_threshold,omitempty"`
	UDPResponseTimeout badoption.Duration `json:"udp_response_timeout,omitempty"`
	UDPFailureWindow   badoption.Duration `json:"udp_failure_window,omitempty"`
	UDPMinSessions     int                `json:"udp_min_sessions,omitempty"`
	UDPFailureRatio    float64            `json:"udp_failure_ratio,omitempty"`
	MinTestInterval    badoption.Duration `json:"min_test_interval,omitempty"`
	TestConcurrency    int                `json:"test_concurrency,omitempty"`
	MaxConcurrentDials int                `json:"max_concurrent_dials,omitempty"`
}

// GroupImport copies membership, retaining the original outbound identities.
type GroupImport struct {
	Group   string                     `json:"group" reference:"outbound"`
	Include badoption.Listable[string] `json:"include,omitempty"`
	Exclude badoption.Listable[string] `json:"exclude,omitempty"`
	Types   badoption.Listable[string] `json:"types,omitempty"`
	Match   string                     `json:"match,omitempty" enum:"name,tag"`
}
