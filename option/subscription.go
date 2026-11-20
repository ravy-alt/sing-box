package option

import "github.com/sagernet/sing/common/json/badoption"

// SubscriptionOutboundOptions describes a group backed by a native sing-box document.
type SubscriptionOutboundOptions struct {
	SourceOnly          bool                        `json:"-"`
	OverrideDialer      *SubscriptionDialerOverride `json:"override_dialer,omitempty"`
	URL                 string                      `json:"url"`
	HTTPClient          *HTTPClientOptions          `json:"http_client,omitempty"`
	BootstrapHTTPClient *HTTPClientOptions          `json:"bootstrap_http_client,omitempty"`
	CachePath           string                      `json:"cache_path,omitempty"`
	UpdateInterval      badoption.Duration          `json:"update_interval,omitempty"`
	DownloadTimeout     badoption.Duration          `json:"download_timeout,omitempty"`
	MaxDownloadSize     int64                       `json:"max_download_size,omitempty"`
	MaxCacheSize        int64                       `json:"max_cache_size,omitempty"`
	DrainTimeout        badoption.Duration          `json:"drain_timeout,omitempty"`
	Outbounds           []string                    `json:"outbounds,omitempty"`
	Import              []GroupImport               `json:"import,omitempty"`
	EmptyFallback       string                      `json:"empty_fallback,omitempty" reference:"outbound"`
	Default             string                      `json:"default,omitempty"`
	Mode                string                      `json:"mode,omitempty" enum:"selector,urltest"`
	TestURL             string                      `json:"test_url,omitempty"`
	TestInterval        badoption.Duration          `json:"test_interval,omitempty"`
	Tolerance           uint16                      `json:"tolerance,omitempty"`
}

// SubscriptionOptions is a data source. Its members are consumed through import;
// the source itself is never a routing target or a selectable proxy group.
type SubscriptionOptions struct {
	Tag                 string                      `json:"tag"`
	URL                 string                      `json:"url"`
	HTTPClient          *HTTPClientOptions          `json:"http_client,omitempty"`
	BootstrapHTTPClient *HTTPClientOptions          `json:"bootstrap_http_client,omitempty"`
	CachePath           string                      `json:"cache_path,omitempty"`
	UpdateInterval      badoption.Duration          `json:"update_interval,omitempty"`
	DownloadTimeout     badoption.Duration          `json:"download_timeout,omitempty"`
	MaxDownloadSize     int64                       `json:"max_download_size,omitempty"`
	MaxCacheSize        int64                       `json:"max_cache_size,omitempty"`
	DrainTimeout        badoption.Duration          `json:"drain_timeout,omitempty"`
	Outbounds           []string                    `json:"outbounds,omitempty"`
	Import              []GroupImport               `json:"import,omitempty"`
	OverrideDialer      *SubscriptionDialerOverride `json:"override_dialer,omitempty"`
	TestURL             string                      `json:"test_url,omitempty"`
	TestInterval        badoption.Duration          `json:"test_interval,omitempty"`
}
