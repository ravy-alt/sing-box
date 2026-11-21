# Subscription sources

Top-level `subscriptions` load native sing-box JSON documents, cache accepted generations and update without restarting the core. A source is not a routing target: consume its members with `import` in selector, urltest or another source. Sources appear in the dashboard's providers view, not in `/proxies`, `GLOBAL`, `/group` or native group lists. Imported nodes and actual groups remain visible. Legacy `outbounds[].type: subscription` remains compatible.

```json
{
  "dns": {
    "servers": [
      { "type": "local", "tag": "local" },
      { "type": "https", "tag": "private-a", "server": "192.0.2.10", "tls": { "server_name": "dns-a.example.com" } },
      { "type": "https", "tag": "private-b", "server": "192.0.2.11", "tls": { "server_name": "dns-b.example.com" } },
      { "type": "race", "tag": "private-race", "servers": ["private-a", "private-b"], "timeout": "5s" }
    ]
  },
  "subscriptions": [{
    "tag": "airport",
    "url": "https://example.com/sub-store.json",
    "http_client": {},
    "cache_path": "subscriptions/airport.json",
    "update_interval": "24h",
    "drain_timeout": "0s",
    "override_dialer": {
      "domain_resolver": { "server": "private-race", "strategy": "prefer_ipv4" },
      "connect_timeout": "10s"
    }
  }],
  "outbounds": [
    { "type": "direct", "tag": "direct" },
    { "type": "urltest", "tag": "Hong Kong", "import": [{ "group": "airport", "include": ["香港|Hong Kong"], "match": "name" }], "empty_fallback": "direct" },
    { "type": "selector", "tag": "proxy", "outbounds": ["Hong Kong", "direct"] }
  ],
  "route": { "final": "proxy", "default_domain_resolver": "local" }
}
```

Replace the example addresses. DoH hostnames need independent bootstrap resolution; an IP address with `tls.server_name` avoids that dependency. Do not route private DNS through a group whose proxy addresses require that same DNS service.

## Fields

`tag` is required and shares uniqueness constraints with local outbound/endpoint/source declarations. Download and lifecycle fields are the same as the [subscription engine](outbound/subscription.md): `url`, `http_client`, `bootstrap_http_client`, `cache_path`, `update_interval`, `download_timeout`, `max_download_size`, `max_cache_size`, and `drain_timeout`. An omitted client inherits the default; `{}` explicitly dials directly. A download through its own unloaded members needs a usable cache, independent bootstrap client, or usable fallback in the download group; failure does not silently switch to direct routing.

HTTP downloaders do not require `bind_interface`; an unbound client follows native default dialer routing. TUN configurations still need interface detection, routing marks or equivalent routing to avoid loops.

Without a usable cache, failure of the download and any configured bootstrap client leaves the subscription empty while core startup continues. The provider reports `ready: false` and `lastError`; no synthetic empty cache is written.

Background retries begin after 5 seconds and back off through 10, 20, 40 and 60 seconds, capped at 60 seconds and the configured `update_interval`. Success restores the normal update schedule and updates all importing groups: implicit `empty_fallback` members disappear as soon as matching nodes become available. Groups that remain empty after filtering retain their fallbacks; explicit static members are unchanged.

Failed refreshes retain existing nodes and retry automatically. Invalid configuration, invalid documents, dependency cycles and startup cancellation are still rejected.

`outbounds` selects original source member tags while preserving dependencies. `import` appends live members from other groups or sources, with the same [filters](shared/group-import.md). Downloaded documents may contain nested top-level `subscriptions`; parent publication validates the whole resource tree. Sources do not have `mode`, `default`, `tolerance` or `empty_fallback`: configure these on the consuming selector/urltest.

`test_url` defaults to `https://www.gstatic.com/generate_204`. A positive `test_interval` enables periodic and post-refresh health checks; omission or `0s` leaves checks manual. Source health checks do not select a proxy for consuming groups.

## Dial overrides

`override_dialer` modifies only downloaded outbounds/endpoints supporting native dial options. It does not modify DNS servers, HTTP downloaders, groups, nested subscription declarations or externally imported nodes. Configure nested sources' overrides on their own declarations.

Supported fields:

* `detour`: replaces the original chain dialer; an empty string clears it and omission preserves it.
* `domain_resolver`: native string/object form; replaces the complete resolver and deprecated `domain_strategy`.
* `connect_timeout`.
* `bind_interface`, `inet4_bind_address`, `inet6_bind_address`, `bind_address_no_port`.
* `routing_mark`, `reuse_addr`.
* `tcp_fast_open`, `tcp_multi_path`, `disable_tcp_keep_alive`, `tcp_keep_alive`, `tcp_keep_alive_interval`.
* `udp_fragment`.
* `network_strategy`, `network_type`, `fallback_network_type`, `fallback_delay`.

Unspecified fields retain source values; explicit false, zero, empty strings and empty arrays replace corresponding values. Null behaves like omission. Resolver/address overrides require valid new values. Negative timeouts are rejected. Resolver references belong to the declaring scope (host DNS for a top-level source, parent DNS for a nested source); downloaded same-named DNS cannot shadow them. Overrides run before initialization/graph validation and are reapplied from current settings when restoring raw cached documents.

Detour references also belong to the declaring scope and cannot be shadowed by downloaded nodes. An explicit override replaces existing chains, including the dialers of relay nodes in the document. The target may be a selector/urltest, but must not be a source or depend on the overridden subscription through imports or other references. Such cycles are rejected at startup or refresh. For example, `override_dialer: {"detour":"equalvpn-upstream"}` can target a selector containing `direct`, `bw-hy2` and imports from other subscriptions, excluding equalvpn itself.

Protocol addresses, authentication, TLS and multiplexing are not bulk-overridden. Native dial semantics still apply: local socket options affect the node establishing the underlying connection, while an explicit resolver can resolve a hostname before a detour.

## Updates and APIs

DNS race members, DNS bootstrap/detour references, proxy chains and imports share startup/update dependency checks. Candidate publication is serialized across providers. Failed updates preserve the previous generation and metadata; new connections use successfully published nodes. Zero drain timeout waits for all old connection leases; positive timeout force-closes remaining old connections. Consuming groups' connection interruption settings still apply. Names and filters retain the subscription engine's Unicode/collision behavior.

Clash-compatible provider endpoints used by zashboard:

| Endpoint | Result |
| --- | --- |
| `GET /providers/proxies` | Root and nested providers |
| `GET /providers/proxies/{name}` | Members, status, generation, update time, last error, `testUrl` |
| `PUT /providers/proxies/{name}` | Refresh and wait |
| `GET /providers/proxies/{name}/healthcheck` | Check all members; 204 on success |
| `GET /providers/proxies/{name}/{proxy}/healthcheck?url=...&timeout=5000` | Probe an actual provider member; JSON `delay`; timeout is 1–60000 milliseconds |

Encode each returned name once as a URL path component. Responses use `vehicleType: HTTP`. Valid `Subscription-Userinfo` response headers populate Upload/Download/Total/Expire in `subscriptionInfo`, persisted with accepted generations. Results from obsolete probes do not replace current-generation latency history.

## Migration

Move the legacy subscription object to `subscriptions`, remove `type` and selection-only fields, and replace fixed references to it with `import: [{"group":"source-tag"}]`. If the old subscription selected nodes or provided an empty fallback, create a separate selector/urltest for that behavior and route to it. Existing regional imports and filters can retain their source tag unchanged.
