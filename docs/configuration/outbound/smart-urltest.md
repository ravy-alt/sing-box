`smart-urltest` ranks live group members with HTTP URL tests and real UDP DNS exchanges and remembers successful routes per destination. It supports the same dynamic imports and subscription isolation as `urltest`.

### Structure

```json
{
  "type": "smart-urltest",
  "tag": "smart",
  "outbounds": ["proxy-a", "proxy-b"],
  "import": [],
  "empty_fallback": "",
  "url": "https://www.gstatic.com/generate_204",
  "udp_test": {
    "server": "8.8.8.8",
    "server_port": 53,
    "domain": "example.com",
    "timeout": "5s"
  },
  "interval": "5m",
  "tolerance": 50,
  "cache_ttl": "5m",
  "cache_capacity": 10000,
  "dial_timeout": "5s",
  "failure_window": "1m",
  "failure_threshold": 5,
  "udp_response_timeout": "5s",
  "udp_failure_window": "1m",
  "udp_min_sessions": 10,
  "udp_failure_ratio": 0.8,
  "min_test_interval": "30s",
  "test_concurrency": 10,
  "max_concurrent_dials": 64
}
```

### Membership and selection

At least one of `outbounds`, `import`, or `empty_fallback` must be nonempty. Static `outbounds` and `empty_fallback` reference declarations in the same configuration document. Use [group imports](../shared/group-import.md) for subscription members, including filters and multiple import levels. Subscription-private DNS, detours and generation leases are preserved.

`empty_fallback` is inserted only while the effective membership is empty. It is removed when regular members return; it is not a connection-failure fallback when members exist. The group may be empty without preventing startup; dialing then returns an unavailable error.

Members with successful TCP and UDP probes rank first, then members with only a successful TCP probe. A failed TCP probe excludes the member. Within a health tier, compare TCP latency first, then UDP latency for equal TCP delays. The TCP `tolerance` applies only while the current member has a successful UDP result; failed or untested UDP cannot retain a worse member inside the tolerance. The default tolerance is 50 ms; zero disables it. If no member passes both tests, TCP-only members remain usable without cycling among equally unhealthy members.

Untested members may serve startup traffic, behind members with a confirmed successful TCP test. If all measured members fail TCP, the group is temporarily unavailable; periodic tests still cover all members and restore recovered members. Candidates must also declare support for the requested network.

For each connection:

1. Use a valid cached member for the destination, otherwise the current best. Affinity cannot override a TCP failure, UDP downgrade, or an improvement in health tier.
2. Give that attempt `dial_timeout` to connect. On failure or timeout, try up to **three** other ranked candidates, excluding the first. Race candidates within the same health tier; move to a lower tier only after the higher tier fails.
3. Return and cache the first successful connection; cancel the other attempts and close their connections, including late successes. Each fallback has its own `dial_timeout`.

Existing connections are retained when rankings change. There is no retry or replay after application data has been sent. Protocols with deferred handshakes can fail on their first write: that failure invalidates the affinity and contributes to health statistics, but application bytes are not resent to a different server.

### Destination cache

`cache_ttl` is a fixed maximum lifetime, default 5 minutes. Reads and repeated successes do not extend it. Keys use the sniffed domain when available, otherwise the dialed domain or IP; domains are case-normalized, and TCP and UDP have separate bindings. Ports do not split a destination's affinity. A group used as a detour caches the actual proxy-server destination rather than the original application's domain.

`cache_capacity` bounds entries, default 10000; the oldest entry is evicted at capacity. Cache entries are in memory only. Membership revisions and interface updates invalidate affinity. A subscription refresh can replace a node without changing its tag, so revision checks also reject stale in-flight cache writes and test results. Clearing a cache prevents an already-running dial from repopulating its previous binding. An old connection's failure cannot delete a newly recreated binding with the same tag.

### Tests and health triggers

| Field | Default | Meaning |
| --- | --- | --- |
| `url` | `https://www.gstatic.com/generate_204` | Absolute HTTP(S) test URL. |
| `udp_test.server` | `8.8.8.8` | UDP DNS server IP or hostname, without a port. |
| `udp_test.server_port` | `53` | UDP DNS port. |
| `udp_test.domain` | `example.com` | Domain whose A record is queried. |
| `udp_test.timeout` | `5s` | Proxy establishment and DNS response deadline, from 1ms to 1m. |
| `interval` | `5m` | Periodic testing, including when idle. Initial startup and membership/interface changes also request tests. |
| `failure_window` | `1m` | Sliding window for business dial failures. |
| `failure_threshold` | `5` | Request a test when failures **exceed** this value, then reset the window; default triggers on the sixth failure. Failed fallback attempts count too. |
| `udp_response_timeout` | `5s` | How long a sampled UDP session can send without receiving a response. |
| `udp_failure_window` | `1m` | Sliding window for completed UDP observations. |
| `udp_min_sessions` | `10` | Minimum observations before evaluating the failure ratio. |
| `udp_failure_ratio` | `0.8` | Request a test when at least this fraction of observed sessions have no reply, then reset the sample window. |
| `min_test_interval` | `30s` | Minimum spacing between starts of automatic test rounds. API tests also advance this cooldown. |
| `test_concurrency` | `10` | Maximum concurrently tested members per group. Each member runs an HTTP probe with a 15-second limit, then, on success, a UDP probe bounded by `udp_test.timeout`. |
| `max_concurrent_dials` | `64` | Maximum active underlying business/fallback dial calls per group. Waiting for capacity shares the attempt timeout. |

Automatic requests are coalesced and rate limited. Concurrent API requests share an active round; explicit API requests may start a new round without waiting for the automatic cooldown. URL-test traffic is marked through nested groups and never contributes to business failure statistics or destination affinity. Cancelled attempts and local capacity exhaustion do not count as failed nodes.

UDP observations cover both ordinary socket methods and buffered packet methods. Idle sessions are ignored. Each session contributes at most once per UDP window; long-lived sessions are sampled again in later windows. Closing an unanswered session does not erase its pending observation. Pending observations are limited to 10000 per group, and excess observations are skipped until capacity returns. A single timer services the observations, and 64 time buckets bound sliding-window memory. The window's resolution is approximately `udp_failure_window / 64`, with a minimum of 1 ms.

Each round tests UDP on the final nodes whose TCP probes succeeded. Omitting `udp_test` still enables the defaults. Every UDP test exchanges a fresh DNS packet through that node, bypassing the core DNS cache and never falling back to TCP. Success requires a matching ID/question/type, an untruncated NOERROR response and an A answer. Errors, timeouts and empty answers fail the test. Passive UDP observations trigger another dual-protocol round without replaying application packets. Uncooperative dialers retain their concurrency slot until they actually return, preventing repeated timeout rounds from creating unbounded workers.

### API and connection chains

Clash-compatible API:

- `GET /group/{name}/delay?timeout=5000`: test the configured URL and update rankings. Supplying the same configured `url` has the same effect. Successful partial results can be returned at the request deadline while the shared round finishes in the background.
- A different `url` runs the existing independent dashboard probe, for example an IPv6 test; it does not overwrite this group's primary ranking.
- `DELETE /group/{name}/cache`: clear the named smart group's entire destination cache. Returns 204; a non-smart outbound returns 400 and a missing name returns 404.
- `DELETE /connections/{id}`: clear the connection's destination bindings in every smart group actually used by its path, then close the connection. Clearing during a pending dial also invalidates the eventual binding.
- `DELETE /connections`: clear every registered smart group's cache, including groups with no active connections, then close connections.

Names use the same public-name mapping and URL path escaping as other proxy endpoints. Native daemon single/all connection-close APIs apply the same invalidation. Ordinary connection closes leave affinity intact.

The dashboard displays the group as `SmartURLTest`. Its `now` field is the default best member. Each connection's `chains` reports the actual winning route, including cache hits, fallback members, nested selections and dial-time detours. Losing attempts never publish their speculative chain. Probes recursively pin the currently selected leaf at their start, including nested smart groups. They do not borrow a business-traffic fallback to hide failure of the tested node. Destination affinity remains subject to the health priority above.

Reused connections retain the route of their actual transport session. This covers generic multiplex, AnyTLS, Snell, SSH, HTTP/2, gRPC (including the full implementation), V2Ray QUIC, Hysteria, Hysteria2 and TUIC. Later default-member changes do not rewrite an existing session's route. Hysteria port hopping updates the route when it replaces the underlying transport. Auxiliary DNS-resolution detours are excluded from the application's connection chain.

Measurements are stored in memory only by final node and complete test URL, never as group history copies. Automatic, dashboard and IPv6 probes share this rule. Each node retains the latest result for at most 64 URLs. Failures affect only the tested URL, and a late completion from an older probe cannot overwrite a newer observation.

For A → B → C → node X, querying A reads X measured with A's URL; querying B reads X measured with B's URL. When B selects Y, A immediately reads Y with A's URL. An unmeasured combination has no result and does not borrow X's latency or another URL. Rankings also use current leaves and the group's own URL.

Proxy, group and proxy-provider GET endpoints accept `url` to scope `history`. Without it, a group uses its configured URL and a leaf uses the default URL. `extra` contains per-URL histories, `testUrl` reports the configured URL, and each record's `outbound` is the measured leaf's public name. Provider health checks also accept `url` and `timeout`.

Independent dashboard probes obey the requested `timeout`, including HTTP deadlines longer than 15 seconds. Automatic probes retain their 15-second per-node deadline. Cancelling a request preserves previous measurements; dials that finish after the deadline close their connections and never publish late results. Independent dashboard probes retain at most 256 active background operations per process, and timing out while waiting for local capacity is not recorded as a node failure.

### UDP probe API

`GET /proxies/{name}/udp-delay?server=8.8.8.8&port=53&domain=example.com&timeout=5000` performs a real UDP DNS exchange (timeout in milliseconds). Omitted parameters use the Smart group configuration or the node defaults. It recursively pins the current TCP-selected leaf and probes UDP through that exact node, without borrowing a fallback. Responses include `alive`, `delay`, `time`, `outbound` and probe parameters. A completed failed test returns HTTP 200 with `alive: false`; invalid parameters return 400, changed membership 409, and inability to start 503.

`/version` advertises `udp-probe-v1`. Proxy metadata includes `udpTest` settings and the current leaf’s `udpHistory`. UDP histories are keyed by node, server, port, domain and timeout, bounded to 64 profiles per node. Node retirement clears them; old-generation completions cannot overwrite replacements, and cancellation preserves prior observations. The legacy `udp` field remains a protocol declaration. The dashboard uses measured history to show unknown, successful latency or failure; clicking it performs a new probe. Normal node, group and provider dashboard tests include UDP probes too.
