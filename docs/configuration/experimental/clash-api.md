!!! quote "Changes in sing-box 1.10.0"

    :material-plus: [access_control_allow_origin](#access_control_allow_origin)  
    :material-plus: [access_control_allow_private_network](#access_control_allow_private_network)

!!! quote "Changes in sing-box 1.8.0"

    :material-delete-alert: [store_mode](#store_mode)  
    :material-delete-alert: [store_selected](#store_selected)  
    :material-delete-alert: [store_fakeip](#store_fakeip)  
    :material-delete-alert: [cache_file](#cache_file)  
    :material-delete-alert: [cache_id](#cache_id)

### Structure

=== "Structure"

    ```json
    {
      "external_controller": "127.0.0.1:9090",
      "external_ui": "",
      "external_ui_download_url": "",
      "external_ui_download_detour": "",
      "secret": "",
      "default_mode": "",
      "access_control_allow_origin": [],
      "access_control_allow_private_network": false,
      
      // Deprecated
      
      "store_mode": false,
      "store_selected": false,
      "store_fakeip": false,
      "cache_file": "",
      "cache_id": ""
    }
    ```

=== "Example (online)"

    !!! question "Since sing-box 1.10.0"

    ```json
    {
      "external_controller": "127.0.0.1:9090",
      "access_control_allow_origin": [
        "http://127.0.0.1",
        "http://yacd.haishan.me"
      ],
      "access_control_allow_private_network": true
    }
    ```

=== "Example (download)"

    !!! question "Since sing-box 1.10.0"

    ```json
    {
      "external_controller": "0.0.0.0:9090",
      "external_ui": "dashboard"
      // "external_ui_download_detour": "direct"
    }
    ```

!!! note ""

    You can ignore the JSON Array [] tag when the content is only one item

### Fields

#### Dashboard delay tests and node identity

`GET /group/{name}/delay?url=...&timeout=...` probes each immediate member using
the requested HTTP or HTTPS URL, independently of automatic URL tests. Nested
groups use their selected path. Members start concurrently, and successful
results are returned even if other members time out. `timeout` is in milliseconds
and must be between 1 and 60000. If every member fails, the endpoint returns 504.
Results from a replaced subscription generation are discarded.

Proxy objects also include the opaque `x-sing-box-revision` extension. It changes
on core restart and on subscription graph publication, including changes to
imported nodes, dialers and DNS resources. Clients must combine it with the
backend identity and proxy name when caching capabilities such as IPv6 reachability.
A graph update invalidates all capability results for that backend, even when a
particular node's name is unchanged. The extension contains no configuration secrets.

#### external_controller

RESTful web API listening address. Clash API will be disabled if empty.

#### external_ui

A relative path to the configuration directory or an absolute path to a
directory in which you put some static web resource. sing-box will then
serve it at `http://{{external-controller}}/ui`.

#### external_ui_download_url

ZIP download URL for the external UI, will be used if the specified `external_ui` directory is empty.

`https://github.com/MetaCubeX/Yacd-meta/archive/gh-pages.zip` will be used if empty.

#### external_ui_download_detour

The tag of the outbound to download the external UI.

Default outbound will be used if empty.

#### secret

Secret for the RESTful API (optional)
Authenticate by spedifying HTTP header `Authorization: Bearer ${secret}`
ALWAYS set a secret if RESTful API is listening on 0.0.0.0

#### default_mode

Default mode in clash, `Rule` will be used if empty.

This setting has no direct effect, but can be used in routing and DNS rules via the `clash_mode` rule item.

#### access_control_allow_origin

!!! question "Since sing-box 1.10.0"

CORS allowed origins, `*` will be used if empty.

To access the Clash API on a private network from a public website, you must explicitly specify it in `access_control_allow_origin` instead of using `*`.

#### access_control_allow_private_network

!!! question "Since sing-box 1.10.0"

Allow access from private network.

To access the Clash API on a private network from a public website, `access_control_allow_private_network` must be enabled.

#### store_mode

!!! failure "Deprecated in sing-box 1.8.0"

    `store_mode` is deprecated in Clash API and enabled by default if `cache_file.enabled`.

Store Clash mode in cache file.

#### store_selected

!!! failure "Deprecated in sing-box 1.8.0"

    `store_selected` is deprecated in Clash API and enabled by default if `cache_file.enabled`.

!!! note ""

    The tag must be set for target outbounds.

Store selected outbound for the `Selector` outbound in cache file.

#### store_fakeip

!!! failure "Deprecated in sing-box 1.8.0"

    `store_selected` is deprecated in Clash API and migrated to `cache_file.store_fakeip`.

Store fakeip in cache file.

#### cache_file

!!! failure "Deprecated in sing-box 1.8.0"

    `cache_file` is deprecated in Clash API and migrated to `cache_file.enabled` and `cache_file.path`.

Cache file path, `cache.db` will be used if empty.

#### cache_id

!!! failure "Deprecated in sing-box 1.8.0"

    `cache_id` is deprecated in Clash API and migrated to `cache_file.cache_id`.

Identifier in cache file.

If not empty, configuration specified data will use a separate store keyed by it.
## DNS management extension

This branch advertises `dns-management-v1` in the `capabilities` array of `GET /version`. Clients must check this capability before using the extension. Existing Clash API authentication applies, including the `token` query parameter for WebSockets.

| Endpoint | Purpose |
| --- | --- |
| `GET /dns/cache` | Snapshot of usable cache entries |
| `WS /dns/cache` | Initial snapshot, then one-second deltas/heartbeats |
| `DELETE /dns/cache` | Clear caches in all scopes |
| `DELETE /dns/cache?id=…` | Delete one opaque cache ID |
| `DELETE /dns/cache?scope=…&server=…` | Clear one server's cache in a scope |
| `GET /dns/servers` | Server addresses, types, dialer chains, resolvers and cache counts |
| `GET /dns/rules` | DNS conditions and actions, including the default rule |
| `GET /dns/query?name=…&type=A&scope=…&server=…` | Rule-based or explicitly selected-server query; adds actual `Server` and `Trace` to the existing response |

The empty scope denotes the host configuration. Subscription DNS routers have separate scopes; preserve returned scope, tag and ID values when addressing them. Omitting `server` enables rule matching. The dashboard issues separate A and AAAA queries by default, each with its own result trace.

`dialerChain` follows the DNS transport's primary network: TCP for TCP/DoT/DoH, UDP for UDP/DoQ/DoH3. Each proxy's encapsulation determines the next hop's network, including UoT, multiplexing and QUIC transports. Nested groups follow their current selection for that network. Subscription inspection retains the declaring generations while walking private groups and detours, so a concurrent refresh cannot replace part of the chain with another generation.

Entries represent actual memory or persistent caches, not a query log. Their `request` describes the latest request using the entry. Source/destination ports belong to the received DNS request, not the upstream socket. Internal proxy-server and DNS-server hostname lookups have distinct purposes and no client ports. Older persistent entries have unknown request metadata until used again.

`server` identifies the cache owner; `request.server` identifies a race transport's winning upstream. A null `expiresAt` means expiry is disabled. Usable optimistic entries remain visible with `stale: true`; unusable entries disappear. Background refresh preserves the latest request metadata. Deletion prevents queries that started before the operation from resurrecting cleared answers; new queries may cache again.

Stream frames contain `{full, revision, entries, removed}`. Replace the local view on `full: true`; otherwise upsert entries by ID and apply removals. Revisions start at 1 on each connection. Never debounce or discard deltas; reconnect for a full snapshot after a gap. Close the stream when pausing, switching backends or leaving the page. The server detects disconnects/shutdown, bounds write time and does not queue unbounded events for slow clients.

The root domain is returned as `"."`. `answers` retains full DNS resource records; `answerData` contains the corresponding RDATA strings without names, TTLs or classes, suitable for one-record-per-line display. The TTL in a raw record is its stored value; use `expiresAt` for the remaining cache lifetime.
