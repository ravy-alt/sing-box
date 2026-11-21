# Subscription

For new configurations, use top-level [subscriptions](../subscriptions.md) to separate sources from selection groups. This page describes the compatible legacy outbound form; it also supports the documented `override_dialer` fields.

A subscription is a stable outbound group whose members are downloaded from a native sing-box JSON document. Nodes, internal groups, DNS servers and DNS rules are validated and published together without restarting the instance.

`empty_fallback` supports an [empty-group fallback](../shared/group-import.md#empty_fallback) declared in the current configuration document. It is inserted only when the loaded document and filtered imports provide no members.

## Structure

```json
{
  "type": "subscription",
  "tag": "subscription-a",
  "url": "https://example.com/subscription.json",
  "http_client": "download",
  "bootstrap_http_client": {},
  "cache_path": "subscription-a.cache",
  "update_interval": "24h",
  "download_timeout": "30s",
  "max_download_size": 16777216,
  "max_cache_size": 67108864,
  "drain_timeout": "0s",
  "outbounds": ["exit"],
  "import": [],
  "default": "exit",
  "mode": "selector",
  "test_url": "https://www.gstatic.com/generate_204",
  "test_interval": "3m",
  "tolerance": 50
}
```

`url` is required and must use HTTP or HTTPS. `http_client` accepts a shared HTTP client tag or inline [HTTP client](../shared/http-client.md) options. An omitted client inherits the host default HTTP client; an explicit `{}` dials directly using the host DNS configuration.

`bootstrap_http_client` is optional. If the regular download fails, this explicitly configured client is tried. Its outbound and DNS dependencies must be available independently of the subscription being bootstrapped. No implicit direct fallback is performed.

`cache_path` optionally stores the complete last successfully applied document, ETag, imported selector choices, nested subscriptions and downloaded remote rule sets. The cache is associated with the URL and written through a temporary file and rename. Parent directories are created with mode 0700 and cache files with mode 0600. Valid cached snapshots are restored before any initial downloads. An invalid, incompatible or missing cache triggers an initial download.

`update_interval` defaults to 24 hours. Stale restored caches are refreshed after startup. `download_timeout` defaults to 30 seconds per download attempt and is also the group health-check time limit. Failed updates preserve the active snapshot and are retried on the next interval or manual refresh.

`max_download_size` limits the decoded response body and defaults to 16 MiB, with a maximum of 1 GiB. HTTP 304 retains the current snapshot. Unsuccessful responses, malformed configurations, missing dependencies, cycles and initialization errors do not replace it.

`drain_timeout` limits how long an obsolete snapshot may retain existing connections. The default `0s` waits for them to close. Set a positive duration to bound retention; remaining connections are closed when that duration expires. Closing the sing-box instance cancels outstanding work, force-closes all generations and waits for cleanup.

`outbounds` selects group members by their original tags. When omitted, every imported outbound and endpoint is selectable. Dependencies are retained even when not selectable. `default` is an original tag or the complete tag of a member added through `import`; a previous selection is retained when still present, otherwise the configured default or first member is selected.

`mode` is `selector` (default) or `urltest`. Both support manual selection and health checks. `urltest` probes immediately after successful refresh, clears obsolete latency records, and periodically selects the fastest tested member for each supported network, with `tolerance` in milliseconds (default 50). `test_interval` defaults to 3 minutes. The standard URL test URL is used when `test_url` is omitted. UDP selection uses TCP health-check results for UDP-capable members; it is not a UDP reachability probe.

`import` supports filtered [group imports](../shared/group-import.md). Imported members are appended to this subscription’s own members and follow all source refreshes, including multi-level import chains. `outbounds` filters only the downloaded document; `default` can also name an imported member by its complete tag.

## Subscription document

```json
{
  "dns": {
    "servers": [
      { "type": "udp", "tag": "resolver", "server": "1.1.1.1" }
    ]
  },
  "outbounds": [
    {
      "type": "socks",
      "tag": "relay",
      "server": "relay.example.com",
      "server_port": 1080,
      "domain_resolver": "resolver"
    },
    {
      "type": "socks",
      "tag": "exit",
      "server": "exit.example.com",
      "server_port": 1080,
      "detour": "relay"
    }
  ]
}
```

Outbounds and endpoints share a tag namespace. DNS servers, HTTP clients, rule sets, network namespaces and certificate providers each have their own namespace. Tags must be unique within each namespace. As in native configuration, omitted outbound, endpoint, DNS server and certificate-provider tags use their array indices starting at `0`. Explicit names avoid reference changes when reordering. HTTP clients, rule sets and network namespaces still require explicit tags. References resolve locally first and are rewritten to isolated tags. Selector and URL test `outbounds` lists must reference declarations in the same document; use `import` for membership across subscription boundaries. Other resource references absent from the document may use existing host or parent resources; unresolved references fail validation.

Every possible group member participates in cycle validation, including DNS-to-outbound and outbound-to-DNS dependencies. IP-only tunnel peers do not introduce implicit DNS dependencies. Before publication, validation combines host and other loaded providers’ dependencies. Validation and publication are serialized so concurrent updates cannot jointly create a cycle. Initial loading may stage references to providers that have not loaded yet; the complete resource tree is validated before serving traffic. This allows mutual references with an acyclic node graph while still rejecting missing nodes and real cycles. Hot updates revalidate the complete candidate tree before publication. Runtime guards track individual nodes; acyclic reentry through a different member retains the generation, group selection, and network capabilities already acquired by that dial.

Imported DNS has a private scope. Server options, resolver chains, rules, default server and client options are preserved without replacing host DNS. Explicit references to external DNS servers reuse those servers. `route.default_domain_resolver` is retained; nodes with no explicit resolver use the source DNS rules/default when provided, otherwise they inherit their parent's DNS behavior. FakeIP is intended for intercepted application traffic, not resolving proxy server addresses.

Supported native resources include:

* `outbounds` and `endpoints`, including selectors, URL test groups and nested subscriptions. Protocol availability follows the binary's build tags.
* `dns` and inline, local or remote `route.rule_set` definitions. URL/path `{tag}` templates use the original source tags.
* `http_clients`, `route.default_http_client` and `route.final` for source resource downloads.
* `certificate`, `certificate_providers` and `network_namespaces`, including references to existing external resources.
* Default resolver, interface, routing mark, network selection and fallback dial options from `route`.

This imports nodes and their dependencies. Source global inbounds, traffic route rules and services are not installed, and host system routing/interface monitoring policies are not changed. File paths resolve against sing-box's configuration base directory. Endpoints retain their native listening-port, system-interface and state-directory constraints. If an exclusive resource cannot coexist across generations, refresh fails and retains the previous generation.

Nested subscriptions use the same download, bootstrap, cache and rollback mechanisms. Repeating a subscription URL in its ancestor chain is rejected. Parent refresh builds a new resource tree; a child can also refresh independently. Inside a source document, `subscription/child/exit` references a child member and is recursively namespaced on import. Child and rule-set updates persist into the parent cache, allowing the committed tree to restart offline with just the parent cache. Imported selector choices are retained across refresh and cache restoration when still valid.

Clash API, the native API and client interfaces prefer the original node or group name from the subscription. Chinese text, spaces and special characters remain readable. When a name conflicts with a static configuration tag or another subscription member, `-1`, `-2`, and subsequent suffixes are tried until a free name is found. Static tags, including an automatically created `direct` outbound, keep priority, and new original names in the same publication are reserved before generated suffixes.

Names are bound to internal identities. Refreshes, configuration changes to a same-tag node, and removal followed by reintroduction retain the assigned name. Retired names remain reserved rather than being reassigned to a different node. Assignments are stored in subscription caches; restoring those caches preserves suffixes. Without a cache, a new process allocates from its current configuration, loading order and membership. A rejected parent update does not reserve names from candidate children. Duplicate original tags within a single source document remain invalid native configuration.

Group lists, current selections, test results, provider lists and connection chains share these names. Control requests use names returned by the API. HTTP path escaping is still applied once for transport, not included in displayed names: the literal name `node%41` uses the path segment `node%2541` and identifies a different node from `nodeA`. Configuration references, filters and proxy chains continue using the stable identities described below.

Members retain internal stable identities of the form `subscription/<escaped-group-tag>/<escaped-node-tag>`. For example, `sub`'s `exit` is `subscription/sub/exit`; `exit` in its child subscription `child` is `subscription/subscription%2Fsub%2Fchild/exit`. Selector and URL test `outbounds` lists cannot directly reference these members from another subscription document. Use `import` to obtain members, or reference a subscription group declared in the current document through `outbounds`. Routing, including `route.final`, can still reference stable member tags directly; a removed member fails explicitly. Referencing the subscription group instead allows the group to apply its selection fallback.

The Clash API and daemon group APIs expose imported groups for inspection and selection. Provider APIs also expose nested subscriptions. Encode the complete name again when using it as an HTTP path parameter. Administrative handles resolve the active generation while traffic retains connection leases.

`max_cache_size` bounds the aggregate cache file, defaulting to the greater of 64 MiB or twice `max_download_size`, with a 2 GiB maximum. Cache write failure is logged and retains the previous file; a successfully activated generation stays active. `max_download_size` applies to individual subscription documents. Neither limit changes the native rule-set downloader's own behavior.

Only native sing-box JSON (including configuration comments) is accepted. Clash YAML, URI lists and Base64 subscription conversion are outside this feature.

## Download through the subscription itself

```json
{
  "http_clients": [
    { "tag": "download", "detour": "proxy" }
  ],
  "outbounds": [
    {
      "type": "subscription",
      "tag": "subscription-a",
      "url": "https://example.com/subscription.json",
      "http_client": "download",
      "bootstrap_http_client": {},
      "cache_path": "subscription-a.cache"
    },
    { "type": "selector", "tag": "proxy", "outbounds": ["subscription-a"] }
  ],
  "route": { "final": "proxy" }
}
```

During refresh, the current snapshot remains usable while the next one downloads and initializes. Downloads use independently owned transports so their connection pools can be released without resetting other HTTP clients. Their active connections retain the snapshot they dialed through.

At cold start, all caches are restored first. Subscriptions without caches are initialized in passes, allowing a ready subscription to bootstrap another. A dependency cycle with no usable cache or independent bootstrap path fails startup with an actionable error instead of waiting indefinitely. An explicitly direct bootstrap client still needs an independently usable DNS path.

Inline DNS rules are active during bootstrap. Remote rule-sets are loaded afterwards, so the bootstrap resolver must not depend on an unloaded remote rule-set. Set an explicit `domain_resolver` on the bootstrap HTTP client when necessary. Imported members support TCP and UDP.

## Update semantics and limits

* Retained member handles dial the new implementation after a same-tag update. Deleted members fail explicitly; reintroducing the same tag makes that handle usable again.
* Explicit `{"outbounds":[]}` or `{"endpoints":[]}` clears its own document members; members contributed by `import` remain. If the combined list is empty, it uses the configured `empty_fallback`, or cannot dial if none is configured. `ready` means a document was loaded, not that it has reachable members. A document missing both fields is rejected.
* Selector and URL test `outbounds` lists are checked statically against declarations in the same document during configuration checking or subscription loading. Members inside other subscriptions are rejected. Removing a node still referenced by a local group rejects the update and retains the old version. Use `import` to follow another group's members dynamically, or reference a subscription group declared in the current document through `outbounds`. A subscription's own `outbounds` whitelist selects declarations in its downloaded document and rejects updates missing required members.
* Cycle validation is conservative: it includes all group alternatives, DNS rule targets and known implicit resolver dependencies. It does not prove conditional rules mutually exclusive and may reject an otherwise usable conditional DNS setup. An explicit independent `domain_resolver` makes the bootstrap dependency unambiguous.
* Member wrappers support TCP/UDP and `preferred_by` queries. Native endpoint ICMP/Flow forwarding interfaces are not forwarded; imported endpoints do not provide every capability of statically configured endpoints.
* Retired trees stop automatic nested-subscription updates. Cleanup runs asynchronously and shutdown waits for completion. TCP half-close and early-handshake capabilities are preserved.
* A superseded parent rejects further child publications. Retired generations stop subscription refreshes, membership watchers, and probes while existing connections drain. Shutdown also waits for admitted manual probes to exit and release their generation references. Closed generations leave the retired list without waiting for another refresh.
* `drain_timeout: 0` has no resource-retention bound while connections remain open. A positive timeout closes remaining connections when it expires.

## Clash API

* `GET /providers/proxies`: subscription status and member lists.
* `GET /providers/proxies/{name}`: readiness, generation, last update, last error and members.
* `PUT /providers/proxies/{name}`: refresh and wait for completion.
* `GET /providers/proxies/{name}/healthcheck`: run a group health check.
* `PUT /proxies/{name}` with `{"name":"Hong Kong-1"}`: select a member using its name returned by the API.

The existing Clash API authentication applies. Concurrent updates to one subscription are serialized; old health-check results cannot change a newer generation's selection.
