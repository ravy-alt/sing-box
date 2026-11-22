# Group imports

`selector`, `urltest`, `smart-urltest`, and `subscription` support `import`. Each entry reads the current members of another group, filters them, and appends them to the destination group. The source may be any of these group types, including an internal group exposed by a subscription.

```json
{
  "type": "selector",
  "tag": "regional",
  "empty_fallback": "direct",
  "import": [
    {
      "group": "subscription-a",
      "include": ["(?i)hong kong", "香港"],
      "exclude": ["expired", "到期"],
      "types": ["shadowsocks", "trojan"],
      "match": "name"
    },
    { "group": "another-group" }
  ]
}
```

For selectors and URL test groups, at least one of `outbounds`, `import`, or `empty_fallback` must be nonempty. A subscription still requires its URL: imports supplement the members selected from its downloaded document. Its `outbounds` whitelist applies only to that document.

| Field | Meaning |
| --- | --- |
| `group` | Required source group tag. Inside a subscription document, local references are namespaced automatically. |
| `include` | Regular expression or list of expressions. At least one must match; omitted or empty accepts all names. |
| `exclude` | Regular expression or list of expressions. Any match rejects the member, even if `include` matches. |
| `types` | Protocol type or list of types, such as `shadowsocks`, `trojan`, or `selector`. Omitted or empty accepts all types. |
| `match` | `name` (default) matches the original node name, with the subscription namespace removed and URL escaping decoded. `tag` matches the complete outbound tag. |

Public names omit subscription prefixes and may receive collision suffixes. `match: "name"` still matches the original source name; `match: "tag"` still matches the complete internal identity. Display names and suffix allocation do not change filtering results.

Expressions use Go regular expression syntax and are case sensitive unless they include `(?i)`. Use `^` and `$` for exact matches. Invalid expressions fail configuration validation. Name and type restrictions must both match; filters apply only to members from that import entry.

Explicit members come first, followed by imports in configuration order and each source's member order. Duplicate complete outbound tags are included once, keeping their first position. Nodes with the same original name from different subscriptions remain distinct. Imports preserve existing identities, proxy chains, private DNS, and connection leases; they do not register copies of nodes.

An import copies the source's immediate effective member list, including members that source obtained through its own imports. A member that is itself a group remains a group; it is not recursively flattened into its children. To import its children, reference that group in another import entry. Import chains can have multiple levels.

## empty_fallback

`selector`, `urltest`, `smart-urltest`, and `subscription` accept `empty_fallback`, an outbound, endpoint, or group tag declared in the same configuration document. It is a group-level field alongside `import`, not a filter property, and cannot directly reference another subscription's internal member.

Static members (a subscription's downloaded members) and all filtered imports are combined first. The fallback is inserted only if that result is empty, and removed immediately when regular members return. A selector or URL test group with only `empty_fallback` is valid. The effective fallback appears in `All()`, APIs, and multilevel imports; consumers still apply their own filters to it.

This tests membership emptiness, not probe failures, dial failures, or availability inside member groups. It does not bypass filters, missing references, invalid subscriptions, or initial download failures. A subscription must first load a valid document or cache.

The fallback always participates in startup ordering and the complete dependency graph, even while inactive. References inside subscriptions are namespaced using local declarations; deleting a still-referenced fallback rejects the update. Existing connections follow the group's interruption setting and the subscription's generation retention policy.

## Refresh behavior

A subscription refresh invalidates dependent membership views. Queries and new dials resolve the current members without waiting for polling, and background workers update selection and health checks. Changes propagate through every import level. Filters are reapplied when a node changes type or name, even if its complete tag stays the same.

Selectors retain an available selection. When it disappears, they select the configured default if present, then the first remaining member. The `default` field uses the effective outbound tag; subscriptions also accept the original name of a node in their own document. For selectors with imports, a default is a selection preference: an absent default falls back to remaining members and does not add a member or dialing dependency. Cached choices and defaults are applied after initial subscription loading. URL test groups and subscriptions in `urltest` mode test refreshed members; results from an older membership revision cannot update shared latency history or selection. Retired generations stop membership watchers and probes; shutdown waits for background tasks and admitted manual probes to exit.

An empty source or a filter matching nothing produces no imported members. A destination with no remaining members inserts its configured `empty_fallback`, or reports an unavailable outbound if none is configured; it never selects an excluded node automatically. A source group inside a subscription may disappear or become a non-group during refresh; that import then contributes no members and recovers if the group returns. Initially missing sources or sources that are not groups are configuration errors.

Previously admitted imports retain their source identity when the consumer or its parent subscription refreshes. A temporarily unavailable source does not block unrelated consumer updates. New imports or changes to a nonexistent source are still rejected. Removing a local source never redirects the import to a same-name host group; the cache preserves that namespace relationship too, while startup still strictly validates source availability. Restoring a source rechecks the complete dependency graph, so temporarily missing sources cannot bypass cycle validation.

For selectors and URL test groups, `outbounds` is a static reference list. Each tag must identify an outbound, endpoint or group declared in the same configuration document, including subscription groups themselves. Complete tags of nodes or groups inside another subscription are rejected even if already loaded. Use `import` to obtain those members, with `include` to select specific nodes. The host checks after merging configuration files, so `sing-box check` rejects invalid references without downloading subscriptions. Downloaded documents undergo the same check before loading. An update that removes a node still referenced by a group in that document is rejected, retaining the old version; changing both the declaration and its references together is valid.

A subscription's own `outbounds` is a whitelist of outbound and endpoint declarations in its downloaded document. An update missing a required member is rejected. Existing connections follow the destination's interruption setting and the source subscription's drain policy.

Self-imports, mutually recursive imports, and cycles involving group members, proxy chains, or DNS dependencies are rejected. Validation conservatively includes the source group's dependencies even when a filter excludes some members; filters cannot be used to break a dependency cycle. A cyclic subscription update is rejected while the previous generation continues running.

Downloads through a group using imports retain the subscription bootstrap rules. If the required members are unavailable initially, configure a usable cache or an independent `bootstrap_http_client`.
