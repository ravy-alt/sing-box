# DNS race

```json
{
  "type": "race",
  "tag": "private-race",
  "servers": ["private-a", "private-b"],
  "timeout": "5s"
}
```

`servers` requires at least two distinct DNS server tags. DoH, other native upstream types and acyclic nested races are supported; FakeIP is not. `timeout` defaults to 10 seconds; negative values are rejected.

Each query starts all upstreams concurrently with independent request copies. The first useful successful response wins and cancels slower requests. A/AAAA queries require the corresponding address records. NXDOMAIN and empty successful replies wait for possible positive replies, then provide a fallback when remaining upstreams finish or the race times out. Truncated responses, SERVFAIL, REFUSED, empty replies and network errors do not win. If all upstreams fail, the query fails. Parent cancellation and scope shutdown cancel outstanding work.

Use the tag directly in `domain_resolver` or DNS routing. This races **DNS responses**, not TCP reachability of the resulting IPs; dialing retains native address selection behavior. Cached answers bypass upstream exchanges according to the caller's DNS cache policy. Use `disable_cache` in the domain resolver to query on every lookup.

Race membership and upstream DNS/outbound dependencies participate in the unified cycle validator. The transport manager owns upstream lifecycle/reset operations.
