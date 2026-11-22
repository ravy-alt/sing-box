### Structure

```json
{
  "type": "urltest",
  "tag": "auto",
  
  "outbounds": [
    "proxy-a",
    "proxy-b",
    "proxy-c"
  ],
  "import": [],
  "empty_fallback": "",
  "url": "",
  "interval": "",
  "tolerance": 0,
  "idle_timeout": "",
  "interrupt_exist_connections": false
}
```

### Fields

#### outbounds

At least one of `outbounds`, `import`, or `empty_fallback` must be nonempty.

Static list of outbound tags to test. Each tag must be declared in `outbounds` or `endpoints` in the same configuration document. Subscription groups themselves are allowed; their internal nodes and groups cannot be referenced directly here. Use `import` to obtain subscription members. Invalid references fail configuration checking or subscription document loading.

#### import

Dynamic [group imports](../shared/group-import.md) with filters. Supports subscription sources and multiple import levels, following source membership changes automatically.

#### empty_fallback

Optional fallback for an empty group. Inserted only when the combined static and filtered imported membership is empty, and removed when regular members return. It must reference an outbound, endpoint, or group declared in the same document and participates in reference and cycle validation. It is not selected or tested while regular members exist, and is not a retry target for connection failures. See [group imports](../shared/group-import.md#empty_fallback).

#### url

The URL to test. `https://www.gstatic.com/generate_204` will be used if empty.

Tests recursively pin each member's current final node and use this group's URL, including for nested groups with different URLs. Histories are stored only by final node and URL; selection changes are reflected on subsequent reads. Different dashboard and IPv6 URLs do not overwrite this URL's result. See [measurement storage and API semantics](smart-urltest.md).

#### interval

The test interval. `3m` will be used if empty.

#### tolerance

The test tolerance in milliseconds. `50` will be used if empty.

#### idle_timeout

The idle timeout. `30m` will be used if empty.

#### interrupt_exist_connections

Interrupt existing connections when the selected outbound has changed.

Only inbound connections are affected by this setting, internal connections will always be interrupted.
