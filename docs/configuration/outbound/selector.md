### Structure

```json
{
  "type": "selector",
  "tag": "select",
  
  "outbounds": [
    "proxy-a",
    "proxy-b",
    "proxy-c"
  ],
  "import": [],
  "empty_fallback": "",
  "default": "proxy-c",
  "interrupt_exist_connections": false
}
```

!!! quote ""

    The selector can only be controlled through the [Clash API](/configuration/experimental/clash-api/) currently.

### Fields

#### outbounds

At least one of `outbounds`, `import`, or `empty_fallback` must be nonempty.

Static list of outbound tags to select. Each tag must be declared in `outbounds` or `endpoints` in the same configuration document. Subscription groups themselves are allowed; their internal nodes and groups cannot be referenced directly here. Use `import` to obtain subscription members. Invalid references fail configuration checking or subscription document loading.

#### import

Dynamic [group imports](../shared/group-import.md) with filters. Supports subscription sources and multiple import levels, following source membership changes automatically.

#### empty_fallback

Optional fallback for an empty group. Inserted only when the combined static and filtered imported membership is empty, and removed when regular members return. It must reference an outbound, endpoint, or group declared in the same document and participates in reference and cycle validation. It is not selected or tested while regular members exist, and is not a retry target for connection failures. See [group imports](../shared/group-import.md#empty_fallback).

#### default

The default outbound tag. The first outbound will be used if empty.

#### interrupt_exist_connections

Interrupt existing connections when the selected outbound has changed.

Only inbound connections are affected by this setting, internal connections will always be interrupted.
