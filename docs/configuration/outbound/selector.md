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
  "default": "proxy-c",
  "interrupt_exist_connections": false
}
```

!!! quote ""

    The selector can only be controlled through the [Clash API](/configuration/experimental/clash-api/) currently.

### Fields

#### outbounds

At least one of `outbounds` or `import` must be nonempty.

Static list of outbound tags to select. Each tag must be declared in `outbounds` or `endpoints` in the same configuration document. Subscription groups themselves are allowed; their internal nodes and groups cannot be referenced directly here. Use `import` to obtain subscription members. Invalid references fail configuration checking or subscription document loading.

#### import

Dynamic [group imports](../shared/group-import.md) with filters. Supports subscription sources and multiple import levels, following source membership changes automatically.

#### default

The default outbound tag. The first outbound will be used if empty.

#### interrupt_exist_connections

Interrupt existing connections when the selected outbound has changed.

Only inbound connections are affected by this setting, internal connections will always be interrupted.
