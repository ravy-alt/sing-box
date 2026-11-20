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
  "url": "",
  "interval": "",
  "tolerance": 0,
  "idle_timeout": "",
  "interrupt_exist_connections": false
}
```

### Fields

#### outbounds

At least one of `outbounds` or `import` must be nonempty.

Static list of outbound tags to test. Each tag must be declared in `outbounds` or `endpoints` in the same configuration document. Subscription groups themselves are allowed; their internal nodes and groups cannot be referenced directly here. Use `import` to obtain subscription members. Invalid references fail configuration checking or subscription document loading.

#### import

Dynamic [group imports](../shared/group-import.md) with filters. Supports subscription sources and multiple import levels, following source membership changes automatically.

#### url

The URL to test. `https://www.gstatic.com/generate_204` will be used if empty.

#### interval

The test interval. `3m` will be used if empty.

#### tolerance

The test tolerance in milliseconds. `50` will be used if empty.

#### idle_timeout

The idle timeout. `30m` will be used if empty.

#### interrupt_exist_connections

Interrupt existing connections when the selected outbound has changed.

Only inbound connections are affected by this setting, internal connections will always be interrupted.
