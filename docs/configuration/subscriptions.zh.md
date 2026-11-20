# 订阅源

顶级 `subscriptions` 管理原生 sing-box JSON 订阅的下载、缓存和热更新。订阅源本身不是出站，不能作为路由目标、`detour`、固定 `outbounds` 成员或 `empty_fallback`；selector、urltest 和其他订阅源通过 `import` 获取它的成员。

订阅源只在 Clash 兼容面板的订阅页显示，不会在代理组列表、`GLOBAL` 或原生客户端组列表中重复出现。导入的普通节点和来源内部策略组仍然可见。旧的 `outbounds[].type: subscription` 保持兼容；新配置建议使用顶级字段。

## 私有 DNS 示例

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
  "subscriptions": [
    {
      "tag": "机场",
      "url": "https://example.com/sub-store.json",
      "http_client": {},
      "cache_path": "subscriptions/airport.json",
      "update_interval": "24h",
      "drain_timeout": "0s",
      "override_dialer": {
        "domain_resolver": { "server": "private-race", "strategy": "prefer_ipv4" },
        "connect_timeout": "10s"
      }
    }
  ],
  "outbounds": [
    { "type": "direct", "tag": "direct" },
    {
      "type": "urltest", "tag": "香港",
      "import": [{ "group": "机场", "include": ["香港|Hong Kong"], "match": "name" }],
      "empty_fallback": "direct"
    },
    { "type": "selector", "tag": "代理", "outbounds": ["香港", "direct"] }
  ],
  "route": { "final": "代理", "default_domain_resolver": "local" }
}
```

示例地址需要替换。DoH 使用 IP 连接并通过 `tls.server_name` 校验证书；如果 DoH 自身使用域名，需要独立的引导解析器。不要让私有 DoH 的拨号路径依赖使用它解析节点地址的策略组。

## 字段

`tag` 必填，与当前文档的出站、端点及其他订阅源标签不能重复。

下载与缓存字段沿用[订阅引擎](outbound/subscription.md)：`url`、`http_client`、`bootstrap_http_client`、`cache_path`、`update_interval`、`download_timeout`、`max_download_size`、`max_cache_size`、`drain_timeout`。HTTP 客户端可以使用共享标签或内联对象；省略继承默认客户端，`{}` 明确直连。下载依赖尚未加载的本订阅时，必须有可用缓存或独立的 `bootstrap_http_client`，不会偷偷绕过配置改为直连。

`outbounds` 可按原始标签筛选下载文档的成员，依赖仍完整保留。`import` 可追加其他组或订阅源的成员，并动态跟随增删；过滤语义见[组导入](shared/group-import.md)。来源中的顶级 `subscriptions` 支持嵌套，并与父资源树一起校验和回收。订阅源不提供选择策略，也没有 `default`、`mode`、`tolerance` 或 `empty_fallback`，这些应放在消费它的策略组中。

`test_url` 指定订阅健康检查 URL，默认 `https://www.gstatic.com/generate_204`。`test_interval` 为正数时启用定期检查及刷新后的检查；省略或 `0s` 只接受手动检查。检查不会替策略组选择节点。

## 拨号覆盖

`override_dialer` 只覆盖本次下载文档中具有原生拨号选项的出站和端点；不修改 DNS 服务器、HTTP 下载器、策略组、子订阅的配置，或通过 `import` 引用的其他来源节点。子订阅需要在自己的声明处设置覆盖。

| 字段 | 用途 |
| --- | --- |
| `domain_resolver` | 指定节点域名解析器，支持原生字符串和对象形式；整体替换已有解析器及旧 `domain_strategy` |
| `connect_timeout` | 连接超时 |
| `bind_interface`、`inet4_bind_address`、`inet6_bind_address`、`bind_address_no_port` | 本机出口接口和源地址 |
| `routing_mark`、`reuse_addr` | 本机套接字策略 |
| `tcp_fast_open`、`tcp_multi_path` | TCP 拨号能力 |
| `disable_tcp_keep_alive`、`tcp_keep_alive`、`tcp_keep_alive_interval` | TCP 保活 |
| `udp_fragment` | UDP 分片 |
| `network_strategy`、`network_type`、`fallback_network_type`、`fallback_delay` | 原生网络选择与回退 |

未配置的字段保留来源值；显式 `false`、`0`、空字符串或空数组可覆盖对应原值。`null` 与省略一样，不表示清除。绑定地址和解析器应给出有效的新值。负数超时会报错。

覆盖解析器的引用属于订阅声明所在作用域：顶级订阅引用主配置 DNS，嵌套订阅引用父文档 DNS。不会被下载内容中的同名 DNS 劫持。未设置覆盖时，来源的自定义 DNS 和链式引用继续保留。覆盖会在依赖检查、初始化前应用；缓存保存原始文档，重启时使用当前覆盖设置重新构建。

不提供 `detour`、协议服务器、认证、TLS 或多路复用的统一覆盖。尤其不能把所有节点的 `detour` 改成同一值，否则可能破坏来源内部链式代理或构成自环。保持原生拨号语义：存在 `detour` 时，本机套接字设置由真正建立底层连接的节点决定；显式 `domain_resolver` 仍可在链路拨号前解析域名。

## 热更新与校验

DNS 竞速成员、DNS 解析器和 `detour`、出站链及导入关系共用依赖图。启动检查主配置，更新检查候选文档及所有活动订阅，串行提交以避免并发更新共同引入环路。下载引导关系单独处理，不把可由缓存打破的下载依赖误判为数据转发环路。

更新失败保留完整旧版本；成功后新连接使用新版本。`drain_timeout: 0s` 等待所有旧连接关闭后回收旧资源；正数在超时后强制关闭剩余旧连接。外层组的 `interrupt_exist_connections` 仍适用。订阅清空后导入组更新成员，`empty_fallback` 只在消费组为空时生效。中文名称、冲突后缀和过滤逻辑与旧订阅引擎一致。

## Clash API / zashboard

| 接口 | 行为 |
| --- | --- |
| `GET /providers/proxies` | 列出根订阅和嵌套订阅 |
| `GET /providers/proxies/{name}` | 成员、更新时间、就绪状态、代次、最后错误、`testUrl` |
| `PUT /providers/proxies/{name}` | 刷新并等待成功或错误 |
| `GET /providers/proxies/{name}/healthcheck` | 检查整个订阅，成功返回 204 |
| `GET /providers/proxies/{name}/{proxy}/healthcheck?url=...&timeout=5000` | 检查该订阅成员并返回 `delay`；超时单位毫秒，范围 1–60000 |

面板收到的名称保留原文；路径参数由客户端编码一次。单节点接口不允许探测不属于该订阅的节点。旧版本的测速结果不会写入新版本历史。响应包含 `vehicleType: HTTP`；若服务器返回合法的 `Subscription-Userinfo` 头，还提供 `subscriptionInfo` 的 Upload/Download/Total/Expire，并随成功版本缓存。刷新失败不会替换流量信息。

## 从旧配置迁移

将原 `type: subscription` 对象移入 `subscriptions`，删除 `type` 和选择策略字段。将对这个订阅的固定 `outbounds` 引用改成 `import: [{"group":"订阅标签"}]`。如果原订阅承担选择或空组备用功能，另外声明 selector/urltest，将这些选项移到新组，并把路由指向新组。已有使用 `import` 的地区组只需保持订阅标签，过滤不变。
