!!! quote "sing-box 1.10.0 中的更改"

    :material-plus: [access_control_allow_origin](#access_control_allow_origin)  
    :material-plus: [access_control_allow_private_network](#access_control_allow_private_network)

!!! quote "sing-box 1.8.0 中的更改"

    :material-delete-alert: [store_mode](#store_mode)  
    :material-delete-alert: [store_selected](#store_selected)  
    :material-delete-alert: [store_fakeip](#store_fakeip)  
    :material-delete-alert: [cache_file](#cache_file)  
    :material-delete-alert: [cache_id](#cache_id)

### 结构

=== "结构"

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

=== "示例 (在线)"

    !!! question "自 sing-box 1.10.0 起"

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

=== "示例 (下载)"

    !!! question "自 sing-box 1.10.0 起"

    ```json
    {
      "external_controller": "0.0.0.0:9090",
      "external_ui": "dashboard"
      // "external_ui_download_detour": "direct"
    }
    ```

!!! note ""

    当内容只有一项时，可以忽略 JSON 数组 [] 标签

### Fields

#### 面板测速与节点身份

`GET /group/{name}/delay?url=...&timeout=...` 使用请求指定的 HTTP 或 HTTPS 地址，
独立于后台自动测速，测试组的直接成员。嵌套组使用当前选中的路径。
所有成员并发开始测试；部分成员超时不会丢弃其他成员的成功结果。
`timeout` 的单位为毫秒，范围为 1 到 60000。全部失败时返回 504。
测试期间订阅版本发生变化时，丢弃旧版本的结果。

节点信息包含不透明扩展字段 `x-sing-box-revision`。内核重启或订阅资源图发布新版本时，
该字段会变化，覆盖节点、链式拨号和 DNS 资源的更新。
面板缓存 IPv6 可达性等能力时，应同时校验后端身份、节点名称和该字段。
资源图更新会使该后端全部能力缓存失效，即使某个节点的名称未改变。
该字段不包含配置密钥等敏感信息。

#### external_controller

RESTful web API 监听地址。如果为空，则禁用 Clash API。

#### external_ui

到静态网页资源目录的相对路径或绝对路径。sing-box 会在 `http://{{external-controller}}/ui` 下提供它。

#### external_ui_download_url

静态网页资源的 ZIP 下载 URL，如果指定的 `external_ui` 目录为空，将使用。

默认使用 `https://github.com/MetaCubeX/Yacd-meta/archive/gh-pages.zip`。

#### external_ui_download_detour

用于下载静态网页资源的出站的标签。

如果为空，将使用默认出站。

#### secret

RESTful API 的密钥（可选）
通过指定 HTTP 标头 `Authorization: Bearer ${secret}` 进行身份验证
如果 RESTful API 正在监听 0.0.0.0，请始终设置一个密钥。

#### default_mode

Clash 中的默认模式，默认使用 `Rule`。

此设置没有直接影响，但可以通过 `clash_mode` 规则项在路由和 DNS 规则中使用。

#### access_control_allow_origin

!!! question "自 sing-box 1.10.0 起"

允许的 CORS 来源，默认使用 `*`。

要从公共网站访问私有网络上的 Clash API，必须在 `access_control_allow_origin` 中明确指定它而不是使用 `*`。

#### access_control_allow_private_network

!!! question "自 sing-box 1.10.0 起"

允许从私有网络访问。

要从公共网站访问私有网络上的 Clash API，必须启用 `access_control_allow_private_network`。

#### store_mode

!!! failure "已在 sing-box 1.8.0 废弃"

    `store_mode` 已在 Clash API 中废弃，且默认启用当 `cache_file.enabled`。

将 Clash 模式存储在缓存文件中。

#### store_selected

!!! failure "已在 sing-box 1.8.0 废弃"

    `store_selected` 已在 Clash API 中废弃，且默认启用当 `cache_file.enabled`。

!!! note ""

    必须为目标出站设置标签。

将 `Selector` 中出站的选定的目标出站存储在缓存文件中。

#### store_fakeip

!!! failure "已在 sing-box 1.8.0 废弃"

    `store_selected` 已在 Clash API 中废弃，且已迁移到 `cache_file.store_fakeip`。

将 fakeip 存储在缓存文件中。

#### cache_file

!!! failure "已在 sing-box 1.8.0 废弃"
 
    `cache_file` 已在 Clash API 中废弃，且已迁移到 `cache_file.enabled` 和 `cache_file.path`。

缓存文件路径，默认使用`cache.db`。

#### cache_id

!!! failure "已在 sing-box 1.8.0 废弃"
 
    `cache_id` 已在 Clash API 中废弃，且已迁移到 `cache_file.cache_id`。

缓存 ID。

如果不为空，配置特定的数据将使用由其键控的单独存储。
## DNS 管理扩展

此分支在 `GET /version` 的 `capabilities` 中公布 `dns-management-v1`。面板应先检测能力，再使用下列接口。接口沿用 Clash API 的认证方式；WebSocket 使用 `token` 查询参数。

| 接口 | 用途 |
| --- | --- |
| `GET /dns/cache` | 当前有效缓存的完整快照 |
| `WS /dns/cache` | 首次完整快照，此后每秒发送增量和心跳 |
| `DELETE /dns/cache` | 清除所有作用域的缓存 |
| `DELETE /dns/cache?id=…` | 删除指定缓存；ID 是不透明标识，不应自行拼接 |
| `DELETE /dns/cache?scope=…&server=…` | 清除一个作用域内指定服务器的缓存 |
| `GET /dns/servers` | 服务器地址、类型、拨号链路、解析器及缓存数量 |
| `GET /dns/rules` | DNS 匹配条件和动作，包括默认规则 |
| `GET /dns/query?name=…&type=A&scope=…&server=…` | 按规则查询，或使用指定服务器查询；兼容原有响应并增加实际 `Server` 和 `Trace` |

不传 `scope` 表示主配置；订阅内部 DNS 使用独立作用域，操作时必须保留接口返回的 `scope`、`tag` 和 ID。查询省略 `server` 时匹配规则；面板默认分别请求 A 和 AAAA。每种查询类型独立返回规则、服务器和缓存命中情况。

`dialerChain` 按 DNS 传输的主要协议追踪：TCP/DoT/DoH 使用 TCP，UDP/DoQ/DoH3 使用 UDP。每一跳根据代理的封装方式决定下一跳的协议，包括 UoT、多路复用和 QUIC 传输；嵌套组按该协议的当前选择继续追踪。读取订阅内部组和链式代理时，会保持所属版本的生命周期，避免并发刷新把新旧版本的节点拼接到同一条链路中。

缓存快照来自实际内存或持久化缓存，不是查询日志。`request` 记录最近一次使用该缓存的请求；源/目的端口是被接收的 DNS 请求端口，不是上游连接的临时端口。`purpose` 区分客户端、代理节点域名解析、DNS 服务器域名解析和手动查询。内核解析没有客户端端口；旧磁盘记录未保存的请求信息为空，下一次命中后补齐。

缓存所属服务器在 `server`，竞速 DNS 的实际胜出服务器在 `request.server`。`expiresAt: null` 表示禁用过期；乐观缓存仍在可用期时保留记录并设置 `stale: true`，超出可用期后从快照移除。后台刷新保留最近一次请求信息。逐条、按服务器和全部清理都会阻止清理前启动的查询重新写回旧结果，之后的新查询可以正常生成缓存。

流式帧为 `{full, revision, entries, removed}`：首帧 `full: true`，客户端替换全部记录；后续按 ID 合并 `entries` 并删除 `removed`。每条连接的 `revision` 从 1 递增；丢帧或重连后必须重新获取完整快照，不能对增量帧做防抖丢弃。暂停、切换后端、离开页面时应关闭连接。服务端检测断开和关闭信号，限制写入等待时间，不为慢客户端积压事件队列。

根域返回为 `"."`。`answers` 保留完整 DNS 记录；`answerData` 按相同顺序提供不含名称、TTL 和类别的 RDATA 文本，便于逐条展示。原始记录中的 TTL 是存储时的值，剩余缓存有效期应根据 `expiresAt` 计算。
