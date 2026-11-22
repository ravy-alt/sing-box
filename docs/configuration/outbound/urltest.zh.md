### 结构

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
  "tolerance": 50,
  "idle_timeout": "",
  "interrupt_exist_connections": false
}
```

### 字段

#### outbounds

`outbounds`、`import` 和 `empty_fallback` 至少一个非空。

用于测试的静态出站标签列表。每个标签必须在同一配置文档的 `outbounds` 或 `endpoints` 中声明；允许引用订阅组本身，不能直接引用其他订阅组内部的节点或组。获取订阅成员请使用 `import`。无效引用会在配置检查或订阅文档加载时被拒绝。

#### import

带筛选条件的动态[组导入](../shared/group-import.md)。支持订阅组和多级导入，自动跟随来源节点变化。

#### empty_fallback

可选的空组备用节点。静态成员与过滤后的导入成员合并为空时，临时插入该节点；普通成员恢复后自动移除。必须引用同一配置文档中声明的出站、端点或组，并参与静态引用与环路校验。它不在普通成员存在时参与选择或测速，也不是连接失败时的重试节点。详见[组导入](../shared/group-import.md#empty_fallback)。

#### url

用于测试的链接。默认使用 `https://www.gstatic.com/generate_204`。

测速递归固定每个成员当前选中的最终节点，并使用本组 URL；嵌套组自己的 URL 不替代本组 URL。结果只按最终节点和 URL 存储，后续查询会跟随选择变化。面板使用其他 URL 或 IPv6 测速不会覆盖本 URL 的记录。详见 [测速存储及 API 语义](smart-urltest.zh.md)。

#### interval

测试间隔。 默认使用 `3m`。

#### tolerance

以毫秒为单位的测试容差。 默认使用 `50`。

#### idle_timeout

空闲超时。默认使用 `30m`。

#### interrupt_exist_connections

当选定的出站发生更改时，中断现有连接。

仅入站连接受此设置影响，内部连接将始终被中断。
