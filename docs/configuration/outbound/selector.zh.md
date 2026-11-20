### 结构

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

    选择器目前只能通过 [Clash API](/zh/configuration/experimental/clash-api/) 来控制。

### 字段

#### outbounds

`outbounds`、`import` 和 `empty_fallback` 至少一个非空。

用于选择的静态出站标签列表。每个标签必须在同一配置文档的 `outbounds` 或 `endpoints` 中声明；允许引用订阅组本身，不能直接引用其他订阅组内部的节点或组。获取订阅成员请使用 `import`。无效引用会在配置检查或订阅文档加载时被拒绝。

#### import

带筛选条件的动态[组导入](../shared/group-import.md)。支持订阅组和多级导入，自动跟随来源节点变化。

#### empty_fallback

可选的空组备用节点。静态成员与过滤后的导入成员合并为空时，临时插入该节点；普通成员恢复后自动移除。必须引用同一配置文档中声明的出站、端点或组，并参与静态引用与环路校验。它不在普通成员存在时参与选择或测速，也不是连接失败时的重试节点。详见[组导入](../shared/group-import.md#empty_fallback)。

#### default

默认的出站标签。默认使用第一个出站。

#### interrupt_exist_connections

当选定的出站发生更改时，中断现有连接。

仅入站连接受此设置影响，内部连接将始终被中断。
