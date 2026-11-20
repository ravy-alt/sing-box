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
  "url": "",
  "interval": "",
  "tolerance": 50,
  "idle_timeout": "",
  "interrupt_exist_connections": false
}
```

### 字段

#### outbounds

`outbounds` 和 `import` 至少一个非空。

用于测试的静态出站标签列表。每个标签必须在同一配置文档的 `outbounds` 或 `endpoints` 中声明；允许引用订阅组本身，不能直接引用其他订阅组内部的节点或组。获取订阅成员请使用 `import`。无效引用会在配置检查或订阅文档加载时被拒绝。

#### import

带筛选条件的动态[组导入](../shared/group-import.md)。支持订阅组和多级导入，自动跟随来源节点变化。

#### url

用于测试的链接。默认使用 `https://www.gstatic.com/generate_204`。

#### interval

测试间隔。 默认使用 `3m`。

#### tolerance

以毫秒为单位的测试容差。 默认使用 `50`。

#### idle_timeout

空闲超时。默认使用 `30m`。

#### interrupt_exist_connections

当选定的出站发生更改时，中断现有连接。

仅入站连接受此设置影响，内部连接将始终被中断。
