# Phase 00 扩展协议

内置扩展使用编译期 Registry；外部 Provider/Integration 将来使用独立进程 SPI，
不得在 Control Plane 中加载任意动态库。

所有 descriptor 共享：

```text
id
version
owner
supported_versions
required_capabilities
config_schema
status
deprecation
```

支持的类型：

- `EvidenceSourceDescriptor`
- `DiagnosticPackDescriptor`
- `ActionDescriptor`
- `ProviderDescriptor`
- `IntegrationDescriptor`

Registry 支持注册、查询、升级、禁用、废弃和回滚。未知 major 或 required
capability 必须 fail closed。Phase 00 的 ActionDescriptor 只能描述能力，
`execution_supported=false`，不能注册 handler 或目标凭据。
