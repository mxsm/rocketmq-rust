# ADR 0002：MCP 只读与 Fail-Closed 边界

- 状态：Accepted
- 日期：2026-07-26
- 适用阶段：Phase 00

## 决策

RocketMQ MCP 的编译依赖、Tool/Resource surface 和运行身份均为只读。SRE Connector
把 MCP 当成黑盒协议服务，不链接 server 类型。

统一读取流水线为：

```text
tenant/cluster authorization
→ begin audit
→ execute read query
→ canonical envelope
→ sensitive-field sanitization
→ row/byte bounding
→ stable error mapping
→ finish audit
```

Tool 与 Resource 必须使用同一流水线。输出不得包含 token、secret、ACL/TLS
material、消息正文、客户端 IP、内部地址或完整配置。数组超限时截断并返回
`partial=true` 与稳定 warning；最小合法 envelope 仍超限时返回
`output_too_large`。

Capability Manifest 必须声明：

- MCP protocol：`2025-11-25`
- 业务 schema：`rocketmq-mcp.v2`
- 当前身份可见的 Tool/Resource
- 每个 Tool 的 canonical schema digest 与总 Tool surface digest
- `mutation_supported=false`
- planning Tool 的 `mutates_cluster=false`

## 握手拒绝条件

Connector 遇到以下任一条件时必须停止映射，不允许匿名或猜测降级：

- 未知 schema major 或 required feature
- Tool schema digest、Tool surface 或 Resource surface drift
- `mutation_supported=true`
- audience、read scope、tenant 或 cluster allowlist 不匹配
- TLS 服务端证书验证失败
- token 刷新一次后仍返回 401

401 恢复仅允许清除缓存 token、重新获取一次并重试一次幂等读请求。

## Phase 00 明确禁用

- Apply/Delete/Update/Reset Tool
- MCP Tasks
- Approval、Executor、Execution Agent
- 业务 Topic 写权限
- 匿名 HTTP fallback
- 无上限重试

## 验证

MCP 的 resolved feature graph 不得出现 `admin-mutation`、
`mutation-client-adapter` 或 `client-adapter`。默认 Tool/Resource catalog 不得出现
Apply/Delete/Update/Reset。运行身份仍需只读权限，编译隔离不能替代授权。
