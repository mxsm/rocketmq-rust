---
title: "安全契约与信任边界"
---

安全能力在服务边界上装配。共享契约描述主体、资源、决策和秘密；认证实现验证身份；网络监听器建立可信对端/TLS 事实；操作适配器将请求映射到权限。仅编译安全 crate 不意味着已经完成任何一项接线。

## 职责地图

| 层 | 职责 |
| --- | --- |
| `rocketmq-security-api` | 不依赖运行时的主体/资源、请求视图、入站策略、下游签名、分层决策、秘密和维护契约 |
| `rocketmq-auth` | Access Key 签名认证、ACL 求值、本地元数据、导入/重载及服务拥有的认证生命周期 |
| 传输 / gRPC 监听器 | 连接对端信息、实际 TLS 握手和已验证证书事实 |
| Broker / Proxy 集成 | 将受支持操作映射为授权上下文，并在分发前检查 |
| 应用 / 部署所有者 | 选择 profile、提供凭据和策略、配置监听器、保留生命周期所有者 |

```mermaid
flowchart LR
  U["调用方与不可信请求元数据"] --> T["监听器：对端与已验证 TLS 事实"]
  T --> A["认证：确定主体"]
  A --> Z["操作映射与授权"]
  Z --> S["受保护服务操作"]
  S --> O["显式下游签名器及凭据"]
  O --> B["下游 Broker 策略"]
```

下游调用是独立信任边界。Proxy 接受客户端身份，不代表 Broker 接受 Proxy 的下游身份。同样，请求头声明的对端身份不等于监听器验证过的证书。

## 认证与授权

`AuthRuntime` 加载提供方并能够检查 Remoting 请求。普通认证、授权均默认关闭，且分别配置。构建运行时不会安装网络拦截器；集成必须在受保护分发前，携带可信 `RemotingAuthContext` 调用检查。

认证执行 Access Key 查找、用户启用状态检查及 HMAC 签名校验。可选时间戳偏差检查允许请求不带时间戳，也不包含 nonce/重放缓存。因此仅启用该时间窗口不构成完整重放保护。

授权将请求映射为资源/动作上下文，要求每个生成的上下文均允许访问。ACL 优先选择匹配的自定义策略，再回退默认策略，随后按资源具体程度求值；同等具体程度时拒绝优先。“任何策略中的拒绝永远覆盖一切”会误述选择算法。

未映射的普通 Remoting 操作可能不生成上下文，因此没有 ACL 决策。通用 auth gRPC 适配器解析元数据，但不提供完整方法授权拦截器；其默认授权上下文构造器返回空集合。Proxy 具有自己的操作映射及服务集成。新增公开操作需要明确权限映射，不能假设通用认证已覆盖。

普通 Remoting 路径中，匹配全局或账户 IP 白名单会同时绕过认证和授权。请求码白名单分别绕过对应检查。超级用户在授权时绕过 ACL 查找，但仍需认证，除非命中另一个已配置绕过条件。开放监听器时必须考虑这些实际策略语义。

## TLS 与部署启动校验

`ROCKETMQ_SECURITY_PROFILE` 选择 `development-insecure-loopback` 或 `secure-enforced`。开发启动校验要求传入的监听地址为环回地址。安全启动校验检查所需信任锚、证书/私钥、挂载文件秘密提供方、管理员身份和请求策略材料。

既无 profile 也无启动材料时，返回禁用结果。提供材料却没有明确 profile 会被拒绝。启动校验检查配置和材料，不执行 TLS 握手，也不会自动给所有监听器附加请求检查。

所有者必须另行配置实际传输或 gRPC TLS 监听器及客户端认证策略。Proxy gRPC 从服务端连接扩展取得已验证 TLS 身份，不信任任意用户元数据。下游 TLS 和签名也需要自己的配置。feature 选择和加密文件传输行为见[传输](protocol-transport.md)。

## 元数据、重载与秘密

本地认证提供方可选择持久化 `users.json`、`acls.json`。用户快照以明文包含 HMAC 验证所需秘密；Debug 脱敏不会加密这些文件或 ACL YAML。因此需要保护存储权限及秘密分发。

ACL 观察器重新加载变化的输入。读取/解析/校验错误发生在导入前，保留之前导入的元数据。实际用户/ACL 更新按顺序执行，不是跨整个导入的事务：导入过程中元数据或持久化失败可能留下部分效果。文件重载成功，也不能证明每个独立装配的有状态缓存都已刷新。

`Secret<T>` 会对格式化输出脱敏，但不承诺任意 `T` 都被清零。`SecretMaterial` 拥有会清零的字节缓冲区；显式访问方法仍会向调用方暴露材料。日志中应避免凭据、token、完整请求/配置对象和消息体。

## 特权适配器与生命周期

一次性初始化、秘密提供方注册表、开发用加密文件秘密、凭据轮换和维护策略属于单独装配的适配器。构建 `AuthRuntime` 不会启用它们。一次性初始化在实际配置管理员前先持久化认领记录；配置失败也不会重新开放该认领。

由于尚未实现等价文件系统 ACL 验证，持久化一次性初始化及加密文件秘密适配器目前在 Windows 上拒绝执行。此限制与普通 Remoting 认证、JSON 元数据支持不同。

维护授权将已认证身份与明确的能力、目标及预算策略组合。其运行时策略材料、审计要求仍属于产品行为；简化文档编写流程不会取消这些边界。允许/拒绝/弃权决策也不同于提供方或契约错误；必需层不能将提供方失败解释为许可。

关闭会停止新增认证请求，排空已接纳工作，停止 ACL 观察器，并刷写/关闭拥有的提供方。调用方注入的元数据 actor 仍由调用方拥有。受保护请求工作停止后、运行时消失前，应关闭认证服务。

## 源码地图

- [Security API](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-security-api/README.md)、[部署校验](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-security-api/src/secure_deployment.rs)。
- [Auth 语义及适配器](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-auth/README.md)。
- [Proxy 认证集成](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-proxy/src/auth.rs)、[gRPC 可信 TLS 上下文](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-proxy/src/grpc/middleware.rs)。
- [错误与可观测性](errors-observability.md)、[Broker 流水线](broker.md)。
