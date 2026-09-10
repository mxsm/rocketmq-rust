---
title: "配置部署安全"
---

把每条连接分别作为部署边界：客户端到 NameServer、客户端到 Broker、客户端到 Proxy、Proxy 到 Broker，以及 Controller 的管理连接与 peer 连接。为每条暴露路径选择加密方式、认证身份和操作权限。权限计算与提供者语义见[安全设计](../architecture/security.md)。

## 盘点实际监听入口

| 路径 | 配置责任 | 观察结果 |
| --- | --- | --- |
| NameServer remoting | NameServer 认证字段与实际监听/传输部署 | 授权的路由查询成功，无效凭据失败 |
| Broker remoting | Broker 认证配置、客户端签名、可达的注册地址，以及所选传输路径的实际 TLS 配置 | 授权收发成功，其他主题/消费者组被拒绝 |
| Proxy gRPC | `grpc.tls`、`auth` 和 Proxy 方法权限映射 | 证书验证与应用授权都符合预期 |
| Proxy / Broker 内部客户端 | 出站凭据和接收方 ACL | 注册、路由发现与下游请求使用预期服务身份成功 |
| Controller remoting / Raft | 独立端口、管理凭据，以及实际 peer 传输与访问边界 | 运维身份可访问管理接口，不可信网络无法访问 peer 流量 |
| Admin 与观测端点 | 运维凭据、网络暴露范围和受限的遥测访问 | 只读运维身份不能变更状态，遥测仅对指定采集器可达 |

依赖的 TLS feature 只提供编译能力，不会选择并配置所有监听器。不能由 Proxy TLS 握手成功推断 remoting、HA 或 Raft 也已加密。对于尚未证实 TLS 接线的路径，应设计明确的私有网络或外部传输边界，并单独验证该部署。

## 配置服务认证与授权

TOML 使用 camelCase 字段。下面这些公共字段在 NameServer、Controller 配置中位于根层，在 Broker 中位于 `[broker]`，在 Proxy 中位于 `[auth]`：

```toml
authenticationEnabled = true
authorizationEnabled = true
aclFile = "/etc/rocketmq/acl/plain_acl.yml"
authConfigPath = "/var/lib/rocketmq/auth"
```

将这些字段合入已有服务配置的对应作用域；该片段不是完整的独立启动配置。每个进程使用独立且可写的认证元数据目录。保护输入 ACL 和生成的 `users.json`、`acls.json`；用户快照以明文保存 HMAC 校验所需密钥。

应用 ACL 可从默认拒绝的资源权限开始：

```yaml
globalWhiteRemoteAddresses: []
accounts:
  - accessKey: docs-publisher
    secretKey: replace-with-a-private-secret
    admin: false
    defaultTopicPerm: DENY
    defaultGroupPerm: DENY
    topicPerms:
      - DocsProxyMessage=PUB
```

此例仅授予发布权限，不会创建消费、管理或内部复制身份。其他身份应按实际请求与资源映射分别定义。通过私有凭据分发流程替换占位值，不要在应用中复用超级用户凭据。

普通 remoting 路径命中 IP 白名单后，会同时绕过认证与授权。请求码白名单绕过其对应检查。除非明确需要并理解这种行为，否则保持白名单为空。时间戳偏差校验本身不能提供完整的重放保护。

## 接入出站身份

Chart 管理的 Broker、Proxy 进程使用以下内部客户端 Secret 内容：

```json
{
  "accessKey": "site-service-identity",
  "secretKey": "replace-with-a-private-secret"
}
```

`ROCKETMQ_INNER_CLIENT_CREDENTIALS_FILE` 指定挂载的 JSON，还可提供 `securityToken`。接收方服务需要配置匹配的身份及所需权限。在 chart 之外，已有的内联 `innerClientAuthenticationCredentials` 优先，因此切换来源时应移除过期的内联覆盖。

Cluster 模式 Proxy 除入站认证外，还应在根层启用 `enableAclRpcHookForClusterMode = true`。出站凭据缺失或无效会导致初始化失败。入站客户端的授权结果不会自动提供 Proxy 访问下游所需的凭据。

Admin CLI 成对读取 `ROCKETMQ_ACL_ACCESS_KEY`、`ROCKETMQ_ACL_SECRET_KEY`，以及可选的 `ROCKETMQ_ACL_SECURITY_TOKEN`。通过受保护的进程环境提供，再执行[管理操作](../operations/admin.md)。避免把秘密写进命令历史，或将完整环境复制到故障报告。

## 配置 Proxy gRPC TLS

为所选后端编译 TLS，例如：

```bash
cargo build -p rocketmq-proxy --bin rocketmq-proxy-rust --no-default-features --features cluster-mode,tls
```

将以下片段合入 [Cluster 模式配置](proxy.md)，使用实际私有路径，并让证书匹配客户端使用的端点 DNS 名称：

```toml
[grpc.tls]
enabled = true
certificatePath = "/etc/rocketmq/tls/tls.crt"
privateKeyPath = "/etc/rocketmq/tls/tls.key"
clientAuth = "require"
clientCaPath = "/etc/rocketmq/tls/ca.crt"
reloadIntervalMs = 5000
```

`none` 不请求客户端证书；`optional` 在客户端提供证书时验证；`require` 拒绝未提供指定 CA 签发证书的连接。后两种模式要求 `clientCaPath`。TLS 身份不会自动获得发布、订阅或管理权限，仍需配置应用认证与授权。

客户端应信任服务端 CA，并校验端点名称。要求 mTLS 时还需提供客户端证书与私钥。回环教程中的明文 `grpcurl` 探测应按客户端的 TLS 与认证配置调整，不要对该端点继续使用 `-plaintext`。

内置 TLS 接收器按 `reloadIntervalMs` 轮询证书、私钥和 CA 文件元数据，该值必须为正。发生有效变更后，新的材料代次用于新连接。候选材料被拒绝时保留上一套可用接收器并记录警告，已有连接不会重新握手。失败候选在解析前已记录为观察过；未变化的错误文件不会持续重试。应修正挂载文件，再确认新代次和新建客户端连接，而不是一直等待未变化的失败候选。

## 选择进程引导 profile

[本地教程](../getting-started/local-source.md) 显式使用 `ROCKETMQ_SECURITY_PROFILE=development-insecure-loopback`。传入该校验的所有监听地址都必须为回环地址。把绑定地址改成 `0.0.0.0` 前，需要明确的共享网络部署设计；开发 profile 不能用作生产捷径。

`secure-enforced` 需要以下环境输入：

| 环境变量 | 必需材料 |
| --- | --- |
| `ROCKETMQ_SECURITY_TRUST_ANCHOR` | 信任锚文件 |
| `ROCKETMQ_SECURITY_TLS_CERT` / `ROCKETMQ_SECURITY_TLS_KEY` | 证书与私钥文件 |
| `ROCKETMQ_SECURITY_SECRET_PROVIDER` | `mounted-files` |
| `ROCKETMQ_SECURITY_ADMIN_IDENTITY` | 管理员身份文件 |
| `ROCKETMQ_SECURITY_REQUEST_POLICY` | 请求策略文件 |

使用已接入安全边界要求的实际文件；创建空占位文件不构成管理员登记流程。安全模式还会拒绝关闭服务认证或授权的配置。既无 profile 又无引导材料时，引导功能关闭；只提供材料而不显式指定 profile 会被拒绝。

该启动检查验证材料与配置，不负责构建 TLS 监听器或安装所有请求策略适配器。Core Helm chart 的 `securityProfile` 是另一个认证默认值选择器。持久化一次性管理员引导和加密文件开发凭据适配器，目前因尚未实现对应文件系统 ACL 验证而在 Windows 上失败关闭；这不代表 Windows 不能使用普通 remoting 认证。

## 轮换材料并验证行为

1. 找出使用旧身份或证书的全部接收方与出站调用方，包括 Admin 和内部客户端。确认部署路径是否支持身份或信任根重叠。
2. 私下分发下一套材料。支持重叠时，先更新接收方权限与信任，再切换调用方。不要提前撤销唯一可用的运维凭据。
3. 按实际路径执行重载或重启。ACL 监听可选且默认关闭；启动时的身份种子不是轮换接口。Core chart 文档规定轮换后受控重启受影响进程。Proxy TLS 具有上文的文件重载行为，但仅观察到 Secret 更新不能证明它已生效。
4. 使用新凭据建立新连接，验证允许的操作、禁止的资源和被拒绝的身份。同时检查下游调用。调用方迁移完毕后撤销旧访问，再验证旧凭据确实被拒绝。

ACL 读取、解析或校验失败时保留先前导入状态，但实际多记录导入按顺序执行，不是原子事务。宣布轮换完成前检查重载错误与实际权限。受保护请求工作排空后，再关闭认证提供者，并让其先于所属运行时退出。

本文依据当前代码给出部署步骤，未声明已经完成安全或凭据轮换演练。

## 源码索引

[认证配置与语义](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-auth/README.md)、[引导 profile](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-security-api/src/secure_deployment.rs)、[Proxy TLS 配置](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-proxy-core/src/config.rs)、[TLS 重载实现](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-proxy/src/grpc/tls_acceptor.rs)、[core chart 凭据接线](https://github.com/mxsm/rocketmq-rust/blob/main/distribution/helm/rocketmq-rust-core/README.md)。
