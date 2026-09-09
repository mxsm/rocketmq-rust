# rocketmq-auth

[English](README.md) | [简体中文](README-zh_cn.md)

为 [RocketMQ-Rust](../README-zh_cn.md) 提供认证、ACL 授权以及由服务管理生命周期的认证运行时。

`rocketmq-auth` 实现 RocketMQ 访问密钥签名、Java 风格 ACL 文件导入、本地用户和 ACL
元数据管理，以及 Remoting 请求检查。它还提供需要单独接入的首次管理员注册、密钥提供程序、
凭据轮换和维护策略适配器。构建 `AuthRuntime` 不会自动安装网络拦截器，也不会自动启用这些适配器。

## 能力与边界

| 领域 | 当前行为 |
|------|----------|
| 认证 | 访问密钥查找、用户启用状态检查、HMAC 签名验证，以及可选的时间戳偏差检查。 |
| 授权 | 按用户、资源、操作和环境评估策略，支持超级用户绕过授权；已生成的授权上下文没有适用 ACL 时拒绝访问。 |
| ACL 兼容 | Java 风格 YAML 文件、递归目录加载、全局和账号级 IP 白名单，以及可选的 v1 迁移。 |
| 运行时 | 提供程序初始化、请求准入、元数据初始化、ACL 重载、指标和协调关闭。 |
| 元数据 | 内存中的用户和 ACL，以及可选的 JSON 快照；自定义元数据提供程序组合具有明确的操作接口和生命周期接口。 |
| 策略 | 独立的无状态和有状态评估器。运行时直接执行 Remoting 检查的服务不使用有状态策略缓存。 |
| gRPC | 认证元数据解析和可选的 Tonic 适配器；不提供完整的 gRPC 认证授权拦截器。 |

## 公共 API 与源码结构

实现模块均为私有模块。请从 crate 根路径导入公开类型，例如使用
`rocketmq_auth::AuthConfig`，而不是 `rocketmq_auth::config::AuthConfig`。

| 源码 | 职责 |
|------|------|
| [公共导出](src/lib.rs)、[配置](src/config.rs) | 支持的导入路径和 Serde 配置默认值。 |
| [运行时](src/runtime.rs)、[Remoting 上下文](src/remoting_auth_context.rs) | 服务生命周期和可信入口信息。 |
| [提供程序操作接口](src/provider_ports.rs)、[提供程序生命周期管理](src/provider_owner.rs) | 元数据操作、准入、初始化和清理。 |
| [认证](src/authentication.rs)、[授权](src/authorization.rs) | 上下文构建器、提供程序、策略、签名和客户端 RPC 钩子。 |
| [ACL](src/acl.rs)、[迁移](src/migration.rs)、[权限](src/permission.rs) | YAML 导入、旧版模型、地址匹配和权限转换。 |
| [首次管理员注册](src/bootstrap.rs)、[密钥提供程序](src/secret_provider.rs)、[凭据轮换](src/credential_rotation.rs) | 显式接入的管理员注册和凭据管理适配器。 |
| [维护策略](src/maintenance.rs)、[分层授权](src/layered_authorization.rs) | 策略加载和组合安全决策所需的适配器。 |
| [认证授权指标](../rocketmq-observability/src/metrics/auth.rs) | 指标实现归属 `rocketmq-observability`；本 crate 重新导出 `AuthMetrics`、`AuthMetricsSnapshot` 和 `AuthMetricSample`。 |

与运行时无关的安全契约归属 `rocketmq-security-api`：

```rust
use rocketmq_security_api::{Principal, Resource};
```

认证授权策略模型使用不同的规范名称：

```rust
use rocketmq_auth::{AuthorizationRequest, PolicyDecision, PolicyResource};
```

根路径下的 `rocketmq_auth::Resource` 和 `RequestContext` 仍保留为策略模型的兼容别名。
`SecurityPrincipal` 和 `SecurityResource` 已弃用，请直接使用安全 API 中的类型。
维护契约也直接来自 `rocketmq-security-api`。普通的访问拒绝属于决策结果；
`AuthServiceError`、`SecurityContractViolation` 和 `SecurityProviderError` 表示运行故障或契约错误。
这些 Rust API 层面的区分不会改变 RocketMQ Remoting 的数字响应码。

## 依赖

对于属于本仓库工作区的包：

```toml
[dependencies]
rocketmq-auth = { workspace = true }
rocketmq-runtime = { workspace = true }
tokio = { workspace = true }
```

对于与 `rocketmq-auth`、`rocketmq-runtime` 目录同级的独立包，快速开始示例使用以下依赖：

```toml
[dependencies]
rocketmq-auth = { path = "../rocketmq-auth" }
rocketmq-runtime = { path = "../rocketmq-runtime" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

请按实际检出目录调整相对路径。本文描述当前源码树的 API，不保证具有相同工作区版本号的
已发布 crate 包含这里展示的全部 API。

本 crate 没有默认特性。在 `rocketmq-auth` 依赖上启用 `grpc` 特性后，可通过
`DefaultAuthenticationContextBuilder` 实现的 `AuthenticationContextBuilder::build_from_grpc`
解析 Tonic 的 `MetadataMap`；调用时需要导入 `AuthenticationContextBuilder` trait。
接收 `HashMap<String, String>` 的 `build_from_grpc_metadata_map` 不需要此特性。
默认授权提供程序的 gRPC 上下文方法返回空向量，因此仅启用 `grpc` 不会对 gRPC 方法实施授权检查。

## 快速开始

运行程序前，请相对于进程工作目录创建 `conf/plain_acl.yml`。
本地示例允许 Alice 向 `TopicA` 发布消息，以及使用 `GroupA` 订阅，不配置 IP 白名单绕过。
在本地测试之外使用时，请替换示例密钥。

```yaml
globalWhiteRemoteAddresses: []
accounts:
  - accessKey: alice
    secretKey: replace-with-a-local-test-secret
    admin: false
    defaultTopicPerm: DENY
    defaultGroupPerm: DENY
    topicPerms:
      - TopicA=PUB
    groupPerms:
      - GroupA=SUB
```

认证和授权都必须显式启用。构建器需要 `rocketmq-runtime` 提供的
`ChildServiceContext`，用于管理后台任务：

```rust,no_run
use rocketmq_auth::{AuthConfig, AuthRuntimeBuilder, AuthServiceResult};
use rocketmq_runtime::RuntimeContext;

#[tokio::main]
async fn main() -> AuthServiceResult<()> {
    let service_runtime = RuntimeContext::from_current("auth-example");
    let config = AuthConfig {
        auth_config_path: "store/auth".into(),
        acl_file: "conf/plain_acl.yml".into(),
        authentication_enabled: true,
        authorization_enabled: true,
        acl_file_watch_enabled: true,
        ..AuthConfig::default()
    };

    let runtime = AuthRuntimeBuilder::new(config, service_runtime.service_context("auth"))
        .build()
        .await?;

    println!("auth reload attempts: {}", runtime.metrics_snapshot().acl_reload_attempts);

    runtime.shutdown().await?;
    Ok(())
}
```

该程序加载元数据后关闭，不接收业务请求。Broker/Proxy 接入时，必须在分发受保护请求前调用
`runtime.check_remoting(&auth_context, &command).await?`。
通过 `from_request(&RemotingRequest)` 从可信传输信息构建 `RemotingAuthContext`，或在可信网络入口调用
`network(source_ip, channel_id)`。网络上下文要求来源地址和通道标识非空；嵌入式调用路径从可信的
Broker-Proxy 调用方信息派生。

如果中间件可能改写命令的操作码，应在入口保存原始操作码并使用 `check_remoting_for_code`。
在所属运行时退出前关闭认证运行时：关闭过程停止接收新请求，等待已准入的操作完成，
停止 ACL 监听任务，并刷新、关闭提供程序。外部注入的元数据 I/O Actor 仍由调用方管理。

## 配置

`AuthConfig` 使用 camelCase 格式的 Serde 字段名，省略的字段由 `Default` 补齐。
Rust 结构体字段使用 snake_case。主要默认值及作用范围如下：

| 字段 | 默认值 | 含义 |
|------|--------|------|
| `authenticationEnabled`、`authorizationEnabled` | `false` | 分别启用普通运行时路径中的认证和授权检查。 |
| `authConfigPath` | 空 | 可选的本地快照根路径；为空时使用内存元数据。 |
| `aclFile` | 空 | YAML 文件或目录；目录会递归搜索 `.yml` 和 `.yaml` 文件。 |
| `aclFileWatchEnabled` | `false` | 同时配置 ACL 路径时才启动监听任务。 |
| `aclFileWatchIntervalMillis` | `5000` | 轮询间隔，下限为 1 毫秒。 |
| `authenticationWhitelist`、`authorizationWhitelist` | 空 | 逗号分隔的十进制 Remoting 请求码；各自仅绕过对应的检查。 |
| `signatureAlgorithm` | `HmacSHA1` | 也支持 `HmacSHA256` 和 `HmacMD5`；客户端必须使用相同算法。 |
| `requestTimestampExpiredMillis` | `0` | 可选的时间戳偏差窗口；限制见下文。 |
| `authenticationProvider`、`authorizationProvider` | 空 | 运行时使用内置默认提供程序；不支持的配置名称会被拒绝。 |
| `authenticationMetadataProvider`、`authorizationMetadataProvider` | 空 | 运行时构建本地提供程序；自定义实现通过 `ProviderBundle` 注入。 |
| `authenticationStrategy`、`authorizationStrategy` | 空 | 用于工厂和评估器选择，默认无状态；不会为 `AuthRuntime::check_remoting` 选择策略。 |
| `configName` | 空 | 未注入运行时注册表时，旧版工厂按此名称缓存实例。 |
| `clusterName` | 空 | Remoting 授权映射使用的集群资源名称。 |
| `initAuthenticationUser`、`innerClientAuthenticationCredentials` | 空 | 可选的启动超级用户初始数据，仅在用户不存在时创建；不用于凭据轮换。 |
| `migrateAuthFromV1Enabled` | `false` | 通过 v1 普通权限管理器导入旧版 ACL。 |
| `aclCacheMaxNum`、`aclCacheExpiredSecond`、`aclCacheRefreshSecond` | `1000`、`600`、`60` | 本地 ACL 查询缓存的容量、有效期和回源刷新间隔。 |
| `userCacheMaxNum`、`userCacheExpiredSecond`、`userCacheRefreshSecond` | `1000`、`600`、`60` | 兼容性配置；当前本地用户提供程序不使用这些缓存设置。 |
| `statefulAuthenticationCacheMaxNum`、`statefulAuthenticationCacheExpiredSecond` | `10000`、`60` | 独立有状态认证策略的容量和过期时间，时间单位为秒。 |
| `statefulAuthorizationCacheMaxNum`、`statefulAuthorizationCacheExpiredSecond` | `10000`、`60` | 独立有状态授权策略的容量和过期时间，时间单位为秒。 |
| `statefulAuthorizationCacheNegativeEnable` | `false` | 仅在显式启用时缓存拒绝授权的决策；不缓存错误。 |
| `maintenanceEnabled` | `false` | 显式启用维护配置，通过 `maintenance_policy_reference()` 校验。 |
| `maintenancePolicyPath`、`maintenancePolicyVersion`、`maintenancePolicySha256` | 空、`0`、空 | 启用维护时必须提供的策略引用，同时要求普通认证和授权开关均已启用。 |

## 请求检查语义

- 普通运行时路径依次校验入口上下文、检查全局及账号级 IP 白名单、执行认证，再执行授权。
  命中 `globalWhiteRemoteAddresses`，或命中所选账号的 `whiteRemoteAddress` 时，
  **认证和授权都会被绕过**。请求码白名单在 IP 检查之后分别生效。
- 已启用的超级用户在授权时绕过 ACL 查询。除非命中适用白名单或关闭认证，否则仍需要通过认证。
- ACL 评估优先选择匹配的自定义策略，仅在没有匹配项时回退到默认策略。
  在选定层级内，先比较资源类型和模式的具体程度：精确名称优先于前缀，前缀优先于通配符，
  更长的前缀优先；具体程度相同时，`DENY` 优先。拒绝规则并非无条件覆盖所有策略。
  对于具体的请求操作，策略项只需匹配至少一个操作，无需匹配上下文中的所有操作。
  普通授权服务要求每个已生成的上下文都允许访问。
- 默认拒绝针对已经生成的授权上下文。Remoting 构建器将支持的请求码映射为资源和操作；
  未映射的请求码可能不生成上下文，因此普通检查路径不会产生 ACL 决策。
  对外提供新操作时，接入方必须检查其授权映射。受监督的变更请求码会显式拒绝缺少必要上下文的请求。
- 时间戳窗口非零时，已提供的时间戳必须可解析，且与当前时间的过去或未来偏差不得超过窗口。
  未提供时间戳的请求仍可通过此项检查。实现没有 nonce 或重放缓存，因此仅靠此设置不能防止重放。
  窗口为零时，不执行时间戳检查。

详细评估 API 在授权关闭时返回 `Abstain`。调用方需要按显式指定的分层要求处理该结果；
错误仍然表示失败，不会转换为弃权。
`authenticate_maintenance_principal` 要求认证已启用，并验证凭据，不采用普通路径的白名单绕过。
它为独立的维护策略提供身份，本身不会授予维护操作权限。

## 持久化、重载与缓存

设置 `authConfigPath` 后，本地提供程序持久化 `users.json` 和 `acls.json`。
如果路径带扩展名，实现会先去掉扩展名，再拼接这两个文件名；建议直接使用 `store/auth`
这样的目录。用户快照以明文保存 HMAC 验证所需的密钥。Debug 脱敏不会加密 JSON 或 ACL YAML 文件。

启动时加载配置的 ACL。监听任务比较文件路径和内容，跳过未变化的输入；
`reload_acl_file().await` 强制重载并返回导入的账号数量。
导入成功后替换内存中的地址白名单，递增共享 ACL 代数，并删除该运行时先前通过文件导入管理、
但在新文件中已经不存在的账号。

读取、解析和校验失败发生在导入之前，不会改变已导入的元数据。
随后，本地导入器按顺序更新用户和 ACL，**整个导入过程没有统一事务或回滚**。
如果导入期间发生元数据操作或持久化错误，可能留下部分更新。只有导入成功后才发布新的白名单和代数。
监听任务会记录失败并在后续轮询中重试；这不代表并发请求能够观察到原子的整体快照切换。

使用自定义存储时，通过 `ProviderBundle` 注入 `UserMetadataPort`、`AclMetadataPort`
和 `ProviderControl` 实现。配置 ACL 文件导入或 v1 迁移还需要 `AclSnapshotImport`；
缺少该能力时，构建器会在启动修改操作之前拒绝该组合。

有状态评估器需要显式接入。认证策略按代数、通道和用户名缓存成功及失败结果；
命中缓存后，该请求不再重新验证签名和时间戳。
授权缓存键还包含主体、资源、操作和来源 IP，默认只缓存允许决策。
将评估器绑定到运行时的 `ProviderRegistry` 或共享代数计数器，ACL 重载才能使对应缓存失效。
独立创建的策略不会自动感知另一个运行时的重载。

本地 ACL 查询缓存与上述决策缓存相互独立。刷新时读取提供程序的内存存储，不会读取外部对
`acls.json` 的修改；文件重载由 YAML 轮询机制负责。
运行时指标覆盖使用其指标句柄的组件。独立策略默认持有各自的指标，不会自动汇总到
`runtime.metrics_snapshot()`。

## 需要单独接入的安全适配器

以下 API 均需由所属服务显式接入，`AuthRuntimeBuilder` 不会自动组装这些功能。

| API | 接入边界 |
|-----|----------|
| `OneTimeBootstrap` | 校验受范围和有效期约束的证明材料及可信 TLS 确认，在调用 `BootstrapAdminProvisioner` 前持久化占用状态，防止重启后重复使用。传输层负责验证 TLS；管理员创建失败后，已占用的注册流程保持关闭。 |
| `SecretProviderRegistry` | 按显式注册的提供程序 ID 查找实现。`EnvironmentSecretProvider` 仅从映射的环境变量读取密钥，且不支持写入。 |
| `EncryptedFileSecretProvider` | 使用 AES-256-GCM、版本化文件和严格 Unix 文件权限的本地开发适配器；不会加密普通用户或 ACL 快照。 |
| `CredentialRotationManager` | 管理经过校验的凭据快照、重叠有效期、轮换完成、回滚，以及带审计记录且有期限的紧急访问。注入的解析器负责验证证书、私钥和证明材料之间的关系；不会自动轮换访问密钥或重新配置 TLS 监听器。 |
| `MaintenancePolicyReference::load_from` | 按显式路径、版本和固定 SHA-256 摘要加载并校验 JSON。维护策略评估归属安全 API，服务负责将其与认证组合使用。 |

加密文件密钥适配器和持久化的一次性管理员注册目前在 Windows 上会拒绝运行，
因为尚未实现等效的文件系统 ACL 校验。其 Unix 成功路径需要在 Unix 上验证；
普通 Remoting 认证和本地 JSON 元数据属于独立功能。

## 示例与验证

从工作区根目录运行以下示例，了解各个独立 API：

```bash
cargo run -p rocketmq-auth --example authentication_strategy_usage
cargo run -p rocketmq-auth --example authentication_manager_usage
cargo run -p rocketmq-auth --example authorization_evaluator_usage
cargo run -p rocketmq-auth --example acl_authorization_handler_usage
cargo run -p rocketmq-auth --example metadata_provider_example
```

根据修改涉及的行为选择检查：

```bash
cargo fmt -p rocketmq-auth -- --check
cargo test -p rocketmq-auth --test public_api_contract --test security_api_identity --test java_alignment
cargo test -p rocketmq-auth --examples --no-run
cargo check -p rocketmq-auth --features grpc
```

`public_api_contract` 检查导出和私有模块边界；`security_api_identity` 检查共享类型身份。
`java_alignment` 覆盖签名内容、YAML 语义、相同资源下的拒绝优先规则、超级用户和重载行为。
其中的无效文件重载测试不能证明存储失败时具有事务回滚能力。
`secure_bootstrap_contract`、`secret_provider_contract`、`credential_rotation_contract`
和 `release_checkpoint_authorization` 集成测试分别覆盖对应适配器。
需要更广泛的 crate 测试时，可运行 `cargo test -p rocketmq-auth`。

## 基准测试

```bash
cargo bench -p rocketmq-auth --bench auth_hot_path_bench
cargo bench -p rocketmq-auth --bench auth_acl_watcher_lifecycle_bench
```

第一项测量签名、IP 匹配、ACL 匹配和有状态缓存命中。
第二项测量 ACL 重载与关闭生命周期，并将报告写入 `target/runtime-baseline/prototype`。
只编译基准测试目标、不采集测量数据时，可使用
`cargo test -p rocketmq-auth --benches --no-run`。
请在相同工具链和硬件环境下比较结果，并将生成的报告保留在 `target/` 下。

## 许可证

RocketMQ-Rust 使用 Apache License 2.0。参见 [LICENSE-APACHE](../LICENSE-APACHE)。
