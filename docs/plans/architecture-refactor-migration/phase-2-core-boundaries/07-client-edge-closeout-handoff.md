# M07 Client 边界收口与 M08 交接清单

## 1. 结论

PR-M07-07 完成后，M07 的目标边界已经成立：Broker、NameServer、MCP、Tauri/Web Dashboard、
`rocketmq-common`、`rocketmq-remoting` 均不能直接或通过 normal dependency 闭包获得完整 Client。
永久合法的 Client 入口与临时迁移债务已经分离；临时账本只剩现有 Proxy，归 M08 删除。

当前根 workspace 为 29/32，缺少的三个 package 均属于 M08：

- `rocketmq-proxy-core`
- `rocketmq-proxy-cluster`
- `rocketmq-proxy-local`

目标 guard 仍为预期失败，共 66 项：Client manifest 1、Client source 13、目标 DAG 直接边 50、
TieredStore 既有传递闭包边 2。14 项 Client finding 全部属于 `rocketmq-proxy`；这不是 M07 的
永久 allowlist，而是 owner=`proxy`、remove_by=`M08` 的精确临时账本。

新增的一项目标 DAG 差距是现有 Proxy 在物理拆分前直接使用 `rocketmq-model` 中立结果；PR-M08-01/02
把这些类型迁入 `proxy-core` 后，由 facade 经 core 间接获得，不应把该临时直边加入目标 DAG。

## 2. 永久 Client allowlist

Manifest allowlist 按 caller、target、kind、path、alias 五元组精确匹配，每个身份最多一条：

| Caller | Kind | Manifest | Alias | 目标职责 |
|---|---|---|---|---|
| `rocketmq-admin-core` | normal | `rocketmq-tools/rocketmq-admin/rocketmq-admin-core/Cargo.toml` | `rocketmq_client_rust` | 受 feature 控制的管理 adapter |
| `rocketmq-proxy-cluster` | normal | `rocketmq-proxy-cluster/Cargo.toml` | `rocketmq_client_rust` | 完整 Client lifecycle 的唯一 Proxy owner |
| `rocketmq-example` | dev | `rocketmq-example/Cargo.toml` | `rocketmq_client_rust` | standalone SDK 示例 |

Source allowlist 同时匹配 caller、目录前缀和 alias：

| Caller | 唯一合法源码范围 |
|---|---|
| `rocketmq-admin-core` | `src/client_adapter/` |
| `rocketmq-proxy-cluster` | `src/` |
| `rocketmq-example` | `examples/` |

错误 dependency kind、rename alias、第二条相同边、错误 manifest path、越界源码目录或错误 source
alias 都必须触发 guard。永久 allowlist 不再复制到 baseline exception；baseline 只记录可删除债务。

## 3. M08 必须删除的临时账本

| 类别 | 文件 | 数量 | 内容 |
|---|---|---:|---|
| manifest | `rocketmq-proxy/Cargo.toml` | 1 | composition crate 仍直接依赖 Client |
| source | `rocketmq-proxy/src/cluster.rs` | 12 | Client config、callback/result/status、instance/manager、producer、PullResult |
| source | `rocketmq-proxy/src/remoting.rs` | 1 | lock/unlock 路径直接接收 `MQClientInstance` |

M07 已把原 35 个 source finding 中的 22 个中立 DTO 引用改为 canonical model path：

- `SendResult`、`SendStatus` 使用 `rocketmq_model::result`；
- `PullStatus` 使用 `rocketmq_model::result`；
- gRPC/remoting ingress、service/status/processor、local adapter 与测试不再借 Client re-export 获取中立类型。

因此 M08 不得把这些中立引用搬进 cluster 来伪造收口；只迁移真正的 Client runtime 实现。

## 4. 物理拆分所有权

### `rocketmq-proxy-core`

- proto/generated contract 的唯一 owner，以及 context、error、status、session、normalized ingress config；
- `processor.rs` 的 request/plan/result entry 与 `MessagingProcessor`；
- `ResourceIdentity`、`ProxyTopicMessageType`、`SubscriptionGroupMetadata`；
- Route、Metadata、Assignment、Message、Consumer、Transaction 六组 port；
- `ServiceManager` contract 以及不接触 backend 的 default/static 实现；
- gRPC/remoting ingress 的中立解析、校验、status mapping 与 dispatch contract。

Core 禁止依赖 Client、Broker、Store facade、`rocketmq-auth` provider 类型和 legacy facade。当前
`MetadataService::user/acl` 暴露的 auth `User`/`Acl` 是已知污染点，PR-M08-02 必须改为
proxy-core owned snapshot 或 security-api 中立 primitive，在 cluster/facade adapter 处转换。

### `rocketmq-proxy-cluster`

- `ClusterClient`、cluster worker/state/cache、Cluster service 实现与 `ClusterServiceManager`；
- `ClientConfig`、`MQClientInstance`、`MQClientManager`、`DefaultMQProducer`；
- pop/ack callback、pull、route、producer、consumer、transaction 和 remoting lock/unlock backend；
- Client 类型到 model/core owned DTO 的唯一转换层；
- 注入式 outbound signer，不拥有 auth provider composition。

### `rocketmq-proxy-local`

- `local.rs` 的 Broker/store capability adapter；
- Local route/metadata/assignment/message/consumer/transaction service；
- `LocalServiceManager` 与 local lifecycle；
- 只依赖窄 Broker facade、store-api、model 和 core，不依赖 Client 或 cluster。

### 现有 `rocketmq-proxy`

只保留 binary/bootstrap、ProxyConfig 到 normalized config 的分区转换、auth provider 与 observability
composition、mode 选择和 R0 兼容 re-export。完成 PR-M08-05 后 manifest/source 均不能直接依赖 Client。

## 5. Client 类型转换 seam

| Client 内部类型 | 转换位置 | Core/model 输出 |
|---|---|---|
| `PopResult` / `PopStatus` | cluster receive adapter | `ReceiveMessagePlan`、owned `ReceivedMessage`、`ProxyPayloadStatus` |
| `PullResult` / `PullStatus` | cluster Client boundary，再由 service adapter 投影 | model `PullOutcome<MessageExt>`，再转 `PullMessagePlan` |
| `AckResult` / `AckStatus` | cluster ack adapter | `AckMessageResultEntry` / `ProxyPayloadStatus` |
| producer result | cluster send adapter | model `SendResult`、`SendMessageResultEntry` |
| Client config/instance/manager/producer/callback | cluster private runtime | 不得进入任何 core public port |

`PullResult` 必须先在 cluster 内去除 `ArcMut<MessageExt>` 并形成 owned `PullOutcome<MessageExt>`；core port 只观察
model outcome 或已投影的 `PullMessagePlan`，不得直接接收 Client `PullResult`。

`remoting.rs` 的 LockBatch/UnlockBatch 当前会检查 cluster mode、解析单 broker/topic、直接创建 Client
instance 并解析地址。这组 backend 行为必须整体迁入 cluster port；remoting ingress 只保留协议 decode、
core request、dispatch 与 response encode。

## 6. 生命周期风险

当前 cluster worker 使用 `ActorRuntime::spawn_current_thread`，构造失败路径包含 `expect`，且 owner/shutdown
关系没有在 core port 上显式表达。M08 必须：

- 由注入的 `ServiceContext`、`TaskGroup` 或等价已审计 owner 创建 worker；
- shutdown 取消并等待 worker、producer 和 Client instance；
- 使用一个绝对 `ShutdownDeadline` 贯穿 drain/join，不在分层时重置；
- 不新增 detached `tokio::spawn`、裸线程、嵌套 runtime 或恢复 ArcMut escape；
- 为活动 send/pull/pop/ack 与 lock/unlock 的关闭路径提供确定性测试。

## 7. M08 串行执行顺序

1. PR-M08-01：创建 core 与 proto owner，旧 Proxy 只 re-export，禁止双 build.rs。
2. PR-M08-02：迁中立 plan/port/service/ingress，先消除 auth/backend 类型污染。
3. PR-M08-03：创建 cluster，迁完整 Client runtime 和上述五个转换 seam。
4. PR-M08-04：创建 local，迁 Broker/store capability，证明 normal closure 无 Client。
5. PR-M08-05：Proxy 降为 composition/facade，删除现有 Proxy manifest/source Client 临时账本。
6. PR-M08-06：验证 R0 default/no-default 与下一 major feature fixture，删除 baseline 临时例外。

物理移动必须按 core → cluster/local → facade 串行；同一源码切片不能由两个 crate 同时拥有。

## 8. 每个 M08 PR 的最低验收

- `cargo test -p rocketmq-proxy` 及当次新增 crate 的 test/check/strict Clippy；
- 根 workspace `cargo fmt --all -- --check` 与 all-target/all-feature strict Clippy；
- `python scripts/architecture_dependency_guard.py --mode baseline`；
- `python scripts/architecture_dependency_guard.py --mode target --allow-missing-planned-crates`，并记录分类下降；
- architecture guard 单测、M07 closeout contract、runtime audit、ArcMut guard/fixtures、AGENTS routing；
- canonical/legacy Proxy API、gRPC/remoting contract、ProxyConfig/Serde/env/CLI 与 default/no-default fixture；
- 若共享依赖影响 standalone consumer，按最近 `AGENTS.md` 累计验证 Example、Tauri、Web；只有
  `dashboard-common` 实际变化时才触发 GPUI。

M08 完成条件不是“代码已移动”，而是三个新 crate 成为真实物理 owner、现有 Proxy 只做 composition，
Client 临时账本归零且 32-package/target-DAG 证据可重复。

## 9. PR-M08-01 消费记录（2026-07-17）

- `rocketmq-proxy-core` 已创建，根 workspace 从本交接快照的 29/32 推进到 30/32；剩余 package 为
  `rocketmq-proxy-cluster` 与 `rocketmq-proxy-local`。
- proto/error/status/context/session/ResourceIdentity/normalized ingress config 已由 Core 唯一拥有，旧 Proxy 只保留
  兼容 re-export、认证证明/Remoting `Channel` 专用 type alias 与 provider/composition adapter；白名单信任位仍由
  Proxy auth facade 私有签发，Core context 不公开可伪造的授权捷径。
- Core normal closure 无 Client、Broker、store、auth provider、common、remoting 或 legacy facade；M07 冻结的
  Client 临时账本仍精确为 Proxy manifest 1、`cluster.rs` 12、`remoting.rs` 1，归 PR-M08-03～05 删除。
- target guard finding 总数保持 66，新 Core 零 finding；下一串行工作包为 PR-M08-02。

## 10. PR-M08-02 消费记录（2026-07-17）

- processor request/plan/result、六组 service port、default/static service、gRPC 中立策略和 Remoting dispatch contract 已由
  `rocketmq-proxy-core` 拥有；Core message DTO 与无 principal context 阻止 Common/auth provider 类型跨入 port。
- Metadata port 的 `UserInfo`/`AclInfo` 已改用 Protocol canonical DTO；gRPC 事务 producer group 改由 Transaction port
  返回，ingress 源码不再直接引用 Cluster 模块。
- M07 冻结的 Client 临时账本仍精确为 Proxy manifest 1、`cluster.rs` 12、`remoting.rs` 1；本切片没有迁移或伪装
  Client runtime，PR-M08-03 将把这些真实 Cluster lifecycle/source edge 搬入 `rocketmq-proxy-cluster`。
- target guard finding 总数保持 66，Core 零 finding；49/82 工作包已完成、33 未完成，下一串行工作包为 PR-M08-03。
