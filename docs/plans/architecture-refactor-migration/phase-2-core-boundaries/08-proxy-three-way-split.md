# M08：Proxy Core、Cluster、Local 三向物理拆分

## 元数据

| 字段 | 值 |
|---|---|
| 阶段 | Phase 2：核心边界与 API 收敛 |
| 状态 | 实施中；PR-M08-02 已完成，下一工作包为 PR-M08-03 |
| 预计周期 | 3–4 周 |
| 工作包 | WP19 `proxy-three-way-split` |
| 前置条件 | model/protocol/transport/security/store-api 边界稳定；除 Proxy 外 Client 清边完成 |
| 可并行项 | core、cluster、local fixture 可并行准备；实际 proto/port/adapter 移动严格按 core→cluster/local→facade 串行 |
| 完成后解锁 | M09 |

## 目标

- 创建 `rocketmq-proxy-core`、`rocketmq-proxy-cluster`、`rocketmq-proxy-local` 三个物理依赖边界。
- core 只包含中立 plan/port/status/error/session/ingress；cluster 独占完整 Client lifecycle；local 只接 Broker/store capability。
- 现有 proxy 只负责 binary、config、auth provider/observability composition 和旧路径 re-export。
- R0 保持当前 `default = []` 且两端无条件可用；下一 major 才切换 optional mode feature。

## 非目标

- R0 不改变 Proxy 默认运行配置、Serde/env/CLI 字段或 generated gRPC contract。
- 不允许 core/local 通过 common/remoting/admin-core re-export 间接获得完整 Client。
- 不在 core 放 backend adapter，不让 cluster 依赖 Broker/store，也不让 local 依赖 Client。

## 入口条件

M07 已交付 [`Client 边界收口与 M08 交接清单`](07-client-edge-closeout-handoff.md)，其中包含精确临时账本、
物理 owner、转换 seam、remoting lock/unlock 切片与 lifecycle 风险；以下项目在 PR-M08-01 候选快照上正式签署。

- [x] `[ARCH]` 冻结 core port、model/PullOutcome 边界、cluster/local lifecycle 和 facade feature 两阶段策略。
- [x] `[TEST]` 准备 gRPC ingress、remoting ingress、send/pull/pop/ack/route/transaction、mode closure corpus。
- [x] `[DEV]` 确认 proxy build.rs/proto/service/remoting/cluster/local 文件无用户修改重叠。
- [x] `[HUMAN]` 批准 R0 不实现 optional mode feature，下一 major 才改变 `--no-default-features` 语义。

## 交付物

| 类型 | 交付物 |
|---|---|
| Core | proto generation、context/error/status/session/config、plan/port/service、gRPC/remoting ingress |
| Cluster | Client runtime、producer/consumer/route/remoting、注入式 OutboundSigner、auth metadata adapter |
| Local | Broker runtime、message/consumer/route/transaction、LocalServiceManager，无 Client |
| Facade | bootstrap/config partition/auth runtime/observability/binary/compat re-export |
| Features | R0 非 optional adapter；目标 major 的 cluster/local/compat-all-modes 计划和 compile fixture |
| Tests | core/cluster/local/facade integration、normal closure、canonical/legacy path |

## PR 级开发步骤

### PR-M08-01：创建 `rocketmq-proxy-core` 与 proto owner

- [x] `[ARCH]` 冻结 core 允许依赖及 canonical root exports。
- [x] `[DEV]` 创建 crate并迁 build.rs/proto/generated contract 的唯一 owner；现有 proxy re-export。
- [x] `[DEV]` 迁 context/error/status/session 和 ingress config，替换 client/provider 类型为 model/security/core DTO。
- [x] `[TEST]` gRPC schema/hash/generated API、canonical/legacy compile 和 ingress error mapping 差分。
- [x] `[REV]` 检查 core closure 无 Client/admin-core/Broker/store facade/auth provider/legacy。
- [x] 回滚点：proto generation 归还旧 proxy；不允许两个 build.rs 同时生成同一 contract。

#### PR-M08-01 实施结果

- `rocketmq-proxy-core` 已加入根 workspace（30/32），`default = []`；唯一拥有 build script、两个 proto schema、
  generated module、Proxy error/status、request context、session registry、resource identity 与 normalized ingress config。
- Context 对认证证明类型泛化，通过 `rocketmq-transport::ConnectionContext` 读取连接元数据；旧 Proxy 将 context
  专用于 facade 私有构造的 `AuthenticatedPrincipal`，白名单信任位无法由外部调用者伪造。Session registry 对 Channel
  slot 泛化，旧 Proxy 继续精确导出 `ClientSessionRegistry<rocketmq_remoting::Channel>`，没有复制第二份 state owner。
- Lite topic compose/parse helper 迁至 `rocketmq-model::lite`，Common 只做兼容 re-export；status 的 ResponseCode
  使用 `rocketmq-protocol` canonical path。Core 不依赖 common/remoting/auth provider/Client/Broker/store/legacy facade。
- `definition.proto` 与 `service.proto` 的 SHA-256 分别冻结为
  `28706c9d2dee01dadf54daaf7a070a1ab4b30172f284ffb2f8189569f13ac2c1` 和
  `b7e1026b16c2284921a4d11a0b348074df82a15a8399c43f00e6005142bb5128`；Proxy family 的 v2 schema 只有一个
  `compile_protos` 和一个 `include_proto!("apache.rocketmq.v2")` owner。
- canonical/legacy type identity、generated client/wire、七类 ingress error mapping、ProxyConfig Serde/default、gRPC 与
  remoting ingress 均由可重复测试覆盖。target guard 仍为预期 66 项，但新 Core 零 finding，缺失计划 package 由 3 降至 2。
- 回滚边界是整个 Core owner slice：恢复旧 Proxy build/proto/source owner并删除单一 facade re-export；禁止保留双生成或双状态 owner。

### PR-M08-02：迁中立 plan、port、service 与 ingress

- [x] `[DEV]` 按 send/pull/pop/ack/route/transaction 迁 plan 和 port，跨边界只用 model/PullOutcome/core DTO。
- [x] `[DEV]` 消费已迁入 Core 的 ResourceIdentity，从混合 `service.rs` 精确提取 port/default/static service、ServiceManager contract。
- [x] `[DEV]` 从混合 `remoting.rs` 精确提取 ingress processor/dispatcher/转换；不迁 cluster address resolution。
- [x] `[DEV]` 迁 gRPC handler/middleware/server/service，只调用 port，不持 backend。
- [x] `[TEST]` 对旧 proxy 与 core+test adapter 执行 ingress/status/error/plan 差分。
- [x] `[REV]` 检查 source slice 不重叠、core 无 backend 类型、public enum/DTO 兼容。
- [x] 回滚点：以 plan/port/gRPC/remoting 四批回滚，旧 proxy facade 始终可组合。

#### PR-M08-02 实施结果

- Core 新增 owned `ProxyMessage`/`ProxyMessageExt`，send/pull/pop/ack/route/transaction 的 request/plan/result、
  `MessagingProcessor`/`DefaultMessagingProcessor`、六组 service port、`ServiceManager` 与 default/static service 均迁入
  canonical owner；旧 `processor.rs` 降为精确 re-export，旧 `service.rs` 只保留 Cluster/Local provider 实现与兼容导出。
- Core port 统一接收无认证证明的中立 `ProxyContext`；facade 在调用 port 前执行 `without_principal()`。Metadata
  service 返回 Protocol `UserInfo`/`AclInfo`，provider/auth facade 承担转换，Core 不再持有 auth provider 类型。
- Core 拥有 gRPC wire adapter、transport-context middleware、listener/shutdown lifecycle，以及 admission、topic、consumer、
  producer、transaction、telemetry、housekeeping 中立策略。现有 Proxy 只保留 generated tonic service、auth/metrics/hook
  composition；事务 producer group 通过 Transaction port 获取，不再从 gRPC ingress 直接调用 Cluster 模块。
- Core Remoting 拥有 request support matrix、classifier、dispatch contract 与 send/pull/offset status mapping；旧 Proxy 保留
  TCP/auth、legacy header/body codec、session channel binding 和 cluster lock/unlock address resolution，未把 backend 地址解析迁入 Core。
- canonical/legacy type identity、gRPC/remoting ingress、status/error/plan 由差分与集成测试覆盖：Core 45 项 unit + 2 项
  proto contract，Proxy 104 项 unit/bin/compat/gRPC/remoting test 全绿；default/no-default、根 30-package strict Clippy、
  runtime audit 以及 Example/Tauri/Web standalone 累计路线通过。Typed-error guard 仅报告 main 既有 11 项，Core/Proxy 零新增。
- target guard 仍精确为 66 项，Core 零 finding；normal closure 无 Client/Broker/store/auth/common/remoting/legacy facade。
  回滚可按 message/plan+port、gRPC、Remoting 四个批次独立恢复，facade 始终保持可组合。下一工作包为 PR-M08-03。

### PR-M08-03：创建 Cluster adapter

- [ ] `[DEV]` 创建 `rocketmq-proxy-cluster`，迁 MQClientInstance/Manager、Cluster service/manager、producer/consumer/route/remoting。
- [ ] `[DEV]` 将 Client SendResult/PullResult/callback 在 crate 边界转换为 model SendResult/PullOutcome/core status。
- [ ] `[DEV]` 只消费注入的 security-api OutboundSigner；auth provider composition 留 proxy facade。
- [ ] `[TEST]` 覆盖 send/pull/pop/ack/route、retry、client lifecycle、signing 和 shutdown。
- [ ] `[REV]` 检查 cluster closure 无 Broker/store/local/auth provider，Client 类型不泄漏到 core port。
- [ ] 回滚点：facade 临时选择旧 cluster adapter；Client direct edge不得回到 composition manifest作为最终状态。

### PR-M08-04：创建 Local adapter

- [ ] `[DEV]` 创建 `rocketmq-proxy-local`，迁 LocalBrokerFacade、LocalServiceManager 和 message/consumer/route/transaction。
- [ ] `[DEV]` 使用 Broker 窄 facade、store-api 和 model result；tiered 只能从 local 路径启用。
- [ ] `[TEST]` 覆盖 local send/pull/pop/ack/route/transaction、embedded lifecycle 和 tiered feature。
- [ ] `[REV]` 检查 manifest/source/normal closure完全无 Client及其 re-export，local 类型不泄漏 cluster。
- [ ] 回滚点：facade 临时选择旧 local adapter；禁止恢复 Client 依赖。

### PR-M08-05：现有 Proxy 降为 composition/facade

- [ ] `[DEV]` 只保 bootstrap、分区 config conversion、auth runtime、observability、binary 和旧 public path re-export。
- [ ] `[DEV]` R0 facade 对 cluster/local 使用非 optional dependency，继续 `default = []`；移除 facade 自身的完整 Client direct edge。
- [ ] `[DEV]` 保持 ProxyConfig Serde/env/CLI 默认模式；core/cluster/local 分别消费 normalized config。
- [ ] `[TEST]` 运行现有默认和 no-default，两者继续编译两端并保持运行行为。
- [ ] `[REV]` 检查 facade 不新增业务算法，core/local 不经 facade 反向到 cluster/client。
- [ ] 回滚点：按 bootstrap/config/re-export 适配器回滚；不得提前启用下一 major feature 语义。

### PR-M08-06：Feature closure 与下一 major fixture

- [ ] `[TEST]` R0 实际验证 proxy-core、cluster、local、local+tiered、facade default/no-default、observability。
- [ ] `[ARCH]` 为下一 major 固化 `cluster-mode`、`local-mode`、`compat-all-modes` 和 default 的预期，不在 R0 manifest 启用。
- [ ] `[DEV]` dependency guard 移除 Proxy 临时例外，目标 Client allowlist正式达标。
- [ ] `[REV]` 使用 `cargo tree -e normal` 检查完整传递闭包，test/dev edge单独报告。
- [ ] `[TEST]` canonical/legacy path、gRPC/remoting integration 与 shutdown/fault 全绿。
- [ ] `[HUMAN]` 批准 R0 功能等价和下一 major feature 迁移公告。

## 公共兼容面

- generated gRPC/protobuf、ProxyConfig、CLI/env、status/error 和现有 `rocketmq-proxy` public path 不变。
- R0 `rocketmq-proxy default = []` 且 cluster/local 都编译的语义保持；三个新 crate 自身 `default = []`。
- 下一 major 才把 adapter 设 optional，并令 `default = [compat-all-modes]`；这是公开 feature 破坏性边界。
- Client 类型只在 cluster 内存在；compat facade re-export 必须转换为 core/model 类型。

## 验证命令

### 当前即可执行

```powershell
cargo test -p rocketmq-proxy
cargo check -p rocketmq-proxy --no-default-features
cargo fmt --all -- --check
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline
.\scripts\check-error-hygiene.ps1
git diff --check
```

### 本里程碑新增后执行

```powershell
cargo check -p rocketmq-proxy-core --no-default-features
cargo test -p rocketmq-proxy-core
cargo check -p rocketmq-proxy-cluster --no-default-features
cargo test -p rocketmq-proxy-cluster
cargo check -p rocketmq-proxy-local --no-default-features
cargo test -p rocketmq-proxy-local
cargo check -p rocketmq-proxy-local --features tieredstore
cargo tree -p rocketmq-proxy-core -e normal
cargo tree -p rocketmq-proxy-cluster -e normal
cargo tree -p rocketmq-proxy-local -e normal
python scripts/architecture_dependency_guard.py --mode target --allow-missing-planned-crates
```

`compat-all-modes`、`cluster-mode`、`local-mode` 在下一 major 实际定义前只作为设计 fixture，不作为 R0 当前命令。

## 回滚触发器

- core/local normal closure 出现 Client，或 cluster 出现 Broker/store。
- protobuf/generated API、ProxyConfig、默认/no-default 运行模式发生未批准变化。
- Client result/callback 泄漏到 core port，或 local/cluster 通过 facade 互相依赖。
- proto 由两个 crate 重复生成，或 old/new status/error 语义不一致。
- Client allowlist仍依赖临时 Proxy 例外。

按 core、cluster、local、facade 四个 adapter seam 回滚。若物理边界无法在保持 R0 兼容的同时成立，停止并升级 `[HUMAN]`，不得用依赖 guard 豁免收尾。

## Exit Checklist

- [ ] `[REV]` core 任何 feature closure 无 Client/Broker/store/auth provider。
- [ ] `[REV]` local normal closure无 Client，cluster normal closure无 Broker/store。
- [ ] `[TEST]` send/pull/pop/ack/route/transaction 与 gRPC/remoting 差分全绿。
- [ ] `[DEV]` 现有 proxy 只 composition/re-export，R0 default/no-default 语义不变。
- [ ] `[TEST]` local+tiered、observability、canonical/legacy fixture通过。
- [ ] `[DEV]` Client allowlist无需临时例外即可通过。
- [ ] `[ARCH]` 下一 major mode feature 迁移公告和 fixture 已冻结。
- [ ] `[HUMAN]` R0 Proxy 兼容与 M08 Gate 已签署。

## 交接物

- 向 M09 交付三个 crate 的 closure 证据、proxy facade ledger、Client allowlist和下一 major feature 计划。
- 向 M11 交付 Proxy security/observability/shutdown composition hooks。
- 向 M12 交付中立 Proxy control/status port，不向 AI 暴露 backend runtime。
