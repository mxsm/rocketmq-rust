# M03：基础合同、Client 中立类型与安全预检

## 元数据

| 字段 | 值 |
|---|---|
| 阶段 | Phase 1：安全性与基础治理 |
| 状态 | 已完成（2026-07-11） |
| 预计周期 | 3–4 周 |
| 工作包 | WP07 `observability-decouple`、WP08 `model-security-leaves`、WP16 `secure-profile-dry-run`、WP17 `client-neutral-types` |
| 前置条件 | M01 foundation 禁边与兼容基线可执行 |
| 可并行项 | model、security-api、observability、secure dry-run 可按 crate 并行；canonical type/re-export 顺序必须串行 |
| 完成后解锁 | M04、M06，并为 M07/M08 提供中立 DTO |

## 目标

- 创建 `rocketmq-model` 和 `rocketmq-security-api` 两个空默认 feature 的低层 crate。
- 将保持类型身份的 Client/Common 值类型迁到 model；为旧 `PullResult` 提供新的 owned `PullOutcome` 边界。
- 将 exporter 配置归还 observability owner，并解除 `rocketmq-observability → rocketmq-common`。
- 建立 secure/development/compatibility profile 的无副作用 dry-run 和 secret redaction 基线。

## 非目标

- 不创建泛化 `client-api`、config、extension-api 或 broker-core crate。
- 不把 `TraceDataEncoder`、`TopicPublishInfo`、client callback/cache/lifecycle 移入 model/protocol。
- secure dry-run 不在本里程碑静默改变已有生产部署默认行为。
- security-api 不实现 auth provider、TLS、crypto 或 RemotingCommand adapter。

## 入口条件

- [x] `[ARCH]` 逐项确认“原样迁移并精确 re-export”和“新 DTO + adapter”的边界。
- [x] `[TEST]` 冻结字段、Serde、Display、排序、非法输入、Debug redaction 差分语义。
- [x] `[DEV]` 确认 common/client/auth/observability 目标文件无用户修改重叠。
- [x] `[HUMAN]` 批准不迁移旧 `PullResult` 类型身份，跨 crate 使用 `PullOutcome`。

## 交付物

| 类型 | 交付物 |
|---|---|
| Crate | `rocketmq-model`：message/queue/topic/identity/result/filter 等稳定值对象 |
| Crate | `rocketmq-security-api`：context/request view/peer/principal/resource/action/decision/policy/signer |
| Client | SendResult、QueryResult、SendStatus、PullStatus 精确 re-export；PullResult ↔ PullOutcome adapter |
| Queue | 6 个无 runtime 状态的分配算法和差分 corpus |
| Observability | exporter owner、model 类型导入、Broker/Controller metric manager 上移、common 闭包清零 |
| Security | profile dry-run、unknown fail、redacted Debug、受限文件检查和兼容报告 |

## PR 级开发步骤

### PR-M03-01：创建最小 `rocketmq-model`

- [x] `[ARCH]` 固定 model 允许依赖为 error 和必要外部值类型库；禁止 Tokio/runtime/transport/facade/service。
- [x] `[DEV]` 新建 crate，继承 workspace version/edition/license/rust-version/lints，`default = []`。
- [x] `[DEV]` 先迁 `MessageQueue` 和 `TopicConfig` 作为垂直切片，common 保原路径精确 re-export。
- [x] `[TEST]` 为 canonical/legacy path 编译、Serde round-trip、Hash/Ord/Display 和类型同一性增加 fixture。
- [x] `[REV]` 检查只定义一次类型，不复制 schema，不把 owner-specific config 或动态 registry 下沉。
- [x] 回滚点：删除新 crate/成员和 re-export，原类型仍在旧 owner；本 PR 不迁消费者。

### PR-M03-02：迁移中立结果与分配算法

- [x] `[DEV]` 依次迁 SendResult、SendStatus、QueryResult、PullStatus；每种类型单独提交或保持独立 diff。
- [x] `[DEV]` 在 model 新建 immutable/owned `PullOutcome`；旧 `PullResult<Vec<ArcMut<MessageExt>>>` 留 client。
- [x] `[DEV]` 实现双向 adapter，明确复制/所有权成本和无法无损转换的错误。
- [x] `[DEV]` 迁移 `AllocateMessageQueueStrategy` 与 6 个无状态实现；client 旧路径精确 re-export。
- [x] `[TEST]` 差分覆盖排序、空集、非法输入、Display、i32 转换、Serde 和 Pull message 顺序。
- [x] `[REV]` 检查 callback、cache、TopicPublishInfo、AccessChannel、LocalTransactionState 未泄漏到 model。
- [x] 回滚点：逐类型恢复旧定义；adapter 测试作为兼容证据保留，不能留下两份 public 类型。

### PR-M03-03：创建 `rocketmq-security-api`

- [x] `[ARCH]` 冻结借用式 `SecurityRequestView`：code/version/fields/body/peer 的最小只读投影。
- [x] `[DEV]` 新建空默认 feature crate，定义 RequestContext、PeerInfo、Principal、Resource、Action、Decision、RequestPolicy、OutboundSigner。
- [x] `[DEV]` common/auth 旧路径用精确 re-export 或 adapter 保持；provider 实现继续归 `rocketmq-auth`。
- [x] `[TEST]` 验证 request view 不复制 body/ext map、缺少身份 fail closed、Debug 永不输出 body/token/credential。
- [x] `[REV]` 检查 dependency tree 不含 protocol、RemotingCommand、Channel、Tokio、transport、auth provider。
- [x] 回滚点：保持 auth 现有实现，撤销新 contract adapter；不修改现有 wire 行为。

### PR-M03-04：解除 observability 对 common 的依赖

- [x] `[ARCH]` 将 exporter enum/config 归 observability；Broker/Controller 私有 metric manager 归各自 service owner。
- [x] `[DEV]` observability 的 message/property/queue 导入改用 model，bench fixture 同步改 canonical 类型。
- [x] `[DEV]` 上移 broker/controller manager，Broker 直接消费 protocol version 的动作留到 M04 或受控 adapter，避免形成环。
- [x] `[DEV]` 删除 observability 的 common manifest 边；此时不提前添加 common → observability compat 边。
- [x] `[TEST]` 覆盖 normal/dev/all-targets dependency closure 和现有 observability feature matrix。
- [x] `[REV]` 证明 `cargo tree -p rocketmq-observability` 不含 common/remoting/rocketmq-rust，且未新增 protocol 直接边。
- [x] 回滚点：按 exporter、model import、manager 上移三个切片回滚；任何中间提交不得形成 common↔observability 环。

### PR-M03-05：secure profile dry-run

- [x] `[ARCH]` 固定 development/compatibility/secure 的校验规则；unknown 配置不得落到 permissive。
- [x] `[DEV]` 实现只读 config validator 和迁移报告；secure 缺 trust/secret/bootstrap 时报告 readiness 失败原因。
- [x] `[DEV]` 实现 secret wrapper 的 redacted Debug、受限文件权限检查、原子读取错误；不把 secret 放入通用 snapshot。
- [x] `[TEST]` 覆盖 unknown profile、过期 bootstrap、宽松 downgrade、日志/Debug 泄密和兼容部署报告。
- [x] `[REV]` 检查 dry-run 无写配置、无创建身份、无改变现有运行默认值的副作用。
- [x] `[HUMAN]` 批准 M11 将 secure 作为新部署默认的迁移前提，而非本 PR 立即切换。
- [x] 回滚点：关闭 dry-run 暴露入口即可；不得回滚 redaction 测试或把 unknown 恢复成 permissive。

### PR-M03-06：Phase 1 foundation 收口

- [x] `[DEV]` 更新 dependency policy，登记两个实际新 crate；root workspace 从 22 临时演进为 24。
- [x] `[TEST]` 执行 canonical/legacy compile fixture、dependency guard、feature/no-default 和受影响 consumer。
- [x] `[REV]` 核对 public 类型只定义一次，old path 至少保留一个 major。
- [x] `[ARCH]` 向 M04/M06 发布已冻结的 model/security contract 和禁止边。
- [x] `[HUMAN]` 批准 Phase 1 Gate，记录 M02/M03 未完成项时不得进入依赖它们的里程碑。

## 公共兼容面

- SendResult、QueryResult、SendStatus、PullStatus、MessageQueue、TopicConfig 的字段/Serde/Display/枚举值保持不变，旧路径精确 re-export canonical 类型。
- `PullResult` 不迁移类型身份；旧 API 继续可用，跨 crate 新合同使用 `PullOutcome`，由显式 adapter 转换。
- common/auth 的安全合同旧路径在 R0 保留；security-api 不暴露 provider 实现。
- secure dry-run 只报告，不改变已有部署默认；正式 secure 默认切换在 M11 通过版本化配置和迁移说明完成。

## 验证命令

### 当前即可执行

```powershell
cargo test -p rocketmq-common
cargo test -p rocketmq-client-rust --lib
cargo test -p rocketmq-auth
cargo test -p rocketmq-observability
cargo tree -p rocketmq-observability
cargo fmt --all -- --check
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
.\scripts\check-error-hygiene.ps1
git diff --check
```

Observability 还必须按 CI 当前定义执行 `observability`、`otlp-metrics`、`otel-metrics,prometheus`、`otlp-traces`、`otlp-logs` 和组合 OTLP/Prometheus 的适用 check/Clippy/test。

### 本里程碑新增后执行

```powershell
cargo check -p rocketmq-model --no-default-features
cargo test -p rocketmq-model
cargo check -p rocketmq-security-api --no-default-features
cargo test -p rocketmq-security-api
cargo tree -p rocketmq-model -e normal
cargo tree -p rocketmq-security-api -e normal
python scripts/architecture_dependency_guard.py --mode baseline
python scripts/arc_mut_guard.py
```

测试符号、crate 和 guard 文件实际创建后才能计为通过。

## 回滚触发器

- canonical 与 legacy path 不再是同一类型，或 Serde/wire/Display 发生未批准变化。
- foundation crate 的 normal closure 引入 Tokio、transport、facade、service 或 auth provider。
- PullResult adapter 丢失消息顺序/属性，或需要隐式可变别名。
- observability 形成 common 环，或 all-targets 闭包仍包含 facade/legacy。
- dry-run 修改配置、写入身份或将 unknown 降级为 permissive。

回滚按垂直切片撤销；若类型已被后续 consumer 使用，先恢复兼容 re-export 再回滚 consumer。禁止通过复制第二份类型来快速解环。

## Exit Checklist

- [x] `[DEV]` model/security-api 均 `default = []` 并继承根 workspace 元数据。
- [x] `[TEST]` canonical/legacy 编译、Serde/Display/分配算法/Pull adapter 差分全绿。
- [x] `[REV]` 两个低层 crate 的 dependency closure 符合禁边。
- [x] `[TEST]` SecurityRequestView 不复制敏感载荷，Debug 已脱敏，缺身份 fail closed。
- [x] `[REV]` observability normal/dev/all-targets closure 不含 common/remoting/rocketmq-rust。
- [x] `[TEST]` secure dry-run 正确拒绝 unknown/缺失/过期配置且无副作用。
- [x] `[DEV]` workspace 实际 package 数和 M01 policy 一致。
- [x] `[HUMAN]` 类型身份例外、secure 默认切换时点和 Phase 1 Gate 已签署。

## 交接物

- 向 M04 交付 model canonical 类型、route/trace 前置值对象和兼容 fixture。
- 向 M05 交付 SecurityRequestView/RequestPolicy/OutboundSigner 合同。
- 向 M06 交付 model 查询值和 foundation dependency policy。
- 向 M07/M08 交付 Client 中立结果、PullOutcome 和分配算法。
- 向 M11 交付 secure dry-run、redaction corpus 与 observability owner 拆分证据。
