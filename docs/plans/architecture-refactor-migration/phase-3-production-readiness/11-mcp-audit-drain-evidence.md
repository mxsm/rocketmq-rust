# M11-06 MCP Audit Writer 与 Shutdown Drain 实施证据

## 1. 结论与进度边界

PR-M11-06 已把 MCP audit 从仅有记录条数上限、关闭时被 runtime 直接取消的 writer，收紧为按记录数和
真实序列化字节双重有界、生产者无等待、可在单一绝对截止时间内 drain/flush 并生成健康报告的生命周期
组件。文件 sink 的目录创建、append 和 `sync_all` 全部通过注入的 `BlockingExecutor`；writer 仍由
`ServiceContext` 持有，不新增 detached task、ad-hoc runtime 或数据面等待。

工作包完成后总进度为 **70/82**，剩余 12 个工作包：M11 6 个、M12 6 个；下一工作包为 PR-M11-07。
这里的“工作包完成”只表示目标实现、复核与工程验证完成，不替代尚未签署的 M11 入口 `[ARCH]`、安全默认
迁移 `[HUMAN]`、M10 真实固定硬件 baseline/candidate 与 `[HUMAN]` Gate，也不等于 M11、Phase 3 或最终
目标态 Gate 完成。

| 项目 | 结果 |
|---|---|
| 工作包 | `PR-M11-06` |
| GitHub Issue | `#8280` |
| 主要 owner | `rocketmq-mcp::guard::audit`；应用生命周期组合位于 `rocketmq-mcp::app` |
| 新增依赖边 | 无；复用现有 `rocketmq-mcp -> rocketmq-runtime` 注入边界 |
| 非目标 | M11-07～12 镜像/部署/fault/SLO、M12 Apply/AI Native、M10 真实性能签署 |

## 2. 稳定 schema、脱敏与记录上限

- `AuditRecord` 增加固定 `schema_version = 1`。NDJSON 字段顺序由 Rust struct 定义，构造者传入的版本在准入
  前统一重置为当前版本，避免伪造或混用版本。
- `request_id`、principal/operator、client、cluster、tool、arguments hash 与 error 分别设置确定性 UTF-8
  字节上限。所有字符串先复用敏感 assignment redaction，再把控制字符替换为 U+FFFD，最后只在字符边界
  截断。
- 最大单条记录按脱敏后实际 `serde_json` 字节加换行计算；超过 `audit.max_record_bytes` 的记录不进入内存
  history、queue 或 sink，并增加 `dropped/oversized`。
- sink 错误只记录稳定事件与计数，不记录路径、底层 I/O 文本、credential、token、请求体或完整配置。

## 3. 双容量无等待准入与 FIFO writer

每个异步记录先用 `Semaphore::try_acquire_many_owned` 取得与实际 NDJSON 字节数一致的 permit，再用
`mpsc::Sender::try_send` 竞争记录槽位。生产端没有 `.await`；字节满、条数满和关闭分别增加
`byte_capacity_drops`、`count_capacity_drops`、`closed_drops`。成功入队才增加 `accepted/queued`。

envelope 持有 byte permit 和 pending guard：正常写入、sink 失败、channel 回收、future abort 或 runtime 取消
都会通过析构释放字节并扣减 `pending_records/pending_bytes`。单 writer 顺序消费 channel，成功记录按 FIFO
写入 sink 与有界 history；单条写入失败只增加 `sink_failures`，不会阻塞生产者或停止后续记录。

`memory` sink 保持同步、本地且有界的测试/查询语义；`tracing` sink 不改变 stdout，应用 subscriber 仍把日志
写到 stderr；`file` sink 的所有阻塞文件操作只通过现有 `BlockingExecutor::spawn_io` 执行。

## 4. 单一绝对关闭预算与可观测报告

`AuditLog::close_and_drain(deadline)` 原子取走 sender，拒绝新准入，等待 writer 把已接收记录按 FIFO 写完并
flush。`McpApp::shutdown_with_deadline` 随后把同一个 `ShutdownDeadline` 传给
`RuntimeContext::shutdown_tasks_until`，audit 阶段不会为 runtime 重新分配完整 timeout。

`AuditDrainReport` 返回 status、elapsed、accepted、written、各类 drop、sink/flush failure 和 pending
count/bytes。writer 被截止时间后的 runtime shutdown 中止时，pending guard 仍会归零，但 completion 不会被
误标为 drained；后续报告保持 `timed_out`。兼容入口 `McpApp::shutdown()` 仍使用十秒预算并记录不健康报告。

## 5. 公共 API 与配置兼容性

默认特性公共 API 快照对 31 个 workspace library 全量重建：

- `rocketmq-mcp` public path 从 210 增至 217。新增路径为 `AUDIT_SCHEMA_VERSION`、`AuditDrainStatus` 及其
  3 个 variant、`AuditDrainReport`、`McpShutdownReport`；其余 30 个包路径与 fingerprint 不变。
- `AuditConfig` 新增 `max_record_bytes` 与 `queue_max_bytes`。Serde 均提供默认值，旧 TOML 继续解析；直接使用
  Rust struct literal 的 embedder 需要补齐两个字段。
- `AuditRecord` 新增 `schema_version`，`AuditMetrics` 增加 admission/drain 明细字段；直接构造或穷尽解构这些
  public struct 的 embedder 需要按 v1 schema 迁移。既有 `AuditRecord::new`、`AuditLog::record/records/metrics`
  与 `McpApp::shutdown` 保留。
- 更新受管 baseline 后再次全量复核为 31/31 package、`differences=0`。

## 6. 验证矩阵

| Gate | 命令/范围 | 结果 |
|---|---|---|
| Audit focused | `cargo test -p rocketmq-mcp guard::audit::tests --lib` | 7/7 通过：count/byte overflow、oversized/redaction、FIFO、sink/flush failure、stall/deadline、runtime cancellation、file flush |
| MCP default | `cargo check -p rocketmq-mcp`；`cargo test -p rocketmq-mcp` | 通过；82 library、typed boundary 与 2 个本地 integration 通过，1 个外部集群 E2E 按环境合同 ignored |
| MCP all features | `cargo test -p rocketmq-mcp --all-features` | 104 library、typed boundary 与 2 个本地 integration 通过；`change-planning` 仍无副作用 |
| MCP strict Clippy | `cargo clippy --all-targets -p rocketmq-mcp --features streamable-http -- -D warnings` | 通过 |
| Rustdoc | `cargo doc -p rocketmq-mcp --no-deps` | 通过 |
| Runtime ownership | `scripts/runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 退出码 0，runtime boundary baseline passed，无新 raw spawn/blocking/root 边界 |
| Dependency/release | target + baseline dependency guard；architecture release guard | 通过；target ledger 35、dev-only 3、release topology 32/32 |
| Public API | 31 包快照重建、增量分类、baseline 更新后零差异复核 | MCP 210→217 additive；其余 30 包路径不变 |
| Root final | `cargo fmt --all -- --check`；workspace all-target/all-feature strict Clippy | 通过 |
| Text | `git diff --check` | 通过 |

Windows/MSVC linker message 与 `proc-macro-error2` future-incompatibility note 是既有工具链输出；严格 Clippy
退出码为 0，没有新增 `allow` 或降低 lint。外部集群 integration 未配置 Namesrv/topic/group/broker，按既有测试
合同 ignored；本工作包全部验收项不依赖外部集群。

## 7. 回滚与剩余目标

代码回滚必须同时回滚双容量配置、v1 schema、writer lifecycle、shutdown report、测试、README 与公共 API
baseline。运行期若 audit writer/sink 不健康，应停止暴露 Streamable HTTP 管理入口并保留 stdio/人工 CLI，
不得通过改成无界队列、生产端等待、记录敏感底层错误或跳过 drain 来恢复服务。

剩余 M11 目标为：PR-M11-07 容器镜像基础、M11-08 五服务入口、M11-09 Helm/Kustomize、M11-10
probe/preStop/drain、M11-11 Kind/K3d fault matrix、M11-12 ArcMut/stable/SLO 收口。M12 仍有 6 个 AI Native
工作包。此外，M10 仍缺真实固定硬件 baseline/candidate、原始数据 hash 与 `[HUMAN]` Gate；M11 入口
`[ARCH]`、安全默认迁移 `[HUMAN]`、Phase 3 和最终目标态 Gate 均未签署。
