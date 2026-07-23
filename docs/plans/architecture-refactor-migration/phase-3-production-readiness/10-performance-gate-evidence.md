# M10-05 Benchmark、soak 与性能 Gate 实施证据

## 结果

M10-05 固化了八类性能/soak profile、目标硬件采集合同、报告 schema、5% 回归门槛、噪声判定、
正确性前置门禁和限期例外流程，并把 profile 漂移检查接入 CI。该工作包完成后顶层工作包进度为
**64/82**，剩余 18 个，下一工作包为 PR-M11-01。

这里的“工作包完成”不等于“M10 性能 Gate 已通过”。本次没有可用的生产等价固定硬件
baseline/candidate，也没有伪造 benchmark 数字；真实采样、原始 sidecar hash 和 `[HUMAN]` M10 Gate
签署仍待完成。因此 M10 里程碑状态为 `待验收`，Phase 3 Gate 仍未通过。

## 可追溯性

| 字段 | 值 |
|---|---|
| 工作包 | `PR-M10-05` |
| Issue | [#8268](https://github.com/mxsm/rocketmq-rust/issues/8268) |
| 分支 | `mxsm/architecture-refactor-performance-gate` |
| Main 基线 | `d77a820cf9dd7ac6740dcaeba977e070c5581c6b` |
| 冻结代码候选 | `0fac2bfb85c0fc90f1a8cb42dfd4b39b4b077990` |
| Profile 文件 SHA-256 | `10280364541942a27be90a083c61c10ce9e894acac8d9e8b3678c856462ff448` |
| Profile canonical SHA-256 | `acfd0dd8710d978abb503a03df29c5807057aa506df9a57dfd5049d2563d225e` |
| Exception 文件 SHA-256 | `81a515050e5d312ac4a453fa1bdaa999cfc11ad6a652659776bf5747c6b69168` |
| 真实性能运行 | **未执行；不得记录为性能通过** |

## 冻结的采集合同

- profile 精确固定为 Local append、SyncFlush、Local pull、Rocks pull、Tiered append、Tiered pull、
  connection soak 和 overload，共 8 个 profile、11 个变体、50 个 profile 指标合同；展开后产生 66 个比较项。
- 每个 profile 都要求吞吐、p99、peak RSS、每操作 allocation 和 I/O amplification；方向敏感的中位数回归
  上限固定为 5%，每个指标至少 5 个有限、非负样本。
- 噪声 fail closed：相对 median absolute deviation 不得超过 5%，任一样本相对中位数偏离不得超过 15%。
- profile 中不声明不存在的 benchmark 命令。`collection` 明确使用 `target-hardware-sidecar`，状态保持
  `requires-target-hardware-run`；仓库内已有 Criterion 命令只作为局部参考，不冒充完整 10k 连接、冷暖
  Tiered pull 或 overload 采集器。
- 每个运行必须记录硬件标识、OS、内核、架构、CPU、逻辑核、内存、文件系统、rustc/cargo、release profile、
  feature 和采样方法；每个变体绑定原始 sidecar 路径及 SHA-256。
- baseline/candidate 环境必须完全一致，候选时间不得早于基线；真实 measurement 必须来自 clean Git tree，
  且不能把同一提交同时当作 baseline 和 candidate。

## 目标硬件 sidecar runner

Issue [#8682](https://github.com/mxsm/rocketmq-rust/issues/8682) 增加了
`scripts/architecture_performance_sidecar.py`，把上述采集合同从手工报告装配推进为可执行且 fail-closed 的
目标机流程：

- 只接受完整 manifest；四项 correctness 和 8 profiles/11 variants 必须与冻结 policy 精确一致；
- 命令以 argument list 直接执行，不经 shell；每项必须声明 1–86400 秒的有限 timeout；
- 拒绝 placeholder、fixture/synthetic/mock 标记、脏 Git tree、运行期间 commit/worktree 漂移、
  非 release profile、重复 output directory，
  以及 `target/architecture-refactor/M10` 之外的输出位置；
- 四项 correctness 全部成功并保存 transcript SHA-256 后才开始任何性能命令；
- 每个性能 runner 的 stdout 必须是单个 JSON object，精确包含 `schema_version`、`profile`、`variant`
  和完整 `metrics.*.samples`；identity 或 metric inventory 不匹配即失败；
- stdout、stderr、命令参数、timeout、开始/结束时间、耗时、exit code 与 timeout 状态统一写入原始
  transcript，report 只引用仓库内忽略路径及其 SHA-256；
- 最终 report 固定为 `report_kind=measurement`，不能生成 fixture，也没有 `allow-dirty`、
  `allow-partial` 或跳过 correctness 的开关；写入前再次通过现有 `validate_report`。

生成的模板故意不可直接运行，目标机 owner 必须先替换全部 placeholder：

```powershell
python scripts/architecture_performance_sidecar.py `
  --generate-manifest target/architecture-refactor/M10/runner-manifest.json

python scripts/architecture_performance_sidecar.py `
  --manifest target/architecture-refactor/M10/runner-manifest.json `
  --run-id baseline-<commit>-<host> `
  --output-dir target/architecture-refactor/M10/baseline-<commit>-<host>
```

每个 measurement runner 必须向 stdout 输出：

```json
{
  "schema_version": 1,
  "profile": "local-append",
  "variant": "producers-1",
  "metrics": {
    "throughput_per_second": { "samples": [1, 2, 3, 4, 5] }
  }
}
```

示例只展示协议形状；真实输出必须包含该 profile 的全部冻结指标及至少五个真实样本。sidecar runner
解决采集一致性和可追溯性，不提供性能数字，也不替代固定硬件执行、原始数据保管或 `[HUMAN]` M10 签署。

## 真实采集器就绪进度

Issue [#8688](https://github.com/mxsm/rocketmq-rust/issues/8688) 为 `local-append` 增加首个真实 profile
采集器，覆盖 `producers-1`、`producers-8`、`producers-32`；Issue
[#8690](https://github.com/mxsm/rocketmq-rust/issues/8690) 又增加 `sync-flush/concurrency-64`；Issue
[#8692](https://github.com/mxsm/rocketmq-rust/issues/8692) 增加 `local-pull/batch-32`；Issue
[#8694](https://github.com/mxsm/rocketmq-rust/issues/8694) 增加 `rocks-pull/batch-32`；Issue
[#8696](https://github.com/mxsm/rocketmq-rust/issues/8696) 增加 Tiered append/pull 三个变体；Issue
[#8698](https://github.com/mxsm/rocketmq-rust/issues/8698) 最后增加 connection soak 与 bounded overload，
因此当前真实性能 runner 就绪进度为 **11/11 variants**：

```powershell
cargo run --release --quiet -p rocketmq-store `
  --example architecture_store_performance_collector -- local-append producers-1

cargo run --release --quiet -p rocketmq-store `
  --example architecture_store_performance_collector -- sync-flush concurrency-64

cargo run --release --quiet -p rocketmq-store `
  --example architecture_store_performance_collector -- local-pull batch-32

cargo run --release --quiet -p rocketmq-store --features rocksdb_store `
  --example architecture_store_performance_collector -- rocks-pull batch-32

cargo run --release --quiet -p rocketmq-store --features tieredstore `
  --example architecture_store_performance_collector -- tiered-append batch-64

cargo run --release --quiet -p rocketmq-store --features tieredstore `
  --example architecture_store_performance_collector -- tiered-pull cold-32

cargo run --release --quiet -p rocketmq-store --features tieredstore `
  --example architecture_store_performance_collector -- tiered-pull warm-32

cargo run --release --quiet -p rocketmq-transport `
  --example architecture_network_performance_collector -- connection-soak mixed-tls-churn

cargo run --release --quiet -p rocketmq-transport `
  --example architecture_network_performance_collector -- overload bounded-rejection
```

- 每个变体先执行两个同负载 priming 子进程，再固定采集五个不筛选、不改写的原始样本；父进程为每个
  priming/measurement 样本启动全新的子进程，避免冷启动偏差以及进程生命周期峰值 RSS 无法重置而形成
  伪独立样本。
- 每个子进程使用独立临时目录和 owned LocalFile wiring，对真实 CommitLog 执行 1 KiB AsyncFlush append；
  producer 数严格来自 profile variant。
- `throughput_per_second` 和 `p99_latency_us` 来自真实 put workload；`peak_rss_bytes` 使用 Windows
  process peak working set 或 Unix `getrusage(RUSAGE_SELF)`；`allocations_per_operation` 由进程全局
  allocator 的 measured-window 调用差值产生；`io_amplification_ratio` 为 CommitLog 实际编码
  `wrote_bytes` 与 payload bytes 的比值。
- `sync-flush` 会初始化并启动真实 LocalFile Store，以 64 个并发 producer 等待 GroupCommit 持久化确认；
  `fsync_per_ack` 使用 measured window 内成功 mapped-file flush 计数增量除以全部 `PutOk` ack，不使用
  固定常量或请求批次数推测。
- `local-pull` 真实写入 32 条 1 KiB 消息并执行 `reput_once` 构建 ConsumeQueue，随后从 offset 0 重复
  热读 batch-32；`body_copies_per_message` 来自返回 mapped-buffer source，CQ-unit allocation 使用
  batch-1/batch-32 与 direct CommitLog 的匹配增量控制，剔除零拷贝 buffer wrapper 自身分配。
- collector 显式关闭与三类 profile 无关的 TimerWheel，避免隔离样本构造和 peak RSS 被 TimerStore
  污染；local-pull 使用足以容纳固定种子数据的 4 MiB CommitLog 与 20 KiB CQ 映射。
- `rocks-pull` 使用真实 `RocksDBMessageStore` 和 typed CQ range scan；measured window 从 RocksDB
  operation counters 计算 point read + range scan 的 `native_read_calls_per_batch`，core I/O 同时计入
  CommitLog 编码字节与 RocksDB ticker read bytes，并拒绝 counter 回退和 hot window cache miss。
- `tiered-append` 使用真实 POSIX provider 批量写入并提交 64 条 1 KiB CommitLog/CQ 记录；provider
  write/byte 与 JSON metadata successful-persist 均以只读单调累计计数的 measured-window delta 计算，
  不把逻辑方法调用伪装为物理 I/O。
- `tiered-pull` 以同一真实 POSIX fixture 写入 32 条 1 KiB 记录；每个 cold 样本使用全新进程和 cache，
  warm 样本先执行一次不计量的完整 pull，再从 provider read/byte delta 证明 read-ahead cache 命中。
- `connection-soak` 在 900 秒内均匀执行 10,000 次真实请求连接，明文/TLS 各 50%，随后固定 cooldown
  60 秒；client pending table、server-owned task 和 child ownership 必须回到测量前基线。
- `overload` 使用真实 Transport admission processor budget，以 data capacity 的 2.0 倍并发持续 300 秒；
  明文/TLS 各 50%，必须同时观测 typed `SystemBusy`、server rejection counter、资源有界收敛和
  `control_plane_success_ratio=1.0`。
- network core I/O 使用进程内真实 client/server 成功编码帧写入的单调 counter delta；计数位于
  RocketMQ framing 与可选 TLS record 之间，不把逻辑请求数或固定常量伪装成 I/O bytes。
- stdout 只输出 sidecar 要求的单个 JSON object，并精确包含完整 core metric inventory；未知
  profile/variant、样本缺失、非有限或负值均 fail closed。

这 11 个变体只是 runner readiness。它们尚未在批准的固定硬件上对不同 clean baseline/candidate 执行，
没有写入任何性能结论，也没有完成 R19 或 `[HUMAN]` M10 Gate。

Issue [#8700](https://github.com/mxsm/rocketmq-rust/issues/8700) 补齐四个 correctness runner，并新增统一
`scripts/architecture_target_runner.py`。sidecar 模板的 4 个 correctness 与 11 个 measurement command
现在均为可直接执行的仓库命令，只有批准目标主机的 hardware/filesystem/toolchain 等环境元数据仍保留占位符。

```powershell
python scripts/architecture_target_runner.py correctness sync_flush_crash_recovery
python scripts/architecture_target_runner.py correctness derived_replay_no_holes
python scripts/architecture_target_runner.py correctness bounded_overload
python scripts/architecture_target_runner.py correctness no_raw_commitlog_fallback
```

- `sync_flush_crash_recovery` 启动真实 SyncFlush Store 子进程，在每个 `PutOk` durable ack 后记录物理位置，
  收齐 16 条 ack 后由父进程强杀，随后重启并逐条验证已确认消息、queue offset、编码大小与 CQ 最大 offset。
- `derived_replay_no_holes` 写入 32 条 SyncFlush 消息后删除 ConsumeQueue、Index 与 checkpoint，写入 abnormal
  marker 后重启，从权威 CommitLog 重放并逐 offset 验证 CQ 连续性、读取状态、next offset 与 payload。
- `bounded_overload` 执行真实 Transport admission 的 data rejection/control reserve 生命周期回归，以及
  缩放后的真实 TCP/TLS overload collector，要求 typed rejection、控制面成功和资源有界收敛同时成立。
- `no_raw_commitlog_fallback` 固定 Compaction Recovering 状态 fail closed，不允许以 raw CommitLog
  读取绕过派生状态 readiness。

上述四个 runner 在开发机聚焦执行通过，只证明命令和正确性场景就绪；批准主机仍须在 baseline/candidate
两次采集中重新先行执行它们，不能复用开发机结果代替目标硬件 Gate。

## 正确性优先与例外边界

- `sync_flush_crash_recovery`、`derived_replay_no_holes`、`bounded_overload` 和
  `no_raw_commitlog_fallback` 四项正确性证据全部通过后，才进入性能比较。
- fixture 报告必须显式使用 `--allow-fixture`；该开关仅用于 guard 自测，不能作为生产验收证据。
- throughput/p99/RSS/allocation/I/O amplification 的 5% 回归是硬门禁。目标提升值和 provider/native call
  等绝对观察值仅作为非门禁 hypothesis 单独报告，未命中不会改写正确性结论。
- 性能例外只允许作用于已知的相对回归指标，必须包含 owner、理由、批准人、到期时间和回退配置；过期、
  越界或未知例外 fail closed。例外不能压过 correctness、schema、环境、缺失数据、噪声或绝对观察项。

## 验证记录

| 命令/门禁 | 结果 |
|---|---|
| `python -m py_compile scripts/architecture_performance_guard.py scripts/tests/test_architecture_performance_guard.py` | 通过 |
| `python -m unittest scripts.tests.test_architecture_performance_guard -v` | 11/11 通过；全部报告明确为 synthetic fixture、非 benchmark 证据 |
| `python -m py_compile scripts/architecture_target_runner.py scripts/architecture_performance_sidecar.py scripts/tests/test_architecture_target_runner.py scripts/tests/test_architecture_performance_sidecar.py` | Issue #8700 通过 |
| `python -m unittest scripts.tests.test_architecture_target_runner scripts.tests.test_architecture_performance_sidecar -v` | 13/13 通过；覆盖 4/11 frozen inventory、具体 argument vector、未知入口和首个失败 fail closed、完整采集、correctness-first、timeout process-group 终止及 Git 漂移 |
| `python scripts/architecture_performance_sidecar.py --generate-manifest target/architecture-refactor/M10/runner-manifest-8700.json` | 通过；生成 4 个 correctness 与 11 个 measurement 具体 runner command，未保留 runner placeholder |
| `python scripts/architecture_target_runner.py correctness <check-id>`（四项逐一执行） | Issue #8700 全部通过；SyncFlush 强杀恢复 1/1、派生状态删除重放 1/1、受控过载 2/2、禁止 raw CommitLog fallback 1/1 |
| `python scripts/architecture_performance_guard.py --validate-profiles` | 通过；8 profiles、11 variants、50 metric contracts |
| `python -m unittest discover -s scripts/tests -p "test_*guard.py"` | 125/125 通过 |
| `cargo test -p rocketmq-store --example architecture_store_performance_collector` | Issue #8688/#8690/#8692/#8694 默认 feature 合同 9/9 通过；覆盖原五种 variant、精确 metric inventory 和反向拒绝，并验证无 RocksDB feature 时 fail closed |
| `cargo test -p rocketmq-store --features rocksdb_store --example architecture_store_performance_collector` | 10/10 通过；增加 rocks-pull 精确 metric inventory、native counter 正向计算及 counter 回退、cache miss、缺失/非有限指标的反向拒绝 |
| `cargo test -p rocketmq-tieredstore provider::posix_file_segment::tests --lib` | Issue #8696 provider I/O 计数 2/2 通过；覆盖成功读写、clone 共享累计值和失败读取只增加 call、不伪增 byte 的反向合同 |
| `cargo test -p rocketmq-tieredstore metadata::metadata_store::tests --lib` | Issue #8696 metadata 3/3 通过；成功 replace 精确累计，失败 persist 不增加计数 |
| `cargo test -p rocketmq-store --features tieredstore --example architecture_store_performance_collector` | 12/12 通过；覆盖三个 Tiered 变体、精确 metric inventory、counter 回退、缺失/非有限和错误 profile/variant 的反向拒绝 |
| `cargo test -p rocketmq-transport --example architecture_network_performance_collector -- --test-threads=1` | Issue #8698 合同 7/7 通过；覆盖固定生产参数、精确 metric inventory、缺失/非有限/无 rejection/未收敛/counter 回退反向拒绝，以及缩放后的真实明文/TLS churn 与 bounded overload |
| `cargo test -p rocketmq-transport --test client_server_lifecycle tls_client_invocation_releases_pending_and_server_ownership -- --exact` | Issue #8698 TLS client、pending 清理、成功编码写计数与 server task/child ownership 收敛 1/1 通过 |
| `cargo test -p rocketmq-store get_message_returns_dispatched_messages_after_reput --lib` | Issue #8692 受限 batch 回归 1/1 通过；`max_msg_nums=1` 在 CQ 尚有后续消息时按期返回，不再因 iterator exhausted 空转 |
| `cargo test -p rocketmq-store --lib io_stats_aggregate_mapped_file_metrics` | Issue #8690 I/O 聚合正向测试 1/1 通过 |
| `cargo clippy -p rocketmq-store-local --lib -- -D warnings` 和采集器 focused Clippy | 通过 |
| 三个 `local-append` variant 的 release collector 本机烟测 | 均真实执行并输出五样本完整协议；开发机未获批准且部分 P99 样本超出噪声限制，明确不作为 baseline/candidate 或性能通过证据 |
| `sync-flush/concurrency-64` release collector 本机烟测 | 真实 Store/GroupCommit 启停和五样本完整协议通过；观测到真实 `fsync_per_ack=0.03125`，仅为未批准开发机诊断值，不作为 Gate 结论 |
| `local-pull/batch-32` release collector 本机烟测 | 真实 append/reput/hot pull、匹配 allocation control 和五样本完整协议通过；开发机观测仅验证 runner，不作为 baseline/candidate 或 Gate 结论 |
| `rocks-pull/batch-32` release collector 本机烟测 | 真实 RocksDB CQ typed range read、Local WAL hot read 和五样本完整协议通过；五个 `native_read_calls_per_batch` 样本均为 3，开发机观测仅验证 runner |
| 三个 Tiered release collector 本机烟测 | 2 priming + 5 measurement 完整协议通过；真实 POSIX cold pull 每批 2 reads、warm pull 0 reads，append 每批 128 writes；metadata 与延迟值仅为未批准开发机诊断，不作为 baseline/candidate 或 Gate 结论 |
| dependency target/baseline guards | 通过；target compatibility 35/35、dev-only 3/3，baseline 无增长 |
| release guard | 通过；32/32 topology、10/10 R0 crates，无提前移除/Proxy feature 激活 |
| ArcMut guard | 通过 |
| `.\scripts\check-agents-routing.ps1` | 通过；4 standalone Cargo、3 Node、8 routes |
| `git diff --check` | 通过 |
| `cargo fmt --all -- --check` | Issue #8688/#8690/#8692/#8694/#8696 通过 |
| `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` | Issue #8688/#8690/#8692/#8694/#8696 通过 |

测试覆盖稳定样本通过、双方向超过 5% 回归、MAD 与离群样本、环境漂移、缺失/NaN、正确性不可豁免、
例外有效期与作用域、fixture 显式 opt-in、原始 hash/dirty measurement/未知例外，以及输出报告对
policy、环境、baseline 和 candidate 的 SHA-256 绑定。

## 待验收项与回滚

- 在同一生产等价固定硬件上分别采集已批准 baseline 与 candidate，保存每个变体的原始 sidecar 和 hash；
  不得以 synthetic fixture、Criterion 局部参考命令或推测数字代替。
- 运行不带 `--allow-fixture` 的真实比较；只有 correctness 全通过、噪声稳定、所有硬指标在预算内或存在
  有效人工例外时，才能把 M10 性能 Exit Checklist 改为通过。
- `[HUMAN]` 仍需审查真实报告、任何性能例外和回退配置，并单独签署 M10 Gate。
- guard/profile 回滚只能恢复到同样保留 correctness-first、完整元数据、5% 预算和 fail-closed 例外语义的
  上一已验证版本；不得通过放宽预算、删除 profile 或使用 fixture 报告让候选通过。
