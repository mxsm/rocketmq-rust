# M11-01 Telemetry semantic registry 实施证据

## 1. 目标完成结论

PR-M11-01 已建立可生成、可版本化、可与 Rust 源码双向校验的 telemetry semantic registry，并实现
collector outage 的有界非阻塞队列合同和 provider 共享绝对关闭预算。工作包完成后总进度为 **65/82**，
剩余 17 个工作包：M11 11 个、M12 6 个；下一工作包为 PR-M11-02。

这里的“工作包完成”不等于 M11 或 Phase 3 Gate 完成。SecretProvider、secure profile、rotation、MCP
HTTPS/audit、镜像、Helm/Kustomize、Kind/K3d fault matrix、ArcMut/stable/SLO 收口仍属于 PR-M11-02～12。
M10 的真实固定硬件 baseline/candidate 与 `[HUMAN]` Gate 也仍待验收。

| 项目 | 值 |
|---|---|
| 工作包 | `PR-M11-01` |
| Issue | `#8270` |
| Branch | `mxsm/architecture-refactor-telemetry-semantic-registry` |
| API candidate snapshot | `84e2af503e8908f1ea2b7cbb988d3634300d367e` |
| 日期 | 2026-07-18 |
| Registry | `scripts/telemetry-semantic-registry.json`，schema 1，registry `1.0.0` |
| Guard | `scripts/telemetry_semantic_guard.py` |
| 生成器 | `scripts/generate_telemetry_semantic_registry.py` |
| 违规样例 | `scripts/telemetry-semantic-guard-violations.json` |

## 2. Semantic registry 与源码闭环

注册表不是手工复制的一份旁路清单。生成器读取 canonical Rust 来源，guard 再独立解析同一来源并执行精确集合校验：

- `semantic.rs` 的 119 个 metric 常量必须与 Java 兼容目录 93 个和 Rust 原生目录 26 个一一对应；
- `trace/span_names.rs` 的 4 个 span 与 `semantic::events` 的 7 个 stable log event 必须全部登记且不得新增未知项；
- metric 的 symbol、kind、unit、attribute 顺序、owner 和 Java/Rust catalog provenance 必须与源码一致；
- 66 个 attribute 全部声明 cardinality、privacy、最大 distinct/value bytes、默认启用与 redaction；
- request、watermark/lag、connection/bytes、task、recovery/cache、auth/MCP、exporter 七类信号缺一即失败；
- 删除/重命名必须使用 semantic version 和至少两个 minor 的 deprecation window；
- 高基数字段默认关闭，敏感字段必须 hash/drop，message body、token、secret、credential 等字段名直接拒绝；
- span/log 必须声明有限 ratio/rate limit 和每操作 event 上限，不能建立每消息无限 span/event。

CI 在 Linux 与 Windows 的 architecture-guards job 中直接运行 guard；同一 job 的 `test_*guard.py` discovery
同时运行正向测试和故意违规样例。

## 3. Collector outage 与关闭合同

`TelemetryOutageQueue<T>` 是 exporter adapter 的公共有界 admission 原语：

| 边界 | 默认值/行为 |
|---|---|
| queue count | 2,048 records |
| queue bytes | 8 MiB estimated payload |
| single record | 64 KiB |
| export batch | 512 records |
| scheduled delay | 5,000 ms |
| exporter timeout | 3,000 ms（OTLP exporter 默认配置） |
| provider shutdown | 所有 logs/traces/metrics 共享 5,000 ms 总预算 |
| admission | `try_enqueue`；锁竞争、关闭、count/byte/record 超限时立即 drop，不等待数据面 |
| measurement | accepted/drained/dropped items+bytes、queued items+bytes，并由 outcome 暴露 drop reason |

OTLP trace/log processor 的 count/batch/delay 上限已从依赖默认值改为显式配置；SDK processor 本身使用
non-blocking bounded channel。自定义 exporter adapter 使用本次的 count+byte queue，不得退回无界 channel。
`TelemetryGuard::shutdown_with_timeout` 从一次调用开始计算共享预算，按 logs→traces→metrics 传递剩余时间，
避免 collector outage 把默认 5 秒按 provider 数量累加。

七个 Rust 单测覆盖 count、bytes、单记录、锁竞争、FIFO batch、deadline drop、正常 drain 和配置拒绝。
该原语不创建 task/thread/runtime；worker 继续由现有 exporter/provider lifecycle 所有。

## 4. 失败注入与审查结果

提交的七类违规 fixture 均被 fail closed：未知 signal、未声明 attribute、高基数默认开启、event rate
无上限、deprecation window 过短、数据面阻塞和 byte bound 弱化。额外单测覆盖缺失 signal family、敏感字段
关闭 redaction 以及 message body 字段。

初次执行精确 feature matrix 时，`observability` 组合在未启用 OTLP 的情况下发现
`trace_batch_config` dead code，strict Clippy 失败。实现随后把 helper 的 cfg 从 `otel-traces`/`otel-logs`
收窄为真实消费者 `otlp-traces`/`otlp-logs`，并从失败组合重新执行，最终 7/7 组合通过。初次失败不计为通过。

默认特性 public API 快照审查显示 `rocketmq-observability` 公共路径由 561 增至 598：新增 36 个 outage
合同路径和一个显式 timeout API，属于本工作包的有意 additive；没有删除或改名。其余快照变化只有 Rustdoc
JSON 原始 hash，公开路径数量和指纹不变。基线已绑定候选快照 `84e2af503e89…` 重新冻结，并从新生成的
31 个 Rustdoc JSON 复核为 `PUBLIC_API_SNAPSHOT_OK packages=31 differences=0`。

## 5. 验证矩阵

| Gate | 命令/范围 | 结果 |
|---|---|---|
| Guard unit | `python -m unittest discover -s scripts/tests -p "test_*guard.py" -v` | 132/132 通过 |
| Semantic CLI | `python scripts/telemetry_semantic_guard.py` | PASS：119 metric、4 span、7 log、66 attribute |
| Rust focused | catalog 4 tests；outage 7 tests | 11/11 通过 |
| Feature matrix | default、observability、otlp-metrics、otel-metrics+prometheus、otlp-traces、otlp-logs、OTLP+Prometheus | 每组 workspace check、strict Clippy、package test 均通过 |
| Root final | `cargo fmt --all -- --check`；workspace all-target/all-feature strict Clippy | 通过 |
| Public API | 31 包默认特性快照重新生成并检查 | 31/31，differences=0；observability 增量已归类 additive |
| Runtime | `runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` | 通过 |
| Architecture | dependency fixtures/baseline、release、performance profile、ArcMut fixtures/baseline | 全部通过 |
| Routing | `check-agents-routing.ps1` | `AGENTS_ROUTING_CHECK_OK` |
| Standalone | `rocketmq-example` fmt 与 all-target strict Clippy | 通过 |
| Text | `git diff --check` | 通过 |

Windows linker 的既有 `linker stdout` warning 和 `proc-macro-error2` future-incompatibility note 不受本变更影响；
strict Clippy 退出码为 0，未通过 allow 或 baseline 扩张掩盖。

## 6. 回滚与剩余风险

回滚时必须同时回到上一 registry version、catalog、event 常量、guard/fixture 和 OTLP 配置；不能只删除
guard 或放宽 privacy/cardinality/deprecation/outage 检查。新增 API 为 additive，不改变已有 `shutdown()`
调用，其默认语义现在更严格地共享一个 5 秒预算。

当前 OTel SDK 的内置 processor 原生提供 count-bounded non-blocking queue，但不暴露 byte estimator；本次公共
adapter queue 提供独立的 count+byte/record 硬上限。后续自定义 exporter 必须使用该原语或提供等价可证明
实现，不能把本次合同解释为允许 SDK 之外的无界队列。真实 collector 长时中断、进程级 RSS 与生产 SLO
仍需在 PR-M11-11 fault matrix 和 PR-M11-12 SLO 收口中验证。
