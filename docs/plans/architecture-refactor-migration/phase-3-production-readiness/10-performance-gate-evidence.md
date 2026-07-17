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
| Profile 文件 SHA-256 | `1905cef6f78f10cd9ae8274502deb0fad8e6b899727f57a1770ce39eb8481c30` |
| Profile canonical SHA-256 | `4c28b93447189b27a3769e13cc7d40476fd5f73b3a7cff2be3701253ea1b96e4` |
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
| `python scripts/architecture_performance_guard.py --validate-profiles` | 通过；8 profiles、11 variants、50 metric contracts |
| `python -m unittest discover -s scripts/tests -p "test_*guard.py"` | 125/125 通过 |
| dependency target/baseline guards | 通过；target compatibility 35/35、dev-only 3/3，baseline 无增长 |
| release guard | 通过；32/32 topology、10/10 R0 crates，无提前移除/Proxy feature 激活 |
| ArcMut guard | 通过 |
| `.\scripts\check-agents-routing.ps1` | 通过；4 standalone Cargo、3 Node、8 routes |
| `git diff --check` | 通过 |
| Cargo fmt/Clippy/test | 未执行；本工作包仅修改 Python、JSON、CI 与 Markdown，不含 Rust/build 配置变更 |

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
