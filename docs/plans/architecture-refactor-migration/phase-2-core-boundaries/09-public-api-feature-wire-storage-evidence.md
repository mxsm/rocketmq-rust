# M09-03 Public API、feature、wire/storage 兼容证明

> 工作包：PR-M09-03
> Issue：#8252
> 候选快照：`ac45630a55512713de7a13350086e3c186ad97f2`
> 快照日期：2026-07-17
> 结论：31 个 public API target 差异为 0，兼容矩阵 40/40 通过，R0 feature 语义未变化。

## 1. Public API 快照与差异分类

仓库此前没有可执行的 public API snapshot 工具。本工作包新增
`scripts/public_api_snapshot.py`，使用 nightly Rustdoc JSON 覆盖根 workspace 的全部 31 个 library/
proc-macro target。基线同时记录 crate version、公开路径数量/指纹、完整 Rustdoc JSON SHA-256、默认 feature
profile 和精确 Rust/Cargo 工具链，任何 package 增删、路径或 surface 变化都会 fail closed。

| 项目 | 冻结值 |
|---|---|
| 基线/候选提交 | `ac45630a55512713de7a13350086e3c186ad97f2` |
| target | 31/31 |
| feature profile | `default` |
| Rustc/Rustdoc | `1.99.0-nightly (3659db0d3 2026-07-05)` |
| Cargo | `1.99.0-nightly (2f0e7011e 2026-07-05)` |
| 最终状态 | `PUBLIC_API_SNAPSHOT_OK packages=31 differences=0` |

单次串行刷新在 604 秒外层上限处被终止，当时已经实际刷新 28/31 target；这次超时不计为通过。随后分别用
相同 Rustdoc JSON 命令成功刷新 `rocketmq-store-rocksdb`、`rocketmq-tieredstore` 和 `rocketmq-transport`，再以
`--from-existing` 汇总同一提交上的 31 个刚刷新产物，最终报告 `differences=0`。运行期报告写入
`target/m09-03-public-api-report.json`，不提交构建产物。

差异签署结论：

- additive: 0
- deprecated: 0
- breaking: 0
- unclassified: 0

因此没有允许差异需要额外产品决策，也没有未批准 breaking 需要修复。后续出现 surface 变化时，新增/变更默认
标记为 `unclassified`，package 删除直接标记为 `breaking`，必须审查后重新冻结基线，不能静默覆盖。

## 2. Feature/default 兼容矩阵

`scripts/m09_compatibility_matrix.py` 冻结 24 条独立 feature 命令。Store 的十项命令分别执行，未用
`--all-features` 替代；报告中的 24/24 均为退出码 0。

| 领域 | 实际覆盖 | 结果 |
|---|---|---|
| Protocol | no-default；`simd` 完整测试 | 2/2 |
| Transport | no-default；默认 TLS 测试；observability；all-features | 4/4 |
| Store | no-default、default、local_file_store、fast-load、safe-load、fast+safe、io_uring、rocksdb_store、tieredstore、observability | 10/10 |
| Admin Core | no-default；client-adapter；默认 legacy 完整测试 | 3/3 |
| Proxy facade | no-default；默认 R0 完整测试；observability；tieredstore；all-features | 5/5 |

manifest 审查确认 R0 默认值仍为 Protocol `[]`、Transport `[tls]`、Admin
`[legacy-common-compat]`、Proxy `[]`。Proxy 当前只有 `default`、`observability`、`tieredstore`，下一 major 的
`cluster-mode`、`local-mode`、`compat-all-modes` 均未提前启用。

## 3. Wire、canonical/legacy 与 storage golden

| 组 | 证明内容 | 结果 |
|---|---|---|
| Wire/canonical-legacy | Common protocol message codec；Remoting extraction、JSON/binary command、M04 legacy facade；Proxy Core facade；legacy Runtime | 6/6 |
| Local format | Java 兼容的 20-byte CQ record；40-byte header/20-byte Index entry codec | 2/2 |
| CommitLog/facade | Store Local record/facade/CommitLog 精确兼容；CommitLog parser fail-closed 与 whole-value golden | 4/4 |
| Rocks/Broker | Rocks foundation、Rocks semantics、Broker Rocks、POP consumer | 4/4 |

上述 storage 行合计 10/10；加上 feature 24/24 和 wire 6/6，完整 runner 报告为
`M09_MATRIX_PASSED commands=40 groups=feature,storage,wire`。运行期 JSON 位于
`target/m09-03-compatibility-matrix-report.json`，记录每条命令、退出码和耗时；矩阵总耗时 1224.371 秒。

## 4. 可执行合同与质量门禁

| Gate | 命令 | 结果 |
|---|---|---|
| M09-03 contract | `python -m unittest scripts.tests.test_m09_public_api_compatibility` | 5/5 通过 |
| API diff | 31 个 Rustdoc JSON 刷新；`python scripts/public_api_snapshot.py --check ... --from-existing` | 31/31，differences=0 |
| 兼容矩阵 | `python scripts/m09_compatibility_matrix.py --output target/m09-03-compatibility-matrix-report.json` | 40/40 通过 |
| Rocks strict Clippy | Store 与 Broker 的 `rocksdb_store` all-target strict Clippy | 通过 |
| 根质量门禁 | `cargo fmt --all -- --check`；workspace all-target/all-feature strict Clippy | 通过 |
| 文本/合同 | M09-01～03 contract；`git diff --check` | 通过 |

完整矩阵首次成功执行时，Windows 的隐藏 Cargo 输出含 UTF-8 linker message，而 Python 3.14 默认按 GBK 解码，
导致成功命令结束后出现 reader-thread `UnicodeDecodeError` 噪声；40 条子进程退出码和 JSON 报告均完整、runner
退出码为 0。工具随后显式使用 UTF-8 并以 replacement 处理非 UTF-8 字节，聚焦重跑不再出现该噪声。

## 5. 回滚与总体目标衔接

若 API/feature/wire/storage 证明失败，应回到产生差异的 M03～M08 owner 修复并重新刷新完整快照；不得通过覆盖
baseline、删除 golden、扩大 feature 默认值或恢复重复算法规避失败。工具本身需要回滚时，只删除 snapshot/matrix
工具和对应证据，不改变 R0 public API、feature、wire 或 storage format。

M09-03 完成后，56/82 个工作包完成、26 个未完成；下一工作包为 PR-M09-04。剩余分布为 M09 3、M10 5、
M11 12、M12 6。M09-04 将验证 Client allowlist 与跨项目消费者；本工作包不提前宣称 M09 或 Phase 2 Gate 完成。
