# PR-M06-11 Store Facade、Tiered 与 Feature 兼容台账

## 元数据

| 字段 | 值 |
|---|---|
| 所属里程碑 | M06 Store API、Local 与 RocksDB 边界提取 |
| 状态 | PR-M06-11 已完成；R0/R1 兼容面冻结 |
| Canonical owner | Local：`rocketmq-store-local`；Rocks：`rocketmq-store-rocksdb`；Tiered：`rocketmq-tieredstore`；组合 facade：`rocketmq-store` |
| 下一工作包 | PR-M06-12 依赖图与消费方收口 |

## 目标完成定义

- `rocketmq-tieredstore` 依赖 backend-neutral `rocketmq-store-api`，不反向依赖 Store facade、Local 或 Rocks。
- `rocketmq-store` 长期保留 backend enum/factory、legacy config/trait/public path、Tiered decorator 与精确 re-export。
- Local fallback、CommitLog dispatch、lifecycle 顺序和 Store DTO 映射属于 facade composition；Tiered provider、fetcher、dispatcher 与服务生命周期仍由 Tiered owner 实现。
- Local 拥有 `fast-load`、`safe-load`、`io_uring`；Rocks owner 独占 native RocksDB；Store 只做弱 feature 转发并保留历史 alias。
- R0 `--no-default-features` 仍可编译 Local 兼容 facade；这不是“已完全移除 Local 依赖”的声明。

## 兼容面与退出条件

| 兼容面 | Canonical owner | Store facade 保留内容 | 当前冻结语义 | 删除/变更条件 |
|---|---|---|---|---|
| Local load feature | `rocketmq-store-local` | `fast-load`、`safe-load`、`io_uring` 精确转发 | 默认 `fast-load`；`fast+safe` 时 fast 保持优先；safe-only 才关闭并行 load | 下一 major、consumer 已迁移且精确矩阵重新签署 |
| Rocks feature | `rocketmq-store-rocksdb` | `rocksdb_store` 与 `rocksdb-store` alias | 默认/Local tree 不引入 native Rocks；显式 Rocks 才启用 owner | alias 经过 deprecation window 且所有 consumer 使用 canonical 名称 |
| Tiered lifecycle | `rocketmq-tieredstore` | `TieredStoreDecorator` lifecycle adapter | Tiered 实现 `rocketmq_store_api::StoreLifecycle`；Store 保持 legacy load/start/shutdown 顺序 | M06-12 关闭依赖图且 legacy lifecycle consumer 有迁移证据 |
| Tiered read/query/dispatch | Tiered provider/fetcher/dispatcher；Store composition | fallback gate、Store DTO/status/result 映射、CommitLog body resolver | Local miss 才 fallback；过滤、大小上限、metrics、offset/timestamp 与 query 结果不变 | M10 新 Tiered contract 有独立兼容方案和回归 corpus |
| `GenericMessageStore::LocalFileStore` | Store facade | enum variant、constructor、126 方法 legacy forwarding | no-default 仍保 Local 变体；`local_file_store=[]` 是 R0 compatibility alias | 下一 major 删除 legacy trait/path 后再评估 Local optional 化 |
| legacy config/trait/re-export | Store facade | `MessageStoreConfig`、`MessageStore`、Local/Rocks 深路径 | Serde/default/alias、公开签名和路径不变 | consumer 清单清零并跨越兼容窗口 |

## 精确 Feature Matrix

以下组合均以 `cargo check -p rocketmq-store` 独立执行，`--all-features` 不替代任何一项：

- `--no-default-features`
- 默认 features
- `--no-default-features --features local_file_store`
- `--no-default-features --features fast-load`
- `--no-default-features --features safe-load`
- `--no-default-features --features fast-load,safe-load`
- `--no-default-features --features io_uring`
- `--no-default-features --features rocksdb_store`
- `--no-default-features --features tieredstore`
- `--no-default-features --features observability`

## 验证证据

- 新增 `test_m06_store_facade_tiered_contract.py` 5 项，冻结 dependency、feature owner、no-default compatibility、Tiered decorator 和 fast/safe 优先级。
- Local fast/safe truth table 1/1、Tiered neutral lifecycle 1/1、Store Tiered write dispatch 1/1 与 Local-miss read fallback 1/1 通过。
- 完整 M06 contract 最终 195/195 通过（603.261s）。前两轮各 194/195，分别暴露精确 import 未登记新 owner 函数和 canonical scanner 未识别 `pub const fn`；补充正/负向契约后全绿。
- Architecture dependency baseline、35 项单测、6 个违规 fixtures、runtime enforcing audit 与 AGENTS routing 通过。
- ArcMut 7 条同 identity 一对一 fingerprint relocation 经 ADR-013 approval、promotion、compare 与 final guard；台账保持 1,170 identities/3,232 occurrences，63 项单测和 24 fixtures 通过，零新增债务。
- Error architecture guard 仅复现 main 既有 11 项：Broker source stringification 1、MCP anyhow 8、缺失治理文档 2；本工作包无新增 finding，不将该门禁记为通过。
- Local all-feature lib 185/185、Tiered all-feature lib 55/55、Store all-feature lib 484/484、workspace exact fmt、workspace all-target/all-feature strict Clippy 与 `git diff --check` 通过。

## 分层回滚

1. 可先撤销 Store 的 feature forwarding 调整，恢复上一版 alias/factory 接线；不得删除 Local/Rocks owner 或改变默认 backend。
2. 可单独撤销 `TieredStoreDecorator`，把 Local fallback/dispatch/lifecycle 调用恢复到上一 facade 实现；Tiered 数据目录、provider 与 metadata 不迁移。
3. 可单独撤销 Tiered 的 `StoreLifecycle` 中立实现及 store-api 依赖；既有 `TieredLifecycle` 保持可用。
4. legacy config、126 方法 trait、Local/Rocks 深路径和 no-default Local compatibility 始终保留，回滚不得关闭旧路径。
5. 任何回滚都不得恢复第二 CommitLog、把 Tiered 依赖指回 Store facade，或改变 CQ/Index/CommitLog 持久格式。

## PR-M06-12 交接

- 对四个 closure（Local、Rocks、Tiered、Store facade）生成最终依赖图并用 policy/fixtures 固定。
- 复核 Broker、standalone path consumer、canonical/legacy compile 与完整 feature matrix对应同一候选快照。
- 只有消费方清单、兼容 ledger、唯一 WAL 和 M06 Exit Checklist 全部签署后，才可关闭 M06。
