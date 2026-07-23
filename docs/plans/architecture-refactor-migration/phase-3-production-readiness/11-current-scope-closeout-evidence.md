# R25 当前范围收口证据

## 冻结候选

| 字段 | 值 |
|---|---|
| R25 Issue | [#8704](https://github.com/mxsm/rocketmq-rust/issues/8704) |
| 候选 commit | `1571e73fd458a7c493172787e6c2d001bd0dcba6` |
| 候选主题 | `[ISSUE #8702]♻️Remove shared-mutation compatibility APIs (#8703)` |
| 签署日期 | 2026-07-24 |
| 当前执行范围 | R01～R18、R20、R22～R25 |
| 后续范围 | R19、R21、R26～R31 |

候选 commit 是完成 R09/R18 后的最新 `main`。R25 分支只更新治理 guard、完成计数和证据文档，
不修改生产 Rust 行为，因此四方签署绑定上述代码候选；任何后续生产修复都会产生新候选并使本签署失效。

## 完成证据

- R09/R18：Issue #8702、PR #8703；12 个共享可变兼容表面已删除，ArcMut guard 为 0/0。
- Canonical replacement：`OwnedMessageStore`、`wire_owned_root_dependencies`、
  `new_with_message_store_config` 保持唯一、公开且未弃用。
- 正向验证：Timer canonical constructor、owned-root wiring、Store adapter 和 architecture correctness 通过。
- 反向验证：无 Store context 的 Timer 返回 `ServiceNotAvailable`；release guard 拒绝恢复已删除表面、
  弃用 canonical replacement、重新激活 follow-up 或缺少任一 R25 签署。
- 最终 Rust Gate：根 workspace format/Clippy 与 `rocketmq-example` standalone format/Clippy 通过。

R19 固定目标硬件性能、R21 Kind/K3d 动态证据以及 Phase 4 R26～R31 均未冒充完成，
只作为独立 follow-up 保留。

## 四方签署

- [x] `[ARCH]`：批准边界、兼容 override、唯一 WAL 与当前范围定义；无未批准例外。
- [x] `[REV]`：完成 API diff、guard fail-closed 行为与文档一致性复核；无 material finding。
- [x] `[TEST]`：批准候选 commit 的适用格式、Clippy、聚焦测试、Rustdoc 与治理 guard 结果。
- [x] `[HUMAN]`：批准 R09/R18 提前移除、排除 R19/R21/R26～R31，并授权完成当前重构目标。

## 结论

当前范围达到 76/76 顶层工作包和 23/23 执行项，剩余任务为 0。后续项不影响本次收口，
也没有在本证据中声明为完成。
