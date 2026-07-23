# R09/R18 共享可变兼容面移除证据

## 决策

Issue [#8702](https://github.com/mxsm/rocketmq-rust/issues/8702) 关闭 R09 与 R18。HUMAN 于
2026-07-24 明确批准在原定两个弃用 release / 一个 major boundary 之前移除本文件列出的 12 个兼容表面；
该授权不扩展到其他兼容 API 或 Proxy feature。回滚点为 `v0.9.0`。

R19 固定目标硬件性能验收同时划为独立后续项，不计入本轮架构重构 Gate。仓库内已有的 4 个 correctness
runner 与 11 个 measurement runner 保持就绪。

## 移除结果

- 删除 `rocketmq/src/arc_mut.rs` 及根级 `ArcMut`、`WeakArcMut`、`SyncUnsafeCellWrapper` re-export。
- 删除 `GenericMessageStore`、`LocalFileMessageStore::set_message_store_arc`，以及 Timer 的完整 Store
  field、constructor 和 setter。
- `scripts/arc-mut-baseline.json` 经 `arc_mut_guard.py --prune-resolved` 从 20 identities /
  58 occurrences 单调裁剪为 0/0；没有 relocation 或临时 approval。
- 保留 `OwnedMessageStore`、`LocalFileMessageStore::wire_owned_root_dependencies` 和
  `TimerMessageStore::new_with_message_store_config` 三个 canonical replacement。

## 正向与反向约束

- 正向测试验证 owned Store adapter 和 canonical Timer constructor 仍可使用。
- 反向测试验证未注入 Store capability 的 Timer fail closed 为 `ServiceNotAvailable`，且不会读写消息。
- release guard 验证 12 个已删除 declaration 不得恢复、三个 replacement 唯一且不得 deprecated/hidden。
- ArcMut guard 验证 production、test 与 compatibility 三类扫描均为零。

## R25 输入

R25 只需在 R09/R18 已完成且 R19、R21、R26～R31 明确为 follow-up 的当前范围上冻结候选 commit，
汇总适用验证结果，并完成 `[ARCH]`、`[REV]`、`[TEST]`、`[HUMAN]` 四方签署。
