# M09-01 Workspace 与目标 DAG 收口证据

## 结论

PR-M09-01 将根 workspace 固定为精确 32 个 package，并把目标依赖检查切换到不可绕过的严格模式。
严格模式中的“目标 DAG 为零”指 **未被目标 DAG 接受、也未进入精确 R0 兼容/组合台账的边为零**；
R0 仍需保留或后续迁移的边单独计数，不能用“guard 通过”宣称物理依赖已经全部删除。

| 指标 | 收口前 | 收口后 |
|---|---:|---:|
| 根 workspace package | 32 | 32 |
| 未授权目标 DAG finding | 48（46 direct + 2 transitive） | 0 |
| 到期临时 manifest/source 例外 | 2 / 0 | 0 / 0 |
| 活动 R0 兼容/组合台账 | 37（另有 5 条已归零记录） | 49 |
| 精确测试专用边 | 未分离 | 3 |
| TieredStore → Common | 1 direct + 2 transitive | 0 |

49 条活动台账不是新增依赖。收口过程删除 5 条已归零记录，物理移除 TieredStore 的 Common 边，
并把原先未登记却已经存在的 12 条 facade/composition/consumer 边纳入精确治理。任何 caller、target、
kind、manifest path 或 alias 的变化，以及重复边增长，都会使严格检查失败。

## 实际迁移

- `BoundaryType` 的唯一实现从 Protocol 下沉到 runtime-neutral `rocketmq-model`。
- Protocol 与 Common 原路径改为同一类型的 re-export，R0 public path 和 Serde/Display 行为保持不变。
- TieredStore 的生产源码、集成测试和 manifest 改用 Model canonical path，移除 Common 直接边。
- 原来为 TieredStore → Common/Rust 放行的两条到期传递例外已删除。

## R0 台账与退出窗口

| 退出窗口 | 数量 | 处置范围 |
|---|---:|---|
| M09-02 | 11 | facade purity、NameServer/Controller composition、Filter 与 store-inspect 迁移输入 |
| M09-04 | 3 | MCP 仅经 Admin Core 的消费方收口 |
| R1 | 29 | workspace 内 Common/Remoting/legacy runtime 兼容边迁移 |
| next-major | 4 | standalone Example 与 Common Protocol public re-export 删除窗口 |
| long-term | 2 | 已批准的 Broker/Store 与 store-inspect/Store composition |

测试边单独固定为 Broker → Controller、Broker → NameServer 和 legacy runtime → Observability 三条 `dev`
依赖；精确 allowlist 不接受将其提升为 normal/build 依赖。

## Guard 语义

- `--allow-missing-planned-crates` 已删除，目标模式只接受与 policy 完全一致的 32-package 集合。
- `manifest_exceptions` 和 `source_exceptions` 必须为空；重新加入临时例外会在输入校验阶段失败。
- R0 台账只按完整 manifest edge identity 匹配，台账允许下降、不允许增长。
- foundation → facade、protocol → transport、store-api → backend、proxy-local → Client 等违规 fixture
  继续 fail closed。
- cycle guard 检查完整内部依赖图；normal closure 检查不受 dev 边污染。

## 验证入口

```powershell
cargo metadata --format-version 1 --no-deps
python -m unittest scripts.tests.test_architecture_dependency_guard -v
python -m unittest scripts.tests.test_m09_target_dag_closeout -v
python scripts/architecture_dependency_guard.py --fixtures
python scripts/architecture_dependency_guard.py --mode target --output target/architecture-refactor/M09/target-after.json
python scripts/architecture_dependency_guard.py --mode baseline
cargo tree -e normal --workspace
```

目标报告必须包含 `TARGET_COMPATIBILITY_LEDGER active_edges=49 entries=49`、
`TARGET_TEST_DEPENDENCIES active_edges=3 entries=3` 和 `ARCHITECTURE_DEPENDENCY_GUARD_OK mode=target`，
且 findings 数组为空。

本次冻结快照的结果为：Model 33、Protocol 1,373、TieredStore 56 项通过；architecture contract 按
210 项常规合同与 160 项 M06 mutation 合同拆分执行，合计 370 项通过；根 workspace fmt 与 strict
Clippy、baseline/target/fixture、ArcMut 及其 24 fixtures、runtime enforcing audit、AGENTS routing、
`cargo tree -e normal --workspace` 和 `git diff --check` 均以退出码 0 完成。

## 回滚与交接

- 回滚 guard 时不得恢复缺失 package 绕过参数或 TieredStore 的 Common 边。
- 某条兼容边影响 R0 行为时，恢复精确 forwarding/composition adapter，并保留原 owner 与退出窗口；
  不允许恢复重复算法或扩大台账 identity。
- M09-02 必须处理 11 条到期 purity 输入并审计五个 facade；M09-04 必须处理 3 条 MCP 输入。
