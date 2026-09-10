---
title: "测试策略与工程入口"
---

从可能失败的行为出发选择测试。仓库包含包级测试、独立应用测试、协议互操作、恢复场景、fuzz 目标及性能工具，它们回答不同问题。存在测试矩阵或网站构建成功，不代表所有场景都已执行。

## 让测试对应修改

| 测试类型 | 回答的问题 | 现有入口 | 实际限制 |
| --- | --- | --- | --- |
| 单元与定向回归 | 局部状态转换、映射或校验是否正确？ | 所属包内联测试与 `tests/` 目标 | 不能证明完整集群路径 |
| 确定性属性/状态测试 | 有界生成用例是否保持协议、存储或状态机不变量？ | `scripts/property-state-suite-registry.json` 与 `scripts/run_property_state_suites.py` | 用例覆盖有界，不是对所有输入的证明 |
| 组件与生命周期 | 协作组件是否正确取消、排空并释放资源？ | Client 集成目标；Runtime 生命周期与编译失败测试 | 进程内对端不能重现所有网络/存储故障 |
| 集群功能测试 | 已配置的客户端和服务能否完成预期消息路径？ | `scripts/run_client_broker_functional_tests.ps1` 与 `.github/workflows/v1-functional-acceptance.yml` | 记录实际拓扑、feature 选择及断言 |
| Java 互操作 | 所选客户端/服务端和 HA 组合是否保持指定语义？ | `scripts/interop/v1-interop-matrix.json` 与 `scripts/interop/run_v1_interop.py` | 矩阵指定 Java 5.5.0，并明确排除 Java Controller、Java AutoSwitchHA 与 DLedger CommitLog |
| 存储与部署故障 | 崩溃、重启、副本丢失或发布中断后哪些状态仍然有效？ | `scripts/interop/v1-storage-fault-matrix.json`；`.github/workflows/kubernetes-fault-matrix.yml` | 需要隔离且可丢弃的状态及场景所需实际环境 |
| Fuzz | 畸形或异常输入能否破坏解析/恢复不变量？ | 独立 `fuzz/` 工程与 `.github/workflows/fuzz-ci.yml` | 有限执行不能证明不存在缺陷 |
| 性能与长稳 | 指定负载下的吞吐量、延迟、资源占用和持续行为如何？ | 包级基准；架构 SLO 工作流；[容量指南](../operations/capacity-performance.md) | 在等价负载下比较，同时报告失败与成功操作 |
| 产品与网站 | 独立应用或渲染文档在自身工程中是否正常？ | Dashboard/MCP/SRE 工作流；`rocketmq-website/` | 根 Cargo 验证不覆盖这些独立前端/应用 |

存储故障清单包含 LocalFile、多路径、RocksDB、压缩、POP、定时器、分层存储、Controller 和升级场景。完成 LocalFile 测试不能证明其他后端或恢复行为。结果解释参见[协议兼容性](../reference/protocol-compatibility.md)和[备份恢复](../operations/backup-recovery.md)。

## 执行小而有效的本地检查

仅修改模型时，在仓库根目录运行：

```bash
cargo fmt -p rocketmq-model -- --check
cargo test -p rocketmq-model --lib
```

测试调用已经编译该目标，无需仅为重复编译而再运行 `cargo check`。修改范围更小时，选择具名目标或测试，并确认结果包含预期测试，而非零匹配。

例如，以下现有协议属性测试运行一个已注册的确定性用例族：

```bash
cargo test -p rocketmq-protocol --test remoting_wire_golden deterministic_remoting_cases_round_trip_without_trailing_bytes -- --exact
```

注册表记录了 32 个生成用例和一个 Rust 测试结果。用例数量与测试数量是不同指标。注册表执行器运行全部已注册测试集并拒绝零测试通过；仅在工作涉及这些测试集时使用，不作为无关页面编辑的常规要求。

独立示例以 `rocketmq-example/` 为工作目录，并选择精确示例。前端修改使用对应前端的包脚本。选择命令前阅读最近的 `AGENTS.md` 和 manifest；[开发指南](./development-guide.md)列出这些边界。

## 测试发生变化的契约

- **协议或持久化：** 覆盖新旧字段、默认值、布局行为、畸形输入及相关读取方。只用新写入方和新读取方往返，可能遗漏兼容性失败。
- **异步所有权：** 同步启动和取消过程，等待所拥有的任务，并观察资源释放。优先使用通道、屏障或虚拟时间，避免任意休眠。调用者超时后，阻塞闭包仍可能继续执行。
- **可选能力：** 测试相关的启用与禁用配置。单独使用 `--all-features` 不能证明 feature 缺省行为。
- **错误与安全：** 验证稳定标识、边界映射和脱敏；断言被拒绝的操作未到达产生副作用的适配器。不要为方便断言而记录秘密或消息正文。
- **消费行为：** 区分监听器完成、ACK/偏移量持久化与业务效果；必要时包含重复投递或所有权变化。

可行时使用临时目录和动态分配端口。测试应独立于开发者集群，并保留无关运行进程。新发现的失败应形成最小可复现回归，覆盖真实不变量。

## 修改 fuzz 目标

独立工程选择 `nightly-2026-07-05`，拥有四组同名目标/feature：`protocol_decode`、`raw_broker_config`、`controller_snapshot` 和 `store_recovery_record`。修改协议测试驱动或其消费接口时，在 `fuzz/` 中运行：

```bash
cargo +nightly-2026-07-05 check --locked --bin protocol_decode --features protocol_decode
```

这只检查测试驱动能否编译，不运行 fuzz 探索。fuzz 工作流负责较短的每夜运行和较长的每周运行。保留经过审查的小型回归种子，崩溃输出、生成语料、性能分析文件和构建输出不进入提交。仅在其他目标的接口或行为受影响时，选择对应目标。

## 报告实际观察

记录所属工程、所选命令/目标/feature、实际结果和重要环境限制。集群与故障实验还需记录拓扑、负载、故障触发方式及恢复观察。CI 中配置了命令不代表成功执行；被忽略的测试不是已执行场景。

纯文档修改只需相关内容检查。网站渲染内容变化时，在 `rocketmq-website/` 中使用 `npm run build`，并检查受影响页面。可运行片段可能损坏时检查对应示例。编写文档不要求指纹、固定工作副本、清除全部历史问题，也不新增审批或 CI 门禁。

## 源码参考

- [工程指南](https://github.com/mxsm/rocketmq-rust/blob/main/AGENTS.md)、[属性测试注册表](https://github.com/mxsm/rocketmq-rust/blob/main/scripts/property-state-suite-registry.json)与[执行器](https://github.com/mxsm/rocketmq-rust/blob/main/scripts/run_property_state_suites.py)。
- [互操作矩阵](https://github.com/mxsm/rocketmq-rust/blob/main/scripts/interop/v1-interop-matrix.json)与[存储故障矩阵](https://github.com/mxsm/rocketmq-rust/blob/main/scripts/interop/v1-storage-fault-matrix.json)。
- [Fuzz 指南](https://github.com/mxsm/rocketmq-rust/blob/main/fuzz/AGENTS.md)、[Client 测试目标](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/Cargo.toml)和[现有工作流](https://github.com/mxsm/rocketmq-rust/tree/main/.github/workflows)。
