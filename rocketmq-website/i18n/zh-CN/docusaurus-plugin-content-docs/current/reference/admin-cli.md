---
title: "Admin CLI 参考"
---

# Admin CLI 参考

`rocketmq-admin-cli` 通过领域子命令调用 Admin Core，用于操作选定的集群或 Broker，不负责启动这些服务。当前注册包含 17 个领域、102 个末级命令，以及根级 `show` 命令。本目录依据解析器和本地检查的帮助输出编写，不沿用 README 中的旧命令数量。

## 调用与发现命令

从仓库根目录运行：

```bash
cargo run -p rocketmq-admin-cli -- --help
cargo run -p rocketmq-admin-cli -- topic updateTopic --help
cargo run -p rocketmq-admin-cli -- broker updateBrokerConfig --help
```

构建后使用 `target/debug/rocketmq-admin-cli`，Windows 使用 `target/debug/rocketmq-admin-cli.exe`。`--` 之前的参数由 Cargo 处理，之后由 CLI 处理。`--verbose` 是全局参数，用于为错误增加受控诊断字段。`--generate-completion` 支持解析器提供的 Shell，包括 Bash、Zsh、Fish 和 PowerShell。`show` 显示领域分类表。

不存在通用的全局 `-n`、`--yes`、`--json`、`--format` 或超时参数。末级命令可能嵌入公共参数，也可能自行定义参数，应检查该命令的帮助。参数规范化仅将旧式 `-bn` 转为 `--brokerName`，并非通用的 Java mqadmin 兼容层。

## 命令目录

查询与变更命令可能属于同一领域。影响列说明该类操作的边界；具体帮助和服务响应决定本次操作的实际目标与结果。

| 领域 | 已注册子命令 | 影响 |
| --- | --- | --- |
| `auth` | `copyAcl`, `copyUser`, `createAcl`, `createUser`, `deleteAcl`, `deleteUser`, `getAcl`, `getUser`, `listAcl`, `listUser`, `updateAcl`, `updateUser` | get/list 读取身份和 ACL；copy/create/update/delete 修改访问控制。 |
| `broker` | `brokerConsumeStats`, `brokerStatus`, `cleanExpiredCQ`, `cleanUnusedTopic`, `deleteExpiredCommitLog`, `getBrokerConfig`, `getBrokerEpoch`, `getColdDataFlowCtrInfo`, `removeColdDataFlowCtrGroupConfig`, `resetMasterFlushOffset`, `sendMsgStatus`, `switchTimerEngine`, `updateColdDataFlowCtrGroupConfig`, `updateBrokerConfig`, `setCommitLogReadAheadMode` | 状态、配置和纪元查询；配置更新、清理、偏移量与定时引擎命令修改状态。sendMsgStatus 发送探测消息。 |
| `cluster` | `clusterList`, `clusterRT` | clusterList 读取拓扑；clusterRT 执行消息延迟探测。 |
| `connection` | `consumerConnection`, `producerConnection` | 读取生产者和消费者连接。 |
| `consumer` | `consumerStatus`, `consumer`, `deleteSubGroup`, `getConsumerConfig`, `setConsumeMode`, `startMonitoring`, `updateSubGroupList`, `updateSubGroup`, `consumerProgress` | 读取状态、配置、进度或持续监控；update/delete 与 setConsumeMode 修改消费者组元数据或行为。 |
| `controller` | `cleanBrokerMetadata`, `electMaster`, `getControllerConfig`, `getControllerMetaData`, `updateControllerConfig` | 读取元数据和配置；cleanBrokerMetadata、electMaster、updateControllerConfig 修改控制平面状态。 |
| `export` | `exportConfigs`, `exportMetrics`, `exportMetadataInRocksDB`, `exportMetadata`, `exportPopRecord`, `rocksDBConfigToJson` | 将选定信息导出到本地输出；这些命令不构成协调一致的消息存储备份。 |
| `ha` | `getSyncStateSet`, `haStatus` | 读取 HA 和同步副本集合状态。 |
| `lite` | `getBrokerLiteInfo`, `getLiteClientInfo`, `getLiteGroupInfo`, `getLiteTopicInfo`, `getParentTopicInfo`, `triggerLiteDispatch` | 读取 Lite 路由、客户端和消费者组状态；triggerLiteDispatch 主动触发分发。 |
| `message` | `checkMsgSendRT`, `consumeMessage`, `decodeMessageId`, `dumpCompactionLog`, `printMsg`, `printMsgByQueue`, `queryMsgById`, `queryMsgByKey`, `queryMsgByOffset`, `queryMsgByUniqueKey`, `queryMsgTraceById`, `sendMessage` | 查询、解码、打印或拉取数据；sendMessage 和 checkMsgSendRT 生产消息。拉取或打印不代表业务处理完成。 |
| `nameserver` | `addWritePerm`, `deleteKvConfig`, `getNamesrvConfig`, `updateKvConfig`, `updateNamesrvConfig`, `wipeWritePerm` | getNamesrvConfig 读取配置；其余命令修改配置、KV 元数据或 Broker 写权限。 |
| `offset` | `cloneGroupOffset`, `getConsumerStatus`, `resetOffsetByTime`, `resetOffsetByTimeOld`, `skipAccumulatedMessage` | getConsumerStatus 读取状态；clone/reset/skip 修改消费进度，可能重放或跳过保留消息。 |
| `producer` | `producer` | 读取生产者信息。 |
| `queue` | `checkRocksdbCqWriteProgress`, `queryCq` | 检查 ConsumeQueue 和 RocksDB ConsumeQueue 进度。 |
| `release-checkpoint` | `capabilities`, `create-set`, `verify-set`, `restore-verify` | 校验本地 JSON 输入；create-set 写入新的组合清单。不会自动通过 RPC 创建检查点或恢复数据。 |
| `stats` | `statsAll` | 读取汇总统计。 |
| `topic` | `allocateMQ`, `deleteTopic`, `remappingStaticTopic`, `topicClusterList`, `topicList`, `topicRoute`, `topicStatus`, `updateOrderConf`, `updateStaticTopic`, `updateTopicList`, `updateTopicPerm`, `updateTopic` | 读取列表、路由、状态或计算分配；创建、更新、删除和重新映射操作修改主题元数据。 |

## 地址与凭据

| 输入 | 作用范围与默认行为 |
| --- | --- |
| `NAMESRV_ADDR` | 客户端发现地址环境变量；用引号包围分号分隔的地址列表。适用于不接受 `-n` 的 `clusterList`、`updateSubGroup` 等命令。 |
| `-n / --namesrvAddr` | 仅适用于声明该参数的末级命令，包括 `topic updateTopic` 和 `nameserver updateNamesrvConfig`。 |
| `-b / --brokerAddr` | 在声明该参数的命令中表示 Broker 端点；不要与 `message sendMessage -b / --broker` 混淆，后者接受 Broker 名称。 |
| `-c / --clusterName` | 在支持该参数的命令中选择集群，可能把一次操作扩展到多个 Broker。 |
| `ROCKETMQ_ACL_ACCESS_KEY` 与 `ROCKETMQ_ACL_SECRET_KEY` | CLI 加载的可选成对凭据；空白值视为缺失，仅提供一项会产生参数错误。 |
| `ROCKETMQ_ACL_SECURITY_TOKEN` | 与凭据一起使用的可选安全令牌。 |

通过已有密钥管理流程向进程环境提供凭据，避免写入命令示例或收集的诊断输出。认证成功不代表拥有全部操作权限，目标服务仍执行授权。

对于已经在文档所示端口运行的本地集群：

```bash
export NAMESRV_ADDR='127.0.0.1:9876'
cargo run -p rocketmq-admin-cli -- cluster clusterList
```

```powershell
$env:NAMESRV_ADDR = '127.0.0.1:9876'
cargo run -p rocketmq-admin-cli -- cluster clusterList
```

预期得到拓扑表。查询为空或失败不意味着应创建替代集群元数据。按照[首次诊断](../operations/first-diagnosis.md)区分发现服务、广播地址、Broker 就绪状态和凭据问题。

## 常用参数

| 命令 | 必需或重要输入 | 默认值与影响 |
| --- | --- | --- |
| `topic updateTopic` | `-t / --topic`；用 `-b` 选择 Broker 或用 `-c` 选择集群 | 读写队列数 `-r`/`-w` 默认为 8；`-p / --perm` 接受 2 写、4 读、6 读写。创建或更新元数据。`--order`、`--unit`、`--hasUnitSub` 接受布尔值。 |
| `consumer updateSubGroup` | `-g / --groupName`；选择 `-b` 或 `-c` | 创建或更新消费者组；可选消费开关接受布尔值。重试次数和策略、Broker 选择及属性由对应参数指定。本命令没有 `-n`。 |
| `broker updateBrokerConfig` | `-b`/`-c` 二选一；单个 `-k` 配合 `-v`，或重复 `-p / --property KEY=VALUE` | `--dryRun`（别名 `--dry-run`）预览校验而不应用变更。`--noRollback` 禁用集群部分失败后的自动回滚。`-y` 跳过该命令的确认提示。 |
| `nameserver updateNamesrvConfig` | `-k / --key` 与 `-v / --value`；可选 `-n` | NameServer 的持久化期望配置与实际生效配置不同，部分已接受变更需要重启。 |
| `offset resetOffsetByTime` | `-g / --group`、`-t / --topic`、`-s / --timestamp` | 时间接受 `now`、Unix 毫秒或 `yyyy-MM-dd#HH:mm:ss:SSS`。`now` 跳过积压；命令可能影响该主题的全部队列。 |
| `message sendMessage` | `-t / --topic` 与 `-p / --body` | 消息体为 UTF-8 字符串；可选 `-k / --key`、`-c / --tags`、`-b / --broker`、`-i / --qid`。`-m / --msgTraceEnable` 默认 false。发送真实数据。 |

运维脚本需要避免格式化日期解释歧义时，使用明确的数值时间戳。重置前协调消费者所有权；帮助说明了在线通知和离线旧路径回退，但请求成功不代表每个应用都已应用新游标。操作后检查消费者组进度和业务重放影响。

## 预览受支持的 Broker 变更

下列命令连接选定的本地 Broker，校验更新而不应用。它要求服务和凭据已配置；编写本参考时没有对运行中的 Broker 执行此命令。

```bash
cargo run -p rocketmq-admin-cli -- broker updateBrokerConfig -b 127.0.0.1:10911 -p autoCreateTopicEnable=false --dryRun
```

应用前阅读 [Broker 配置](../configuration/broker-config.md)。普通运行时更新只支持文档列出的动态键，并返回 `persisted=false`；若需重启后保留，还应单独修改受管理的启动配置。多个 Broker 之间的自动回滚属于尽力恢复，不是分布式原子提交。保留各 Broker 结果，在重试前排查部分失败。

## 输出、退出状态与脚本

输出由具体命令决定，可能为表格、文本、JSON 或文件。不要把所有命令都按 JSON 解析，也不要假设表格列是稳定的机器接口。应用集成优先使用类型化 Admin Core 操作，并显式处理响应。

| 退出码 | 规范错误类别 |
| --- | --- |
| 0 | 执行成功，或显示帮助/版本 |
| 64 | 用法或参数无效 |
| 65 | 数据无效，或映射到此类别的冲突/状态错误 |
| 66 | 实体不存在 |
| 69 | 服务不可用 |
| 70 | 内部软件故障 |
| 75 | 临时故障 |
| 77 | 权限失败 |
| 78 | 配置失败 |

命令的规范错误描述符决定映射的退出码。重试提示见[错误参考](./errors.md)：退出码 75 并不意味着可以盲目重复发送消息或重试部分生效的变更。默认错误保留稳定代码和安全消息；`--verbose` 增加有界诊断上下文，不会公开原始原因链或凭据。

捕获 CLI 进程的实际退出状态，PowerShell 使用 `$LASTEXITCODE`。通过 `cargo run` 调用时，编译失败属于 Cargo 错误，不属于本表的服务错误。可靠运维应先构建，再直接调用二进制。

## 检查点与导出边界

`release-checkpoint capabilities` 读取能力响应 JSON 文件，不调用服务。`create-set` 组合提供的 Controller、Store 清单并写入新文件。`verify-set`、`restore-verify` 检查提供的清单和证明材料。这些命令不会停止写入、复制存储、启动替代节点，也不能证明运行中的应用已恢复。周边步骤见[维护](../operations/maintenance.md)和[备份与恢复](../operations/backup-recovery.md)。

本文检查了现有二进制的根级、领域帮助和代表性末级帮助，并核对当前解析器及执行边界。没有执行破坏性命令、偏移量重置或远程变更。

来源：[注册与公共参数](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-tools/rocketmq-admin/rocketmq-admin-cli/src/commands.rs)、[CLI 解析与凭据](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-tools/rocketmq-admin/rocketmq-admin-cli/src/rocketmq_cli.rs)、[入口](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-tools/rocketmq-admin/rocketmq-admin-cli/src/main.rs)、[检查点命令](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-tools/rocketmq-admin/rocketmq-admin-cli/src/commands/release_checkpoint.rs)。
