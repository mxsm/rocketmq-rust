---
title: "使用 Admin CLI 管理集群"
---

使用 `rocketmq-admin-cli` 观察集群，并执行明确选定的元数据操作。它是本仓库的 Rust CLI，通过 `topic`、`consumer`、`ha` 等领域子命令组织功能。命令已注册代表客户端具备入口，接收请求的服务仍需支持该操作并授予权限。

## 连接目标集群

从仓库根目录构建匹配版本的 CLI：

```bash
cargo build -p rocketmq-admin-cli
```

Unix 下使用 `target/debug/rocketmq-admin-cli`，Windows 下使用 `target/debug/rocketmq-admin-cli.exe`。下文使用 `cargo run -p rocketmq-admin-cli --`，因此不必修改 PATH。原生构建依赖见[源码安装](../getting-started/installation.md)。

在 Admin 终端设置 NameServer 环境：

```powershell
$env:NAMESRV_ADDR = "127.0.0.1:9876"
```

```bash
export NAMESRV_ADDR='127.0.0.1:9876'
```

多个 NameServer 使用带引号的分号分隔列表。变量名是 `NAMESRV_ADDR`，不是 `ROCKETMQ_NAMESRV_ADDR`。`-n` 只由部分子命令提供，不是根命令的全局选项。特别是 `cluster clusterList`、`consumer updateSubGroup`，发现地址来自环境变量。

服务启用认证时，通过运维人员受保护的环境成对提供 `ROCKETMQ_ACL_ACCESS_KEY`、`ROCKETMQ_ACL_SECRET_KEY`，以及可选的 `ROCKETMQ_ACL_SECURITY_TOKEN`，详见[部署安全](../deployment/security.md)。变更状态前确认端点与身份。

## 诊断客户端前先读取路由

以下命令观察[教程集群](../getting-started/local-source.md)：

```bash
cargo run -p rocketmq-admin-cli -- cluster clusterList -c DocsCluster
cargo run -p rocketmq-admin-cli -- topic topicList -n 127.0.0.1:9876
cargo run -p rocketmq-admin-cli -- topic topicRoute -t DocsFirstMessage -n 127.0.0.1:9876
cargo run -p rocketmq-admin-cli -- topic topicStatus -t DocsFirstMessage -n 127.0.0.1:9876
cargo run -p rocketmq-admin-cli -- broker brokerStatus -b 127.0.0.1:10911
```

`clusterList` 展示注册信息；`topicRoute` 展示主题返回的 Broker 地址与队列；`topicStatus` 展示队列偏移量边界；`brokerStatus` 查询指定 Broker 的运行数据。路由可见不能证明持久化、复制或应用消费已成功。

有多个 NameServer 时，分别指定单个地址查询，以定位观察差异。从应用网络测试路由返回的地址。Broker 主机上的 Admin 可能成功，但远端客户端仍无法访问对外公布的回环地址。

## 观察消费者与进度

```bash
cargo run -p rocketmq-admin-cli -- connection consumerConnection -g docs_first_message_consumer -b 127.0.0.1:10911
cargo run -p rocketmq-admin-cli -- consumer consumerProgress -g docs_first_message_consumer -t DocsFirstMessage -n 127.0.0.1:9876
```

连接查询面向一个 Broker，展示该组当前的客户端、订阅观察结果，其领域是 `connection`，不是 `consumer`。进度查询把消费者组已提交的队列位置与 Broker 队列边界关联起来。调查是否推进时，应在已知时间间隔内查询两次，并保持主题、组、Broker 名称和队列 ID 对应一致。

| 观察结果 | 解释与下一步 |
| --- | --- |
| 没有注册客户端 | 检查启动生命周期、组名、心跳/连接、认证，以及所用客户端模式是否产生该观察信息 |
| 有连接但进度不动 | 检查订阅过滤、队列分配、处理结果、重试与偏移量/回执完成 |
| 进度落后于最小保留偏移量 | 数据可能已超出本地保留范围；检查消费者的偏移量修正和业务核对 |
| 显示积压为零 | 该时刻观察到的提交位置已追平，不能证明独立业务数据库已经提交 |
| 重启后重复处理 | 检查业务完成是否早于偏移量/ACK 持久化，并实施[幂等投递处理](../guides/delivery-and-retry.md) |

LitePull 本地 `commit_all` 与随后 Broker 可见的持久化不是同一步。POP 使用回执完成与不可见期，不能把传统组偏移量查询当作所有已取出消息均被 ACK 的证明。

## 检查复制与 Controller 权限

在 [HA 教程](../deployment/high-availability.md) 中，选择**当前** Controller Leader 的 remoting 地址：

```bash
cargo run -p rocketmq-admin-cli -- controller getControllerMetaData -a 127.0.0.1:19878
cargo run -p rocketmq-admin-cli -- ha getSyncStateSet -a 127.0.0.1:19878 -c DocsCluster -b docs-ha
cargo run -p rocketmq-admin-cli -- ha haStatus -b 127.0.0.1:10911
```

这些地址是示例，不代表第一个节点始终为 Leader，也不代表 `10911` 始终对应可写 Broker。计划重启前，比较实际 Leader/角色、权限 epoch、同步成员关系与复制进度。副本已注册不等于它能满足当前写入要求。Controller 保存权限元数据，不保存消息正文备份。

## 创建隔离的教程资源

以下命令会**创建或更新元数据**，仅针对隔离的教程资源名和目标集群执行：

```bash
cargo run -p rocketmq-admin-cli -- topic updateTopic -t DocsFirstMessage -c DocsCluster -r 4 -w 4 -n 127.0.0.1:9876
cargo run -p rocketmq-admin-cli -- consumer updateSubGroup -g docs_first_message_consumer -c DocsCluster
```

复用已有主题、组可能修改其配置。操作后查询路由并观察组行为。面向整个集群的操作可能影响多个 Broker，不是跨节点事务；应逐目标检查失败与最终状态。

## 改变消费或移除状态的操作

| 命令类别 | 执行前需要明确的影响 |
| --- | --- |
| `offset resetOffsetByTime` | 修改组的队列位置；向前回退会重放保留数据，向后跳转会跳过处理 |
| `offset cloneGroupOffset` | 按另一个组替换目标进度；两组订阅与业务完成情况可能不同 |
| `offset skipAccumulatedMessage` | 跳过积压，不会补做对应业务工作 |
| `consumer deleteSubGroup` / `topic deleteTopic` | 移除应用依赖的元数据；数据清理与路由收敛是其他效果 |
| Broker 配置与清理命令 | 按具体操作影响请求接纳、保留策略或持久化状态 |

重置前停止或协调相关消费者，记录各队列原位置，确认仍保留的数据区间，并决定应用如何处理重放或跳过。可先查询当前帮助，不变更状态：

```bash
cargo run -p rocketmq-admin-cli -- offset resetOffsetByTime --help
```

时间戳参数为 `-s` / `--timestamp`，接受 epoch 毫秒、帮助中规定的时间格式或 `now`。`now` 明确表示跳过当前积压。在线通知与旧版离线回退取决于服务端、客户端路径，应验证实际位置和应用行为；CLI 成功不能替代该观察。

## 错误与离线检查

记录命令领域、非秘密参数、端点、时间、退出码与稳定错误码。`--verbose` 增加受控诊断字段，不应因此打印完整凭据或消息正文。服务端不支持操作、认证拒绝、参数无效和 Broker 不可达，需要不同处理。

`rocketmq-store-inspect` 是单独的离线工具包，二进制名称为 `rocketmq-cli-rust`，不连接集群。按[备份恢复](backup-recovery.md)与[升级回退](upgrade-rollback.md)的说明，用它检查已停止或一致性复制的存储状态，不把它当作在线 Admin 替代品。

## 源码索引

[CLI 命令注册](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-tools/rocketmq-admin/rocketmq-admin-cli/src/commands)、[凭据与错误处理](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-tools/rocketmq-admin/rocketmq-admin-cli/src/rocketmq_cli.rs)、[Admin Core](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-tools/rocketmq-admin/rocketmq-admin-core)、[离线检查工具](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-tools/rocketmq-store-inspect/README.md)。
