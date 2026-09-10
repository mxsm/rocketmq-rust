---
title: "部署主副本与 Controller HA"
---

选择固定主副本组或 Controller 管理组中的一条路径。以下两条路径的数据目录不同，但使用重叠端口，因此一次只运行一种。它们是在单主机上的环回实验环境，不是能够承受主机故障的生产部署。

解释发送结果前先阅读 [HA 权限与确认设计](../architecture/ha-controller.md)。两个进程、已连接 HA 套接字、成功健康探针，与副本持久化确认属于不同观察。

## 公共准备

使用[本地 NameServer 配置](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-website/examples/first-message/namesrv.toml)及其启动说明。停止占用 `10911`、`10931` 的旧教程 Broker。在每个服务终端设置 `ROCKETMQ_HOME` 为仓库根目录，并设置 `ROCKETMQ_SECURITY_PROFILE=development-insecure-loopback`。NameServer 仍为 `127.0.0.1:9876`。

创建所选模式的目录。PowerShell：

```powershell
$docsHaNodes = @('default-master','default-slave')
foreach ($docsHaNode in $docsHaNodes) {
  New-Item -ItemType Directory -Force ".rocketmq-ha-demo/$docsHaNode/metadata", ".rocketmq-ha-demo/$docsHaNode/store"
}
```

Controller 模式将数组替换为 `@('controller-broker-1','controller-broker-2','controller-broker-3')`，另外创建 `.rocketmq-ha-demo/controller-1`、`controller-2`、`controller-3`。Linux 使用 `mkdir -p` 创建相同目录。这些名称均位于 `.rocketmq-ha-demo` 下。

全部文件在 [HA 示例目录](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-website/examples/ha)。保持进程工作目录一致，使配置解析到预期存储。

## 路径 A：固定同步主节点与副本

| 进程 | Remoting / HA | 身份及角色 |
| --- | --- | --- |
| 主节点 | `10911 / 10912` | Broker `docs-ha`，ID 0，`SYNC_MASTER` |
| 副本 | `10931 / 10932` | Broker `docs-ha`，ID 1，`SLAVE`；主 HA 端点 `127.0.0.1:10912` |

两者均使用 `SYNC_FLUSH` 和双副本配置。Fast Remoting 端口为 `10909`、`10929`。每个进程的元数据及存储目录独立。

在不同终端启动：

```bash
cargo run -p rocketmq-broker --bin rocketmq-broker-rust -- -c rocketmq-website/examples/ha/default-master.toml -n 127.0.0.1:9876
cargo run -p rocketmq-broker --bin rocketmq-broker-rust -- -c rocketmq-website/examples/ha/default-slave.toml -n 127.0.0.1:9876
```

发送前检查 HA 进度。必需副本尚未就绪时，可能无法得到请求的同步结果。不要仅为了让试验输出成功而将策略改为异步。

此路径没有 Controller 选举服务。停止主节点不会自动授权配置的副本成为主节点。恢复应恢复预期主节点，或另行规划包含数据/身份协调的角色切换。

## 路径 B：三个 Controller 与三个 Broker 副本

使用默认 RocksDB 后端构建 Controller；需要原生 RocksDB 工具链和 `protoc`：

```bash
cargo build -p rocketmq-controller --bin rocketmq-controller-rust
```

| 节点 | Remoting | Raft gRPC | 持久化目录 |
| --- | --- | --- | --- |
| Controller 1 | `19878` | `19879` | `controller-1` |
| Controller 2 | `19888` | `19889` | `controller-2` |
| Controller 3 | `19898` | `19899` | `controller-3` |

每份文件使用相同三个 Raft peer 和不同 `nodeId`。Remoting 与 Raft 端口必须不同。不要原样复制通用 distribution Controller 示例；使用上述专门准备的不同端点文件。

首次初始化时，在 Controller 终端设置 `ROCKETMQ_CONTROLLER_AUTO_INITIALIZE_CLUSTER=true`。PowerShell 使用 `$env:ROCKETMQ_CONTROLLER_AUTO_INITIALIZE_CLUSTER = "true"`；Unix shell 使用 `export ROCKETMQ_CONTROLLER_AUTO_INITIALIZE_CLUSTER=true`。

每条命令在独立终端启动：

```bash
cargo run -p rocketmq-controller --bin rocketmq-controller-rust -- -c rocketmq-website/examples/ha/controller-1.toml
cargo run -p rocketmq-controller --bin rocketmq-controller-rust -- -c rocketmq-website/examples/ha/controller-2.toml
cargo run -p rocketmq-controller --bin rocketmq-controller-rust -- -c rocketmq-website/examples/ha/controller-3.toml
```

明确启用后，只有最小配置节点 ID 初始化完整成员集合。已有提交状态不会重新初始化。启动 Broker 前先观察选出的 leader 和已提交成员关系，不要假设 1 号节点一直是 leader。

```bash
cargo run -p rocketmq-broker --bin rocketmq-broker-rust -- -c rocketmq-website/examples/ha/controller-broker-1.toml -n 127.0.0.1:9876
cargo run -p rocketmq-broker --bin rocketmq-broker-rust -- -c rocketmq-website/examples/ha/controller-broker-2.toml -n 127.0.0.1:9876
cargo run -p rocketmq-broker --bin rocketmq-broker-rust -- -c rocketmq-website/examples/ha/controller-broker-3.toml -n 127.0.0.1:9876
```

这些 Broker 共享 `docs-ha` 组，使用不同 ID 和存储，初始配置为 `SLAVE` 并开启 Controller 模式。Remoting 端口为 `10911`、`10931`、`10951`；HA 端口加一，Fast 端口减二。所选主节点必须取得 Controller 角色分配及有效写租约，才能接纳写入。存储身份文件跨重启保留。

## 故障前后的观察

为 Admin 命令设置 `NAMESRV_ADDR=127.0.0.1:9876`。查询实际当前 Controller leader 的 Remoting 端点；下方 `19878` 是示例，其他节点当选时需替换：

```bash
cargo run -p rocketmq-admin-cli -- controller getControllerMetaData -a 127.0.0.1:19878
cargo run -p rocketmq-admin-cli -- ha getSyncStateSet -a 127.0.0.1:19878 -c DocsCluster -b docs-ha
cargo run -p rocketmq-admin-cli -- ha haStatus -b 127.0.0.1:10911
```

前两条命令适用于 Controller 模式；两种模式都可通过 HA status 检查所选 Broker。按[快速开始](../getting-started/quick-start.md)创建 `DocsFirstMessage` 及消费者组，再记录发送状态、接收 ID、偏移量、当前权限和副本持久化进度。

受控故障试验一次只停止一个专用进程。记录不可用或结果不确定的写入区间，观察新角色/epoch、路由，并在恢复后核对已确认 ID。使用相同身份/存储重启原节点，等待追平后再做下一次故障。不要删除旧主数据来制造干净的重新加入。

Controller 丢失法定多数可能阻止租约续期和安全晋升；仅有两个 Broker 进程存活不能替代该法定多数。当前同步集合之外的副本不能构成足够确认依据。不要把非同步副本选主当作排障捷径。

## 生产与回退边界

将副本和 Controller 放到不同故障域，使用可达通告端点及[安全配置](security.md)，并为保留、重放、追平准备足够持久化容量。单主机试验无法测量主机或磁盘故障容忍能力。

修改配置中的 Controller peer 不执行 Raft 成员变更。滚动维护必须保留法定多数和所选 Broker ACK 容量。两个成员且要求全部确认的组，在一个成员重启时没有额外同步容量。

这些步骤描述基于源码的预期搭建与观察，不声明文档编写时已经运行故障切换或已确认消息恢复试验，也不提供固定 RPO/RTO。可恢复状态的保留方式见[备份恢复](../operations/backup-recovery.md)及[升级回退](../operations/upgrade-rollback.md)。
