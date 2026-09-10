---
title: "多 NameServer 与多 Broker 组"
---

这套可复现开发布局在同一主机上增加第二个 NameServer 和第二个独立 Broker 组，用于观察发现服务及主题在多个 Broker 上的分布。两个 Broker 分别是不同组的主节点，因此这不是主副本高可用部署。

复用端口前先停止[单 Broker 教程](../getting-started/local-source.md)。所有终端保持在仓库根目录。已提交的[多节点配置](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-website/examples/multi-node) 使用新数据目录及环回监听器。

## 先固定身份与路径

| 进程 | 客户端端点 | 其他端口 | 身份 | `.rocketmq-multi-demo` 下的数据目录 |
| --- | --- | --- | --- | --- |
| NameServer 1 | `127.0.0.1:9876` | — | 独立路由服务 | `ns-1` |
| NameServer 2 | `127.0.0.1:9877` | — | 独立路由服务 | `ns-2` |
| Broker A | `127.0.0.1:10911` | Fast `10909`、HA `10912` | `docs-broker-a`，ID 0 | `broker-a` 元数据、`store-a` 消息 |
| Broker B | `127.0.0.1:10931` | Fast `10929`、HA `10932` | `docs-broker-b`，ID 0 | `broker-b` 元数据、`store-b` 消息 |

两个 Broker 组都属于 `DocsCluster`，并向两个 NameServer 注册。不同 Broker 名称使它们成为独立组；不同组均使用 ID 0 合法。Broker A 的副本则应共享其 Broker 名称，使用不同 ID 和存储，详见 [HA 部署](high-availability.md)。

## 准备并启动四个进程

创建六个数据目录。PowerShell：

```powershell
New-Item -ItemType Directory -Force .rocketmq-multi-demo/ns-1, .rocketmq-multi-demo/ns-2, .rocketmq-multi-demo/broker-a, .rocketmq-multi-demo/broker-b, .rocketmq-multi-demo/store-a, .rocketmq-multi-demo/store-b
$env:ROCKETMQ_HOME = "$PWD"
$env:ROCKETMQ_SECURITY_PROFILE = "development-insecure-loopback"
$env:NAMESRV_ADDR = "127.0.0.1:9876;127.0.0.1:9877"
```

Unix shell：

```bash
mkdir -p .rocketmq-multi-demo/{ns-1,ns-2,broker-a,broker-b,store-a,store-b}
export ROCKETMQ_HOME="$PWD"
export ROCKETMQ_SECURITY_PROFILE=development-insecure-loopback
export NAMESRV_ADDR='127.0.0.1:9876;127.0.0.1:9877'
```

在各服务终端重复环境设置。先在不同终端执行前两条命令，观察启动成功，再分别启动两个 Broker：

```bash
cargo run -p rocketmq-namesrv --bin rocketmq-namesrv-rust -- -c rocketmq-website/examples/multi-node/namesrv-1.toml
cargo run -p rocketmq-namesrv --bin rocketmq-namesrv-rust -- -c rocketmq-website/examples/multi-node/namesrv-2.toml
cargo run -p rocketmq-broker --bin rocketmq-broker-rust -- -c rocketmq-website/examples/multi-node/broker-a.toml -n '127.0.0.1:9876;127.0.0.1:9877'
cargo run -p rocketmq-broker --bin rocketmq-broker-rust -- -c rocketmq-website/examples/multi-node/broker-b.toml -n '127.0.0.1:9876;127.0.0.1:9877'
```

每条命令会持续运行；这不是在一个终端顺序执行的脚本。Broker 显式 `-n` 避免其他环境变量替换预期端点列表。shell 参数中的分号必须加引号。

## 创建资源并比较路由

在 Admin 终端按上述方式设置 `NAMESRV_ADDR`。以下命令会在专用集群中创建/更新元数据：

```bash
cargo run -p rocketmq-admin-cli -- topic updateTopic -t DocsFirstMessage -c DocsCluster -r 4 -w 4 -n '127.0.0.1:9876;127.0.0.1:9877'
cargo run -p rocketmq-admin-cli -- consumer updateSubGroup -g docs_first_message_consumer -c DocsCluster
```

分别查询两个 NameServer：

```bash
cargo run -p rocketmq-admin-cli -- cluster clusterList
cargo run -p rocketmq-admin-cli -- topic topicRoute -t DocsFirstMessage -n 127.0.0.1:9876
cargo run -p rocketmq-admin-cli -- topic topicRoute -t DocsFirstMessage -n 127.0.0.1:9877
```

注册收敛后，两份路由都应包含 `docs-broker-a`、`docs-broker-b` 及不同端口，各具有四个读/写队列。检查每条命令的结果：集群级元数据更新可能发生部分失败。

当前 `clusterList` 和 `updateSubGroup` 使用 NameServer 环境设置，不接受 `-n`。主题命令提供自己的 `-n` 参数，它不是通用根参数。

## 验证消息与发现

运行[第一批消息的消费者和生产者](../getting-started/quick-start.md)。它们固定的 NameServer 端点仍为 `127.0.0.1:9876`，所以未经修改的示例验证通过第一个发现节点进行消息路由，不证明客户端 NameServer 故障切换。路由仍包含两个 Broker 组。

测试应用的发现故障切换时，为其配置两个 NameServer 端点，记录路由/消息操作成功，然后只停止一个专用 NameServer，观察后续操作及路由刷新。热缓存可以在没有新发现请求时继续工作，因此发送成功本身不足以证明端点切换。

不要停掉一个 Broker，就把另一个 Broker 称作它的副本。组 A 的消息不会自动存在于组 B。消息持久性和晋升应使用 [HA 拓扑](high-availability.md)。

## 扩展到不同主机

将环回地址替换成所有目标客户端及对端可达的地址。绑定地址描述本地接口，`brokerIp1` 向客户端通告，HA 地址用于复制。NameServer 可达但通告的 Broker 不可达，仍会造成发送失败。

每个进程保留唯一数据根目录。所有 Broker 和应用配置相同 NameServer 列表。检查实际 Remoting、Fast、HA 及健康端口的防火墙规则。开发环回安全 profile 不能用于非环回监听器；暴露共享网络前应配置[部署安全](security.md)。

## 停止与证据范围

先停止应用客户端，再通过终端中断停止 Broker，最后停止 NameServer，并检查关闭结果。保留数据目录用于重启，不要求删除。

可使用相应二进制的 `-c <file> -p` 模式检查配置文件。打印配置证明解析，不证明四进程消息运行或故障恢复测量。记录部署试验时应使用实际路由、消息和进度观察。
