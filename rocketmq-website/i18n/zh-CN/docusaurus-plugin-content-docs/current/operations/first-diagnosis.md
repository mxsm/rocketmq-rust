---
title: "首次消息链路诊断"
---

当[本地搭建](../getting-started/local-source.md)或[快速开始](../getting-started/quick-start.md)没有得到预期结果时，使用本页逐步定位尚未完成的边界：进程、注册、路由、发送结果、队列分配、业务处理，最后是进度。保持两个服务终端可见。

## 先收集只读证据

运行管理命令前，在当前终端中设置 NameServer。PowerShell：

```powershell
$env:NAMESRV_ADDR = "127.0.0.1:9876"
```

Unix shell：

```bash
export NAMESRV_ADDR=127.0.0.1:9876
```

当前 `clusterList` 和 `updateSubGroup` 子命令使用环境变量，不接受 `-n`。下面的主题和进度查询命令各自支持 `-n`，它不是 CLI 全局参数。

在仓库根目录，对教程 NameServer 执行：

```bash
cargo run -p rocketmq-admin-cli -- cluster clusterList
cargo run -p rocketmq-admin-cli -- topic topicRoute -t DocsFirstMessage -n 127.0.0.1:9876
cargo run -p rocketmq-admin-cli -- consumer consumerProgress -g docs_first_message_consumer -t DocsFirstMessage -n 127.0.0.1:9876
```

这些命令分别查询集群成员、主题路由和组消费进度。新建或不活跃的组可能尚无有效进度，这本身不能证明数据丢失。进度查询返回错误，与有效结果显示没有积压，是不同情况。

记录命令、错误码/信息、相关服务启动输出、实际地址及主题/组名。保留复现问题所需的上下文，排除凭证、令牌和消息体。

## 服务无法启动

| 现象 | 检查内容 | 下一步 |
| --- | --- | --- |
| 配置解析或未知字段错误 | 文件格式、拼写和分节位置 | Broker 配置应位于 `[broker]`、`[store]` 等规范分节，对照教程配置 |
| 地址已占用 | NameServer、Broker、fast 和 HA 监听端口 | 确认所属进程，选择适用的空闲端口，或仅停止自己持有的服务 |
| 开发 profile 拒绝监听地址 | 每个已配置监听地址 | 教程保持回环监听，包括 Broker 嵌套监听地址和 HA 地址 |
| 存储无法打开 | 工作目录、路径可写性和目录所有权 | 使用教程目录，不让两个 Broker 共用一个存储 |
| Broker 报告注册失败 | NameServer 进程和 `-n` 地址 | 先启动 NameServer，再确认所配置端点可达 |

Windows 上可以只读检查监听器：

```powershell
Get-NetTCPConnection -State Listen |
    Where-Object { $_.LocalPort -in 9876, 10909, 10911, 10912 } |
    Select-Object LocalAddress, LocalPort, OwningProcess
```

Linux 上若已安装 `ss`，可用 `ss -ltnp` 查看 TCP 监听。关键证据是实际本地地址、端口和所属进程，不能只看是否存在窗口或 PID。

## NameServer 可响应，但没有 Broker 或路由

`clusterList` 缺少 `docs-broker` 时，先检查注册，再检查消费者代码。确认 Broker 指向当前 NameServer，使用预期身份并走正常启动路径。`-p` 打印配置不会注册 Broker。

Broker 存在但主题路由缺失时，确认 `updateTopic` 已对 `DocsCluster` 和 `DocsFirstMessage` 成功执行。教程关闭了主题自动创建。等待注册更新传播，再次查询路由。

路由存在但客户端无法连接时，检查公布地址。`brokerIp1` 必须对这些客户端可达，仅修改监听地址并不足够。容器中的回环地址指向容器自身，不是宿主机服务。

## 生产者启动成功，但发送失败

调整超时前，先阅读返回状态和错误。检查可写队列数、主题拼写、Broker 可达性、资源权限和所选安全配置。

刷盘或副本等待超时，与路由查询失败不同。这些超时可能对应不确定的写入结果，重复运行应用可能产生重复消息。发送成功，也独立于当前消费者组是否订阅或运行。

认证/授权拒绝不能通过换一个消费者组名解决。应为目标部署对齐凭证、资源权限和传输要求。回环教程的开发 profile 仅适用于对应本地配置。

## 消费者持续轮询，但没有消息

按以下顺序检查：

1. 确认精确主题名具有可读路由，且 Broker 地址可达。
2. 确认消费者实际输出 `CONSUMER_STARTED`，组名与订阅符合预期。
3. 检查同组其他实例是否持有可用队列。同组成员协作消费，不是各自接收完整副本。
4. 比较组进度与队列末尾。已有提交进度可能使当前没有新数据可读。
5. 在消费者 60 秒窗口内发送新教程消息；旧数据可能已被消费。
6. 仍为空轮询时，检查客户端诊断和分配状态。轮询 API 的空向量不是“主题没有消息”的结构化断言。

`ConsumeFromFirstOffset` 用于没有可用已存储进度的组，不会重置已有组的位置。过滤条件不匹配或同组订阅不一致，也可能排除消息。

## 消息重复，或进度看起来落后

发送超时后的重试可能产生重复。业务处理成功但进度尚未持久化时，消费者也可能再次处理同一业务。再平衡和进程重启可能暴露这些窗口。

当前 LitePull 的 `commit_all` 更新客户端偏移量存储状态，与持久化分离，并可能只记录单个队列失败。示例中的 `OFFSET_COMMIT_REQUESTED` 不是持久的 Broker 回执。阅读 [LitePull 提交语义](../consumer/pull-consumer.md)，并按[投递与重试](../guides/delivery-and-retry.md)实现业务幂等。

进度指标描述队列位置，不会检查数据库事务是否正确执行。如果进度在业务完成前已经推进，应检查应用的提交与工作任务顺序。

## 具有状态影响的操作

| 操作 | 影响 |
| --- | --- |
| `updateTopic` / `updateSubGroup` | 创建或修改元数据 |
| 再次运行教程生产者 | 增加消息 |
| 启动消费者 | 注册成员，并可能推进消费进度 |
| 重置偏移量 | 改变组可能重放或跳过的数据 |
| 删除主题或存储目录 | 移除配置或数据 |

前三项是教程中有明确目的的操作。偏移量重置和删除不是自动排障步骤。确认原因与预期恢复动作前，应保留已有数据。

报告问题时，提供使用的源码/发行版、操作系统、所选二进制和 feature、脱敏配置、准确命令以及首个有效错误。区分仅检查过编译，还是实际收发链路失败，不要用“整个集群故障”替代观察到的证据。

来源：[Admin CLI](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-tools/rocketmq-admin/rocketmq-admin-cli/README.md)、[Broker 搭建](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-broker/README.md)、[LitePull 实现](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/consumer/consumer_impl/default_lite_pull_consumer_impl.rs)。
