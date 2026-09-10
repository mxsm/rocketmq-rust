---
title: "部署并验证 Proxy"
---

需要通过 v2 gRPC 暴露现有 NameServer/Broker 集群时选择 Cluster 模式；需要在 Proxy 进程内嵌入 Broker 后端时选择 Local 模式。两者使用相同 gRPC 契约，但后端所有权和资源创建方式不同，详见 [Proxy 架构](../architecture/proxy.md)。

## 构建所选模式

使用仓库工具链，通过 `PATH` 或 `PROTOC` 提供 `protoc`，并包含 `service.proto` 使用的标准 protobuf 导入。在仓库根目录执行：

```bash
cargo build -p rocketmq-proxy --bin rocketmq-proxy-rust --no-default-features --features cluster-mode
```

Local 模式将 `cluster-mode` 替换为 `local-mode`。省略 `--no-default-features` 则构建两种模式。使用内置 TLS 监听器时增加 `tls`；导出器 features 另行选择。选择二进制未包含的模式会产生配置错误。

## Cluster 模式

先启动[单 Broker 集群](../getting-started/local-source.md)。本教程使用其 `DocsCluster`、`docs-broker` 和 NameServer `127.0.0.1:9876`。创建专用普通消息主题和组：

```bash
cargo run -p rocketmq-admin-cli -- topic updateTopic -t DocsProxyMessage -c DocsCluster -r 4 -w 4 -n 127.0.0.1:9876
cargo run -p rocketmq-admin-cli -- consumer updateSubGroup -g docs_proxy_probe -c DocsCluster
```

为 `updateSubGroup` 在 Admin 终端设置 `NAMESRV_ADDR=127.0.0.1:9876`。这些命令修改元数据。使用隔离教程集群；复用运行中的主题/组可能改变其配置。

[集群配置](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-website/examples/proxy/cluster.toml) 将 gRPC 绑定到环回 `8081` 并关闭 Remoting。设置 `ROCKETMQ_HOME` 为仓库根目录、`ROCKETMQ_SECURITY_PROFILE=development-insecure-loopback`，然后运行：

```bash
cargo run -p rocketmq-proxy --bin rocketmq-proxy-rust --no-default-features --features cluster-mode -- -c rocketmq-website/examples/proxy/cluster.toml
```

在同一命令后添加 `--printConfig`，可在不绑定服务时查看启动配置。打印配置不证明路由、认证元数据或 Broker 调用正常。

## 验证 gRPC 路径

安装 [grpcurl](https://github.com/fullstorydev/grpcurl)，在同一主机运行。提供仓库 proto 文件即可避免依赖服务端反射。以下 Bash 命令从标准输入读取已提交 JSON；`-plaintext` 仅用于本环回试验。

```bash
grpcurl -plaintext -import-path rocketmq-proxy-core/proto -proto service.proto -H 'x-mq-client-id: docs-proxy-probe' -d '@' 127.0.0.1:8081 apache.rocketmq.v2.MessagingService/QueryRoute < rocketmq-website/examples/proxy/query-route.json
grpcurl -plaintext -import-path rocketmq-proxy-core/proto -proto service.proto -H 'x-mq-client-id: docs-proxy-probe' -d '@' 127.0.0.1:8081 apache.rocketmq.v2.MessagingService/SendMessage < rocketmq-website/examples/proxy/send.json
grpcurl -plaintext -import-path rocketmq-proxy-core/proto -proto service.proto -H 'x-mq-client-id: docs-proxy-probe' -d '@' 127.0.0.1:8081 apache.rocketmq.v2.MessagingService/PullMessage < rocketmq-website/examples/proxy/pull.json
```

PowerShell 使用管道代替输入重定向，例如：

```powershell
Get-Content -Raw rocketmq-website/examples/proxy/query-route.json | grpcurl -plaintext -import-path rocketmq-proxy-core/proto -proto service.proto -H 'x-mq-client-id: docs-proxy-probe' -d '@' 127.0.0.1:8081 apache.rocketmq.v2.MessagingService/QueryRoute
```

对 `send.json`、`pull.json` 使用相同管道形式并替换方法名。

查询应返回 OK 消息体状态及队列。发送向队列 0 写入一条普通消息，应同时检查整体状态和各结果条目。消息体是 `Hello` 的 protobuf JSON base64 表示。拉取从逻辑偏移量 0 流式读取队列 0 记录；需要检查流状态及消息字段，不能只看传输成功。

这些请求针对单个 `docs-broker` 后端。如果路由返回其他 Broker，应按实际路由更新 `pull.json`。新建主题可以让首条拉取记录适合本次探测。重复发送会复用示例 ID，不会自动去重；每次独立试验应使用新 ID，并通过返回偏移量定位消息。

这里拉取用于检查，不确认 POP receipt，也不提交业务进度。生产消费者应采用分配/订阅、业务处理及对应偏移量或 receipt 完成路径。POP 应接收、处理后再 ACK 当前 receipt，详见 [POP 语义](../consumer/pop.md)。

## Local 模式

停止占用 `8081` 的 Proxy。使用平台目录命令创建 `.rocketmq-proxy-demo/store`，再运行[本地配置](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-website/examples/proxy/local.toml)：

```bash
cargo run -p rocketmq-proxy --bin rocketmq-proxy-rust --no-default-features --features local-mode -- -c rocketmq-website/examples/proxy/local.toml
```

保持相同环回环境 profile。嵌入式后端具有独立 `docs-local-proxy` 身份和存储根目录；`brokerListenPort=10971` 是后端设置，不是外部 gRPC 端口。其构造器关闭 NameServer 注册，不复用独立集群的主题/组元数据。

应用探测前，应通过受支持嵌入式服务集成创建本地后端资源。不要把上述独立 Admin 创建命令发往 NameServer，就认为已在另一个嵌入式存储中创建资源。未修改的集群探测针对 Cluster 模式；Local 模式需要匹配本地主题、组及 Broker 元数据。

## 共享网络部署与关闭

配置入站 TLS、客户端身份/ACL，再单独配置 Proxy 下游签名器及 Broker 权限。入站允许决策不等于下游调用已认证，详见[部署安全](security.md)。

终止前停止生产者/消费者，或将该 Proxy 从应用路由中排空。观察会话、receipt、预备事务、保留响应流及在途调用，再请求正常进程关闭并检查结果。TCP 监听器关闭不证明后端工作或 receipt 续期已经结束。

这些命令和请求基于源码编写，不是已记录的在线 gRPC 试验。应在所选模式中验证实际收发及关闭行为，再将部署记为已运行验证。

## 源码地图

[Proxy 清单](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-proxy/Cargo.toml)、[gRPC 适配器](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-proxy/src/ingress/grpc/adapter.rs)、[协议定义](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-proxy-core/proto/service.proto)、[本地装配](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-proxy-local/src/local.rs)。
