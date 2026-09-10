---
title: "构建并运行本地服务容器"
---

本教程将本地构建的 Linux 二进制封装到仓库的社区运行时镜像。采用 Linux host 网络运行一个 NameServer 和一个 Broker，所有监听器只绑定环回地址。这是开发拓扑，不包含副本故障切换或公开监听器。

使用具备 Rust、仓库构建前提及 Docker 的 Linux 主机或 Linux 虚拟机。客户端也在同一台 Linux 主机运行。Windows 原生二进制不能在 Linux 镜像中执行，本教程也不假设 Docker Desktop 的 host 网络行为。Windows 原生体验请使用[源码本地部署](../getting-started/local-source.md)。

## 选择镜像路径

| 资产 | 行为 |
| --- | --- |
| `distribution/container/core-service.Dockerfile` | 将指定二进制和配置复制进 distroless 运行时，不编译 Rust |
| `docker/Dockerfile.base` | 包含服务及独立测试驱动的多阶段构建/运行目标 |
| Core Helm chart 镜像 values | 需要填入实际可用制品的引用；默认标签不证明远端仓库可用 |

以下命令使用第一条路径。二进制 CPU 架构、动态库要求必须与所选 distroless 运行时匹配。主机编译成功本身不能证明容器可以启动。

## 构建二进制及最小上下文

在 Linux 的仓库根目录执行：

```bash
cargo build --release -p rocketmq-namesrv --bin rocketmq-namesrv-rust
cargo build --release -p rocketmq-broker --bin rocketmq-broker-rust
mkdir -p .rocketmq-container-demo/context
cp distribution/container/core-service.Dockerfile .rocketmq-container-demo/context/Dockerfile
cp LICENSE-APACHE NOTICE .rocketmq-container-demo/context/
cp target/release/rocketmq-namesrv-rust target/release/rocketmq-broker-rust .rocketmq-container-demo/context/
cp rocketmq-website/examples/first-message/namesrv.toml .rocketmq-container-demo/context/
cp rocketmq-website/examples/first-message/broker.toml .rocketmq-container-demo/context/
docker build -f .rocketmq-container-demo/context/Dockerfile \
  --build-arg SERVICE=namesrv --build-arg BINARY=rocketmq-namesrv-rust \
  --build-arg CONFIG=namesrv.toml -t rocketmq-docs/namesrv:local .rocketmq-container-demo/context
docker build -f .rocketmq-container-demo/context/Dockerfile \
  --build-arg SERVICE=broker --build-arg BINARY=rocketmq-broker-rust \
  --build-arg CONFIG=broker.toml -t rocketmq-docs/broker:local .rocketmq-container-demo/context
```

这里有意使用独立构建上下文：仓库根 `.dockerignore` 排除了 `target/`。两个镜像都以 UID/GID `10001:10001` 运行指定程序，默认配置在 `/etc/rocketmq/service.toml`，工作目录为 `/var/lib/rocketmq`。镜像不包含用于交互修复命令的 shell。

## 准备持久化目录

复制的 first-message 配置使用 `.rocketmq-doc-demo` 下的相对路径。它们相对于容器工作目录解析，不是主机源码目录。为每个服务分配独立主机挂载：

```bash
mkdir -p .rocketmq-container-demo/data/namesrv/.rocketmq-doc-demo/namesrv
mkdir -p .rocketmq-container-demo/data/broker/.rocketmq-doc-demo/broker
mkdir -p .rocketmq-container-demo/data/broker/.rocketmq-doc-demo/store
sudo chown -R 10001:10001 .rocketmq-container-demo/data
```

只修改本次新建教程目录的所有权。在 rootless Docker 或启用强制访问控制的主机上，应安排等价映射 UID 权限和挂载标签。不要通过让既有生产存储对所有用户可写来解决权限错误。

## 启动与观察

确保主机 `9876`、`10911`、`10909`、`10912` 端口空闲。先启动 NameServer：

```bash
docker run -d --name rocketmq-docs-namesrv --network host \
  --read-only --tmpfs /tmp:rw,nosuid,noexec,size=64m \
  -e ROCKETMQ_HOME=/var/lib/rocketmq \
  -e ROCKETMQ_SECURITY_PROFILE=development-insecure-loopback \
  --mount type=bind,src="$(pwd)/.rocketmq-container-demo/data/namesrv",dst=/var/lib/rocketmq \
  rocketmq-docs/namesrv:local
docker logs rocketmq-docs-namesrv
```

NameServer 报告成功启动后：

```bash
docker run -d --name rocketmq-docs-broker --network host \
  --read-only --tmpfs /tmp:rw,nosuid,noexec,size=64m \
  -e ROCKETMQ_HOME=/var/lib/rocketmq \
  -e ROCKETMQ_SECURITY_PROFILE=development-insecure-loopback \
  --mount type=bind,src="$(pwd)/.rocketmq-container-demo/data/broker",dst=/var/lib/rocketmq \
  rocketmq-docs/broker:local
docker logs rocketmq-docs-broker
```

两个配置都绑定、通告环回地址。Linux host 网络使该地址与主机客户端共享，因此不使用 `-p` 映射。不要直接把命令改成 bridge 网络：此时环回地址会分别指向各自容器。

需要覆盖配置时，在创建容器时把自己的文件只读挂载到 `/etc/rocketmq/service.toml`。可变数据保存在数据挂载。镜像另有 `/var/log/rocketmq` 卷，可供明确配置到该路径的文件日志使用；上述命令依照所选配置的日志行为，通过 `docker logs` 观察。

## 验证消息链路

在该 Linux 主机上运行 Admin CLI 和[第一批消息教程](../getting-started/quick-start.md)，使用 `127.0.0.1:9876`、集群 `DocsCluster`、Broker `docs-broker`、主题 `DocsFirstMessage`。创建主题及消费者组，发送五条消息、消费，再比较消费进度。

`docker ps` 只说明容器状态。日志说明启动观察。路由查询和实际收发提供另外的独立证据。本文容器命令尚未在当前 Windows 文档编写环境中作为 Docker 部署执行。

## 停止但保留数据

```bash
docker stop --time 60 rocketmq-docs-broker
docker stop --time 60 rocketmq-docs-namesrv
docker rm rocketmq-docs-broker rocketmq-docs-namesrv
```

移除容器前检查关闭输出。如果超时导致强制终止，应记录为关闭不完整。绑定挂载的数据仍位于 `.rocketmq-container-demo/data`，供后续重启使用。删除数据是另行明确执行的操作，不属于本教程。

其他主机或共享网络部署应结合[多节点部署](multi-node.md)及[部署安全](security.md)规划通告端点和安全配置。

## 源码资产

[运行时 Dockerfile](https://github.com/mxsm/rocketmq-rust/blob/main/distribution/container/core-service.Dockerfile)、[多阶段 Dockerfile](https://github.com/mxsm/rocketmq-rust/blob/main/docker/Dockerfile.base)、[first-message 配置](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-website/examples/first-message)。
