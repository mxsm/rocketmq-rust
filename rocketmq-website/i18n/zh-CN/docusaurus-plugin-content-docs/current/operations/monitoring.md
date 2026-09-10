---
title: "监控服务与消息进度"
---

围绕应用完成消息处理的路径，以及支撑它的服务建立监控。进程健康时，仍可能出现磁盘满、同步副本不足、下游请求被拒绝，或消费者保持连接却没有业务进展。应结合[管理查询](admin.md)、日志、指标与追踪观察。

## 选择导出路径

信号启用需要两个独立条件：二进制已编译对应 Cargo feature，运行配置选择了活跃 exporter。服务的便捷 feature 不一定包含全部信号或 exporter。Controller 没有 `observability` 便捷 feature，Proxy 的该 feature 仅覆盖指标。各服务的 feature 表与完整参数见[可观测性配置](../configuration/observability.md)。

| 路径 | 用途与限制 |
| --- | --- |
| 本地控制台/文件日志 | 查看启动、错误、生命周期与受控诊断；独立于 OTLP 日志导出 |
| OTLP gRPC 到 Collector | 集中处理服务已编译且启用的指标、追踪、日志；逐条验证 pipeline |
| 直接 Prometheus | 使用支持该 exporter 的服务构建，目前为 Broker 或 Controller；显式控制指标监听入口 |
| Log exporter | 本地查看选定遥测，不等同于持久化的远端监控系统 |

选择未编译的 exporter 会返回类型化的 feature-disabled 错误。关闭的信号可使用空操作句柄，因此源码存在指标名称不能证明实际导出了样本。

## 验证本地 Broker 到 Collector 的路径

复用[源码启动](../getting-started/local-source.md)中的 Broker、NameServer 环境。替换 Broker 进程前，先停止旧进程。构建所需信号：

```bash
cargo build -p rocketmq-broker --bin rocketmq-broker-rust --features otlp-metrics,otlp-traces,otlp-logs
```

把 Broker TOML 复制到本地部署目录，保留已有 `broker`、`store` 配置，并添加以下**根层级** section：

```toml
[observability]
environment = "development"
serviceInstanceId = "docs-broker"

[observability.metrics]
exporter = "otlp_grpc"

[observability.traces]
exporter = "otlp_grpc"
sampleRatio = 1.0
recordMessageId = false
recordMessageKeys = false

[observability.logs]
exporter = "otlp_grpc"

[observability.otlp]
endpoint = "http://127.0.0.1:4317"
protocol = "grpc"
headers = {}
```

全量追踪采样只用于小规模本地验证，生产环境应选择合适比例。文件已有 `[observability]` 时合并字段，不要重复定义同名 section。

仓库提供 `distribution/config/otel-collector-observability.yaml` 和 `prometheus-observability.yaml`。安装兼容的 Collector、Prometheus 二进制后，从仓库根目录在不同终端运行：

```bash
otelcol-contrib --config distribution/config/otel-collector-observability.yaml
prometheus --config.file=distribution/config/prometheus-observability.yaml
```

Collector 示例在 `4317` 接收 OTLP gRPC，在 `9464` 导出供 Prometheus 抓取的指标。追踪、日志只进入 debug exporter，没有配置追踪/日志存储后端或仪表盘。示例还在 `4318` 暴露 HTTP receiver，但本文 RocketMQ Rust 的活跃 OTLP 路径使用 gRPC。该测试配置监听所有网络接口，应限制在预期测试环境与访问策略内。

使用复制的配置重启 Broker，保留本地教程的运行环境与安全 profile。运行[第一条消息应用](../getting-started/quick-start.md)，检查 Collector 的服务观察结果，以及 Prometheus 中 `otel-collector` 抓取目标是否成功。编写查询前确认实际序列与单位，exporter 转换可能改变名称和后缀。

这里给出搭建流程，未声明已经运行 Collector。[配置指南](../configuration/observability.md)仍用于查询优先级和 exporter 参数。

## 解释遥测缺失或与预期不符

1. 确認当前服务二进制包含所需信号与 exporter feature。
2. 查看有效文件配置以及**确实存在**的环境覆盖。缺失的环境变量不会覆盖文件值。
3. 检查 `OTEL_EXPORTER_OTLP_ENDPOINT`：非空值会为指标、追踪、日志选择 OTLP，并要求 `OTEL_EXPORTER_OTLP_PROTOCOL=grpc`。因此，仅在文件中请求指标的进程，可能因继承标准 endpoint 变量而出现不同表现。
4. 检查进程所在网络命名空间到 endpoint 的可达性、Collector receiver/pipeline、导出错误及抓取健康。容器内的 `127.0.0.1` 指向该容器所在网络命名空间。
5. 产生少量已知工作负载，比较时间戳、服务身份与实例身份。没有活动的 instrument 或关闭的采样可能解释部分观察缺失。

现有配置指南中的 `global.observability` 全局选择器属于另一个五服务集成 chart：`distribution/helm/rocketmq-rust`。不能把它们复制进 `rocketmq-rust-core` 并假定模板会渲染这些字段。应检查所选 chart 的实际 values、模板，或配置自定义工作负载的服务文件。

## 跨层观察症状

| 症状 | 对照内容 | 处理方向 |
| --- | --- | --- |
| 生产延迟上升 | 客户端截止时间/重试、Broker 追加/刷盘延迟、HA ACK/复制落后、磁盘服务时间 | 先定位完成链路中变慢的阶段，再决定是否增加超时 |
| 消费积压增长 | 各队列流入与提交推进、处理延迟、重试/ACK 错误、队列分配 | 找出热点队列或下游瓶颈，评估追平能力 |
| HA 写入等待或失败 | 当前写权限、同步集合、持久化复制进度、peer/网络/磁盘健康 | 恢复缺失条件，不能凭连接数推断仲裁满足 |
| Proxy 传输成功但应用失败 | gRPC 传输状态、响应内状态、下游延迟、接纳/会话状态 | 检查响应载荷以及负责该操作的一跳 |
| 服务从路由中消失 | Broker 注册、各 NameServer 观察、网络与进程生命周期 | 查询各 NameServer 和实际 Broker，不先重启所有节点 |
| 内存或磁盘压力增长 | 常驻内存、保留的请求/流字节数、存储/日志增长、文件系统容量、操作系统压力 | 限制工作量、恢复余量，同时保持所需持久性 |

依据规范指标定义和实际导出序列建立仪表盘。存储 recorder 区分追加、刷盘、分发、传输和 HA 观察；Proxy 区分响应载荷失败与传输失败。计数器、仪表值、字节数、队列偏移量、延迟单位不能混作同一数量。

## 告警与诊断数据

按工作负载定义告警窗口，例如持续积压增长、磁盘余量下降、复制条件不足以写入、导出失败或完成错误率升高。告警应包含受影响的服务/组/队列范围与第一步诊断操作。遥测缺失需要排查采集健康，不能解释为零流量或零错误。

保持标签有界。主题、消费者组标签需要基数限制；消息 ID、偏移量、请求 ID、事务 ID 不应进入指标标签。日志中不包含消息正文、凭据、ACL/TLS 材料或完整请求/配置对象，优先记录公开稳定错误码与受控诊断字段。

服务提供 `--log-filter` 时使用该选项，并限定相关模块。不能假定嵌入式应用会自动读取 `RUST_LOG`，也不能假定可以安装第二个进程级 tracing subscriber。详见[错误与可观测性设计](../architecture/errors-observability.md)。

关闭时先停止应用工作并排空服务，再刷新和关闭所拥有的遥测，最后退出运行时。检查关闭报告：丢弃 telemetry guard 或关闭网络端口不能证明数据已成功导出。

## 源码索引

[可观测性所有权与 exporter](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-observability/README.md)、[指标语义](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-observability/src/semantic.rs)、[存储 recorder](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-observability/src/metrics/store.rs)、[Proxy 观察](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-observability/src/metrics/proxy.rs)、[Collector 示例](https://github.com/mxsm/rocketmq-rust/blob/main/distribution/config/otel-collector-observability.yaml)。
