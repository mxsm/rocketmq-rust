# Phase 00 本地联调

Docker Compose 是日常开发入口。PostgreSQL 运行在 Compose 容器中，主机无需安装
PostgreSQL；数据保存于 `postgres-data` volume。超过内联上限的 Evidence 内容保存
在 `evidence-objects` volume，该 volume 只挂载给 Control Plane；普通的
Control Plane 或整栈重启不会让数据库中的 Evidence URI 失效。

## 端口

| 服务 | 端口 |
| --- | ---: |
| AI SRE UI | 3004 |
| Control Plane | 8090 |
| Control Plane Connector mTLS | 8444（仅 127.0.0.1） |
| Control Plane Connector upstream | 8093（仅共享网络命名空间内的 loopback，不发布） |
| Connector（经 MCP 网络命名空间） | 8091 |
| MCP Streamable HTTP | 8089 |
| NameServer | 9876 |
| Broker | 10911/10912 |
| Proxy | 8080/8081 |
| PostgreSQL | 5432 |
| Prometheus | 9090 |
| Loki | 3100 |
| Tempo | 3200 |
| OTLP gRPC/HTTP | 4317/4318 |

## 启动

从仓库根执行：

```powershell
.\rocketmq-sre\scripts\dev.ps1 -Action Up
```

脚本会先生成仅用于本地的 TLS fixture、独立的 Control Plane 服务端证书、
Connector 客户端证书、secure-bootstrap identity 和只读 request policy，再执行
Compose 配置校验和启动。Broker、NameServer、Controller、Proxy
与 MCP 均以 `secure-enforced` 启动；容器只挂载运行时所需文件，CA 私钥保留在主机
fixture 目录且不会挂载。观测组件通过 `observability` profile 一并启动。不要把
`target/phase00-certs` 中的私钥复制到生产镜像或配置库。

RocketMQ ACL 使用三套互不复用的开发身份：MCP reader 只有
Topic/Group/Cluster `GET` 权限；Probe 只能对固定 `SRE_PROBE_` Topic 和 Group
执行有界 PUB/SUB；bootstrap admin 只注入一次性 Topic 创建容器。MCP 不挂载
Broker ACL 或 bootstrap secret，Probe 也无法读取 reader/bootstrap 凭据。

Control Plane 的 onboarding 使用经过鉴权的租户、集群和主体上下文，并要求
`rocketmq:onboard` 角色；开发模式由 Compose 显式传入固定的 tenant、cluster 和
subject header。Connector 发起的 handshake 与 offboard 仍要求内部 Bearer
身份；Compose 通过 `ROCKETMQ_SRE_INTERNAL_TOKEN` 注入仅用于本地的 fixture。
Connector 到 Control Plane 的主动反向通道只使用
`https://sre-control-plane:8444`。该端口由非 root、只读文件系统的 Nginx
代理终止 mTLS，只接受专用 Connector CA 签发且主体为
`CN=rocketmq-sre-connector` 的证书。代理会覆盖客户端自填的 subject/issuer
Header，再把证书 DN 转发给仅在与 Control Plane 共享网络命名空间的
`127.0.0.1:8093` 上监听的 Connector-only
Axum listener；公开 `8090` 不挂载任何 Connector internal 路径。请求体限制为
640 KiB、禁止重定向且只开放 Connector GET/POST 路径。MCP capability handshake
上报和 cluster-state 检查同样走该私有 surface。Bearer token 作为第二层校验，
不能替代客户端证书。
Connector 还会主动查询 MCP Runtime/Observability System Resource，并验证
Prometheus、Loki、Tempo 端点；已配置的必需数据源不可用时，集群保持
`read_only_degraded`，不会匿名降级。

仅补生成 Connector 通道证书而不轮换 RocketMQ/MCP 现有开发证书：

```powershell
.\rocketmq-sre\scripts\dev.ps1 -Action ChannelCerts
```

静态检查 Compose、Kind、代理策略和已生成证书：

```powershell
.\rocketmq-sre\scripts\verify-mtls-deployment.ps1 -CheckCertificates
```

## Smoke

```powershell
.\rocketmq-sre\scripts\phase00-smoke.ps1 -Target Compose
```

Smoke 使用固定前缀 `SRE_PROBE_`/`SRE_PROBE_G_` 的预创建 Topic 与 Group，限制消息
数量、大小与运行时长。流程会断言 Lag 大于零并在恢复消费后下降，真实查询
Prometheus/Loki/Tempo，校验 Connector 已读取版本化 System Resource，重启
PostgreSQL 与 Control Plane 后验证接入状态仍存在，再验证 Collector 中断恢复、
token rotation 和 offboard。开发 issuer 会在两把不同的 RSA fixture 之间轮换；
Smoke 同时校验 `kid` 与公钥模数均已变化、旧 token 被拒绝以及 Connector 单次恢复。
这两把固定密钥只用于本地联调，已从 Docker build context 排除，也不会进入最终镜像。
Offboard 验收要求固定的 `403/cluster_not_allowed`，并进一步确认 Connector
转为 not-ready、清空能力缓存且 PostgreSQL 中的 Connector identity 已撤销。

Offboard 是保留历史的终态。成功运行一次 smoke 后，如需从头重复完整流程，应先
执行 `dev.ps1 -Action Reset -Force` 清除本地测试 volume，再重新 `Up`；系统不会
为了测试方便而自动复活已下线的集群。

## 清理

```powershell
.\rocketmq-sre\scripts\dev.ps1 -Action Down
```

默认保留 PostgreSQL、Evidence 对象和观测数据 volume。若明确需要删除本地
Phase 00 数据和生成的证书 fixture：

```powershell
.\rocketmq-sre\scripts\dev.ps1 -Action Reset -Force
```

该命令会删除 `rocketmq-rust-ai-sre-phase00` 项目的本地 volume，无法恢复。
这同时删除 `postgres-data` 与 `evidence-objects`；二者必须一起保留或一起重置，
否则 PostgreSQL 中的大 Evidence 引用将无法解析。

## 常见错误

- `/readyz` 返回 503：先检查 PostgreSQL health 与数据库 migration 日志。
- Connector 进入 degraded：检查 tenant、audience、scope、cluster allowlist、
  protocol/schema digest 和 `mutation_supported`。
- Connector 通道无法注册：确认 URL 为 HTTPS、server CA 与 combined client
  identity 已挂载，并检查 `sre-control-plane-mtls` 日志；禁止改回普通 HTTP。
- MCP TLS 失败：重新运行 `dev.ps1 -Action Certs`，不要禁用证书校验。
- 没有观测数据：确认 Compose 使用了 `--profile observability`，并检查 Collector；
  Collector 不可用不得阻塞 RocketMQ 或 MCP 数据面。

## Phase 01 fixture-only Shadow

The optional Phase 01 overlay runs all 24 Wave A normal/fault/missing cases in
a one-shot container. It has `network_mode: none`, a read-only root filesystem,
no Linux capabilities, no Executor, and no cluster mutation surface.

```powershell
.\scripts\phase01-shadow.ps1 -Target Compose -Provider Mock
.\scripts\phase01-shadow.ps1 -Target Compose -Provider RulesOnly
.\scripts\phase01-shadow.ps1 -Target Compose -Provider Outage
```

The overlay is `compose.phase1-shadow.yaml`. It can be used without starting
the Phase 00 services because all inputs are committed, redacted Evidence
fixtures.

## Phase 01 live read-only smoke

Run this before the Phase 00 smoke offboards the fixed development cluster.
The script uses the real Compose Connector/MCP/RocketMQ path, waits for
Connector-generated inventory, creates a Conversation/Investigation/Incident,
runs bounded diagnosis and inspections, downloads Markdown and HTML reports,
and checks knowledge, coverage, audit, message-body exclusion, cross-cluster
denial, and persisted model-provider lineage. The Compose stack starts a
separate `sre-model-mock` process only on the internal Control Plane backend
network. It has no credentials, tools, RocketMQ access, host port, or outbound
client and returns a bounded OpenAI-compatible result citing an Evidence ID
from the request. This fixture proves `Evidence -> Model -> Citation` without
external network access; provider outage still degrades to `rules_only`.

```powershell
.\scripts\phase01-smoke.ps1 -Target Compose -BootstrapProbe
```

The script does not reset data or mutate arbitrary RocketMQ resources. Its only
RocketMQ write is the Phase 00 bounded probe on the dedicated `SRE_PROBE_`
Topic and Group. If the fixed cluster has already been offboarded, explicitly
reset the disposable development volumes with `dev.ps1 -Action Reset -Force`,
start the stack again, and rerun the smoke.
