# Phase 00 本地联调

Docker Compose 是日常开发入口。PostgreSQL 运行在 Compose 容器中，主机无需安装
PostgreSQL；数据保存于 `postgres-data` volume。

## 端口

| 服务 | 端口 |
| --- | ---: |
| AI SRE UI | 3004 |
| Control Plane | 8090 |
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

脚本会先生成仅用于本地的 TLS fixture、secure-bootstrap identity 和只读 request
policy，再执行 Compose 配置校验和启动。Broker、NameServer、Controller、Proxy
与 MCP 均以 `secure-enforced` 启动；容器只挂载运行时所需文件，CA 私钥保留在主机
fixture 目录且不会挂载。观测组件通过 `observability` profile 一并启动。不要把
`target/phase00-certs` 中的私钥复制到生产镜像或配置库。

RocketMQ ACL 使用三套互不复用的开发身份：MCP reader 只有
Topic/Group/Cluster `GET` 权限；Probe 只能对固定 `SRE_PROBE_` Topic 和 Group
执行有界 PUB/SUB；bootstrap admin 只注入一次性 Topic 创建容器。MCP 不挂载
Broker ACL 或 bootstrap secret，Probe 也无法读取 reader/bootstrap 凭据。

Control Plane 的 onboarding、handshake 和 offboard POST 接口要求内部 Bearer
身份；Compose 通过 `ROCKETMQ_SRE_INTERNAL_TOKEN` 注入仅用于本地的 fixture。
Connector 还会主动查询 MCP Runtime/Observability System Resource，并验证
Prometheus、Loki、Tempo 端点；已配置的必需数据源不可用时，集群保持
`read_only_degraded`，不会匿名降级。

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

默认保留 PostgreSQL 和观测数据 volume。若明确需要删除本地 Phase 00 数据和
生成的证书 fixture：

```powershell
.\rocketmq-sre\scripts\dev.ps1 -Action Reset -Force
```

该命令会删除 `rocketmq-rust-ai-sre-phase00` 项目的本地 volume，无法恢复。

## 常见错误

- `/readyz` 返回 503：先检查 PostgreSQL health 与数据库 migration 日志。
- Connector 进入 degraded：检查 tenant、audience、scope、cluster allowlist、
  protocol/schema digest 和 `mutation_supported`。
- MCP TLS 失败：重新运行 `dev.ps1 -Action Certs`，不要禁用证书校验。
- 没有观测数据：确认 Compose 使用了 `--profile observability`，并检查 Collector；
  Collector 不可用不得阻塞 RocketMQ 或 MCP 数据面。
