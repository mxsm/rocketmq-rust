---
title: "AI SRE 部署与开发"
---

使用仓库 Docker Compose 开发环境启动协作服务。它包含 PostgreSQL、私有 Evidence 存储、RocketMQ 服务、查询 MCP、Connector、Control Plane、Executor、Execution Agent、UI 及观测组件。这是可丢弃开发集群，不是现有业务集群的配置。启用单个执行动作前，阅读 [AI SRE 架构](./ai-sre.md)。

## 准备环境与端口

安装带 Compose 的 Docker、Git 和 PowerShell；本地源码开发需要宿主 Rust 1.95.0 与 Node/npm。PostgreSQL 在 Docker 中运行，无需宿主安装。确认 Docker 能构建仓库 Linux 容器镜像，并且预期开发端口可用。

| 服务 | 开发访问 | 含义 |
| --- | --- | --- |
| AI SRE UI | `http://localhost:3004` | 独立浏览器工作台 |
| Control Plane | `http://localhost:8090` | 公共版本化 API；`/healthz` 为存活，`/readyz` 为数据库就绪 |
| 查询 MCP | `https://localhost:8089` | Connector 使用的 TLS MCP 端点 |
| Connector | 8091 | 开发集成服务 |
| Executor / Agent | 8094 / 8095 | 独立内部执行服务；健康不等于执行权限 |
| Connector 反向通道 | 8444，仅发布到环回地址 | 到 Connector 专属监听器的强制 mTLS 代理 |
| 内部 Connector 上游 | 8093，不发布 | 共享 Control Plane 网络命名空间中的环回地址 |
| NameServer / Broker | 9876 / 10911 和 10912 | 专用开发 RocketMQ 服务 |
| Proxy | 8080 / 8081 | 开发 Proxy 接口 |
| PostgreSQL | 5432 | 持久化 SRE 元数据 |
| Prometheus / Loki / Tempo | 9090 / 3100 / 3200 | 指标、日志和追踪来源 |
| OTLP | 4317 / 4318 | gRPC / HTTP 遥测接收 |

MCP Control 示例监听同样使用 8090，教程 NameServer/Broker 也可能已占用对应端口。不要同时启动冲突栈或终止无关服务。选择隔离环境，或一致调整部署发布端口及引用。

## 启动并检查栈

从仓库根目录运行：

```powershell
.\rocketmq-ai\rocketmq-sre\scripts\dev.ps1 -Action Up
.\rocketmq-ai\rocketmq-sre\scripts\dev.ps1 -Action Status
```

`Up` 准备缺少的本地证书/身份材料，检查 Compose 配置，并使用 `observability` profile 构建启动、等待依赖。证书与开发身份文件生成到仓库 `target/phase00-certs`。仅挂载所需运行文件，CA 私钥留在宿主。不要将这些 fixture 复制到生产环境。

开发栈分别使用 MCP reader、Agent reader、Probe、引导管理员和 Agent mutation 身份。Probe 流量限制在专用 `SRE_PROBE_` 主题/组。Compose 显式启用窄化 Broker/主题配置处理器，这是覆盖 Agent 默认关闭开关，不是通用写接口。Executor 仍不拥有目标凭据或目标网络。

一次性接入服务在租户 `00000000-0000-4000-8000-000000000002` 中创建开发集群 `00000000-0000-4000-8000-000000000001`，逻辑 MCP 别名为 `sre-dev`。Connector 依赖接入完成，再进行认证能力握手。数据库就绪本身不表示握手及全部证据来源都已就绪。

无需凭据即可检查最小公共健康信息：

```powershell
Invoke-RestMethod 'http://127.0.0.1:8090/healthz'
Invoke-RestMethod 'http://127.0.0.1:8090/readyz'
```

打开 `http://localhost:3004`。开发 UI 显式配置 fixture 身份，不使用 Dashboard 登录。开发 OAuth issuer 支持 Connector 到 MCP 的 client credentials，不支持浏览器 OIDC/PKCE。生产 UI 不能将其作为 OIDC authority。

## 完成一条诊断路径

1. 打开 `/clusters`，选择已引导开发集群。确认状态 `ready_read_only` 及能力/来源详情。若为 `read_only_degraded`，先检查不可用必需来源，再继续解释结果。
2. 打开 `/coverage` 和 `/topology`，检查实际观察内容。缺失拓扑或部分来源覆盖必须保持可见，不能用标签推断路径代替 RocketMQ 证据。
3. 打开 `/ask`，提交有界读取问题，例如：“当前集群哪些消费者组存在积压？结论对应哪些证据？”选择预期集群上下文。这是诊断请求，不是重置偏移量。
4. 检查持久化对话/答案、证据引用、观察时间、部分结果警告及模型/仅规则状态。默认 Compose 模型是名为 `phase01-read-only-fixture` 的本地 fixture，其答案不构成生产模型验证。
5. 工作流产生相应运维记录后，通过 `/incidents` 或 `/inspections` 跟进。缺少记录或证据是需要调查的结果，不是编造 Incident 或可执行方案的理由。

此步骤描述 UI/API 流程，本次文档任务未在真实运行栈中执行。现有 `phase00-smoke.ps1 -Target Compose` 是更广的可选集成场景，会发送有界合成消息，检查数据源与重启，轮换 fixture 身份，最后将集群下线。它改变测试状态，不是只读健康检查。下线保留历史且为终态；重复完整 smoke 需要按本地运行手册有意重置。

## 每次开发一个部分

Rust 服务属于独立 `rocketmq-ai/rocketmq-sre/` 工作区。在该目录选择所需包：

```powershell
cargo build --locked -p rocketmq-sre-control-plane -p rocketmq-sre-connector
cargo run --locked -p rocketmq-sre-cli --bin rocketmq-sre -- --url http://127.0.0.1:8090 status
```

构建服务不会提供数据库、对象存储、身份或依赖配置。开发 UI 时保留 Compose 管理的后端，除非有意替换服务。在新 Shell 中从仓库根目录开始：

```powershell
cd rocketmq-ai/rocketmq-sre/ui
npm ci
$env:ROCKETMQ_SRE_API_URL = 'http://127.0.0.1:8090'
npm run dev -- --host 127.0.0.1 --port 3005
```

端口 3005 避开 Compose UI 发布的 3004。Vite 将 `/v1`、`/healthz` 和 `/readyz` 代理到 `ROCKETMQ_SRE_API_URL`，默认 `http://127.0.0.1:8090`。独立宿主 UI 必须按 Compose UI 构建参数显式配置匹配的开发身份，不会继承运行中容器的环境。测试生产身份路径时使用生产 OIDC 配置，绝不在 `VITE_` 变量放真实秘密。`npm run build` 生成 `ui/dist/`，不构建 Rust 服务。

TypeScript SDK 是 `sdk/typescript/` 下的私有源码包 `@rocketmq-rust/sre-client`，不宣称已发布到 npm。它导出 `SreClient`，提供固定有界 GET 操作；Bearer 令牌来自值或 provider 回调，不得随重定向发送到其他 Origin。在该目录使用现有 `npm run build` / `npm test` 脚本构建/测试。没有通用请求、审批、执行或目标变更 API。

Rust CLI 二进制为 `rocketmq-sre`。选项位于命令前，`--url` 覆盖 `ROCKETMQ_SRE_URL`。受保护读取使用 `ROCKETMQ_SRE_TOKEN` 或 `--token-env` 指定的环境变量，不接受 `--token` 参数。本地 `draft-plan` 和 `draft-runbook` 校验有界文件，不访问网络，也无执行权限。参见 [CLI 指南](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-ai/rocketmq-sre/crates/rocketmq-sre-cli/README.md)和 [SDK 源码](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-ai/rocketmq-sre/sdk/typescript/src/index.ts)。

## 生产配置边界

| 边界 | 生产要求 |
| --- | --- |
| UI 身份 | OIDC；构建时提供公开 `VITE_SRE_OIDC_AUTHORITY` 和 `VITE_SRE_OIDC_CLIENT_ID`。缺少必需 OIDC 配置时关闭访问。开发 fixture 令牌不是生产身份。 |
| Control Plane 持久化 | 配置 PostgreSQL 并应用前向迁移；共同保留元数据与私有 Evidence 对象。`/readyz` 要求数据库初始化成功。 |
| Evidence 对象 | 超过默认 64 KiB 内联上限时，配置 HTTPS S3 兼容端点、bucket 和独立 access/secret 凭据。本地文件存储要求显式开发认证，不提供运行时内存回退。 |
| Connector 到 MCP | TLS、OAuth client credentials 和匹配的只读能力/schema/租户/集群协商；无匿名回退。 |
| Connector 反向通道 | 强制 mTLS 代理后的专用客户端证书身份及独立 Bearer 校验；不暴露 8093，不接受客户端自填的转发身份头。 |
| 模型提供方 | 显式启用、能力 profile 与秘密引用。默认网络调用关闭；生产秘密提供方与开发环境/文件解析分开配置。 |
| Executor / Agent | 内部监听器位于强制工作负载 mTLS 代理后，并使用独立 Bearer 令牌。Axum 监听器本身不终止 TLS。限制直连，并由可信代理替换入站身份头。 |
| 目标变更 | 仅启用已配置类型化 Agent 动作，提供独立读取/变更身份、目标允许列表、PostgreSQL 效果状态、租约/隔离权限及验证依赖。 |

Control Plane 生产对象存储使用 `ROCKETMQ_SRE_OBJECT_STORE_ENDPOINT`、`ROCKETMQ_SRE_OBJECT_STORE_BUCKET`、`ROCKETMQ_SRE_OBJECT_STORE_ACCESS_KEY` 和 `ROCKETMQ_SRE_OBJECT_STORE_SECRET_KEY`。模型 profile 使用引用型凭据和提供方特定校验；启用真实提供方前，参阅 [Control Plane 配置指南](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-ai/rocketmq-sre/crates/rocketmq-sre-control-plane/README.md)。容器内提供方端点不能直接使用宿主环回地址，除非已明确建立对应网络安排。

## 停止、保留状态与排查

在仓库根目录停止栈并保留 volume：

```powershell
.\rocketmq-ai\rocketmq-sre\scripts\dev.ps1 -Action Down
```

`Down` 保留 PostgreSQL、Evidence 对象和观测 volume。独立 `Reset -Force` 动作删除开发 volume 和生成证书 fixture，仅用于有意重置可丢弃环境，不作为通用重启命令。不要通过复制 fixture volume 迁移生产数据。

| 现象 | 首先检查 |
| --- | --- |
| 启动端口冲突 | 已有教程、Dashboard、MCP Control 或其他开发栈是否占用 |
| UI 正常，API 不可用 | 反向代理/Vite 目标、Control Plane 就绪及浏览器身份模式 |
| 集群持续降级 | MCP 握手、必需 Prometheus/Loki/Tempo 来源、Connector mTLS 和允许列表 |
| 模型不可用或仅规则结果 | 模型启用、实际 fixture/提供方 profile、能力/秘密策略及稳定错误 |
| 重启后 Evidence 引用无法读取 | PostgreSQL/对象存储 volume 配对、存储配置与授权 |
| 执行动作未注册 | Agent 单项开关及必需驱动配置；不要扩大查询 MCP 范围 |
| 完整 smoke 无法重复接入 | 先前下线墓碑；需要重复时使用有意重置的测试环境 |

来源：[开发运行手册](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-ai/rocketmq-sre/deploy/dev/README.md)、[Compose](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-ai/rocketmq-sre/deploy/dev/compose.yaml)、[开发脚本](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-ai/rocketmq-sre/scripts/dev.ps1)、[UI 身份指南](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-ai/rocketmq-sre/ui/README.md)和 [UI 路由](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-ai/rocketmq-sre/ui/src/App.tsx)。
