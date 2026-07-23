# M11-10 Probe、PreStop 与统一 Drain 证据

## 1. 结论与目标完成边界

PR-M11-10 将 Broker、NameServer、Controller、Proxy、MCP 接入同一个进程生命周期合同，并把该合同接到
canonical Helm 与确定性 Kustomize base。readiness 不再由业务 TCP 端口是否打开推断；preStop、SIGINT、
SIGTERM 与内部失败汇合到第一次 shutdown request 冻结的绝对 deadline，服务阶段、telemetry 和 RuntimeOwner
都只消费剩余预算。不健康 shutdown report 会进入 `Failed` 并返回失败，不再只告警后以成功退出。

工作包完成后总进度为 **74/82**，剩余 8 个工作包：M11 2 个、M12 6 个；下一工作包为 PR-M11-11。
这里的“完成”表示状态机、五服务接线、probe/preStop、deadline、静态/单元/集成门禁和回滚边界已交付；不表示
容器动态套件、生产签名 digest、目标集群 Controller Service IP、Kind/K3d fault matrix、已确认消息的集群恢复、
M10 真实硬件或任何 HUMAN/Phase Gate 已验收。该段是历史状态；容器动态套件已由后续 R20 关闭，production
签名 digest、R21 集群 fault/rolling 和 R25 签署仍未完成。

| 项目 | 值 |
|---|---|
| Milestone | `M11` |
| 工作包 | `PR-M11-10` |
| GitHub Issue | `#8288` |
| Branch | `mxsm/architecture-refactor-probe-drain` |
| Lifecycle owner | `rocketmq-runtime/src/service_lifecycle.rs` |
| Deployment policy | `distribution/kubernetes/deployment-policy.json`，schema 1，milestone M11-10 |
| 下一工作包 | `PR-M11-11` Kind/K3d fault matrix |

## 2. 统一状态与 deadline 合同

共享状态机为 `Starting → Ready → Draining → Stopped/Failed`：

- `Ready` 只在服务关键依赖、认证材料与业务监听器实际初始化后发布。Proxy 通过双监听 barrier 等待 gRPC 和
  remoting（启用时）都绑定；MCP HTTPS 等待 TCP bind、TLS 初始化和 JWKS warm-up。
- `/readyz` 只有 `Ready` 返回 200；进入 drain、停止或失败后立即返回 503。
- `/livez` 检测 runtime progress heartbeat 和终态，不把业务端口可连接等同于健康。
- `/drainz` 幂等地记录 `PreStop`；操作系统 signal 和内部错误使用同一入口。第一次请求固定 reason 和
  `ShutdownDeadline`，后续 preStop/signal 不得延长。
- probe server 与 heartbeat 由注入的 `ServiceContext`/`TaskGroup` 拥有；没有 detached task、临时 runtime 或
  新的 blocking 逃逸。

默认预算由 versioned policy 和环境合同同时固定：

| 合同 | 值 |
|---|---:|
| Health bind | `0.0.0.0:8088`（仅 kubelet，Service 不暴露） |
| Readiness | HTTP `/readyz` |
| Liveness | HTTP `/livez`，stale window 30 秒 |
| PreStop | HTTP GET `/drainz` |
| 内部 shutdown deadline | 45 秒 |
| Pod termination grace | 60 秒 |

## 3. 五服务关闭顺序

关闭阶段固定为：拒绝新准入 → drain session/in-flight → flush/replicate → 停止后台任务 → telemetry → report。
Broker 复用已有强类型 shutdown report 和 message-store final flush；NameServer 的 in-flight、scheduled、route、
server、remoting 与 root phase 改用同一 deadline；Controller manager task、processor、metadata 与 Raft 失败会聚合为
typed error；Proxy 同时检查 gRPC/remoting/auth/runtime report；MCP 先关闭 audit admission、FIFO drain/flush，再关闭
owned runtime。`TelemetryRuntimeGuard::shutdown_with_timeout` 接收 caller 剩余预算，入口 RuntimeOwner 最后消费同一
deadline，任何一层都不重新分配完整 30/45 秒。

## 4. Helm、Kustomize 与 guard

五个 workload 都设置 `terminationGracePeriodSeconds: 60`、health 8088、三个 lifecycle 环境变量、HTTP
readiness/liveness 和 HTTP preStop。guard 明确禁止 `tcpSocket` 与 `startupProbe`，要求三个 path、named health
port、45/60/30 预算和固定 phase order完全一致，并证明任何 Service 都没有暴露 8088。canonical secure Helm
render 仍为 37 个资源，提交的 Kustomize base 与其文本确定一致；secure overlay render 同样通过 Kubernetes 1.32
strict schema。12 组正负测试会故意弱化 grace、重复 deadline、probe type/path、quorum、secret、capability、digest
和 stateless 边界，均被 guard 拒绝。

容器 policy 同步登记六个 signal owner（五服务加 MCP 两种 transport），要求共享 lifecycle/SIGINT/SIGTERM
waiter，并把 health 8088 纳入五镜像 EXPOSE/port 合同。health port 不进入服务网络暴露面。

## 5. 已完成验证

| Gate | 命令/范围 | 结果 |
|---|---|---|
| Kubernetes full contract | `.\scripts\kubernetes-assets-contract.ps1` | 通过；Helm lint/template、37/37 双 render schema、确定性 parity、policy guard |
| Kubernetes mutations | `python -m unittest scripts.tests.test_m11_kubernetes_assets scripts.tests.test_m11_service_lifecycle -v` | 12/12 通过 |
| Container policy | `python scripts/container_image_guard.py` 与 `scripts.tests.test_m11_container_foundation` | guard 通过，9/9 mutation tests 通过 |
| Lifecycle state/probe | `cargo test -p rocketmq-runtime service_lifecycle --lib` | 5/5 通过（含同名既有 service test） |
| Service shutdown | Proxy listener barrier、NameServer zero in-flight、Broker expired/blocking deadline、Controller manager shutdown、CommitLog final flush | 6 个 focused test 通过 |
| Observability budget | `noop_runtime_guard_honors_caller_shutdown_budget`（all features） | 通过 |
| Observability 精确矩阵 | default、`observability`、`otlp-metrics`、`otel-metrics,prometheus`、`otlp-traces`、`otlp-logs`、OTLP/Prometheus 联合组合；每组均执行 workspace check、strict Clippy 与 package test | 7/7 通过 |
| MCP required profile | full tests、streamable-http strict Clippy、`cargo doc --no-deps` | 83 unit + contract/integration 通过；外部集群用例按设计 ignored；Clippy/Rustdoc 通过 |
| Root Rust | `cargo fmt --all -- --check`；workspace/all-targets/all-features strict Clippy | 通过；仅既有 MSVC linker stdout warning，不受 `-D warnings` 管理 |
| Architecture guards | runtime boundary audit、error hygiene/architecture、dependency target/baseline/release 与 AGENTS routing | 全部通过；dependency target 35 compatibility + 3 tests，release 32/32 |
| 语法与镜像静态门禁 | Actionlint 1.7.12、Hadolint 2.14.0、Python `py_compile`、PowerShell AST parse | 全部通过；两个下载工具均按官方 SHA-256 校验和核验 |

上述结果均来自本工作包最终交付前取得的成功退出码；此前超时的聚合 observability 命令未计入结果，7 组组合均已
拆分重跑并分别通过。

## 6. 回滚与未签署 Gate

回滚以五个上一已验证服务镜像和对应 Helm revision/Kustomize overlay 为单位，保留 PVC、WAL、Controller state 与
MCP audit format。可以回滚 probe 周期或阈值，但不得恢复 TCP-open 假 readiness、删除 preStop、让 Pod grace 小于
内部 deadline，或把 signal/drain 各自改回独立超时。若旧镜像不实现 M11-10 health endpoint，必须同时回滚对应
workload revision，不能混用新 probe 与旧 binary。

以下 Gate 保持开放：M11-07/08 五镜像动态 build/scan/sign/smoke，M11-09 production digest/Service CIDR 入口，
M11-11 Kind/K3d rollout/quorum/fault/已确认消息恢复，M11-12 ArcMut/stable/SLO 收口，M10 真实固定硬件/HUMAN，
以及 M11、Phase 3、最终目标态 HUMAN Gate。
