# M11：安全、可观测性与云原生生产化

## 元数据

| 字段 | 值 |
|---|---|
| 阶段 | Phase 3：性能、耐久引擎与云原生 |
| 状态 | 实施中（PR-M11-01～11 工程工作包已完成；真实 fault Gate 待验收；下一工作包 PR-M11-12；部分依赖 M10 SLI） |
| 预计周期 | 4–6 周 |
| 工作包 | 延续 WP01、WP03、WP05、WP07、WP14–WP16 |
| 前置条件 | 32-package Gate；secure dry-run、ServiceContext、semantic owner、durability SLI 可用 |
| 可并行项 | semantic registry、安全 provider、镜像/Helm 可分泳道；安全默认切换和 E2E Gate 串行 |
| 完成后解锁 | M12 |

## 目标

- 建立 versioned telemetry semantic registry、SLI/SLO、dashboard/runbook 和 collector 故障隔离。
- 将 secure 作为新部署默认，提供 SecretProvider、bootstrap、rotation、原子 reload 和 fail-closed readiness。
- 交付 Broker/NameServer/Controller/Proxy/MCP 独立镜像与 Helm/Kustomize，统一 drain/deadline。
- 清除 production/public compatibility API 剩余 ArcMut 安全逃逸，默认 stable 构建通过。

## 非目标

- 不静默改变已有部署；compatibility 必须显式迁移并产生 telemetry。
- 不记录 credential、TLS/ACL material、message body、token 或完整 config。
- 不用端口存活冒充 readiness/liveness，不让 telemetry 故障阻塞数据面。
- 当前 MCP manifest 只有 `change-planning` opt-in；本里程碑不假定或创建 `dangerous-tools`/Apply，它们属于 M12 经独立 Human Gate 后的未来交付物。

## 入口条件

- [ ] `[ARCH]` 冻结 semantic schema、安全 profile、bootstrap/rotation、探针/drain 和镜像边界。
- [ ] `[TEST]` 准备 collector outage、secret/reload、audit overflow、kind/k3d upgrade/eviction/disk/leader 场景。
- [ ] `[DEV]` 确认 auth/observability/distribution/docker 目标文件无用户修改重叠。
- [ ] `[HUMAN]` 批准“新部署 secure 默认、已有部署显式迁移”的版本策略。

## 交付物

| 类型 | 交付物 |
|---|---|
| Observability | versioned semantic registry、cardinality/privacy guard、SLI/SLO、dashboard/runbook |
| Security | development/compatibility/secure、SecretProvider、一次性 bootstrap、rotation/reload/audit |
| MCP HTTP | HTTPS/JWKS/kid rotation/principal propagation/audit drain 的生产 Gate |
| Cloud | 5 个镜像、Helm/Kustomize、PDB/NetworkPolicy/PodSecurity/Secret 引用、SBOM/签名 |
| Lifecycle | readiness→drain→flush/replicate→telemetry，Kubernetes grace > internal deadline |
| Soundness | ArcMut/public safe escape 清零或经 ADR 证明 sound，stable default CI |

## PR 级开发步骤

### PR-M11-01：Telemetry semantic registry

- [x] `[ARCH]` 为每个 metric/span/log 声明 owner、unit、stability、attributes、cardinality、privacy、deprecation。
- [x] `[DEV]` 实现计划中的 `telemetry_semantic_guard.py` 和违规 fixture，未登记/超预算字段拒绝。
- [x] `[DEV]` 登记 request、watermark/lag、connection/bytes、task、recovery/cache、auth/MCP、exporter 信号。
- [x] `[TEST]` 覆盖 collector outage：有界队列、可计量 drop、shutdown timeout，数据面不阻塞。
- [x] `[REV]` 检查字段低基数、采样明确、无敏感数据和每消息无限 span/event。
- [x] 回滚点：registry 可回到上一 version；不得关闭 privacy/cardinality 失败。

完成证据：[`11-telemetry-semantic-registry-evidence.md`](11-telemetry-semantic-registry-evidence.md)。注册表覆盖
119 metric、4 span、7 stable log event 和 66 attribute；65/82 工作包完成，剩余 M11 11 个、M12 6 个，
下一工作包为 PR-M11-02。本工作包不提前宣称 M10、M11 或 Phase 3 Gate 完成。

### PR-M11-02：SecretProvider 基础合同与本地 Adapter

- [x] 入口：`[ARCH]` SecretProvider capability、secret wrapper、permission和redaction合同已冻结，不切换安全profile默认值。
- [x] `[DEV]` 实现provider contract、显式registry及开发用环境/受限文件adapter；生产provider只通过注入接入。
- [x] `[TEST]` focused test：owner-only permission、encrypted envelope、atomic read/write、redacted Debug、zeroize和provider缺失。
- [x] `[REV]` 检查secret不进入CLI/env dump/log/config snapshot，security-api不依赖provider实现。
- [x] 回滚点：停止注册新provider，保留dry-run与redaction；不得回滚为pretty JSON或宽松权限。

完成证据：[`11-secret-provider-evidence.md`](11-secret-provider-evidence.md)。合同保持同步且 runtime-neutral；环境
adapter 只接受显式映射，本地文件 adapter 在 Unix 使用 owner-only AES-256-GCM immutable version，并在
Windows ACL 未验证时 fail closed。66/82 工作包完成，剩余 M11 10 个、M12 6 个，下一工作包为
PR-M11-03；本工作包没有切换安全 profile 默认值，也不提前宣称 M10、M11 或 Phase 3 Gate 完成。

### PR-M11-03：Secure Profile 与一次性 Bootstrap

- [ ] 入口：`[HUMAN]` 尚需签署“新部署secure默认、已有部署显式迁移”；目标实现已按该策略完成，SecretProvider基础测试全绿，但实现结果不替代 Gate 批准。
- [x] `[DEV]` secure缺trust/provider/bootstrap、材料无效/过期或unknown配置时校验失败且readiness=false。
- [x] `[DEV]` 一次性token绑定cluster/listener/expiry，仅在已验证TLS通道创建首个admin，成功后原子消费并关闭endpoint。
- [x] `[TEST]` focused test：缺失材料、token重放/过期/跨cluster、unknown fail、compatibility migration report和匿名启动负测。
- [x] `[REV]` 检查secure不能自动降级，bootstrap token不进入命令行/日志/环境dump。
- [x] 回滚点：已有部署显式固定compatibility；新secure部署保持readiness=false并修复材料，不切宽松模式。

完成证据：[`11-secure-profile-bootstrap-evidence.md`](11-secure-profile-bootstrap-evidence.md)。新部署默认安全与既有部署
兼容迁移由 runtime-neutral resolver 明确区分；一次性 bootstrap 在 Unix 使用 owner-only、原子 claim/consume 状态机，
Windows 在 ACL 无法验证时 fail closed。67/82 工作包完成，剩余 M11 9 个、M12 6 个，下一工作包为
PR-M11-04。网络 endpoint、rotation/reload、不可抵赖 audit 和云部署 E2E 仍由后续工作包交付；M10、M11 与
Phase 3 Gate 均保持未完成。

### PR-M11-04：Credential/Certificate Rotation 与原子 Reload

- [ ] 入口：`[ARCH]` overlap窗口、active identity、撤销、last-known-good和break-glass审计状态机已冻结。
- [x] `[DEV]` 实现新旧材料重叠验证→切active→撤销旧材料；reload先完整解析/验证再原子交换。
- [x] `[TEST]` focused test：kid/cert rotation、并发请求、reload partial/invalid、旧材料撤销、break-glass启停与审计。
- [x] `[REV]` 检查失败保留last-known-good或fail closed，不应用半份配置，不丢审计。
- [x] 回滚点：原子指回已验证的last-known-good credential version；已撤销材料不得静默复活。

完成证据：[`11-credential-rotation-evidence.md`](11-credential-rotation-evidence.md)。Auth owner 以 audit-first、不可变
generation 快照完成 active/retiring/revoked/break-glass 状态机；TLS owner 把 certificate/key/trust 完整构建结果与
generation 一起原子发布，并串行化手动 reload 与 watcher 写者。Windows 与 WSL/Linux 的 credential、真实证书、
partial/invalid、last-known-good 和并发 reload 测试通过。68/82 工作包完成，剩余 M11 8 个、M12 6 个，下一工作包为
PR-M11-05。M11 入口 `[ARCH]`、安全默认值 `[HUMAN]`、M10 真实性能、M11 与 Phase 3 Gate 均保持未完成。

### PR-M11-05：MCP HTTPS、JWKS 与 Principal 传播

- [x] 入口：`[TEST]` 当前metadata/401/Origin/scope/cluster/RBAC和`change-planning`无副作用基线已冻结。
- [x] `[DEV]` 补公共HTTPS resource URI、TLS强制、JWKS/kid rotation、secure禁对称算法、principal到protocol handler端到端传播。
- [x] `[TEST]` focused test：HTTPS metadata/challenge、签名/issuer/audience/expiry/scope、kid rotation、缺认证上下文fail closed、stdio stdout/redaction。
- [x] `[REV]` 只验证当前`change-planning` opt-in仍生成`mutates_cluster: false`计划；不假定Apply或`dangerous-tools`存在。
- [x] 回滚点：关闭新HTTP exposure并保留stdio/人工CLI，不降低认证、TLS或principal传播要求。

完成证据：[`11-mcp-https-jwks-evidence.md`](11-mcp-https-jwks-evidence.md)。MCP listener 复用 M11-04 的
TLS generation/reload/LKG 并强制 HTTPS；OAuth 改为 HTTPS-only、有界、RS256-only JWKS generation，失败不清空
last-known-good；verified principal 经真实 protocol handler 进入 RBAC/rate-limit/audit，HTTP 缺上下文不回退 stdio。
69/82 工作包完成，剩余 M11 7 个、M12 6 个，下一工作包为 PR-M11-06。M10 真实性能与 `[HUMAN]`、M11 入口
`[ARCH]`/安全默认 `[HUMAN]`、M11 与 Phase 3 Gate 均保持未完成。

### PR-M11-06：MCP Audit Writer 与 Shutdown Drain

- [ ] 入口：`[ARCH]` audit schema、count+byte上限、overflow policy、绝对deadline和flush报告已冻结。
- [x] `[DEV]` audit writer按count+bytes有界，记录稳定principal/action/outcome，shutdown在同一deadline内drain/flush并报告未完成项。
- [x] `[TEST]` focused test：overflow、sink timeout/failure、进程取消、deadline、redaction和audit ordering。
- [x] `[REV]` 检查audit故障不阻塞数据面、敏感字段不落盘、未完成记录可观测。
- [x] 回滚点：回到上一通过同一audit corpus的writer；否则关闭HTTP管理入口并保留核心stdio/人工运维。

完成证据：[`11-mcp-audit-drain-evidence.md`](11-mcp-audit-drain-evidence.md)。生产者按已脱敏 NDJSON 的真实
字节数与记录数分别执行无等待准入，writer 由 `ServiceContext` 持有且所有文件 I/O 只经过
`BlockingExecutor`。关闭先停止准入，再在同一绝对 deadline 内完成 FIFO drain/flush，随后把剩余预算交给
runtime shutdown；超时、取消、drop、sink/flush failure 与 pending count/bytes 均进入报告。70/82 工作包完成，
剩余 M11 6 个、M12 6 个，下一工作包为 PR-M11-07。M11 入口 `[ARCH]`、安全默认 `[HUMAN]`、M10
真实性能与 `[HUMAN]`、M11 和 Phase 3 Gate 均保持未完成。

### PR-M11-07：容器镜像基础

- [ ] 入口：`[ARCH]` runtime基础镜像、用户/文件系统、供应链和产物命名规则已冻结。
- [x] `[DEV]` 建统一多阶段构建、最小runtime、非root、read-only rootfs、SBOM、签名和漏洞扫描模板。
- [ ] `[TEST]` focused test：image build、non-root/read-only启动、SBOM生成、签名验证和critical vulnerability policy。
- [x] `[REV]` 静态复核 runtime stage 不依赖 builder、不复制源码/secret/debug，manifest/toolchain/package snapshot/action/tool 均有 immutable 输入。
- [x] 回滚点：使用上一签名基础镜像digest；不回滚安全用户/文件系统约束。

完成证据：[`11-container-foundation-evidence.md`](11-container-foundation-evidence.md)。新增 foundation 与现有组合镜像
隔离；builder/runtime manifest digest、Rust nightly 日期和 Debian snapshot 固定，runtime 使用数字非 root 身份与
声明式 read-only/data/tmpfs contract。guard、负向测试、Actionlint、Hadolint 和 routing gate 均通过；Ubuntu
workflow 将执行真实 build/smoke/SBOM/Trivy/Cosign/provenance。当前 Windows 与 WSL 无 Docker/Podman/Syft/
Trivy/Cosign，且按交付约定不等待远端 CI，因此动态 `[TEST]` 保持未签署，必须在 M11-08 消费成功 artifact 后
才能使用该 foundation 发布服务镜像。71/82 工作包完成，剩余 M11 5 个、M12 6 个，下一工作包为 PR-M11-08；
M10 真实性能、M11 入口 `[ARCH]`/安全默认 `[HUMAN]`、M11 与 Phase 3 Gate 均未完成。

### PR-M11-08：五个服务镜像入口

- [ ] 入口：`[TEST]` 镜像基础模板通过；五服务启动/配置/信号合同已记录，但 M11-07 远端 build 暴露 slim runtime 缺 CA bundle，M11-08 已修复且尚未观察重跑成功，未签署动态 Gate。
- [x] `[DEV]` 为五个服务分别建立entrypoint、config mount、data path、port和signal接线，不把服务打进单一镜像。
- [ ] `[TEST]` focused test：每个镜像独立启动、配置错误退出、SIGTERM、只读rootfs和必要volume权限。
- [x] `[REV]` 检查镜像边界与crate/binary owner一致，无secret命令行参数。
- [x] 回滚点：单个服务独立回到上一签名镜像，不回滚其他服务或共享持久数据。

完成证据：[`11-service-image-entrypoints-evidence.md`](11-service-image-entrypoints-evidence.md)。到期组合镜像与例外已删除，
Broker/NameServer/Controller/Proxy/MCP 由五个显式 target 分别生成且 runtime 只含 owner binary；config/data/port/
non-root/read-only/SIGTERM 合同进入 versioned policy。Controller、Proxy 与 MCP 补齐统一 SIGINT/SIGTERM 接线，五份
smoke config 经真实二进制解析，静态 guard/9 组负向测试通过；M11-07 动态失败定位为 slim image 缺 CA bundle，
本包以 Debian 签名 Release/package hash 引导 CA 后切回 HTTPS。workflow 已实现但本机无容器工具且不等待远端
CI，故镜像 `[TEST]` 保持未签署。72/82 工作包完成，剩余 M11 4 个、M12 6 个，下一工作包为 PR-M11-09；
M10 真实性能、M11 入口 `[ARCH]`/安全默认 `[HUMAN]`、M11 与 Phase 3 Gate 均未完成。

### PR-M11-09：Helm 与 Kustomize 资产

- [ ] 入口：`[ARCH]` 五服务镜像digest、配置schema、PVC/topology/secret引用和资源预算已冻结。
- [x] `[DEV]` 在`distribution/helm`、`distribution/kubernetes`提供requests/limits、PVC、PDB、topology、NetworkPolicy、PodSecurity、OTel和Secret引用。
- [x] `[TEST]` focused test：helm lint/template、Kustomize build、schema validation、secure values和禁止inline secret。
- [x] `[REV]` 检查standalone/Stateful服务边界、Controller quorum配置和storage class语义；真实 formed quorum 仍由 M11-11 取证。
- [x] 回滚点：Helm revision/Kustomize overlay回到上一签名版本，PVC和持久格式不降级。

完成证据：[`11-helm-kustomize-assets-evidence.md`](11-helm-kustomize-assets-evidence.md)。canonical Helm chart 与
确定性 Kustomize base/secure overlay 共渲染 37 个资源，固定五服务 requests/limits、stateful/stateless/PVC/PDB、
topology、default-deny NetworkPolicy、restricted Pod Security、OTel 和外部 secret 引用；默认 digest/IP sentinel
fail closed，测试 digest 与 Controller ClusterIP 明确不可发布。Controller remoting/Raft 修正为 60109/60110，
三份 ordinal config 使用稳定 ClusterIP，显式多成员 bootstrap 仅由最小 node ID 执行；真实 quorum/fault 留给
M11-11。Helm/Kustomize/Kubeconform 工具 archive hash 固定，双 render 37/37 schema、确定性 parity、真实配置解析
及 8 组正负测试通过。production 签名 digest 和目标集群 Service IP 未冻结，因此入口 `[ARCH]` 保持开放。
73/82 工作包完成，剩余 M11 3 个、M12 6 个，下一工作包为 PR-M11-10；M10 真实性能、容器动态 `[TEST]`、
M11/Phase 3/HUMAN/ARCH 与集群 fault Gate 均未完成。

### PR-M11-10：Probe、PreStop 与统一 Drain

- [x] 入口：`[ARCH]` readiness/liveness语义、ShutdownCoordinator阶段和Kubernetes grace预算已冻结。
- [x] `[DEV]` readiness仅表示可接受新工作；liveness只检测失去进展；preStop/SIGTERM进入readiness→drain→flush/replicate→telemetry。
- [x] `[TEST]` focused test：挂起 shutdown hook、重复终止请求、grace/deadline边界、ShutdownReport和CommitLog final flush；真实集群已确认消息恢复由M11-11取证。
- [x] `[REV]` 检查grace period大于内部绝对deadline，各层不重新分配完整超时。
- [x] 回滚点：单独回滚probe阈值/manifest接线到上一已验证配置；不得恢复端口存活假readiness或跳过drain。

完成证据：[`11-probe-prestop-drain-evidence.md`](11-probe-prestop-drain-evidence.md)。五服务共享
`Starting/Ready/Draining/Stopped/Failed`，独立 health port 提供 `/readyz`、`/livez`、`/drainz`；首次终止请求冻结
45 秒绝对 deadline，Pod grace 为 60 秒，服务 shutdown、telemetry 与 RuntimeOwner 只消费剩余预算。Helm/Kustomize
双 render 37/37 schema、12 组 Kubernetes 正负测试、9 组容器 guard、五服务 focused Rust test、MCP required
profile 与根 workspace strict Clippy 通过。该段是 PR-M11-10 的历史完成记录；其后的 PR-M11-11 已交付真实执行器和
证据门禁，当前总进度由下节记录为 75/82。M10 真实性能、容器动态套件、Kind/K3d fault/已确认消息恢复、
M11/Phase 3/HUMAN Gate 保持开放。

### PR-M11-11：Kind/K3d Fault Matrix Gate

- [x] 入口实现：五镜像、Helm/Kustomize、probe/drain focused gate 和 Kind/K3d 集群 profile 已版本化；真实签名 digest/Secret 仍由动态运行注入。
- [ ] `[TEST]` 真实执行滚动升级、节点驱逐、collector 中断、磁盘压力、Controller leader 故障、secret rotation 和已确认消息恢复；本机缺少动态前置条件，未伪造 PASS。
- [x] `[DEV]` 实现 `kind-architecture-refactor-e2e.ps1`，保存镜像 digest、chart/overlay hash、事件、PVC UID、message ID 和 Queue/CommitLog watermark 证据。
- [x] `[REV]` versioned policy/guard 强制 durability、drain、SLO、quorum、cleanup 和回滚断言，拒绝仅以 Pod Ready 成功。
- [x] 回滚点：恢复五个 baseline 签名镜像和上一 chart/secret，恢复 collector/节点/Controller，保留 PVC、WAL 和故障证据。

完成证据：[`11-kind-k3d-fault-matrix-evidence.md`](11-kind-k3d-fault-matrix-evidence.md)。Runner 的 Validate/Run
模式严格分离，production evidence 必须来自 `dynamic_execution=true`；11 组正负证据测试、12 组
Kubernetes 既有负向测试、37/37 双 render schema 和管理 CLI ACL 定向测试通过。75/82 工作包完成，剩余 M11 1 个、
M12 6 个，下一工作包为 PR-M11-12；真实 Kind/K3d、M10、M11/Phase 3/HUMAN Gate 保持开放。

### PR-M11-12：ArcMut、Stable 与 SLO Phase 3 收口

- [ ] 入口：`[TEST]` PR-M11-01至11证据对应同一候选快照，M10提供的durability SLI已接入。
- [ ] `[DEV]` 清除production/public compatibility API中的ArcMut、mut_from_ref和clone-safe AsMut/DerefMut。
- [ ] `[REV]` 保留项必须是唯一性可证明的sound wrapper，有局部SAFETY、Miri/Loom证据和Human ADR。
- [ ] `[TEST]` 执行stable default、feature matrix、Miri/Loom可用切片、soak和SLO fault suite。
- [ ] `[DEV]` 发布dashboard/runbook、告警、回滚和证据索引。
- [ ] 回滚点：任一失败返回对应独立PR修复并冻结新快照，不扩大baseline或跳过Gate。
- [ ] `[HUMAN]` 对同一冻结快照批准Phase 3 Gate。

当前进展：M11-12a owned-value leaf 已解除 Common 只读 TopicConfig helper 的 ArcMut 具体类型依赖，移除
`rocketmq-common` 自身未使用的 `sync_unsafe_cell` feature，并将 Remoting `RpcResponse` header 从共享可变 owner
改为独占 `Box`；typed header mutation 仅允许 `&mut self`。ArcMut guard 的 production 条目由 760 降为 733，
production occurrence 由 2,125 降为 2,082；M11-12 总项、stable workspace、其余 owner 与全部动态/HUMAN
Gate 仍未完成。详见 [`11-soundness-closure-progress.md`](11-soundness-closure-progress.md)。

M11-12b 已将 Controller 动态配置改为 `ArcSwap` 不可变快照：唯一写入口串行 clone、应用、整体校验并原子发布，
解析或整体校验失败保持旧指针和值不变；各读操作固定单一 `Arc<ControllerConfig>` 快照。全仓 production 条目累计
降至 711、occurrence 降至 2,029，Controller 配置相关 `ArcMut` 清零；其他 Controller owner 与总 Gate 仍开放。

M11-12c 已将 Controller manager/heartbeat lifecycle owner 改为安全 `Arc`/`Weak` 和内部同步：并发生命周期转换串行，
request processor/housekeeping 不形成强引用环，embedded NameServer 复用相同 owner。实际快照累计降至 697 production/
1,986 occurrences，Controller 降至 17 条/51 occurrences；Raft/remoting-client、NameServer 其余 owner 与总 Gate 仍开放。

M11-12d 已将 OpenRaft node/gRPC shutdown handle 收入内部同步生命周期，以 Tokio async mutex 串行完整启停转换，并只在
短临界区读写状态；`RaftController`、Manager 与 Processor 改用 `Arc`。实际快照累计降至 690 production/1,961
occurrences，Controller 降至 10 条/26 occurrences，Raft/OpenRaft owner 已退出 `ArcMut`；remoting-client、NameServer
其余 owner 与总 Gate 仍开放。

M11-12e 已将 Controller request processor 的 13 个业务 handler 从 `&mut self` 收窄为 `&self`，wrapper payload 改用
`Arc<ControllerRequestProcessor>`，remoting trait 的 mutable receiver 只负责调用共享 dispatch。实际快照累计降至
688 production/1,959 occurrences，Controller 降至 8 条/24 occurrences；remoting-client、`ConnectionHandlerContext`、
NameServer 其余 owner 与总 Gate 仍开放。

M11-12f 已将 `NameServerRuntimeInner` 根改为安全 `Arc`，以 `ArcSwap` 不可变快照和串行 writer 发布 runtime config；
route、KV、housekeeping、batch-unregistration 与 processor 只持有 `Weak` runtime handle，processor wrapper 改用 `Arc`，
legacy V1 route wrapper 以显式 Mutex 串行可变入口且不跨 `.await` 持锁。实际快照累计降至 669 production/1,918
occurrences，NameServer 降至 28 条/58 occurrences；V1 tables、remoting-client、`ConnectionHandlerContext` 与总 Gate
仍开放。

M11-12g 已把 Remoting Channel/ConnectionHandlerContext 改为安全 `Arc` owner、只读 connection lifecycle handle
和显式串行 writer；共享引用不能再取得 socket、encoder 或 Channel 的可变引用。实际快照累计降至 514 production/
1,612 occurrences，Channel/Context 定向债务清零；剩余 514 条集中在 Broker、Client、Store、Remoting client/server/
protocol/RPC、NameServer V1/remoting-client、Controller remoting-client 与 Tools。M11-12 总项、stable/SLO/HUMAN
Gate 仍开放。

M11-12h 已把 Remoting handler/client 改为安全 `Arc`/`Weak` owner、clone-local processor adapter、短临界区 hook
快照与显式同步的 NameServer 选择状态；shutdown、hook 与 connection cleanup 不再要求 clone-safe mutable receiver。
实际快照累计降至 488 production/1,559 occurrences，Controller production 债务清零，Remoting 仅剩 protocol
compatibility 6/9，NameServer 仅剩 V1 tables 16/44。Broker/Client/Store owner 与完整候选快照 Gate 仍开放。

M11-12i 已把 NameServer V1 六张 route table 改为 `RouteInfoManager` 独占普通 `HashMap`，由 wrapper 的单一
Mutex owner 串行复合 mutation，所有变更入口恢复 `&mut self`。实际快照累计降至 472 production/1,515
occurrences，NameServer production 债务清零；Broker/Client/Store、Remoting protocol compatibility 与完整候选
快照 Gate 仍开放。

M11-12j 已删除 Remoting 固定 `ArcMut` header/mapping-detail facade，并把 topic-config wire DTO 收敛为 Protocol
canonical owned `HashMap` 类型；Broker/NameServer 构造端只发布 owned protocol value。实际快照累计降至 466
production/1,505 occurrences，Remoting production 债务清零；Broker/Client/Store、Tools/compatibility 与完整候选
快照 Gate 仍开放。

M11-12k 已把 Client ProduceAccumulator 的 Manager/Producer 共享 owner 从 `ArcMut` 改为安全 `Arc`；批大小和延时
配置以原子状态发布，sync/async guard task handle 与 schedule sender 收入显式 lifecycle mutex，异步 shutdown 在
await 前取出 owned handle。实际快照累计降至 463 production/1,495 occurrences，Client 降至 143/589；其余
Client owner、Broker/Store、Tools/compatibility 与完整候选快照 Gate 仍保持开放。

M11-12l 已把 Client latency fault detector 的 trait/strategy/filter/task capture 改为安全 `Arc`；detector 配置原子
发布，resolver/service detector 通过短 `RwLock` 克隆 `Arc` 快照后再执行网络 await，task 发布与关闭由单一
lifecycle mutex 串行。实际快照累计降至 454 production/1,481 occurrences，Client 降至 134/575；其余 Client
owner、Broker/Store、Tools/compatibility 与完整候选快照 Gate 仍保持开放。

M11-12m 已把 Client 拉取与消费消息值从 `ArcMut<MessageExt>` 改为 owned `PullResult` 和标准
`Arc<MessageExt>`：decode/filter/Admin/Client adapter 不再临时构造共享可变消息，ProcessQueue、consume request、
hook、trace 与 Lite zero-copy 只共享不可变句柄；retry/namespace mutation 使用 clone-on-write，消费开始时间从消息
别名写入迁到 ProcessQueue 受锁 lifecycle map。实际快照累计降至 440 production/1,397 occurrences，Client 降至
120/491；Client lifecycle owner、Broker/Store、Tools/compatibility 与完整候选快照 Gate 仍保持开放。

M11-12n 已把 Client consume service lifecycle owner 从 `ArcMut<Self>` 改为标准 `Arc<Self>`：通用分发器发布
`Arc`/`Weak`，concurrent/orderly 与 Push/Pop service 的 start/shutdown/request task 只共享不可变 service owner；
Pop orderly lock-refresh handle 收入显式 mutex，并在 shutdown await 前取出。实际快照累计降至 436
production/1,337 occurrences，Client 降至 116/431；其余 Client owner、Broker/Store、Tools/compatibility 与完整
候选快照 Gate 仍保持开放。

M11-12o 已把 Client 异步发送 hook 与 trace enrichment 从完整 lifecycle owner 收窄为最小 capability：回调只
快照不可变 hook 列表，`SendMessageContext` 不再传播 Producer owner；trace dispatcher 只保存启动后解析出的
client id，不再持有 host Producer/Consumer 实现。实际快照累计降至 432 production/1,329 occurrences，Client
降至 112/423；其余 Client owner、Broker/Store、Tools/compatibility 与完整候选快照 Gate 仍保持开放。

M11-12p 已删除 Client Admin facade 的 self `ArcMut` owner：Client 与 admin-core facade 直接拥有实现和单一
`ClientConfig`，批量 Admin 转发只传递普通引用；ClientInstance 的 Admin group 注册值收窄为
owner-free marker，不再保活完整 Admin 实现。实际快照累计降至 424 production/1,295 occurrences，Client
降至 107/403 且 Tools production 债务清零；其余 Client Instance/API/Producer/Consumer owner、Broker/Store、
compatibility 与完整候选快照 Gate 仍保持开放。

M11-12q 已删除 Client Producer fault strategy 的 `ArcMut` owner：Producer 直接拥有策略，异步发送回调按发送时刻
克隆延迟阈值快照，只共享 concurrency-safe detector 与原子运行时开关。实际快照累计降至 423
production/1,292 occurrences，Client 降至 106/400；其余 Client Instance/API/Producer/Consumer owner、
Broker/Store、compatibility 与完整候选快照 Gate 仍保持开放。

M11-12r 已删除 Client API factory 的 `ArcMut` owner：factory client 列表和名称服务器刷新任务改用普通 `Arc`，
API client 通过短异步 `RwLock` 串行判重并发布名称服务器地址，lifecycle/address capability 收窄为共享引用。
实际快照累计降至 421 production/1,286 occurrences，Client 降至 104/394；其余 Client Instance/Producer/
Consumer/Admin API owner、Broker/Store、compatibility 与完整候选快照 Gate 仍保持开放。

M11-12s 已删除 Client API instance 的 `ArcMut` owner：`MQClientInstance` 与 Admin/Producer/Consumer 调用链改持
普通 `Arc<MQClientAPIImpl>`，纯转发 receiver 收窄为共享引用，query/pull task 只捕获 `Arc<Self>`，heartbeat
不再取得 safe mutable escape。实际快照累计降至 420 production/1,276 occurrences，Client 降至 103/384；
其余 Client Instance/Producer/Consumer owner、Broker/Store、compatibility 与完整候选快照 Gate 仍保持开放。

M11-12t 已删除 Client internal Admin 的 `ArcMut` owner：`MQClientInstance` 改持普通 `Arc<MQAdminImpl>`，root
client handle 以 `OnceLock` 一次绑定，Admin forwarding receiver 收窄为共享引用；Producer 删除 11 个仅为访问
Admin helper 的 `mut_from_ref`。实际快照 production 条目保持 420、occurrences 降至 1,263，Client 为 103/371；
其余 Client Instance/Producer/Consumer owner、Broker/Store、compatibility 与完整候选快照 Gate 仍保持开放。

M11-12u 已收窄 Client route registry 的可变 capability：route refresh/application、route query、broker lookup 与
Producer 注册只经共享引用，Producer 路由/heartbeat/注册路径删除 4 个 safe `mut_from_ref`，仅保留 lifecycle
start 的实际可变入口。实际快照 production 条目保持 420、occurrences 降至 1,259，Client 为 103/367；其余
Client Instance/Producer/Consumer owner、Broker/Store、compatibility 与完整候选快照 Gate 仍保持开放。

M11-12v 已删除 Push/Lite OffsetStore 的 `ArcMut` owner：Consumer facade、内部实现、rebalance 与 callback 改持
普通 `Arc<OffsetStore>`，persistence receiver 收窄为共享引用；Local task handle 由 lifecycle mutex 串行并在 await
前取出。实际快照降至 418 production/1,224 occurrences，Client 降至 101/332；旧 public OffsetStore ArcMut
signature 不保留安全例外，其余 Client owner、Broker/Store、compatibility 与完整候选快照 Gate 仍保持开放。

M11-12w 已删除 ProduceAccumulator batch producer 的冗余 `ArcMut` owner：每个 batch 直接持有 owned producer
clone，flush 在 batch mutex 内克隆、锁外发送。实际快照降至 415 production/1,219 occurrences，Client 降至
98/327，accumulator production/test ArcMut 债务清零；其余 Client owner、Broker/Store、compatibility 与完整
候选快照 Gate 仍保持开放。

M11-12x 已删除 `RemoteBrokerOffsetStore` 只读 broker lookup、route refresh 与 client API access 的 4 个过时
`mut_from_ref`。实际快照降至 414 production/1,215 occurrences，Client 降至 97/323；查询 header、route-miss
重试、timeout 与错误映射不变，其余 MQClientInstance owner、Client/Broker/Store、compatibility 与完整候选
快照 Gate 仍保持开放。

M11-12y 已将 Push pull/pop request dispatch、retry namespace reset、POP ack/change-invisible receiver 收窄为
`&self`，lazy namespace 通过 immutable resolution 保持显式 namespace 与 endpoint-derived namespace 语义；
RebalancePush heartbeat/dispatch 与 consume service 删除 9 个过时 `mut_from_ref`。实际快照降至
411 production/1,206 occurrences，Client 降至 94/314；真实 rebalance/producer mutation、Push owner 与其余
Client/Broker/Store、compatibility、完整候选快照 Gate 仍保持开放。

M11-12z 已将 Rebalance 单队列 unlock、全队列 lock/unlock capability 与 Push/Lite/inner 实现收窄为 `&self`；
broker lookup、lock/unlock request body、oneway 与 process-queue lock state/timestamp 语义不变。orderly 与 POP-
orderly namespace reset 使用 immutable resolution，删除 3 个过时 `mut_from_ref`。实际快照降至
410 production/1,203 occurrences，Client 降至 93/311；POP-orderly producer send、Rebalance owner/assignment 与
其余 Client/Broker/Store、compatibility、完整候选快照 Gate 仍保持开放。

M11-12aa 已将 Lite Pull 实现与 rebalance 配置从共享可变 owner 改为 `ArcSwap` 不可变快照；setter 使用
copy-update-publish，异步 pull/rebalance/metadata/diagnostics 读取完整代际且不跨 await 持有同步 guard。兼容 facade
入口、namespace、trace、pull、offset 与 Java 配置同步行为保持。实际快照为 410 production/1,169 occurrences，
Client 为 93/277；Lite Pull facade/root lifecycle、其余 Client/Broker/Store、compatibility 与完整候选快照 Gate 仍保持开放。

M11-12ab 已将 Lite Pull facade 的 client/consumer config owner 改为共享 `ArcSwap`；公开 getter 返回 immutable
owned `Arc` snapshot，consumer group 返回 owned value，构造边界接收 owned config，builder 不再传播 `ArcMut`。
这是退出 production/public compatibility API 不安全可变逃逸的显式 API 迁移；facade 路径、builder、Java-compatible
方法名与 namespace/TLS/trace/pull/offset 行为保持。实际快照降至 408 production/1,129 occurrences，Client 降至
91/237；Lite Pull root lifecycle、其余 Client/Broker/Store、compatibility 与完整候选快照 Gate 仍保持开放。

M11-12ac 已将 Lite Pull facade、consumer inner、callback 与 task 的根 owner 改为标准 `Arc`/`Weak`；专用异步
lifecycle mutex 串行 start/shutdown 与订阅控制面，同步状态和组件槽位只在短锁内发布/克隆并在 await 前释放。
Rebalance offset store 使用 `ArcSwapOption` 快照，consumer 注册与订阅表写入收窄为 `&self`。实际快照降至
402 production/1,102 occurrences，Client crate 降至 85/210；其余 Client/Broker/Store、compatibility 与完整候选
快照 Gate 仍保持开放。

M11-12ad 已将 Lite/Push 共用 `PullAPIWrapper` owner 改为标准 `Arc`；unit mode、user-broker selection 与 broker id
通过原子值发布，filter hook 列表通过 `ArcSwap` 发布完整代际，pull/POP/filter-server 调用使用 `&self`。实际快照为
402 production/1,095 occurrences，Client 为 85/203；其余 Client/Broker/Store、compatibility 与完整候选快照 Gate
仍保持开放。

M11-12ae 已将 Push facade config、implementation 与 Java-compatible getter/setter 中的 `MessageListener` handle 改为
标准 `Arc`；concurrent/orderly 注册、替换与清除语义保持。实际快照为 402 production/1,086 occurrences，Client
为 85/194；其余 Client/Broker/Store、compatibility 与完整候选快照 Gate 仍保持开放。

M11-12af 已将 Push deprecated startup subscription map 改为标准 `Arc<HashMap>`；config/builder/getter/setter 发布
immutable owned snapshot，启动时只读复制到独立 dynamic rebalance table。实际快照降至 400 production/1,078
occurrences，Client 降至 83/186；其余 Client/Broker/Store、compatibility 与完整候选快照 Gate 仍保持开放。

M11-12ag 已将 concurrent/orderly 与 POP concurrent/orderly 四类 Push consume service 的 client/consumer config 改为
同一启动代的 immutable `Arc` 快照；服务不再持有配置共享写入口，callback 的实时 Push implementation owner 保持不变。
实际快照降至 398 production/1,054 occurrences，Client owner 降至 80/161，另有 Proxy 1/1；其余 Client/Broker/Store、
compatibility 与完整候选快照 Gate 仍保持开放。

M11-12ah 已将 RebalancePush consumer config 改为 `ArcSwap` 完整不可变代际；相关 facade setter 显式发布新快照，
queue-count 变化只通过 Push implementation owner 回写两个动态 threshold。实际快照为 398 production/1,052
occurrences，Client owner 降至 80/159，另有 Proxy 1/1；其余 Client/Broker/Store、compatibility 与完整候选快照 Gate
仍保持开放。

## 公共兼容面

- development/compatibility仍可显式选择；secure只作为新部署默认，不静默重解释旧配置。
- 安全 enum unknown始终失败；secret/provider实现不暴露到security-api合同。
- telemetry schema版本化，删除/重命名走deprecation窗口。
- 镜像入口、wire/storage和已确认消息语义保持；部署回滚不要求格式降级。

## 验证命令

### 当前即可执行

```powershell
cargo test -p rocketmq-observability
cargo test -p rocketmq-auth
cargo check -p rocketmq-mcp
cargo test -p rocketmq-mcp
cargo test -p rocketmq-mcp --all-features
cargo clippy --all-targets -p rocketmq-mcp --features streamable-http -- -D warnings
cargo doc -p rocketmq-mcp --no-deps
.\scripts\kubernetes-assets-contract.ps1
python -m unittest scripts.tests.test_m11_kubernetes_assets -v
.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline
.\scripts\check-error-hygiene.ps1
.\scripts\check-agents-routing.ps1
cargo fmt --all -- --check
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
git diff --check
```

Observability 使用当前 CI 的精确 7 组适用 feature matrix，不以 all-features 替代。

### 本里程碑新增后执行

```powershell
python scripts/telemetry_semantic_guard.py
python scripts/arc_mut_guard.py
python scripts/architecture_dependency_guard.py --mode target
.\scripts\kind-architecture-refactor-e2e.ps1
```

Telemetry semantic guard 已由 PR-M11-01 交付并接入 CI；kind E2E 仍只在后续脚本和 fixture 实际交付后执行。

## 回滚触发器

- secure配置可匿名/宽松启动、secret泄漏、bootstrap可重放或reload应用半份配置。
- collector/audit故障阻塞数据面或产生无界队列。
- rolling/eviction/disk/leader演练丢失已确认消息，或deadline/drain顺序不一致。
- ArcMut安全逃逸仍存在，stable default失败，或镜像无法回滚。

安全失败保持readiness=false并停止发布；部署回滚上一签名版本，保留审计和持久数据。不得通过compatibility静默降级secure。

## Exit Checklist

- [x] `[TEST]` semantic/cardinality/privacy guard正负fixture全绿。
- [ ] `[TEST]` collector/audit outage有界且不阻塞数据面。
- [ ] `[REV]` secure缺失/过期/unknown均fail closed，secret全链路脱敏。
- [ ] `[TEST]` MCP HTTP认证、rotation、audit、Plan无副作用合同通过。
- [ ] `[TEST]` 五服务镜像和kind/k3d故障/滚动场景通过。
- [ ] `[REV]` drain顺序和绝对deadline统一。
- [ ] `[REV]` ArcMut安全逃逸清零或仅有获批sound wrapper，stable default通过。
- [ ] `[HUMAN]` 新旧部署策略和Phase 3 Gate已签署。

## 交接物

- 向 M12 交付 versioned telemetry、RequestContext/RBAC/audit、SLO、KG允许字段和云原生运维接口。
- 向发布负责人交付 secure migration、镜像签名、Helm rollback和Phase 3 evidence。
