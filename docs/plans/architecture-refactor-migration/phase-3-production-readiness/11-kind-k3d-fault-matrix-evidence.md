# M11-11 Kind/K3d 故障矩阵证据

## 1. 结论与目标完成边界

PR-M11-11 交付了真实 Kind/K3d 集群执行入口、七场景版本化策略、fail-closed 证据校验、正负 fixture、
手动动态 workflow、测试专用管理驱动镜像以及安全集群所需的 CLI 环境 ACL 通道。工作包完成后总进度为
**75/82**，剩余 7 个工作包：M11 1 个、M12 6 个；下一工作包为 PR-M11-12。

这里的“工作包完成”指执行器和证据门禁已实现并通过本机可执行的静态/离线验证，不表示真实动态 Gate 已签署。
当前主机没有 Docker、Kind、K3d、Kubectl、Helm 或目标镜像/Secret，因此未运行七场景动态套件，也没有生成或提交
`dynamic_execution=true` 的 `run.json`。正向 fixture 固定为 `fixture=true`、`dynamic_execution=false`，只能验证
parser，不能替代集群证据。M10 真实固定硬件、M11 动态 fault、M11/Phase 3/HUMAN Gate 均保持开放。

| 项目 | 值 |
|---|---|
| Milestone | `M11` |
| 工作包 | `PR-M11-11` |
| GitHub Issue | `#8290` |
| Branch | `mxsm/architecture-refactor-kind-fault-matrix` |
| Policy | `distribution/kubernetes/fault-matrix-policy.json`，schema 1，evidence schema 1 |
| Runner | `scripts/kind-architecture-refactor-e2e.ps1` |
| Guard | `scripts/fault_matrix_guard.py` |
| Dynamic workflow | `.github/workflows/kubernetes-fault-matrix.yml` |
| 下一工作包 | `PR-M11-12` ArcMut、stable 与 SLO Phase 3 收口 |

## 2. 目标完成矩阵

| 目标 | 已实现边界 | 当前验收状态 |
|---|---|---|
| Kind/K3d 可执行入口 | `Validate/Run` 严格分离；Run 强制 Docker、选定 backend、五服务双 image map、collector digest 和四份 Secret manifest | 实现完成；真实 Run 待环境 |
| 七类故障 | rolling upgrade、node eviction、collector outage、disk-pressure 调度压力、Controller leader failure、secret rotation、acknowledged recovery | 策略/执行步骤完成；动态结果待取证 |
| 耐久性 | 同一 message ID 的 Queue Offset、CommitLog Offset 和 PVC UID 在升级、故障、重启前后精确比较 | 实现完成；动态结果待取证 |
| Drain/SLO | 检查 `FailedPreStopHook`、collector outage 消息往返预算、workload ready 和最终清理状态 | 实现完成；动态结果待取证 |
| 回滚 | 五服务恢复 baseline digest，collector/cordon/taint/Controller 恢复，PVC/WAL 不删除，证据保留 | 实现完成；动态结果待取证 |
| 证据真实性 | production 必须 `fixture=false` 且 `dynamic_execution=true`；policy/chart/overlay/image/artifact SHA-256 和精确字段闭合 | 离线门禁通过 |
| 安全集群驱动 | 独立 `fault-driver` Docker target 标记 test-only；CLI 仅从环境读取成对 ACL，HMAC-SHA256，错误不输出值 | Rust 定向测试通过 |

磁盘压力场景使用 Kubernetes 调度层的标准 `node.kubernetes.io/disk-pressure:NoSchedule` taint 和 stateless Pod
驱逐，不伪装为真实块设备 ENOSPC。它验证节点压力下的调度、可用性和耐久性；真实磁盘耗尽若需纳入生产演练，必须
作为独立受控实验扩展策略，不能静默改变本场景语义。

## 3. 集群与工具链合同

- Kind 固定 `v0.27.0` 和 Kubernetes `1.32.2` node digest；K3d 固定 `v5.9.0`；Kubectl 固定
  `v1.32.2`；Helm 沿用 `v4.2.3`。
- 集群固定 1 control-plane + 3 workers；Controller 3 副本、quorum 2；Kind/K3d 分别记录 `standard` /
  `local-path` storage class 和对应 Service CIDR。
- workflow 下载二进制后先校验官方 SHA-256；动态任务只在 `workflow_dispatch` 执行，Secret manifest 来自受保护
  GitHub Secrets，不进入输入、日志或仓库。
- 五个 baseline/candidate 引用都必须是 canonical `<shared-registry>/<service>@sha256:<64hex>`；全零或单字符
  重复 digest、缺服务、candidate 完全等于 baseline 均失败。

## 4. 证据与故意违规门禁

只有全部七场景按策略顺序通过、所有全局断言为真、`unresolved_faults=[]` 且 artifact hash 重新计算一致时，Runner
才写入最终 `run.json`。异常退出会删除可能存在的 PASS 文件；`KeepCluster` 仅保留调试集群，不放宽证据校验。

提交的正向 fixture 覆盖完整 schema。10 个 deliberate-violation 测试证明以下输入会失败：未显式允许 fixture、
非动态 production 证据、全局回滚失败、场景缺失、场景断言键漂移、placeholder digest、未清理故障、artifact 篡改，
以及执行器删除 CommitLog offset 断言。fixture artifact 本身逐项登记 SHA-256。

## 5. 已完成验证

| Gate | 命令/范围 | 结果 |
|---|---|---|
| Fault policy | `python scripts/fault_matrix_guard.py --policy-only` | 通过 |
| Positive fixture | `python scripts/fault_matrix_guard.py --evidence scripts/tests/fixtures/m11-fault-matrix/pass --allow-fixture` | 通过 |
| Positive + deliberate violations | `python -m unittest scripts.tests.test_m11_fault_matrix -v` | 11/11 通过 |
| Runner validate | `.\scripts\kind-architecture-refactor-e2e.ps1 -Mode Validate` | 通过；明确无动态 PASS |
| Kubernetes full contract | `.\scripts\kubernetes-assets-contract.ps1` | 通过；Helm/Kustomize 37/37 双 render schema |
| Kubernetes mutations | `python -m unittest scripts.tests.test_m11_kubernetes_assets scripts.tests.test_m11_service_lifecycle -v` | 12/12 通过 |
| Client ACL algorithm | `cargo test -p rocketmq-client-rust acl_client_rpc_hook --lib` | 5/5 通过；默认兼容签名与显式 HMAC-SHA256 均覆盖 |
| Admin ACL adapter | `cargo test -p rocketmq-admin-core admin_acl_hook --lib` | 1/1 通过；SHA-256 选择与凭据 redaction 覆盖 |
| Admin ACL hook | `cargo test -p rocketmq-admin-cli rocketmq_cli --lib` | 5/5 通过 |
| Root Rust | `cargo fmt --all -- --check`；`cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` | 通过；仅既有 MSVC linker stdout warning |
| Architecture/safety | target/baseline dependency、32/32 release、AGENTS routing、error hygiene、runtime boundary | 全部通过 |
| Workflow/container syntax | Actionlint 1.7.12、Hadolint 2.14.0、Python `py_compile`、PowerShell AST | 全部通过；下载工具先校验官方 SHA-256 |

首次聚合执行因 `cargo clean` 后重建超过命令的 120 秒上限而超时，不计为通过；随后 Rust 定向测试以 600 秒上限
独立重跑并成功。真实 `-Mode Run` 未执行，原因是本机缺少明确列出的动态前置条件，不计为通过。

## 6. 回滚与未签署 Gate

动态回滚必须恢复五个 baseline 签名 digest 和上一 Helm revision，恢复 baseline runtime Secret、collector 副本、
worker schedulable 状态及 Controller 三副本；不得删除 PVC、WAL 或故障证据。证据目录即使运行失败也保留诊断 artifact，
但不保留可被误读为 PASS 的 `run.json`。

以下 Gate 仍开放：M11-07/08 五镜像真实 build/scan/sign/smoke，M11-09 production digest/Service CIDR 入口，
本工作包 Kind/K3d 七场景的 `dynamic_execution=true` 证据，M11-12 ArcMut/stable/SLO，M10 真实固定硬件/HUMAN，
以及 M11、Phase 3 和最终目标态 HUMAN Gate。
