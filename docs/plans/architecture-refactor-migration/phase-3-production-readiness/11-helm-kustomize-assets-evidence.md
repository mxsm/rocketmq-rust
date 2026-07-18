# M11-09 Helm 与 Kustomize 部署资产证据

## 1. 结论与进度边界

PR-M11-09 交付五服务 canonical Helm chart、由该 chart 确定性生成的 Kustomize base、secure overlay、部署策略、
正负向 guard、Kubernetes schema 校验脚本和最小权限 CI workflow。Broker、NameServer、Controller、Proxy、MCP
均具有显式 image digest、配置、资源、状态、拓扑、网络、Pod Security 与外部 secret 引用合同；默认 chart 和 base
都拒绝或钝化未发布镜像，不会以 mutable tag 或测试 digest 伪装成可发布配置。

工作包完成后总进度为 **73/82**，剩余 9 个工作包：M11 3 个、M12 6 个；下一工作包为 PR-M11-10。
这里的“工作包完成”表示部署资产、focused test、schema/策略门禁与可回滚边界已交付，不表示生产 image digest、
目标集群 Service CIDR、真实 Controller quorum、probe/drain 或 Kind/K3d fault matrix 已验收。M11 入口 `[ARCH]`、
M10 真实固定硬件/HUMAN、容器动态 `[TEST]`、M11、Phase 3 与最终目标态 Gate 均保持开放。

| 项目 | 值 |
|---|---|
| Milestone | `M11` |
| 工作包 | `PR-M11-09` |
| GitHub Issue | `#8286` |
| Canonical owner | `distribution/helm/rocketmq-rust` |
| Kustomize owner | `distribution/kubernetes/base`、`distribution/kubernetes/overlays/secure` |
| Versioned policy | `distribution/kubernetes/deployment-policy.json`，schema 1，Kubernetes 1.32.0 |
| 非目标 | readiness/liveness、preStop/grace/drain（M11-10）；实际集群 quorum/fault 证明（M11-11）；发布与 Phase Gate 签署 |

## 2. 五服务部署边界

| 服务 | 工作负载 | 副本 | 持久化 | PDB | 主要边界 |
|---|---|---:|---|---:|---|
| Broker | StatefulSet | 1 | 100Gi、RWO、显式 StorageClass、Retain | 1 | 独占 WAL/data；10911/10912 |
| NameServer | StatefulSet | 3 | 每副本 2Gi、RWO、显式 StorageClass、Retain | 2 | 稳定身份；9876 |
| Controller | StatefulSet、Parallel | 3 | 每副本 10Gi、RWO、显式 StorageClass、Retain | 2 | 三份 ordinal config；60109 remoting、60110 Raft；hostname+zone spread |
| Proxy | Deployment | 2 | 2Gi bounded `emptyDir` | 1 | 无 PVC 的 stateless ingress；8080/8081 |
| MCP | Deployment、Recreate | 1 | 10Gi audit PVC、RWO、Helm keep | 1 | 文件 audit writer 不水平共享；HTTPS 8089 |

所有容器固定 UID/GID `10001:10001`、`RuntimeDefault` seccomp、read-only rootfs、禁止 privilege escalation、
drop `ALL` capabilities、禁用 service-account token，并为 CPU/memory 同时提供 requests/limits。namespace 标注
restricted Pod Security。网络从 ingress/egress default deny 开始，只开放同部署内部通信、DNS、OTel、显式 client
namespace 与 MCP JWKS HTTPS 路径。

chart 不创建 `Secret`，也不接受内联 `data`/`stringData`。运行时材料只能来自一个已存在的 Kubernetes Secret
或 SecretProviderClass，统一只读挂载到 `/var/run/secrets/rocketmq`。Broker/Proxy ACL、MCP TLS certificate/key
只通过该外部引用消费，values 和生成 manifest 不包含 secret payload。

## 3. Fail-closed Helm 与确定性 Kustomize

`values.yaml` 中五个 image digest 都是全零未发布 sentinel，Controller 三个 peer Service IP 使用文档保留地址；
默认 `helm template` 必须失败并要求注入已验证签名 digest 与目标集群保留的 Service CIDR 地址。schema 拒绝未知
service/value、mutable tag、错误 digest 形状、缺失 StorageClass、弱化资源/secret 引用等漂移。

`ci/secure-values.yaml` 只提供不可发布的测试 digest 与 `10.96.0.201-203` 验证地址。其 Helm render 固定为 37 个
资源，并原样提交为 `distribution/kubernetes/base/manifest.yaml`；validation script 对文本做确定性等值比较，
防止 chart 与 base 双写漂移。base 随后把五个镜像替换为全零 digest，使直接 apply fail closed；secure overlay
只恢复非发布测试 digest。生产 overlay 必须同时替换五个签名 digest，以及三个 ordinal Service 与三份 Controller
配置中的保留 ClusterIP。

资产故意不包含 `readinessProbe`、`livenessProbe`、`startupProbe`、`preStop`、`lifecycle` 或
`terminationGracePeriodSeconds`。这些词在 M11-09 guard 中是禁止项，防止用 TCP-open 假 readiness 提前替代
M11-10 的统一 drain 语义。

## 4. Controller 与 MCP 配置纠正

多节点 Controller 接入暴露了 M11-08 单端口合同无法同时承载 remoting 与 OpenRaft gRPC 的问题。本包将镜像和
部署合同修正为 `60109/60110`，并新增 `ROCKETMQ_CONTROLLER_RAFT_BIND_ADDR`：本地 listener 可绑定
`0.0.0.0:60110`，对外 peer 地址仍来自 typed `SocketAddr` 配置。三份 ordinal config 使用固定 ClusterIP 作为
advertised endpoint，避免把不可解析的 StatefulSet DNS 填入当前 `SocketAddr` 合同。

多成员自动初始化默认关闭；只有显式设置 `ROCKETMQ_CONTROLLER_AUTO_INITIALIZE_CLUSTER=true` 时，最小配置
node ID 才以完整 peer map 初始化，已有 committed log 必须跳过。Helm 显式设置该开关并使用 Parallel Pod 管理，
但本包只证明配置可由真实二进制解析、端口和 membership 合同一致；真实 leader/quorum 形成及 leader failure
仍由 M11-11 的集群 fault matrix 取证。

真实 MCP 配置解析同时发现文档值 `oauth-jwt` 被 Serde acronym 派生为 `o-auth-jwt`。`HttpAuthMode` 现显式接受
文档拼写，并把旧派生拼写保留为 alias；secure ConfigMap 还按实际 schema 提供 cache、diagnosis 与 role permission
配置。该修正不改变鉴权默认值或放宽 HTTPS/JWKS 安全边界。

## 5. Guard、工具链与 CI 合同

`kubernetes_assets_guard.py` 使用独立精确基线校验五服务 owner、镜像、端口、工作负载、副本、资源、PVC/PDB、
拓扑、Pod Security、NetworkPolicy、secret 引用、Controller 三节点 membership 和 MCP 单 writer 边界。它解析 37 个
rendered Kubernetes 资源，并用 Python `tomllib` 实际解析所有生成的 TOML ConfigMap，不仅检查字符串存在。

8 组正向/故意违规测试覆盖 Controller quorum 漂移、内联 Secret、mutable tag、提前添加 readiness、capability
弱化、伪装 production digest 和 Proxy PVC 回归。PowerShell contract 从官方 release 下载并校验以下 pinned archive：

| 工具 | 版本 | Windows amd64 SHA-256 | Linux amd64 SHA-256 |
|---|---|---|---|
| Helm | v4.2.3 | `5ca7de684c92d48b93d5c34a029fdda57b38e1eac04bc8541bdf1eb249388679` | `e9b88b4ee95b18c706839c28d3a0220e5bc470e9cd9262410c90793c45ff8b7c` |
| Kustomize | v5.8.1 | `8ec7f5e815e526d4622c06df0a7793d8cfb6eb1c74f816b46166097fef8b26c6` | `029a7f0f4e1932c52a0476cf02a0fd855c0bb85694b82c338fc648dcb53a819d` |
| Kubeconform | v0.8.0 | `e3f56102bcf4f50b034a567e2482a1c5330799983ddd655952310211aef73d93` | `9bc2bffbf71f261128533edaf912153948b7ff238f9a531ae6d34466ec287883` |

CI workflow 只申请 `contents: read`，checkout 与 artifact upload 均固定 40 位 action SHA；执行正负测试、完整
contract，并上传两份 render 证据。它不发布 chart、镜像或 Secret，也不申请 packages/OIDC 写权限。

## 6. 验证矩阵

| Gate | 命令/范围 | 结果 |
|---|---|---|
| Full Kubernetes contract | `.\scripts\kubernetes-assets-contract.ps1` | 通过：Helm lint/template、37/37 Helm schema、确定性 base parity、Kustomize build、37/37 overlay schema、policy guard |
| Positive/negative | `python -m unittest scripts.tests.test_m11_kubernetes_assets -v` | 8/8 通过 |
| Container correction | container guard 与 M11 container suite | 通过：五服务合同仍闭合，Controller 端口为 60109/60110 |
| Real config parse | 五个 rendered ConfigMap 交给所属真实二进制 | 通过；MCP 仅把 audit sink 临时映射为 memory，stdin 关闭后得到预期 `ConnectionClosed` |
| Controller/MCP Rust | bind/auto-init 与 OAuth spelling focused tests；MCP required profile | 通过；详情以本 PR 最终验证记录为准 |
| Syntax/lint | Python compile、PowerShell AST、Actionlint、Hadolint、Cargo fmt/workspace Clippy | 通过；详情以本 PR 最终验证记录为准 |
| Specialized/architecture | runtime audit、error hygiene、target/baseline/release guard、AGENTS routing、`git diff --check` | 通过；详情以本 PR 最终验证记录为准 |
| Dynamic cluster | Kind/K3d rollout/quorum/fault suite | 未执行；属于 PR-M11-11，不写成 M11-09 成功证据 |

## 7. Checklist、回滚与剩余目标

- 入口 `[ARCH]` 保持未勾选：schema、资源与边界已经 versioned，但尚无已签名 production digest，也没有目标集群
  实际保留的三个 Controller Service IP；测试 fixture 不能替代架构签署。
- `[DEV]`、focused `[TEST]`、`[REV]` 与回滚边界已完成：37 个资源通过双 render/schema/语义验证，stateful/
  stateless、quorum 配置、StorageClass/retain 和 secret 边界已复核。
- 回滚单位是上一签名 Helm revision 或 Kustomize overlay。回滚只替换工作负载与配置，不删除 PVC，不降低 WAL、
  Controller state 或 MCP audit 格式；生产 digest 和 Service IP 必须随上一已验证 revision 一起恢复。

剩余 M11 目标为 PR-M11-10 probe/preStop/统一 drain、M11-11 Kind/K3d fault matrix、M11-12
ArcMut/stable/SLO Phase 3 收口。M12 仍有 6 个 AI Native 工作包；M10 仍缺真实固定硬件 baseline/candidate、
原始数据 hash 与 `[HUMAN]` Gate。
