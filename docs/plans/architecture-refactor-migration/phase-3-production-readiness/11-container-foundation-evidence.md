# M11-07 容器镜像基础与供应链门禁实施证据

## 1. 结论与进度边界

PR-M11-07 已交付与旧组合镜像隔离的 container foundation：builder/runtime 输入按 manifest digest 固定，
Rust nightly 与 Debian package repository 按日期固定；runtime 约束为数字 non-root、read-only rootfs、仅声明
data volume 与 tmpfs 可写，并提供 SBOM、零 CRITICAL 漏洞、Sigstore bundle 和 digest-only keyless image
signature 的执行模板与最小权限 workflow。

工作包完成后总进度为 **71/82**，剩余 11 个工作包：M11 5 个、M12 6 个；下一工作包为 PR-M11-08。
这里的“工作包完成”表示 foundation、policy、guard、动态验证 workflow 与回滚边界已实现，不表示远端容器
workflow 已观察成功。当前动态 `[TEST]`、M11 入口 `[ARCH]`、安全默认迁移 `[HUMAN]`、M10 真实固定硬件
baseline/candidate 与 `[HUMAN]` Gate 均未签署，也不等于 M11、Phase 3 或最终目标态 Gate 完成。

| 项目 | 结果 |
|---|---|
| 工作包 | `PR-M11-07` |
| GitHub Issue | `#8282` |
| 主要 owner | `docker/Dockerfile.base`、`docker/container-policy.json`、container guard/workflow |
| 新增依赖边 | 无 Cargo/目标 DAG 边；仅新增容器供应链 workflow |
| 非目标 | 五服务 binary/entrypoint（M11-08）、Helm/Kustomize（M11-09）、probe/drain（M11-10） |

## 2. 盘点、兼容例外与 immutable 输入

盘点时仓库只有 `docker/Dockerfile`：它使用 mutable `latest-rust-slim`/`trixie-slim`，同时打包多个 binary，
通过 shell 和 `ROCKETMQ_COMPONENT` 分发。M11-07 不静默改变既有部署，因此保留该文件原样，并在 versioned
policy 中登记唯一 compatibility exception，强制 `expires_at = M11-08`。任何新增未登记 Dockerfile 或延长例外
都会使 guard 失败。

foundation 固定以下输入：

| 输入 | 固定值 |
|---|---|
| Builder manifest | `rust:slim-bookworm@sha256:99e09cb2284e2ddbb73a995deee3e91783fd04d177602ccf6eab326d778ee777` |
| Runtime manifest | `debian:bookworm-slim@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818` |
| Rust toolchain | `nightly-2026-07-05`；官方 dist manifest 可用 |
| Debian package snapshot | `20260717T000000Z`；Debian 与 security 的 bookworm Release 可用 |
| Syft / Trivy / Cosign | `v1.48.0` / `v0.72.0` / `v3.1.2` |
| GitHub Actions | checkout/buildx/tool installers/upload 均固定完整 40 位 commit SHA |

Builder snapshot 安装 workspace native build 所需 clang/LLVM/CMake/Ninja/protobuf/OpenSSL 工具；runtime 从独立
pinned Debian stage 开始，只安装同一 snapshot 的 CA 与 `libssl3`，不继承 builder layer，也不复制 repository
source、编译器或调试产物。Hadolint 的 DL3008 只在两个使用 immutable snapshot 的 RUN 上窄化忽略，并在相邻
注释记录理由。

## 3. Runtime、命名与供应链合同

- UID/GID 固定为 `10001:10001`，工作目录 `/opt/rocketmq`；基础镜像不提供 service/shell entrypoint，默认
  command 仅为 `/bin/true`，服务入口由 M11-08 分别拥有。
- rootfs 必须 read-only；唯一持久可写路径为 `/var/lib/rocketmq` volume，临时写路径为
  `/tmp/rocketmq` tmpfs。OCI label 固定 role、uid/gid、read-only、data/tmpfs 语义，停止信号为 SIGTERM。
- 五服务 repository 固定在 `ghcr.io/mxsm/rocketmq-rust/{broker,namesrv,controller,proxy,mcp}`，发布 tag 必须为
  semantic version 加 12 位 commit；`latest/main/master` 禁止。
- 本地 foundation 验证生成 CycloneDX JSON、Trivy JSON、Cosign SBOM bundle/public key 与 provenance JSON；
  ephemeral private key 在 success/failure 路径均删除，不进入 artifact。
- 生产 image signing 只接受五服务 GHCR `@sha256:` digest，keyless verify 固定 GitHub Actions OIDC issuer 和
  本仓库 workflow identity regexp；tag 不能进入签名入口。实际 push/sign 由 M11-08 发布 workflow 调用。

## 4. 动态 workflow 与证据边界

`.github/workflows/container-foundation-ci.yml` 只申请 `contents: read`，不使用 `pull_request_target`、package
write 或 OIDC。workflow 在 Ubuntu 安装固定版本工具，然后执行：

1. 静态 guard 与 6 组故意违规测试；
2. Buildx 构建 `runtime-base-smoke`，检查 OCI user/label；
3. `--network none --read-only` 启动，只挂载声明 data volume/tmpfs，并验证 rootfs 写入失败；
4. Syft 生成 CycloneDX、Trivy 对全部 CRITICAL（含 unfixed）执行 exit-code 1；
5. Cosign 生成临时 key、签名/验证 SBOM bundle、删除 private key；
6. 输出 source/image/base/toolchain/snapshot 与三个 artifact SHA-256 的 provenance，并上传 14 天 artifact。

当前 Windows host 与 Ubuntu WSL 均无 Docker/Podman/Buildah/Syft/Trivy/Cosign；没有通过安装系统级容器
daemon 扩大本工作包权限。因此本地未生成 image ID、SBOM、Trivy report 或 signature bundle，远端 workflow
也按用户约定不等待完成。M11-08 的入口必须读取同一 source commit 的成功 artifact，否则动态 `[TEST]` 仍未
满足，不能发布五服务镜像。

## 5. 本地验证矩阵

| Gate | 命令/范围 | 结果 |
|---|---|---|
| Container guard | `python scripts/container_image_guard.py` | 通过；2 个 Dockerfile、1 个 M11-08 到期例外 |
| Positive/negative tests | `python -m unittest scripts.tests.test_m11_container_foundation -v` | 6/6 通过 |
| Python syntax | `python -m py_compile ...` | 通过 |
| PowerShell AST | `Parser::ParseFile(scripts/container-supply-chain.ps1)` | 通过 |
| Workflow syntax | checksum-verified Actionlint `v1.7.12` | 通过 |
| Dockerfile lint | checksum-verified Hadolint `v2.14.0` | 通过，无 finding |
| Official input check | Docker Hub manifest API、Debian snapshot Release、Rust dist 与 action input | 通过 |
| AGENTS routing | `scripts/check-agents-routing.ps1` | 通过：standalone Cargo 4、Node 3、routes 8 |
| Architecture | target/baseline dependency guard；release guard | 通过：target ledger 35、dev-only 3、release topology 32/32 |
| Text | `git diff --check` | 通过 |
| Dynamic image suite | Docker/Syft/Trivy/Cosign workflow | 已交付，当前未观察执行；保持 open `[TEST]` |

本工作包没有修改 Rust、Cargo manifest、公共 API、wire/storage 或运行时代码，因此不触发 Cargo、public API、
runtime ownership、typed-error 或 RocksDB specialized gate。

## 6. 回滚与剩余目标

回滚必须整体移除 foundation Dockerfile、policy、guard/tests、supply-chain script、workflow 与本证据。旧组合
Dockerfile 在 M11-07 期间仍是兼容路径；若任何服务已在 M11-08 采用新 foundation，则只能回到上一签名
image digest，不能恢复 root、可写 rootfs、mutable tag、无 SBOM/scan/signature 或把五服务重新合并。

剩余 M11 目标为：PR-M11-08 五服务镜像入口、M11-09 Helm/Kustomize、M11-10 probe/preStop/drain、
M11-11 Kind/K3d fault matrix、M11-12 ArcMut/stable/SLO 收口。M12 仍有 6 个 AI Native 工作包。此外，M10
仍缺真实固定硬件 baseline/candidate、原始数据 hash 与 `[HUMAN]` Gate；M11 入口 `[ARCH]`、安全默认迁移
`[HUMAN]`、容器动态 `[TEST]`、Phase 3 和最终目标态 Gate 均未签署。
