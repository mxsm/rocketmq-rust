# M11-07 容器镜像基础与供应链门禁实施证据

## 1. 结论与进度边界

PR-M11-07 已交付与旧组合镜像隔离的 container foundation：builder/runtime 输入按 manifest digest 固定，
Rust nightly 与 Debian package repository 按日期固定；runtime 约束为数字 non-root、read-only rootfs、仅声明
data volume 与 tmpfs 可写，并提供 SBOM、零 CRITICAL 漏洞、Sigstore bundle 和 digest-only keyless image
signature 的执行模板与最小权限 workflow。

工作包完成后总进度为 **71/82**，剩余 11 个工作包：M11 5 个、M12 6 个；下一工作包为 PR-M11-08。
这里的 71/82 是本工作包结束时的历史进度。其后 R20 已由
[Container Foundation run `30011167537`](https://github.com/mxsm/rocketmq-rust/actions/runs/30011167537)
在 main commit `13d50e2d33ddfc1142bba63431b339d07704a4f7` 上完成 foundation 与五服务动态验证；完整证据见
[`11-container-dynamic-evidence.md`](11-container-dynamic-evidence.md)。该 run 关闭容器动态 `[TEST]`，但不等于
M11 入口 `[ARCH]`、安全默认迁移 `[HUMAN]`、M10、R21、R25、Phase 3 或最终目标态 Gate 完成。

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
| Runtime manifest | `docker.io/library/ubuntu:noble@sha256:4fbb8e6a8395de5a7550b33509421a2bafbc0aab6c06ba2cef9ebffbc7092d90` |
| Rust toolchain | `nightly-2026-07-05`；官方 dist manifest 可用 |
| Debian package snapshot | `20260717T000000Z`；Debian 与 security 的 bookworm Release 可用 |
| Syft / Trivy / Cosign | `v1.48.0` / `v0.72.0` / `v3.1.2` |
| GitHub Actions | checkout/buildx/tool installers/upload 均固定完整 40 位 commit SHA |

Builder snapshot 安装 workspace native build 所需 clang/LLVM/CMake/Ninja/protobuf/OpenSSL 工具；runtime 从独立
pinned Ubuntu Noble manifest 开始，只复制固定 Debian snapshot stage 生成的 CA bundle，不在 runtime stage
执行 package resolution，也不继承 builder layer 或复制 repository source、编译器和调试产物。Hadolint 的
DL3008 只在使用 immutable snapshot 的 builder RUN 上窄化忽略，并在相邻注释记录理由。

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

PR-M11-07 完成时 Windows host 与 Ubuntu WSL 均无 Docker/Podman/Buildah/Syft/Trivy/Cosign，因此当时没有
生成动态 artifact。后续 R20 run `30011167537` 已在同一 source commit 上成功执行 foundation 与五服务套件，
上传 artifact `container-foundation`（ID `8565842850`），archive digest 为
`sha256:bc8172178a0527a049a79d7c6be0d0811501067acb7336df94f50b5447d32a7f`。该 artifact 已通过 source
commit、五服务集合、15 个供应链文件 hash、CycloneDX 格式、零 CRITICAL 与 private-key absence 本地复核。

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
| Dynamic image suite | Docker/Syft/Trivy/Cosign workflow | R20 run `30011167537` 成功；foundation + 5 services，artifact digest `sha256:bc8172178a0527a049a79d7c6be0d0811501067acb7336df94f50b5447d32a7f` |

本工作包没有修改 Rust、Cargo manifest、公共 API、wire/storage 或运行时代码，因此不触发 Cargo、public API、
runtime ownership、typed-error 或 RocksDB specialized gate。

## 6. 回滚与剩余目标

回滚必须整体移除 foundation Dockerfile、policy、guard/tests、supply-chain script、workflow 与本证据。旧组合
Dockerfile 在 M11-07 期间仍是兼容路径；若任何服务已在 M11-08 采用新 foundation，则只能回到上一签名
image digest，不能恢复 root、可写 rootfs、mutable tag、无 SBOM/scan/signature 或把五服务重新合并。

R20 容器动态 `[TEST]` 已关闭。当前仍需 R21 的 production 签名 digest、真实 Secret 与 Kind/K3d
fault/rolling/durability 证据，R25 的同一冻结候选快照四方签署，以及 R19 固定硬件 baseline/candidate、
原始数据 hash 与 `[HUMAN]` 性能验收；M11 入口 `[ARCH]`、安全默认迁移 `[HUMAN]`、Phase 3 和最终目标态
Gate 均未签署。
