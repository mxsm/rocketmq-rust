# R20 五服务容器动态验收证据

## 1. 结论

R20 已在 2026-07-23 由
[Container Foundation run 30011167537](https://github.com/mxsm/rocketmq-rust/actions/runs/30011167537)
完成动态验收。run 绑定 `main` 的完整 commit
`13d50e2d33ddfc1142bba63431b339d07704a4f7`，静态合同、runtime foundation、Broker、NameServer、
Controller、Proxy、MCP 五服务镜像以及供应链证据上传全部成功。

| 项目 | 证据 |
|---|---|
| Workflow run | `30011167537`，结论 `success` |
| Source commit | `13d50e2d33ddfc1142bba63431b339d07704a4f7` |
| Artifact | `container-foundation`，ID `8565842850` |
| Artifact digest | `sha256:bc8172178a0527a049a79d7c6be0d0811501067acb7336df94f50b5447d32a7f` |
| Artifact retention | 创建于 `2026-07-23T13:49:54Z`，到期于 `2026-08-06T13:49:53Z` |
| 动态范围 | foundation + 五服务 build、配置失败、non-root/read-only、SIGTERM、SBOM、漏洞、签名 |
| 明确非目标 | GHCR 发布、生产 keyless image signature、Kind/K3d fault matrix、Phase 3 四方签署 |

该 run 关闭容器动态 `[TEST]` 与 R20，不关闭 R19、R21、R25、M11、Phase 3、`[ARCH]` 或
`[HUMAN]` Gate。31 项最小审查清单更新为完成 **20/31**、剩余 **11**；82 个顶层工作包口径仍为
完成 **75/82**、剩余 **7**。

## 2. Runtime foundation

foundation provenance 与服务 provenance 使用相同 source commit、builder/runtime manifest、Rust toolchain 和
Debian package snapshot。runtime 从零 CRITICAL 的 pinned Ubuntu Noble manifest 构建；CA bundle 由固定
builder/snapshot stage 提供，runtime 不执行可变 package install。

| 字段 | 值 |
|---|---|
| Image ID | `sha256:9c4b500b3c1979d80edeb15edf963a43597523551e979829cce63513d491fc6b` |
| Builder | `docker.io/library/rust:slim-bookworm@sha256:99e09cb2284e2ddbb73a995deee3e91783fd04d177602ccf6eab326d778ee777` |
| Runtime | `docker.io/library/ubuntu:noble@sha256:4fbb8e6a8395de5a7550b33509421a2bafbc0aab6c06ba2cef9ebffbc7092d90` |
| Toolchain / snapshot | `nightly-2026-07-05` / `20260717T000000Z` |
| Runtime identity | `10001:10001` |
| Critical vulnerabilities | `0`，不忽略 unfixed |
| SBOM SHA-256 | `e798f085e7c1284799a6b9d6845583fa9c42b6d1f2e58e173d00c42cab738c23` |
| Trivy SHA-256 | `e9609706ecd7e2137979cc09d6ada357d3a2cd4705d1645022876b324dc67912` |
| Sigstore bundle SHA-256 | `3f135d20c95d8247705a467a484d720f9b1e5454810b75a01091e38550731473` |
| Ephemeral private key retained | `false` |

workflow 实际以 `--network none --read-only` 启动 foundation，确认 `/opt/rocketmq` 不可写、声明的数据
volume 与 UID/GID/mode 固定的 tmpfs 可写；CycloneDX、Trivy 和 Cosign sign/verify-blob 均成功。

## 3. 五服务 provenance

`service-images/provenance.json` 恰好包含五个 owner，无额外 service。每个镜像配置用户均为
`10001:10001`、signal 均为 `SIGTERM`、CRITICAL finding 均为 `0`。

| 服务 | Image ID | SBOM SHA-256 | Trivy SHA-256 | Sigstore bundle SHA-256 |
|---|---|---|---|---|
| Broker | `sha256:aaf97a25d55c530d8ff38970800005743790316042b3670c4ab8d82b101d7af3` | `0f02f5c0e12d52b83f50a623ede307cc07cc6a430f3708e3bab3e0d0d0dfa0f4` | `2098deb5a4e5e9b3aa3bdaa8efc0cf229c353ff741c7b775f2db27b369a8d80f` | `92b2dd5bdf28b2da9cfaabd5c512215d3bb9089276173b35aa4dba2a1ff76547` |
| NameServer | `sha256:37f799738bfda05caa5600e7b075ce309234873041199ba7827bd1d4edbd3623` | `a38bd5903f2b0a9274bdd4d394454cb149b124ab52d0e55d98149981736ee239` | `e3f7c519fa8247e0b76c2501b099f874091c72f3652590373a89bad4d2ae0185` | `bf906b6761e28e793382cfeeeff0a3eecaed2301f7253b0151ad742874cd3678` |
| Controller | `sha256:c5fe012f2d2838581052bc982cfe7dc430877cd6b8c9c45f4cec9616ef7d2000` | `d25fe7254ca0b39eadbb558cba158fd35ac7a9b7684dc3ebe5ab34a5884279e0` | `1d3d07393c37077db54586a5b532ab4ee59dce699dcb61dc143c82863a3e20ba` | `aa35a74e01ce1e1ff7fc1a4f528f28722531ab76e27aa314cac6bf35cd6eb548` |
| Proxy | `sha256:22556ab53b15bf3d3c85e550765ca1906523fabeb3b76d6130b0a84d734ed92e` | `18da4cdf88d7fedf9283427ab3452fdebd41fe09ddfa108117582f3e1dafffb8` | `6519c3dcfa3bed18d5c51e9657933a48347f55c3ec8d60ec45407acaf7bd7978` | `6b4343771001b9046695546d58adab033a1b1af7f20973e09bf9198ad036c2d3` |
| MCP | `sha256:49826f3708d870b748a7cb70ec8af6ed1404e00ee354e56ae61e63ea7fa5b9a3` | `1954e868d9d0ee8fe9bfd0bbe9f9d9217c4bb3e60171c33382e25dd982475334` | `2a437c3fb07c352abca1bebff2602161a6f29777c5a50427e04e74e91083e1ff` | `e45d866f7e9f3449ac9fd6ff3beccbdc5c47d5f51addd25942a70d08db0e0a9a` |

对每个服务，workflow 均完成以下真实动态合同：

1. 显式 target build，runtime 只含 owner binary，entrypoint、command、label、port 与 policy 一致；
2. 缺少必需 config mount 时非零退出；
3. read-only rootfs 下 `/opt/rocketmq` 不可写，声明的数据 volume/tmpfs 可写；
4. 挂载版本化 smoke config 后保持运行，发送真实 SIGTERM，并在 60 秒外层 grace 内以 `0` 退出；
5. Syft 生成 CycloneDX，Trivy 扫描 CRITICAL 且不忽略 unfixed，Cosign 对 SBOM bundle 完成签名与验证；
6. success/failure 路径均删除 ephemeral private key。

MCP 的动态闭环还覆盖空闲 interactive stdin：signal 在 RMCP 初始化前后均由共享 lifecycle waiter 观察，
初始化完成后显式取消并等待 RMCP service；Tokio stdin 不可取消的 blocking read 由 1 秒顶层 runtime teardown
边界收口，应用 audit、owned tasks 与 telemetry 仍先消费共享 45 秒 deadline。

## 4. Artifact 完整性复核

下载 artifact 后执行本地只读复核：

- foundation 与 service provenance 的 source commit 必须完全一致；
- service set 必须恰好为 `broker,controller,mcp,namesrv,proxy`；
- 五服务 user、signal、critical finding 必须分别为 `10001:10001`、`SIGTERM`、`0`；
- 对 15 个 service SBOM/Trivy/Sigstore 文件重算 SHA-256，必须与 provenance 完全一致；
- 五份 SBOM 的 `bomFormat` 必须为 `CycloneDX`；
- foundation/service ephemeral private key 文件均不得存在。

复核输出为：

```text
R20_EVIDENCE_OK commit=13d50e2d33ddfc1142bba63431b339d07704a4f7 services=5 critical=0 private_keys=false
```

## 5. 剩余边界

这些镜像使用本地 verification tag 和 ephemeral blob-signing key；workflow 权限只有 `contents: read`，
未 push GHCR，foundation provenance 的 `published_image_verified` 仍为 `false`。因此本证据不能替代：

- R21 所需的五服务 production 签名 digest、真实 Secret、Kind/K3d fault/rolling/durability 证据；
- R19 固定硬件 baseline/candidate 与 HUMAN 性能验收；
- R25 同一冻结候选快照的 `[ARCH]`、`[REV]`、`[TEST]`、`[HUMAN]` 四方签署；
- R09/R18 next-major compatibility 删除窗口或任何 Phase 4 工作包。
