# M11-03 Secure Profile 与一次性 Bootstrap 实施证据

## 1. 目标完成结论

PR-M11-03 已交付新部署安全默认解析、既有部署显式兼容迁移报告、secure fail-closed readiness 合同，以及
owner-only 的一次性首管理员 bootstrap 状态机。工作包完成后总进度为 **67/82**，剩余 15 个工作包：
M11 9 个、M12 6 个；下一工作包为 PR-M11-04。

这里的“工作包完成”指目标实现与工程验证完成，不替代 `[HUMAN]` 对安全默认迁移策略的签署，也不等于 M11 或
Phase 3 完成。credential/certificate rotation 与原子 reload、MCP
HTTPS/JWKS/principal、不可抵赖 audit、镜像、Helm/Kustomize、Kind/K3d fault matrix 和 ArcMut/stable/SLO
收口仍属于 PR-M11-04～12。M10 的真实固定硬件 baseline/candidate 与 `[HUMAN]` Gate 也仍待验收。

| 项目 | 值 |
|---|---|
| 工作包 | `PR-M11-03` |
| Issue | `#8274` |
| Branch | `mxsm/architecture-refactor-secure-profile-bootstrap` |
| API candidate snapshot | `7ee819264789154ed157aea56021ec033077542f` |
| 日期 | 2026-07-18 |
| profile/readiness owner | `rocketmq-security-api/src/secure_deployment.rs` |
| bootstrap owner | `rocketmq-auth/src/bootstrap.rs`、`rocketmq-auth/src/bootstrap/state_file.rs` |

## 2. 安全 Profile 与 Readiness 合同

`rocketmq-security-api` 的新合同不依赖 Tokio、auth adapter 或网络实现。composition root 必须显式提供部署来源，
不能通过缺省值猜测部署身份：

| 输入/状态 | 冻结语义 |
|---|---|
| 新部署、profile 未指定 | 解析为 `secure`，不产生迁移债务 |
| 已识别既有部署、profile 未指定 | 保持 `compatibility`，返回 `CompatibilityProfileMustBePersisted` |
| 显式 development/compatibility | 保留所选行为，同时返回 `MigrationToSecurePending` |
| unknown profile | 返回 typed error，不回退 compatibility/development |
| secure trust/provider | trust anchor 必须存在、可打开且为 regular file；SecretProvider 必须已显式注册 |
| secure 身份来源 | 必须且只能选择预置管理员身份或一次性 bootstrap；零个和多个都不 ready |
| bootstrap readiness | material 必须可用、expiry 必须在未来、listener 必须已验证 TLS |
| insecure downgrade | secure profile 直接记录 `InsecureDowngrade`，readiness 保持 false |

旧 `AuthConfig::default()` 没有被静默修改，因为它仍是已部署兼容面的组成部分；新部署入口必须使用
`resolve_security_profile`/`validate_deployment_security` 冻结来源与迁移决策。后续镜像和 Helm composition root
只能消费该决策，不得重新实现另一套默认规则。

## 3. 一次性 Bootstrap 状态机

`BootstrapGrant` 消费 `SecretMaterial`，拒绝短于 32 字节的 proof，只保留 SHA-256 verifier，并把 grant 绑定到
cluster ID、管理 listener 与 Unix expiry。enrollment 先检查 expiry、已验证 TLS、cluster/listener 和 constant-time
digest，再尝试持久化 claim；任何验证失败都发生在 claim 与首管理员 callback 之前。

```text
available --atomic no-replace claim--> claimed --provision first admin--> consumed
    |                                     |                              |
    +-- invalid/expired/TLS/binding ------+-- callback/persist failure --+-- replay/restart reject
```

- claim 使用同目录 owner-only 临时文件、`sync_all`、0400 权限、hard-link no-replace 与目录 fsync，跨进程只允许一个
  winner；
- 首管理员 provisioner 在 claim 成功后调用，callback 期间不持有 coordinator mutex，并且 provisioner 必须原子拒绝
  已存在管理员；
- provisioner 失败或最终 consumed 写入失败时状态保持 claimed，endpoint 不会重新打开；
- consumed 使用同目录临时文件与 atomic rename 发布，重启同时观察 main state 和 claim marker；
- persisted JSON 只含 schema、grant verifier、binding、expiry、状态、principal 和证书指纹，不含原始 proof；状态与身份
  字段不一致、过大记录、symlink、非 regular file 或宽松权限全部 fail closed；
- Windows 在没有可证明 owner-only 的 ACL verifier 前返回 `UnsupportedPlatform`，不以普通文件权限猜测安全性。

本工作包提供 coordinator 与注入边界，不创建网络 endpoint。MCP HTTPS/JWKS/principal 传播由 PR-M11-05 交付，
不可抵赖 audit writer 与 shutdown drain 由 PR-M11-06 交付。

## 4. 失败注入与公共兼容面

Windows focused test 验证 ACL 未知时拒绝持久化。WSL/Ubuntu 24.04 使用真实 Unix filesystem 执行 7 个测试，覆盖
0700/0755 目录、0400 状态文件、成功消费、重启重放、过期、非 TLS、跨 cluster/listener、错误 proof、provisioner
失败、两个 coordinator 并发竞争以及损坏状态记录。成功路径额外扫描 state/claim 文件，确认原始 32B proof 不存在。

默认特性 public API 快照审查结果：

- `rocketmq-auth` 公共路径从 203 增至 230，27 个新增路径全部属于 `bootstrap` module、类型、trait、error/status
  variant；
- `rocketmq-security-api` 公共路径从 94 增至 122，28 个新增路径全部属于 `secure_deployment` module、resolver、
  readiness 类型和 failure/migration variant；
- 其余 29 个 package 的公共路径数量与路径指纹不变；没有删除、重命名或修改既有 public path；
- 部分依赖者的 raw rustdoc JSON hash 因新增依赖文档 crate ID 变化而改变，已与路径指纹变化分开审查。

## 5. 验证矩阵

| Gate | 命令/范围 | 结果 |
|---|---|---|
| Security API | `cargo test -p rocketmq-security-api` | 8 library + 7 contract 通过 |
| Auth full | `cargo test -p rocketmq-auth --all-features` | 349 library 与全部 integration 通过；59 doctest 按既有标记 ignored |
| Windows focused | `cargo test -p rocketmq-auth --test secure_bootstrap_contract` | 1/1 通过，验证 ACL 未知 fail closed |
| Linux focused | WSL/Ubuntu 运行同一测试 | 7/7 通过，覆盖真实权限、原子状态、重启、并发和损坏状态 |
| Package Clippy | security-api/auth all-target/all-feature `--no-deps -D warnings` | 通过 |
| Public API | 31 包默认特性快照重建、逐路径审查并更新基线 | 仅 auth +27、security-api +28 additive |
| Dependency | target + baseline dependency guard | 通过；security-api 保持 contract leaf |
| Root final | `cargo fmt --all -- --check`、workspace all-target/all-feature strict Clippy | 32 package 通过 |
| MCP | check、73+1+2 tests、streamable-http strict Clippy、doc | 全部通过；1 个外部集群 E2E 按合同 ignored |
| Standalone | example、Tauri、Web backend fmt/strict Clippy；Web all-target build | 全部通过 |
| Error/redaction | `check-error-hygiene.ps1`、`error_architecture_guard.py` | 全部通过 |
| Routing/text | `check-agents-routing.ps1`、`git diff --check` | 全部通过 |

Windows/MSVC 既有 linker message 与 `proc-macro-error2` future-incompatibility note 不受本变更影响；strict Clippy
退出码为 0，没有增加 allow、lint 降级或依赖/错误架构 baseline 例外。Linux 用例使用 WSL 的 Unix 二进制和真实文件
权限，不复用 Windows 测试结果。

## 6. 回滚与剩余风险

回滚必须同时回滚 resolver/readiness 合同、bootstrap coordinator、`subtle` 依赖、四份 lockfile 与公共 API 基线。
已识别的既有部署可显式固定 `compatibility` 并保留迁移报告；新 secure 部署若材料不完整必须保持 not ready，不能改为
匿名或宽松启动。已经持久化为 claimed/consumed 的 token 不得通过代码回滚复活。

剩余边界包括 Windows owner-only ACL verifier、实际 HTTPS endpoint、生产 SecretProvider、credential/certificate
rotation、JWKS、审计 writer 与云部署 readiness/drain E2E。bootstrap verifier 的安全性依赖至少 32B 高熵 proof；
部署系统必须通过受限挂载或继承 secret fd 注入，不得把 material 放入命令行、环境 dump、日志或通用 config snapshot。
