# M11-04 Credential/Certificate Rotation 与原子 Reload 实施证据

## 1. 目标完成结论

PR-M11-04 已交付 credential rotation、certificate reload、last-known-good rollback、撤销与限时 break-glass
合同。认证请求读取单个不可变 generation；写路径在完整验证和审计成功后才发布。TLS reload 在 certificate、key、
trust 全部构建成功后，把 acceptor 与 generation 一起原子发布；手动 reload 与文件 watcher 共享写者串行化。

工作包完成后总进度为 **68/82**，剩余 14 个工作包：M11 8 个、M12 6 个；下一工作包为 PR-M11-05。
这里的“工作包完成”只表示目标实现、审查与工程验证完成，不替代未签署的 M11 入口 `[ARCH]`、安全默认迁移
`[HUMAN]`、M10 真实固定硬件验收，也不等于 M11、Phase 3 或最终目标态 Gate 完成。

| 项目 | 值 |
|---|---|
| 工作包 | `PR-M11-04` |
| Issue | `#8276` |
| Branch | `mxsm/architecture-refactor-credential-rotation` |
| 实现/API candidate snapshot | `a4bd7f849c791cff4ea5f47ae18003fb3d7b4325` |
| 日期 | 2026-07-18 |
| Credential owner | `rocketmq-auth/src/credential_rotation.rs`、`credential_rotation/model.rs` |
| TLS owner | `rocketmq-transport/src/tls.rs` |
| Compatibility facade | `rocketmq-remoting/src/tls.rs` |

## 2. Credential rotation 状态机

`CredentialRotationManager` 使用 `ArcSwap<RotationState>` 持有 active、retiring、revoked、break-glass 与 generation。
读路径只执行一次 snapshot load；写路径由 mutex 串行，并在所有校验及同步 audit sink 成功后一次 store：

```text
generation N: active=A
    -- validate B + audit authorized --> generation N+1: active=B, retiring=A(until T)
    -- after T + audit revoke -------> generation N+2: active=B, revoked={A}
    -- before revoke + audit rollback -> generation N+2: active=A, revoked={B}
```

- `ValidatedCredential` 只保留证书 SHA-256 元数据和 proof digest；原始 `SecretMaterial` 在构造后不进入 snapshot、
  `Debug`、错误或 audit。
- rotation candidate 必须当前有效、ID 不冲突、未撤销，overlap 必须同时落在新旧 credential 有效期内。
- overlap 内 active 与 retiring 均可验证；到期后 retiring 立即不再被读路径接受，finalize 将其加入 revoked 集合。
- rollback 只能恢复未撤销且未过期的 retiring last-known-good，并把失败 active 加入 revoked；已撤销材料不能复活。
- verification 对 proof digest 使用 constant-time equality；返回值的 `Debug` 隐去 credential ID。
- `CredentialAuditOutcome::Authorized` 表示 audit sink 已授权本次发布；它不虚构 store 之后才可能成立的
  “Applied”语义。audit sink 失败时 generation 和整个 snapshot 保持不变。

## 3. Provider reload 与 break-glass

`reload_from_provider` 每次只读取一个 `VersionedSecret`，把 material 与同一 provider version 一次性交给注入的
`CredentialBundleParser`。parser 必须先验证 certificate/key/proof 的完整关系；返回 version 与 provider version
不一致、provider 不可用或解析失败时都会记录 rejected audit，并保留当前 snapshot。

break-glass 的冻结语义如下：

- 可选 credential 在 manager 创建时配置，但默认 `Disabled`，不存在隐式启用路径。
- 启用必须携带 typed `BreakGlassReason`，expiry 必须晚于当前时间且不超过 owner 配置的最大持续时间和 credential
  自身有效期。
- enable/disable 都先写 audit 再发布新 generation；过期后验证 fail closed，不因状态尚未清理而继续接受。
- break-glass ID 不能与 active 或 rotation candidate 冲突，且不作为已撤销 credential 的旁路。

## 4. TLS certificate 原子发布

`TlsServerRuntime` 的 slot 从单独 acceptor 改为 `VersionedTlsAcceptor { generation, acceptor }`，请求路径一次 load
同时取得二者。`reload_now_with_report` 保留原有 `reload_now() -> Result<()>` 兼容包装，并报告 previous/active
generation 与 changed 状态。

- certificate、private key、client trust 与完整 rustls acceptor 在注入的 `BlockingExecutor` 上构建；任何读取、PEM、
  key mismatch、trust 或 generation overflow 错误都发生在 store 前。
- 手动 reload 与 watcher 共享 `tokio::sync::Mutex` 写者边界，完整构建、generation 推进和 publish 串行；8 路并发
  reload 精确发布 generation 2～9。
- watcher 只有成功发布后才推进文件 snapshot。partial certificate/key 更新或 invalid PEM 会被持续重试，并继续使用
  last-known-good acceptor。
- `rocketmq-remoting` 只做兼容 facade re-export/delegate；没有复制 TLS 状态或另建 reload 实现。

真实证书测试使用两套独立 CA：只替换新 certificate 时因旧 key mismatch 拒绝且 generation 保持 1；补齐新 key 后
发布 generation 2，新 CA 成功而旧 CA 失败；再写入无效 PEM 时仍由 generation 2 的新证书完成握手。

## 5. 公共 API 审查

默认特性 public API 快照对 31 个 library package 全量重建：

- `rocketmq-auth` public path 从 230 增至 284，新增 54 条全部属于 `credential_rotation` module、状态 DTO、
  manager/parser/audit trait、typed error/action/status variant。
- `rocketmq-transport` public path 从 111 增至 112，唯一新增路径为 `rocketmq_transport::tls::TlsReloadReport`。
- `rocketmq-remoting` public path 保持 207 且路径指纹不变；新增方法与 re-export 只改变 raw rustdoc JSON。
- 其余 28 个 package 的 path count 与 path fingerprint 均不变；全体除 auth/transport 外共 29 个 package 没有
  public path 漂移。15 个依赖消费者仅 raw rustdoc JSON hash 改变，已与路径变化分开审查。
- 更新受管基线后，`--from-existing` 复核结果为 `packages=31 differences=0`。

## 6. 验证矩阵

| Gate | 命令/范围 | 结果 |
|---|---|---|
| Credential focused | `cargo test -p rocketmq-auth --all-features --test credential_rotation_contract` | 6/6 通过：overlap/finalize、rollback、break-glass、provider failure、并发读、redaction |
| Auth/Remoting full | `cargo test -p rocketmq-auth --all-features`、`cargo test -p rocketmq-remoting --all-features` | 全部通过，含 doctest |
| Transport full | `cargo test -p rocketmq-transport --all-features` | 29 library + 全部 integration 通过；9 个既有示例 doctest 按标记 ignored |
| TLS focused | real certificate/LKG 与 8 路 concurrent reload 两项测试 | Windows 2/2、WSL/Ubuntu 2/2 通过 |
| Linux credential | WSL/Ubuntu 运行 credential contract | 6/6 通过 |
| Feature boundary | transport/remoting `cargo check --no-default-features` | 通过 |
| Package Clippy | auth/transport/remoting all-target/all-feature `--no-deps -D warnings` | 全部通过 |
| Rustdoc | auth/transport/remoting `cargo doc --no-deps` | 通过；仅 auth 旧文件存在既有 broken-link warning |
| Public API | 31 包快照重建、逐路径审查、更新基线、零差异复核 | auth +54、transport +1 additive；最终 31/31 differences=0 |
| Root final | `cargo fmt --all -- --check`、workspace all-target/all-feature strict Clippy | 32 package 通过 |
| MCP | check、73+1+2 tests、streamable-http strict Clippy、doc | 全部通过；1 个外部集群 E2E 按合同 ignored |
| Standalone | example、Tauri、Web backend fmt/strict Clippy；Web all-target build | 全部通过 |
| Error/redaction | `check-error-hygiene.ps1`、`error_architecture_guard.py` | 全部通过 |
| Dependency | target + baseline dependency guard | 通过：35 条 production ledger、3 条 dev-only edge，无未登记扩张 |
| Runtime/routing | enforcing runtime audit、AGENTS routing check | 全部通过 |
| Text | `git diff --check` | 通过 |

第一次 broad Windows 构建时 D 盘达到 0 可用空间，MSVC PDB/ring 报 `No space left on device`；这不是代码测试
失败。按总体目标授权执行根 workspace `cargo clean`，删除 139,867 个文件并释放约 167.2 GiB，随后从 focused、
full package、workspace 到 standalone 的全部最终命令均重新执行并通过。Windows/MSVC linker message 与
`proc-macro-error2` future-incompatibility note 为既有工具链输出；strict Clippy 退出码为 0，没有新增 allow 或降低 lint。

## 7. 回滚与剩余目标

代码回滚必须同时回滚 auth rotation owner、transport/remoting TLS API、`arc-swap` direct dependency、四份 lockfile
和 public API 基线。运行期回滚只能原子恢复已验证、未撤销且未过期的 last-known-good；不得通过代码回滚移除
revocation、复活失败 candidate、延长 break-glass，或在 TLS 材料无效时降级到半份配置/明文。

剩余 M11 目标为：PR-M11-05 MCP HTTPS/JWKS/principal、M11-06 audit writer/drain、M11-07～10 镜像与
Helm/Kustomize/probe/drain、M11-11 Kind/K3d fault matrix、M11-12 ArcMut/stable/SLO 收口。M12 仍有 6 个 AI
Native 工作包。除此之外，M10 仍缺真实固定硬件 baseline/candidate、原始数据 hash 与 `[HUMAN]` Gate；M11 入口
`[ARCH]`、安全默认迁移 `[HUMAN]`、Phase 3 和最终目标态 Gate 都未签署。
