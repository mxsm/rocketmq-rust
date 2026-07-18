# M11-02 SecretProvider 基础合同与本地 Adapter 实施证据

## 1. 目标完成结论

PR-M11-02 已冻结 runtime-neutral `SecretProvider` 合同，并在 `rocketmq-auth` 交付显式 registry、白名单环境
adapter 与受限加密文件 adapter。工作包完成后总进度为 **66/82**，剩余 16 个工作包：M11 10 个、M12
6 个；下一工作包为 PR-M11-03。

这里的“工作包完成”不等于 secure profile 或 M11 完成。安全默认值、一次性 bootstrap、rotation/reload、
MCP HTTPS/audit、镜像、Helm/Kustomize、Kind/K3d fault matrix 和 ArcMut/stable/SLO 收口仍属于
PR-M11-03～12。M10 的真实固定硬件 baseline/candidate 与 `[HUMAN]` Gate 也仍待验收。

| 项目 | 值 |
|---|---|
| 工作包 | `PR-M11-02` |
| Issue | `#8272` |
| Branch | `mxsm/architecture-refactor-secret-provider-contract` |
| API candidate snapshot | `77416b46bd9627b73201705459d3c39a6c57e084` |
| 日期 | 2026-07-18 |
| 合同 owner | `rocketmq-security-api/src/secret_provider.rs` |
| adapter owner | `rocketmq-auth/src/secret_provider/` |

## 2. 合同与依赖边界

`rocketmq-security-api` 只新增 `zeroize` 这一个内存卫生依赖，没有 AES、文件 adapter、Tokio、网络客户端或
生产 provider 实现依赖。公共边界保持同步、object-safe 且由 composition root 注入：

| 合同 | 冻结语义 |
|---|---|
| `SecretProviderId` / `SecretName` | 最长 128B，只允许 ASCII 字母数字和 `-_.`；拒绝空值、隐藏名、分隔符与 traversal |
| `SecretMaterial` | 非空、不可 Clone、Debug 恒为 `[REDACTED]`；显式 `Zeroize` 和 Drop 都清零底层字节 |
| capability | typed access、persistence、versioning enum，不使用位置布尔值 |
| `SecretProvider` | `Send + Sync` 同步 trait；read 返回 material+可选版本，write 消费 material 并执行 optimistic version |
| error | 不包含 secret、logical name、环境变量名或路径；缺失 provider、权限、冲突、envelope 和平台均 fail closed |
| registry | 由 composition root 持有的显式 `BTreeMap`；无全局单例、无默认 fallback、重复 ID 不替换 |

保留原有泛型 `Secret<T>` 以维持已有签名 API 的类型兼容；新的字节材料使用专用 `SecretMaterial`，没有给旧
泛型增加 `Zeroize` trait bound。生产 Vault/KMS/Kubernetes Secret/OS credential provider 不进入本工作包，
未来只能通过相同 trait 显式注入。

## 3. 本地 Adapter 与文件语义

环境 adapter 构造时必须提供 logical name 到环境变量名的完整 allowlist；未映射 logical name 返回
`NotFound`，write 恒定返回 `ReadOnly`。Debug 只显示 provider ID 和映射数量，不显示环境变量名或值，也
不存在把任意 secret name 直接转换为环境变量查询的路径。

Unix 文件 adapter 的持久化合同如下：

- provider root 和每个 secret 目录必须为 owner-only，现有宽权限目录或 symlink fail closed；
- key 必须为 32B，material 最大 1 MiB；envelope 使用 AES-256-GCM 和随机 96-bit nonce；
- AAD 绑定 schema、logical name 和 `u64` version，换名、换版本或修改 ciphertext 都无法通过认证；
- envelope 为固定 magic + nonce + ciphertext length + ciphertext 的紧凑二进制，不写 pretty JSON 或明文；
- `None` expected version 仅允许创建；更新必须提交当前版本，旧版本产生 `VersionConflict`；
- 每个版本写入 owner-only 临时文件、`sync_all`、改为只读后，以同目录 hard-link no-replace 原子发布；
- 版本文件按 20 位十进制编号且不覆盖，reader 只选择已发布最大版本，崩溃遗留 `.tmp` 不可见；
- Windows 在没有可证明 owner-only 的 ACL verifier 前，构造 adapter 即返回 `UnsupportedPlatform`。

文件 adapter 是开发/本地受限实现，不是生产 secure profile 默认。PR-M11-03 仍需单独完成安全 profile、
bootstrap material/readiness 和部署迁移决策。

## 4. 失败注入与公共兼容面

Windows focused tests 直接验证文件 adapter fail closed；WSL/Ubuntu 24.04 使用独立 target 实际执行 Unix
权限与文件系统语义，覆盖 0755 root 拒绝、0600/0400 owner-only、两次原子版本发布、旧版本冲突、磁盘无
明文和 ciphertext tamper 拒绝。平台无关测试还覆盖 name/version AAD、logical name traversal、Debug
redaction、显式 zeroize、provider 缺失/重复、环境 allowlist 和只读写拒绝。

默认特性 public API 快照审查显示：

- `rocketmq-auth` 公共路径从 199 增至 203，只增加 `secret_provider` 模块和三个 facade re-export；
- `rocketmq-security-api` 公共路径从 58 增至 94，只增加 provider 合同及根级精确 re-export；
- 其余 29 个 package 的公共路径数量和路径指纹不变；没有删除、重命名或改变现有 public item；
- 依赖图加入 AES/zeroize 后 Rustdoc 内部 crate ID 使部分 raw JSON hash 改变，已与路径指纹变化分开审查。

## 5. 验证矩阵

| Gate | 命令/范围 | 结果 |
|---|---|---|
| Security contract | `cargo test -p rocketmq-security-api` | 3 个新合同测试 + 7 个既有安全合同测试通过 |
| Auth full | `cargo test -p rocketmq-auth --all-features` | 348 library + 9 Java alignment + 3 SecretProvider + 1 identity，通过；59 doctest ignored |
| Windows focused | `cargo test -p rocketmq-auth --test secret_provider_contract` | 3/3 通过，包含 ACL 未验证 fail closed |
| Linux focused | WSL/Ubuntu 独立 target 执行同一测试 | 4/4 通过，包含真实 owner-only/atomic/tamper 测试 |
| Package Clippy | security-api/auth all-target/all-feature `--no-deps -D warnings` | 通过 |
| Public API | 31 包默认特性快照重新生成并审查 | 仅 auth +4、security-api +36 additive；最终基线复核 differences=0 |
| Dependency | `architecture_dependency_guard.py --mode target` | 通过，security-api 保持 contract leaf |
| Root final | `cargo fmt --all -- --check`；workspace all-target/all-feature strict Clippy | 32 package 通过 |
| MCP | check、73+1+2 tests、streamable-http strict Clippy、doc | 全部通过；1 个外部集群 E2E 按合同 ignored |
| Standalone | example、Tauri、Web backend fmt/strict Clippy；Web all-target build | 全部通过 |
| Error/redaction | `check-error-hygiene.ps1`、`error_architecture_guard.py` | 初次捕获 Debug field 命名，修复后全部通过 |
| Dependency baseline | target + baseline guard | 全部通过，security-api 保持 contract leaf |
| Routing | `check-agents-routing.ps1` | `AGENTS_ROUTING_CHECK_OK` |
| Text | `git diff --check` | 通过 |

Windows/MSVC 既有 linker message 与 `proc-macro-error2` future-incompatibility note 不受本变更影响；strict
Clippy 退出码为 0，未通过 allow 或 baseline 扩张掩盖。Linux 测试使用 WSL 独立 target，未复用 Windows
测试二进制。

## 6. 回滚与剩余风险

回滚应停止在 composition root 注册新 provider，并同时回滚 auth adapter、security-api additive 合同、依赖和
API 基线；原有 secure dry-run、泛型 `Secret<T>` redaction 与受限 secret file 检查继续保留。不得回滚为
pretty JSON、明文文件、宽松权限、任意环境变量查询或缺失 provider 时的静默 fallback。

本地文件 adapter 只承诺单机文件系统和进程内 writer 序列化；跨主机生产一致性、KMS/Vault 身份、rotation、
审计和自动 reload 尚未交付。AES-GCM 随机 nonce 受每 key 调用上限约束，生产 provider 必须提供独立的密钥与
rotation 策略，不能把本地 adapter 当作无限生命周期的生产 secret store。
