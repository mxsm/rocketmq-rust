# M11-05 MCP HTTPS、JWKS 与 Principal 传播实施证据

## 1. 结论与进度边界

PR-M11-05 已把 MCP Streamable HTTP 的生产入口收紧为 HTTPS，交付 RS256-only JWKS generation/rotation，
并把已验证 principal 从 Axum middleware 贯通到真实 MCP protocol handler、RBAC、cluster allow-list、rate-limit
和 audit。默认 stdio 入口、stdout JSON-RPC 与输出脱敏合同保持不变；`change-planning` 仍只生成
`mutates_cluster: false` 的不可变计划，没有新增 Apply、`dangerous-tools` 或 destructive endpoint。

工作包完成后总进度为 **69/82**，剩余 13 个工作包：M11 7 个、M12 6 个；下一工作包为 PR-M11-06。
这里的“工作包完成”不替代 M10 真实固定硬件 baseline/candidate 与 `[HUMAN]` Gate，也不替代 M11 入口
`[ARCH]`、安全默认迁移 `[HUMAN]`、M11、Phase 3 或最终目标态 Gate。

| 项目 | 结果 |
|---|---|
| 工作包 | `PR-M11-05` |
| GitHub Issue | `#8278` |
| 主要 owner | `rocketmq-mcp`；复用 `rocketmq-transport` TLS owner |
| 新增依赖边 | `rocketmq-mcp → rocketmq-transport`，目标 DAG policy 显式登记并通过 target/baseline guard |
| 非目标 | M11-06 audit writer/drain、M11-07～12 cloud/fault/SLO、M12 Apply/AI Native |

## 2. HTTPS 与证书代际

- `server.http.public_base_url` 必须是无 userinfo/query/fragment/path 的绝对 HTTPS origin；protected-resource
  metadata 和 401/403 bearer challenge 都从该 origin 生成绝对 URI，不信任请求 `Host` 拼接公开地址。
- Streamable HTTP 启动必须配置 certificate/key，listener 使用 `TlsMode::Enforcing`；没有成功构建的 TLS
  generation 时启动失败，不降级为明文或 permissive。
- MCP 没有复制 certificate watcher。`TlsServerRuntime::accept_stream` 暴露已原子发布 acceptor 的窄握手入口，
  listener 复用 M11-04 的完整 certificate/key 构建、单写者 reload、generation 单调和 last-known-good。
- 真实网络测试生成临时证书并启动 Axum listener：HTTPS `/health` 成功，同端口明文 HTTP 被关闭；transport
  的真实证书、partial/invalid/LKG 与 8 路并发 reload 测试继续通过。
- development token 只允许 loopback bind；public Host allow-list 同时包含显式 public origin host，Origin、1 MiB
  body limit 和 30 秒 request timeout 保持有效。

## 3. JWKS 与 JWT fail-closed 边界

OAuth 不再从环境读取静态 PEM，也不接受 HS256。`jwt_key_env` 只为配置/API 兼容保留且不参与验证；生产模式要求：

- issuer 与 JWKS endpoint 都是绝对 HTTPS URL；JWKS client 强制 HTTPS、禁止 redirect、5 秒 timeout、256 KiB
  response 上限和 64 key 上限。
- token header 必须存在长度受限的 printable `kid`，且 `alg` 精确为 RS256；JWKS key 必须为 RSA/RS256，
  `use` 只能为 `sig`，`key_ops` 必须包含 `verify`，空 keyset、duplicate kid、symmetric key 和算法漂移全部拒绝。
- candidate 文档先完成全量解析与 key 构建，再一次性发布不可变 generation；读者不会观察半份 keyset。
- TTL 到期或 unknown kid 触发 refresh。成功 refresh 可撤下旧 kid；fetch/HTTP/size/parse/key 失败不清空当前
  generation，已知 key 只在 `jwks_max_stale_seconds` 有界窗口内继续验证，初始加载或 unknown kid 失败均拒绝。
- JWT validation 固定 RS256、零 leeway、校验 `nbf`，并强制 `exp`、`iss`、`aud`、`sub`；签名、issuer、
  audience、expiry、required scope 任一失败返回稳定 401/403，不输出 token、kid 对应材料、URL 或底层错误。

## 4. Principal 端到端传播

middleware 把 verified `principal.id`、`client_id/azp`、`roles`、`scope` 和 `rocketmq_clusters` 写入请求扩展。
rmcp 将 HTTP `Parts` 传入 protocol `RequestContext`；handler 在存在 HTTP Parts 但缺认证扩展时返回
`authenticated HTTP context is unavailable`，不能回退 `local-stdio`。没有 HTTP Parts 的 stdio 请求仍使用本地
profile，保持默认 CLI/desktop 行为。

真实 router 测试携带 development bearer token 调用 `tools/call`，故意选择未配置 cluster 以避免外部网络，随后
从实际 audit record 断言 operator=`development-http-client`、client=`development-token` 且不是
`local-stdio`。这证明身份已到达真实 ToolExecutor/Guard，而不是只停留在 middleware 单测。

## 5. 公共 API 与兼容性审查

默认特性 public API 快照对 31 个 library package 全量重建：

- `rocketmq-mcp` public path 从 209 增至 210，唯一新增路径为 `config::HttpTlsConfig`；`HttpConfig`/
  `HttpAuthConfig` 的字段扩展对 TOML/Serde 输入有 default 兼容，但直接使用 Rust struct literal 的 embedder 需要补齐新字段，
  因此按显式配置 API 迁移记录，而不把 path-only 报告误称为完整 source compatibility。旧静态 key 字段保留，
  但 OAuth 明确不再使用静态 fallback。
- `rocketmq-transport` public path 保持 112 且路径指纹不变；`TlsServerRuntime::accept_stream` 只改变 raw rustdoc
  JSON，作为 HTTPS owner 复用的窄 additive method。
- 其余 29 个 package 的 public path count 与 path fingerprint 不变。更新受管 baseline 后，31/31 package
  将使用同一候选快照复核为 `differences=0`。

## 6. 验证矩阵

| Gate | 命令/范围 | 结果 |
|---|---|---|
| MCP default | `cargo check -p rocketmq-mcp`、`cargo test -p rocketmq-mcp` | 通过；74 library + typed boundary + 2 本地 integration，1 个需外部集群 E2E 按合同 ignored |
| MCP all features | `cargo test -p rocketmq-mcp --all-features` | 96 library + typed boundary + 2 本地 integration 通过；计划工具无副作用合同通过 |
| HTTPS/JWKS focused | 真实 TLS/plaintext、metadata/challenge、JWT fail-closed、JWKS rotation/LKG、真实 handler principal | 全部通过 |
| Transport TLS | `cargo test -p rocketmq-transport --features tls tls::tests` | 12/12 通过，含真实证书、mTLS、LKG 和并发 reload |
| MCP strict Clippy | `cargo clippy --all-targets -p rocketmq-mcp --features streamable-http -- -D warnings` | 通过 |
| Rustdoc | `cargo doc -p rocketmq-mcp --no-deps` | 通过 |
| Dependency | target + baseline dependency guard、release guard | 通过；MCP→transport 为显式目标边，无未登记扩张 |
| Runtime/error/routing | enforcing runtime audit、error hygiene、AGENTS routing | 全部通过 |
| Public API | 31 包快照重建与逐路径审查 | MCP +1 additive、transport path 不变；最终 baseline 零差异复核 |
| Root final | `cargo fmt --all -- --check`、workspace all-target/all-feature strict Clippy | 通过 |
| Text | `git diff --check` | 通过 |

Windows/MSVC 的 linker message 与 `proc-macro-error2` future-incompatibility note 是既有工具链输出；strict
Clippy 退出码为 0，没有新增 `allow` 或降低 lint。外部集群 integration 未配置 Namesrv/topic/group/broker，因此按
测试合同 ignored；本工作包的 HTTPS/JWKS/principal 验收不依赖外部 RocketMQ 集群。

## 7. 回滚与剩余目标

代码回滚必须同时回滚 MCP HTTPS listener/JWKS verifier/principal fail-closed、transport 窄握手 API、manifest/
lockfile、目标 DAG policy 和 public API baseline。运行期只能关闭 Streamable HTTP exposure 并保留 stdio/人工 CLI，
或原子恢复仍在 max-stale 内的已验证 JWKS/TLS last-known-good；不得恢复明文、HS256、静态 PEM fallback、无 `kid`
验证、请求 Host 元数据拼接或 HTTP→`local-stdio` 身份回退。

剩余 M11 目标为：PR-M11-06 audit writer/drain、M11-07～10 镜像与 Helm/Kustomize/probe/drain、M11-11
Kind/K3d fault matrix、M11-12 ArcMut/stable/SLO 收口。M12 仍有 6 个 AI Native 工作包。除此之外，M10 仍缺
真实固定硬件 baseline/candidate、原始数据 hash 与 `[HUMAN]` Gate；M11 入口 `[ARCH]`、安全默认迁移
`[HUMAN]`、Phase 3 和最终目标态 Gate 都未签署。
