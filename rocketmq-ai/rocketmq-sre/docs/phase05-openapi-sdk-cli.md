# Phase 05 OpenAPI、SDK 与只读 CLI

## 目标

Phase 05 对外提供同一套版本化、可生成、可测试的操作接口：

- `openapi/rocketmq-sre-phase05.openapi.json` 是 UI、SDK 和外部集成的
  canonical API 契约。
- `rocketmq-sre-client` 是最小 Rust 只读客户端。
- `sdk/typescript` 是最小 TypeScript 只读客户端。
- `rocketmq-sre-cli` 提供固定的运维查询命令和本地 typed draft 校验。

三种客户端只覆盖 status、cluster、incident、inspection、plan 和 OpenAPI
读取。它们不暴露通用 HTTP request、shell、raw Admin、审批、执行或集群
mutation 接口。

## OpenAPI

Phase 05 文档由 Phase 03 契约增量生成：

```powershell
node scripts/generate_phase5_openapi.mjs
npm --prefix ui run generate:api
npm --prefix ui run check:api
```

新增域包括：

| 域 | 公开能力 |
| --- | --- |
| Fleet | Overview、Cluster、Quota、Regional Endpoint、Asset、Compliance、Inspection |
| DR | Plan、Backup Asset、Exercise、Checkpoint、Finding、Action Item |
| Governance | Artifact、Version、Impact、Admission、Audit、Compliance |
| FinOps | Ledger、Budget、Decision、Allocation Policy、Report |

GET 操作声明 `rocketmq:read`。管理操作分别声明
`rocketmq:fleet:manage`、`rocketmq:dr:manage`、
`rocketmq:governance:manage` 和 `rocketmq:finops:manage`。OpenAPI 明确冻结
以下边界：

- R1 只能在已授权的 bounded autonomy 范围内运行。
- R2 始终要求监督执行。
- R3 对 Execution Agent 不可达。
- 不支持任意 unattended mutation。
- DR 不提供 production cutover。
- CLI 只允许读取和本地 typed draft。

Control Plane 的 `GET /v1/openapi.json` 直接返回这份 checked-in 文档。服务端
测试会验证全部路径、唯一 `operationId`、响应契约、schema 引用和上述安全
边界。

## Rust client

最小示例：

```rust,no_run
use rocketmq_sre_client::Client;
use rocketmq_sre_contracts::ClusterId;

# async fn example() -> Result<(), rocketmq_sre_client::ClientError> {
let cluster_id = ClusterId::new(); // Replace with an authorized cluster ID.
let client = Client::builder("https://sre.example.com/")?
    .bearer_token("OIDC access token")
    .allowed_clusters([cluster_id])
    .build()?;

let status = client.status().await?;
let cluster = client.cluster(cluster_id).await?;
println!("{} {:?}", status.status, cluster.state);
# Ok(())
# }
```

实际应用不要硬编码 token，应从组织的 SecretProvider、workload identity 或
短期 OIDC token provider 注入。`Client` 不实现 `Debug`；Authorization header
被标记为 sensitive；redirect 被禁用，避免 token 跨源转发；响应受 byte limit
约束。配置 cluster allowlist 后，cluster、incident、inspection 和 plan 响应会
再次执行客户端侧 scope 校验。

可用方法：

- `status`
- `readiness`
- `openapi`
- `clusters`
- `cluster`
- `incident`
- `inspection`
- `plan`

## TypeScript SDK

安装与验证：

```powershell
npm ci --prefix sdk/typescript
npm test --prefix sdk/typescript
```

使用示例：

```typescript
import {
  SreClient,
  createLocalPlanDraft,
} from "@rocketmq-rust/sre-client";

const client = new SreClient({
  baseUrl: "https://sre.example.com/",
  token: async () => obtainShortLivedOidcToken(),
  allowedClusters: ["11111111-1111-4111-8111-111111111111"],
});

const clusters = await client.clusters();
const incident = await client.incident(
  "33333333-3333-4333-8333-333333333333",
);

const draft = createLocalPlanDraft({
  cluster_id: clusters[0].id,
  incident_id: incident.incident.id,
  diagnosis_revision_id: "44444444-4444-4444-8444-444444444444",
  steps: [{
    action_id: "rocketmq.broker.config.plan",
    descriptor_version: "1.0.0",
    resource: "broker-a",
    parameters: { maxMessageSize: 4194304 },
    evidence_ids: ["55555555-5555-4555-8555-555555555555"],
  }],
});
```

`createLocalPlanDraft` 只复制并校验本地数据，不进行网络请求，输出固定包含
`mode: "local_only"`，不携带 approval 或 execution authority。

SDK 的 fetch 固定使用 GET 和 `redirect: "error"`；token provider 的值不会进入
异常；响应按 stream 受 byte limit 约束；非 JSON 错误不会回显原始 response
body。

## CLI

查看完整命令：

```powershell
cargo run --locked -p rocketmq-sre-cli -- --help
```

建议把 token 放入短生命周期环境变量，禁止作为参数传递：

```powershell
$env:ROCKETMQ_SRE_URL = "https://sre.example.com/"
$env:ROCKETMQ_SRE_TOKEN = "<short-lived OIDC token>"

cargo run --locked -p rocketmq-sre-cli -- status
cargo run --locked -p rocketmq-sre-cli -- clusters
cargo run --locked -p rocketmq-sre-cli -- `
  --allow-cluster 11111111-1111-4111-8111-111111111111 `
  incident 33333333-3333-4333-8333-333333333333
```

远程命令固定为：

- `status`
- `readiness`
- `openapi`
- `clusters`
- `cluster <UUID>`
- `incident <UUID>`
- `inspection <UUID>`
- `plan <UUID>`

CLI 明确拒绝 `--token`，避免 secret 出现在进程列表和 shell history。
`--token-env` 只能使用大写 ASCII、数字和下划线。远程业务读取默认要求
token；status/readiness 可用于无凭据的进程检查。

### 本地 Plan draft

`draft-plan <JSON_FILE>` 接受至多 256 KiB 的强类型输入：

```json
{
  "cluster_id": "11111111-1111-4111-8111-111111111111",
  "incident_id": "33333333-3333-4333-8333-333333333333",
  "diagnosis_revision_id": "44444444-4444-4444-8444-444444444444",
  "expires_at": "2026-08-01T01:00:00Z",
  "steps": [
    {
      "action_id": "rocketmq.broker.config.plan",
      "descriptor_version": "1.0.0",
      "resource": "broker-a",
      "parameters": {
        "maxMessageSize": 4194304
      },
      "evidence_ids": [
        "55555555-5555-4555-8555-555555555555"
      ]
    }
  ]
}
```

输出是 `rocketmq-sre.local-plan-draft.v1`，仅写 stdout，不会调用 Control
Plane。

### 本地 Runbook draft

`draft-runbook <JSON_FILE>` 只接受三种步骤：

- `read`
- `manual_gate`
- `plan_reference`

类型系统不包含 shell 或 raw action。`plan_reference` 必须绑定
`sha256:<64 hex>` 的 plan hash。输出是
`rocketmq-sre.local-runbook-draft.v1`，同样不进行网络请求。

## 验证

```powershell
cargo test --locked -p rocketmq-sre-client
cargo clippy --locked -p rocketmq-sre-client --all-targets -- -D warnings
cargo test --locked -p rocketmq-sre-cli
cargo clippy --locked -p rocketmq-sre-cli --all-targets -- -D warnings
cargo run --locked -p rocketmq-sre-cli -- --help

npm --prefix ui run check:api
npm --prefix ui run test -- --run src/api/openapiContract.test.ts
npm ci --prefix sdk/typescript
npm test --prefix sdk/typescript
```

这些验证不替代真实多区域和恢复演练；OpenAPI/SDK/CLI 完成只证明接口和
客户端边界可用。
