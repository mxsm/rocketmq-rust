# Phase 5 新工程师运维指南

本指南用于让第一次接触项目的工程师完成一组代表性操作：环境检查、集群接入、Fleet
查询、巡检、事件诊断、发布护航以及恢复演练。所有命令从仓库根目录执行。

## 1. 安全边界

- 普通 RocketMQ Dashboard 与 AI SRE UI 独立，不能复用登录态或 mutation API。
- Fleet、巡检、诊断和 CLI 默认为读取或生成本地 draft。
- R2 变更必须经过内部 Plan、Policy、Critic、人类审批、Executor 和 Execution Agent。
- R3、raw shell、任意 Admin patch、生产 DR cutover 在 Agent 中不可达。
- 测试集群 DR 会删除并重建 Kind 中精确命名的 `rocketmq-broker-0` Pod，不可指向生产
  Kubernetes context。
- Kind Broker 使用临时卷；演练不证明历史消息恢复。

## 2. 准备环境

需要 Docker Desktop、Rust 1.95+、Node.js、PowerShell、Kind 和 `kubectl`。构建输出和临时文件使用
`G:`：

```powershell
$env:CARGO_HOME = 'G:\rocketmq-sre-phase1-cargo-home'
$env:CARGO_TARGET_DIR = 'G:\rocketmq-sre-phase2-cargo-target'
$env:TEMP = 'G:\rocketmq-sre-phase2-temp'
$env:TMP = $env:TEMP
```

检查独立 workspace 和 UI：

```powershell
cargo metadata --manifest-path .\rocketmq-sre\Cargo.toml --locked --no-deps
npm --prefix .\rocketmq-sre\ui run check:api
```

任何 Cargo 编译前检查 `D:`、`G:` 剩余空间。任一分区低于 15 GiB 时执行：

```powershell
cargo clean `
  --manifest-path D:\Github\Rust\rocketmq-rust-phase00-ai-sre\rocketmq-sre\Cargo.toml `
  --target-dir G:\rocketmq-sre-phase2-cargo-target
```

## 3. 启动 PostgreSQL 和 Kind

PostgreSQL 运行在 Docker 中，无需安装到主机：

```powershell
docker compose `
  --project-name rocketmq-rust-ai-sre-phase00 `
  --file .\rocketmq-sre\deploy\dev\compose.yaml `
  up --detach postgres

docker compose `
  --project-name rocketmq-rust-ai-sre-phase00 `
  --file .\rocketmq-sre\deploy\dev\compose.yaml `
  ps postgres
```

已有 Kind 环境先检查；没有时再创建：

```powershell
.\rocketmq-sre\scripts\kind.ps1 -Action Status
.\rocketmq-sre\scripts\kind.ps1 -Action Up
```

验收用 kubeconfig：

```powershell
$env:KUBECONFIG =
  'G:\rocketmq-sre-phase2-temp\kind-access\rocketmq-sre-phase00.kubeconfig'
kubectl config current-context
kubectl -n rocketmq-system get pods
kubectl -n rocketmq-sre get pods
```

执行故障演练前，context 必须是
`kubernetes-admin@rocketmq-sre-phase00`。

## 4. UI 代表性操作

AI SRE UI 是面向桌面的全屏工作区。打开 `http://127.0.0.1:3004` 后依次检查：

1. `/fleet`：Fleet、Region、Cluster、配额和健康概览。
2. `/fleet/compliance`：资产索引、版本、配置摘要和合规 Finding。
3. `/clusters/onboard`：提交只读接入信息，确认能力不兼容时 fail closed。
4. `/inspections`：创建有 cluster allowlist、并发和预算限制的巡检。
5. `/incidents`：查看证据、假设、反证、时间线和诊断状态。
6. `/changes/releases`：查看单集群发布护航；Fleet Release 由 `/v1/fleet/releases`
   聚合独立、已就绪的单集群 Release。
7. `/changes/integrations`：检查 ITSM、ChatOps/Pager、CMDB/GitOps 和 CI/CD 状态。
8. `/fleet/dr`：查看 DR Plan、演练、checkpoint、Finding 和 Action Item。
9. `/governance`、`/finops`：检查版本治理、预算、成本覆盖率和 showback。

任何列表查询都必须保持 tenant、region 和 cluster scope。复制 URL 后重新打开，筛选条件应保持。

## 5. 只读 CLI

CLI 只提供固定的 status、cluster、incident、inspection、plan 和 OpenAPI 读取，以及本地
Plan/Runbook draft 校验：

```powershell
cargo run `
  --manifest-path .\rocketmq-sre\Cargo.toml `
  --locked `
  -p rocketmq-sre-cli `
  -- --help
```

不要把 token 放到命令行参数。CLI 不提供 approve、execute、raw request 或 shell。

## 6. 完整企业场景

一条命令完成 100 集群、两个逻辑 Region、集成幂等、current/N-1、两集群发布、Control Plane
restore 和 Kind 测试集群 DR：

```powershell
.\rocketmq-sre\scripts\phase05-enterprise-smoke.ps1
```

成功标志：

```text
PHASE05_ENTERPRISE_SMOKE_OK
```

结果位于：

```text
G:\rocketmq-sre-phase2-temp\phase05-enterprise-smoke.json
```

应确认：

- `status` 为 `passed`；
- `scenarios` 恰好包含 6 个且全部 `passed`；
- `control_plane_restore.status` 为 `passed`；
- `test_cluster_dr.status` 为 `passed`；
- 恢复前后 probe 均为 10/10/10；
- `message_history_restore_claimed` 和 `secrets_recorded` 都为 `false`。

## 7. 单独执行恢复

Control Plane PostgreSQL 恢复：

```powershell
.\rocketmq-sre\scripts\phase05-control-plane-restore.ps1
```

脚本使用 `pg_dump` custom format，在同一 Docker PostgreSQL 中创建随机命名的隔离恢复库，比较关键
计数，启动当前 Control Plane 检查 `/healthz` 和 `/readyz`，最后删除恢复库和 dump。

RocketMQ 测试集群 DR：

```powershell
.\rocketmq-sre\scripts\phase05-test-cluster-dr.ps1
```

脚本先完成 10/10/10 probe，再重建精确的 Kind Broker Pod，重建专用合成 Topic，完成恢复后
10/10/10 probe，并在 DR Center PostgreSQL 契约中记录 supervised test、checkpoint、Finding 和
Action Item。

## 8. 常见问题

| 现象 | 处理 |
| --- | --- |
| Docker PostgreSQL 不健康 | `docker logs rocketmq-rust-ai-sre-phase00-postgres-1`，确认 5432 未被其他进程占用 |
| Kind context 不匹配 | 设置本指南中的 `KUBECONFIG`；不要绕过 context 检查 |
| 恢复后首次 probe 超时 | 脚本会进行一次有界重试；仍失败时保留 Job，检查 init container、Broker 和 NameServer 日志 |
| Cargo 链接失败或磁盘不足 | 检查 D/G 空间并按空间规则清理指定 SRE target |
| UI 类型不一致 | 运行 `node .\rocketmq-sre\scripts\generate_phase5_openapi.mjs` 和 `npm --prefix .\rocketmq-sre\ui run generate:api` |
| OpenAPI 路径缺失 | 运行 Control Plane `openapi::tests`，不要手改生成的 TypeScript 类型 |

## 9. 结束检查

```powershell
git status --short
kubectl -n rocketmq-system get statefulset rocketmq-broker
kubectl -n rocketmq-sre get deployment sre-control-plane
docker exec rocketmq-rust-ai-sre-phase00-postgres-1 `
  psql --username rocketmq_sre --dbname postgres `
  --tuples-only --no-align `
  --command "SELECT COUNT(*) FROM pg_database WHERE datname LIKE 'rocketmq_sre_restore_%'"
```

最后一条查询必须为 `0`。不要删除用户未提交的文件，也不要把 `G:` 下的运行证据提交为源码。
