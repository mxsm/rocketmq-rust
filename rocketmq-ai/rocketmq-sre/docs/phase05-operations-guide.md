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
- Kind Broker 使用 10 GiB `standard` PVC；演练验证单节点 Pod 替换下的历史消息
  RPO/RTO，但不等同于节点丢失、Kind 集群重建、复制 CommitLog 或生产备份恢复。

## 2. 准备环境

需要 Docker Desktop、Rust 1.95+、Node.js、PowerShell、Kind 和 `kubectl`。构建输出和临时文件
默认使用 `D:`；空间规划需要时可整体切换到 `F:`，不得使用系统盘或 G 盘：

```powershell
$env:CARGO_HOME = 'D:\BuildCache\rocketmq-sre-cargo-home'
$env:CARGO_TARGET_DIR = 'D:\BuildCache\rocketmq-sre-target'
$env:TEMP = 'D:\BuildCache\rocketmq-sre-temp'
$env:TMP = $env:TEMP
```

检查独立 workspace 和 UI：

```powershell
cargo metadata --manifest-path .\rocketmq-ai\rocketmq-sre\Cargo.toml --locked --no-deps
npm --prefix .\rocketmq-ai\rocketmq-sre\ui run check:api
```

任何 Cargo 编译前检查所选 D/F 目标盘的剩余空间。低于 15 GiB 时仅清理该 SRE target：

```powershell
cargo clean `
  --manifest-path D:\Github\Rust\rocketmq-rust-phase00-ai-sre\rocketmq-ai\rocketmq-sre\Cargo.toml `
  --target-dir D:\BuildCache\rocketmq-sre-target
```

## 3. 启动 PostgreSQL 和 Kind

PostgreSQL 运行在 Docker 中，无需安装到主机：

```powershell
docker compose `
  --project-name rocketmq-rust-ai-sre-phase00 `
  --file .\rocketmq-ai\rocketmq-sre\deploy\dev\compose.yaml `
  up --detach postgres

docker compose `
  --project-name rocketmq-rust-ai-sre-phase00 `
  --file .\rocketmq-ai\rocketmq-sre\deploy\dev\compose.yaml `
  ps postgres
```

已有 Kind 环境先检查；没有时再创建：

```powershell
.\rocketmq-ai\rocketmq-sre\scripts\kind.ps1 -Action Status
.\rocketmq-ai\rocketmq-sre\scripts\kind.ps1 -Action Up
```

验收用 kubeconfig：

```powershell
$env:KUBECONFIG =
  'D:\BuildCache\rocketmq-sre-temp\kind\phase00-kubeconfig'
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
  --manifest-path .\rocketmq-ai\rocketmq-sre\Cargo.toml `
  --locked `
  -p rocketmq-sre-cli `
  -- --help
```

不要把 token 放到命令行参数。CLI 不提供 approve、execute、raw request 或 shell。

## 6. 完整企业场景

一条命令完成 100 集群、两个逻辑 Region、集成幂等、current/N-1、两集群发布、Control Plane
restore 和 Kind 测试集群 DR：

```powershell
.\rocketmq-ai\rocketmq-sre\scripts\phase05-enterprise-smoke.ps1
```

成功标志：

```text
PHASE05_ENTERPRISE_SMOKE_OK
```

结果位于：

```text
D:\BuildCache\rocketmq-sre-temp\phase05-enterprise-smoke.json
```

应确认：

- `status` 为 `passed`；
- `scenarios` 恰好包含 6 个且全部 `passed`；
- `control_plane_restore.status` 为 `passed`；
- `test_cluster_dr.status` 为 `passed`；
- 恢复前后 probe 均为 10/10/10；
- `test_cluster_dr.persistent_storage.retained_across_pod_replacement` 为 `true`；
- `test_cluster_dr.message_history.expected_messages` 与 `recovered_messages` 都为 `10`；
- `test_cluster_dr.message_history.rpo_messages` 为 `0`，并记录实际 `rto_seconds`；
- `message_history_restore_claimed` 为 `true`，`secrets_recorded` 为 `false`。

## 7. 单独执行恢复

Control Plane PostgreSQL 恢复：

```powershell
.\rocketmq-ai\rocketmq-sre\scripts\phase05-control-plane-restore.ps1
```

脚本使用 `pg_dump` custom format，在同一 Docker PostgreSQL 中创建随机命名的隔离恢复库，比较关键
计数，启动当前 Control Plane 检查 `/healthz` 和 `/readyz`，最后删除恢复库和 dump。

RocketMQ 测试集群 DR：

```powershell
.\rocketmq-ai\rocketmq-sre\scripts\phase05-test-cluster-dr.ps1
```

脚本先确认 Broker PVC/PV 已 Bound，完成 10/10/10 probe，再以独立 run_id 发送 10 条有界
历史消息。随后删除精确的 Kind Broker Pod，确认替换前后的 PVC/PV UID 不变，消费同一 run_id
的 10 条历史消息并计算 RPO/RTO，最后完成恢复后 10/10/10 probe。DR Center PostgreSQL
契约同时记录 supervised test、checkpoint、Finding 和 Action Item。

## 8. 常见问题

| 现象 | 处理 |
| --- | --- |
| Docker PostgreSQL 不健康 | `docker logs rocketmq-rust-ai-sre-phase00-postgres-1`，确认 5432 未被其他进程占用 |
| Kind context 不匹配 | 设置本指南中的 `KUBECONFIG`；不要绕过 context 检查 |
| 恢复后首次 probe 超时 | 脚本会进行一次有界重试；仍失败时保留 Job，检查 init container、Broker 和 NameServer 日志 |
| Cargo 链接失败或磁盘不足 | 检查所选 D/F 目标盘并按空间规则清理指定 SRE target |
| UI 类型不一致 | 运行 `node .\rocketmq-ai\rocketmq-sre\scripts\generate_phase5_openapi.mjs` 和 `npm --prefix .\rocketmq-ai\rocketmq-sre\ui run generate:api` |
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

最后一条查询必须为 `0`。不要删除用户未提交的文件，也不要把 D/F 临时目录中的运行证据提交为源码。
