# M11-12 R24 Soak/SLO 发布证据合同

> Status: engineering package PASS；R20 container dynamic PASS；R21 cluster/SLO dynamic 与 Phase 3 signatures 保持开放

## 1. 结论与边界

Issue #8649 / M11-12bc114 完成 R24 的可执行工程交付：候选快照绑定、六小时 soak、七项 SLO/故障目标、
Grafana dashboard、Prometheus alerts、英文 runbook、rollback 断言和 SHA-256 evidence index 由同一份
versioned policy 驱动。production evidence 必须是当前 checkout 的完整 Git SHA、五服务 digest、M11-11
动态 fault run、至少 361 个一分钟采样点、全部目标通过、无 unresolved alert/fault 且完整回滚。

本切片完成时本机没有 Docker、Kind、K3d、Kubectl、Helm、签名镜像、Secret 或生产 Prometheus endpoint，因此
没有执行六小时动态 workflow，也没有提交 production `run.json`。正向 fixture 明确记录
`fixture=true`、`dynamic_execution=false`，只有显式 `--allow-fixture` 才能用于 parser 测试，不能批准
R21、R25、M11 或 Phase 3 Gate。R20 容器动态已由后续
[run `30011167537`](https://github.com/mxsm/rocketmq-rust/actions/runs/30011167537) 独立关闭。

| 项目 | 状态 |
|---|---|
| R24 policy/runner/guard | 完成 |
| dashboard/alerts/runbook | 完成并由 guard 与 semantic registry 对齐 |
| fixture 与 deliberate violations | 9/9 通过 |
| 六小时 dynamic soak | 待具备真实集群、镜像、Secret 与 Prometheus endpoint 后执行 |
| R20 容器动态 Gate | 已关闭；run `30011167537`，完整证据见 [`11-container-dynamic-evidence.md`](11-container-dynamic-evidence.md) |
| R21 fault/rolling/SLO Gate | 保持开放 |
| R25 四方签署 | 保持开放 |

R24 作为工程执行单元关闭时，31 项最小执行清单为 **19 项完成、12 项剩余**；R20 后续关闭后为
**20 项完成、11 项剩余**。PR-M11-12 父工作包与正式进度仍为 **75/82**。

## 2. 固化的发布合同

[`architecture-production-readiness-policy.json`](../../../../distribution/config/architecture-production-readiness-policy.json)
固定：

- soak 最短 `21600` 秒、采样间隔 `60` 秒、缺样率不超过 `1%`；
- delivery ratio、send p99、consumer lag、flush-behind、HA replication lag、collector outage round trip 和
  failed preStop 七项精确目标；
- M11-11 rolling upgrade、node eviction、collector outage、disk pressure、Controller leader failure、
  secret rotation、acknowledged recovery 七场景必须全部进入同一证据；
- baseline images/chart、PVC UID、WAL、collector、Controller quorum 和 unresolved faults 七项 rollback
  断言必须全部为真。

dashboard 与 alerts 中出现的 `rocketmq_*` metric 必须登记在
`scripts/telemetry-semantic-registry.json`；故障目标使用动态 fault artifact，不虚构在线 metric。

## 3. Fail-closed evidence

`scripts/architecture_slo_guard.py` 拒绝：

1. fixture 未显式 opt-in 或 fixture 冒充 dynamic execution；
2. production candidate commit 与 checkout 不一致；
3. 五服务 image map 不完整、非 digest 或 placeholder digest；
4. soak 少于六小时、采样不足、缺样超过 `1%`；
5. 任一目标超阈值、目标顺序/单位/证据路径漂移；
6. M11-11 fault snapshot 非动态、候选 image map 不一致、七场景缺失或 global assertion 失败；
7. rollback 断言、unresolved alert/fault 非空；
8. generated artifact 或 committed release artifact SHA-256 不一致；
9. dashboard/alert 引用未登记 metric、缺 panel/alert/runbook route；
10. workflow/runner 不再保持显式 dispatch、read-only permission 或真实 Run 入口。

`scripts/run-architecture-slo-evidence.ps1` 的 `Validate` 模式不创建 PASS evidence；`Run` 模式先调用
M11-11 fault guard，再从 Prometheus API 按 committed query 采样，最后只在所有断言通过时写出并复验
production `run.json`。失败运行会删除可能被误读为 PASS 的 `run.json`，保留其余诊断 artifact。

## 4. 运维资产与回滚

| 资产 | 用途 |
|---|---|
| `distribution/config/grafana-architecture-production-readiness.json` | 同一六小时窗口展示 delivery、p99、consumer/store/HA lag，并明确 fault-only 目标 |
| `distribution/config/prometheus-architecture-production-readiness-alerts.yaml` | 五项可在线观测目标及固定 runbook route |
| `rocketmq-doc/en/architecture-production-readiness-runbook.md` | 告警处置、promotion stop、数据保留和回滚验证 |
| `.github/workflows/architecture-slo-evidence.yml` | PR 静态门禁；显式 dispatch 时串接真实 fault runner 与六小时 SLO runner |

回滚只恢复五服务 baseline digest 与 baseline chart revision；不得删除或改写 CommitLog、ConsumeQueue、Index、
RocksDB、PVC 或证据。回滚完成必须再次验证 acknowledged message、Queue Offset、CommitLog Offset、PVC UID、
collector 和 Controller quorum。

## 5. 本切片验证

| 命令 | 结果 |
|---|---|
| `python scripts/architecture_slo_guard.py --policy-only` | PASS |
| `python scripts/architecture_slo_guard.py --evidence scripts/tests/fixtures/m11-slo/pass --allow-fixture` | PASS；fixture 仅验证 parser |
| `python -m unittest scripts.tests.test_architecture_slo_guard -v` | 9/9 PASS |
| `.\scripts\run-architecture-slo-evidence.ps1 -Mode Validate` | PASS；明确未执行 dynamic 或生成 PASS evidence |
| `python scripts/telemetry_semantic_guard.py` | PASS；125 metrics / 4 spans / 7 logs / 68 attributes |
| `python scripts/fault_matrix_guard.py --policy-only` 与正向 fixture | PASS |
| `python -m unittest scripts.tests.test_m11_fault_matrix -v` | 11/11 PASS |
| `.\scripts\kind-architecture-refactor-e2e.ps1 -Mode Validate` | PASS；明确未执行 dynamic |
| `.\scripts\check-agents-routing.ps1` | PASS |
| `git diff --check` | PASS |

六小时动态 workflow 未运行的原因不是测试失败，而是当前主机缺少已记录的集群、production 签名镜像、
Secret 与监控入口。R20 容器动态已由独立 run 关闭；R21 和 R25 继续承担真实集群/SLO 动态执行与四方签署，
不能用本文件或 fixture 替代。
