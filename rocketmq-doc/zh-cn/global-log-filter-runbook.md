# 全局日志过滤器生产运维手册

## 统一契约

NameServer、Broker、Controller、Proxy、MCP 及本次纳管的 Dashboard 后端统一使用 `rocketmq-observability` resolver，启动优先级固定为：

```text
已授权运行时覆盖 > --log-filter > RUST_LOG > logging.filter/logFilter > info
```

当前仅 Broker 暴露远程临时调级入口。NameServer、Controller、Proxy 在启用 reload 时只保留进程内 handle，不新增未认证管理协议。MCP 和 Dashboard 同样默认 `info`。

显式空值、非法 directive、非 Unicode `RUST_LOG`、`logging.filter` 与 `logFilter` 冲突，都会在业务 lifecycle 启动前失败。Console、File、OTLP Logs 共用最外层全局 filter。禁止通过 `build.rs` 注入日志默认等级。

## 配置方式

YAML：

```yaml
logging:
  filter: info,rocketmq_broker=debug
  reload:
    enabled: false
```

TOML：

```toml
[logging]
filter = "info,rocketmq_broker=debug"

[logging.reload]
enabled = false
```

Java properties 兼容字段：

```properties
logFilter=info,rocketmq_broker=debug
```

四个核心服务均支持 `--log-filter <DIRECTIVE>`，`RUST_LOG` 用于进程级覆盖。MCP 暂时兼容 `server.log_level`，但会输出一次弃用提示，新配置必须改为 `logging.filter`。Dashboard GPUI/Web backend 当前支持 `RUST_LOG` 和默认 `info`。

启动后必须核对结构化字段：`service`、`effective_filter`、`filter_source`、`subscriber_installed`、`reload_enabled`。subscriber 未安装或来源与发布配置不一致时，不得继续发布。

## Broker 远程临时调级

以下条件必须全部成立：

- `logging.reload.enabled=true`；
- Broker authentication 与 authorization 同时开启；
- 请求通过现有 `Cluster/Update` 鉴权；
- 请求带有效 ACL、原因、request ID，TTL 为 60–7200 秒；
- `${store_path_root_dir}/logs/log-filter-audit.jsonl` 可写；
- 受 `ServiceContext/TaskGroup` 管理的 TTL 恢复任务挂载成功。

普通操作员必须显式保留全局 `info`，且只能提升 `rocketmq_*` target。全局 DEBUG/TRACE、span/field directive 仅允许 super-user break-glass。新覆盖会替换旧覆盖并重置 TTL，不维护嵌套栈；到期或 `logFilterRestore=true` 时恢复启动基线。

ACL 只通过环境变量传入，禁止写入脚本参数、日志或证据文件：

```text
ROCKETMQ_ACL_ACCESS_KEY
ROCKETMQ_ACL_SECRET_KEY
ROCKETMQ_ACL_SECURITY_TOKEN（可选）
```

生产验证优先执行：

```powershell
.\scripts\verify-broker-log-filter-reload.ps1 `
  -AdminExecutable .\target\release\rocketmq-admin-cli.exe `
  -BrokerAddress 127.0.0.1:10911 `
  -AuditPath C:\rocketmq\store\logs\log-filter-audit.jsonl `
  -NamesrvAddress 127.0.0.1:9876
```

脚本复用 `broker updateBrokerConfig`，默认等待 TTL 自动恢复；也可用 `-RestoreMode restore` 验证提前恢复。审计顺序为 `intent → success → ttl_restore`，脚本以审计时间戳计算 Broker 内部 reload 延迟，避免把网络 RTT 混入 100ms 门禁。

手工提前恢复：

```powershell
rocketmq-admin-cli.exe -n 127.0.0.1:9876 broker updateBrokerConfig -b 127.0.0.1:10911 `
  -p "logFilterRestore=true" `
  -p "logFilterReason=INC-42 investigation completed" `
  -p "logFilterRequestId=INC-42-restore" --yes
```

审计由有界 `BlockingExecutor` 执行，JSONL append 后调用 `sync_data`。意图审计或 TTL 调度失败时不修改 filter；成功审计失败时立即尝试恢复启动基线。

## 发布验证

四个核心服务分别执行启动探针：

```powershell
.\scripts\verify-log-filter-governance.ps1 -Service broker `
  -Executable .\target\release\rocketmq-broker-rust.exe `
  -CommonArguments @("-c", ".\conf\broker.toml")
```

门禁包括：默认 `DEBUG=0` 且 `INFO>0`、target-only DEBUG、非法 directive 启动失败、Broker reload 不超过 100ms、审计成功、TTL/显式恢复成功。

基线和候选各采集 15 分钟 Prometheus 数据：

```powershell
.\scripts\collect-log-filter-canary.ps1 -PrometheusBaseUrl http://prometheus:9090 `
  -ThroughputQuery '<Broker 吞吐 PromQL>' -P99Query '<Broker P99 PromQL>' -Phase baseline

.\scripts\collect-log-filter-canary.ps1 -PrometheusBaseUrl http://prometheus:9090 `
  -ThroughputQuery '<Broker 吞吐 PromQL>' -P99Query '<Broker P99 PromQL>' -Phase candidate `
  -BaselineSummary .\target\log-filter-evidence\log-filter-canary-baseline-<timestamp>.json
```

候选门禁为平均吞吐下降不超过 1%、平均 P99 增幅不超过 2%。仓库自动化不能代替真实生产 canary；目标集群证据补齐前，不得宣称最终达到 96/100。

## 指标与告警

- `rocketmq_observability_log_filter_reload_total{service,result,source}`
- `rocketmq_observability_log_filter_active{service,source}`
- `rocketmq_observability_log_filter_expiry_timestamp_seconds{service}`
- audit、自动恢复、回滚失败计数

`distribution/config/prometheus-log-filter-alerts.yaml` 覆盖 reload 失败率、审计失败、TTL 超时、运行时覆盖持续过久和自动恢复失败。Broker Grafana Dashboard 已增加 reload 成功率、active source、TTL 和治理失败面板。

## 故障矩阵

| 现象 | Fail-safe 行为 | 处理动作 |
|---|---|---|
| 启动 filter 非法或为空 | 业务 lifecycle 前退出 | 修复最高优先级来源，禁止静默降级 |
| `logging.filter` 与 `logFilter` 冲突 | 业务 lifecycle 前退出 | 删除兼容字段或统一两个值 |
| 默认启动仍有 DEBUG | 发布门禁失败 | 检查 `filter_source`、部署环境与运行二进制是否陈旧 |
| reload 关闭或 TTL 任务不可用 | 返回 `NoPermission` | 明确启用 reload，验证任务所有权后重启 |
| authentication/authorization 未同时开启 | 返回 `NoPermission` | 不得绕过；先启用两层安全控制 |
| 意图审计无法持久化 | filter 保持不变 | 修复磁盘空间、目录权限后使用新 request ID 重试 |
| reload 失败 | 保留 last-known-good | 检查 reload 失败指标及失败审计 |
| 成功审计失败 | 尝试回滚启动基线 | 按事故处理，修复审计存储后再调级 |
| TTL 恢复失败或超时 | 触发严重告警 | 认证执行显式恢复；必要时重启 Broker 恢复基线 |
| 候选性能超预算 | canary 失败 | 恢复 INFO、保留证据并停止晋级 |

## 回滚判定

活动覆盖优先执行 `logFilterRestore=true`，确认成功审计且 expiry gauge 归零。启动配置错误时，删除 CLI/环境/配置覆盖并以默认 `info` 重启。只有默认探针满足 `DEBUG=0`、`INFO>0`，active source 不再是 `runtime`，并稳定经过观察窗口，才算回滚完成。
