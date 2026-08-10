# NameServer 性能优化灰度与回滚手册

## 目标与安全边界

本手册用于把优化后的 Rust NameServer 安全推向生产，不把工程目标冒充实测。
在完整业务峰值周期通过前保留 Java NameServer 回滚容量；9876 端口不得暴露
公网，生产管理请求必须启用 TLS、认证和授权。

## 前置条件

- 同一 release commit 已通过 Java golden corpus、Java/Rust 双向混跑、route
  E2E smoke、write recovery 和 runtime ownership audit。
- 固化主机、电源策略、allocator、网络、Java 版本、manifest 哈希和监控保留期。
- 已加载 `distribution/config/prometheus-namesrv-alerts.yaml` 与 Grafana dashboard，
  且指标标签不含 Topic、Broker、地址、zone、namespace、key/value 或 principal。
- 运维可从地址列表移除单个节点，同时保持 Broker 向其余 NameServer 注册。
- `kvConfig.json`、desired runtime properties、ACL 和 TLS 材料可恢复。

## 发布前 soak 矩阵

| 场景 | 必测负载 | 必留证据 |
|---|---|---|
| 大注册 | 至少 70k Topic 合法连续分片，含重复、重试、缺片和陈旧分片 | 100% 完整注册、无半代可见、wire/decompressed bytes、decode p99 |
| 注册风暴 | 至少 300 Broker 并发重注册，含 unchanged 与 delta | route p99/p99.9、gate wait/hold、dirty/no-op、route digest |
| 故障恢复 | 10%、50%、100% 同时过期 | unregister 零丢弃、恢复延迟、最老 pending、CPU/RSS、safety digest |
| 连接 | 64、256、1024 连接 | active/admitted/rejected、reconnect、slow write、RSS 有界 |
| Zone | 10% zone 路由，宽度 1/4/16 | Java 摘要一致；typed filter 与 encode 各一次 |
| KV/配置 | 并发写并注入 create/write/fsync/replace 故障 | 失败时内存不变；desired/durable/applied 收敛 |
| TLS | 支持热更新时 reload 100/1000 次 | 零失败、内存收敛；1000 次 RSS 增量目标小于 5 MiB |
| 稳态读 | 批准容量下连续 24 小时 | p99/p99.9 稳定、摘要零错误、queue/generation 无泄漏、RSS 有界 |

```powershell
.\scripts\run_namesrv_soak.ps1 -Mode Plan
.\scripts\run_namesrv_soak.ps1 -Mode Smoke -JavaRocketmqHome D:\Github\Java\rocketmq
.\scripts\run_namesrv_soak.ps1 -Mode Full -JavaRocketmqHome D:\Github\Java\rocketmq -SteadyReadHours 24
```

`Plan` 不产生负载；`Smoke` 只验证链路；`Full` 运行 20k/100k route profile、
10/50/100% expiry microbenchmark、KV 故障测试、可用时的 mixed parity，并按
时长重复 steady-read profile。`Full` 只能在独占容量机运行。

TLS reload 依赖部署环境的证书轮换驱动；通过 `-TlsReloadScript` 传入已审核
脚本。未传入只能记为 skipped，不能记为 passed。分片协议故障注入同样由
实际发送分片的 Java/Rust Broker 场景完成。

## 灰度顺序

1. **实验室**：corpus、fault injection、E2E capacity、soak 全部通过。
2. **Shadow**：Broker 向 Rust 注册，但客户端地址列表不含 Rust；比较 route
   digest、Topic 数、Broker generation、expiry mismatch 和 KV 状态。
3. **单节点**：客户端选择比例按 1%→10%→50%，每步至少观察两个路由刷新
   周期和一个注册周期。
4. **单 AZ**：其他 AZ 保留 Java；演练 Broker restart、50% expiry、ACL 拒绝、
   配置持久化失败和节点重启。
5. **全量**：稳定经过一个完整业务峰值周期后再移除 Java 节点。

功能开关顺序：workload admission 保持 observe-only；expiry `off→shadow→active`；
typed zone 先 shadow；registration delta 在 mixed parity 后开启；route response
cache 最后开启且必须监控 RSS。每个开关均独立可回退。

## 自动回滚条件

出现任一路由摘要不一致、半代注册、陈旧事件误删、false-ready、安全绕过、
unregister Full/drop、expiry mismatch 或 KV durability 失败时立即回滚。错误率
增加至少 0.1 个百分点、p99 连续 5 分钟超过基线 2 倍，或 RSS 超预算 10%
同样触发回滚。

## 回滚步骤

1. 停止增加流量并保留 metrics、profile、日志和 route digest。
2. 从客户端发现列表移除 Rust 灰度地址，Broker 继续向其余 NameServer 注册。
3. live 开关恢复安全值；restart-required 配置恢复上一份 desired properties，
   只重启已隔离节点。
4. 等待已接收 unregister/KV drain，durable/applied generation 收敛后再优雅停止。
5. 确认客户端回到 Java/旧 Rust 路由、Broker 全量注册完整、无 stale route，
   protected config 未变化。
6. 保存不可变证据并建立事故记录后，才能重新灰度。

`kvConfig.json` 仍是 Java 兼容 JSON。P3 没有引入 WAL，回滚无需转换日志格式。

## WAL 决策与交付 Checklist

默认不引入 KV WAL。只有代表性环境持续出现 snapshot >16 MiB、写入 >10 次/秒、
mutation p99 >100 ms 或序列化 CPU >5%，才单独建立 ADR，并比较 atomic snapshot
batching 与 WAL 的恢复、损坏处理、compaction 和 Java 回滚兼容性。

- [ ] 记录 Git commit、Java 版本和 workload manifest 哈希。
- [ ] golden corpus 与双向 mixed-version 均通过。
- [ ] 70k+ Topic 分片、300+ Broker 风暴通过。
- [ ] 10/50/100% expiry 与 64/256/1024 连接通过。
- [ ] KV fault injection 保证 durable-before-publish。
- [ ] TLS reload 已通过，或明确标记 unsupported/skipped。
- [ ] 24 小时产物包含 CPU、RSS、p99/p99.9、queue、generation、digest。
- [ ] 流量前告警和 dashboard 已启用。
- [ ] shadow、单节点、单 AZ 回滚演练签字完成。
- [ ] 每个性能数字明确标记“实测”或“未验证工程目标”。
