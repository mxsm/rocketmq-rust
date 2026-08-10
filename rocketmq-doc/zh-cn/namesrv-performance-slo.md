# NameServer 性能与正确性 SLO

## 适用范围

本 SLO 覆盖 Rust NameServer 的路由读取、Broker 注册、故障恢复、KV/配置、
readiness 和管理查询。发布前必须先通过 Java 5.5.0 协议及状态迁移语料；
性能提升不能抵消路由摘要不一致、分片注册半可见或陈旧事件误删。

## 测量约定

- Rust 与 Java 必须使用同一主机、电源策略、manifest 哈希、随机种子、
  客户端、网络路径、预热和测量窗口。
- 记录 QPS、p50/p95/p99/p99.9、错误率、CPU、峰值 RSS、响应字节、准入
  队列/拒绝、mutation gate 等待/持有、unregister 最老年龄、expiry mismatch，
  以及 KV desired/durable/applied generation。
- handler latency 与包含 hook、response-channel completion 的服务端 E2E
  latency 边界不同，后者也不是客户端确认延迟。
- 无法测得的 allocation/native memory 必须写 `N/A`，不得从 RSS 推算。

## 发布指标

| 领域 | 发布目标 | 自动回滚条件 |
|---|---|---|
| 正确性 | Java corpus 与 shadow route digest 零差异 | 任一差异、半注册可见、陈旧注销误删或 false-ready |
| 可用性 | bind/TLS 成功后才 ready；已接收的关闭工作可 drain | bind-ready 约束失败，或 unregister/KV 无法收敛 |
| 路由错误 | 不超过基线 +0.1 个百分点 | 连续 5 分钟增加至少 0.1 个百分点 |
| 路由延迟 | 同容量 Rust p99 低于 Java；工程目标较 P0 降低至少 30% | 连续 5 分钟超过 Java 或旧 Rust 的 2 倍 |
| 路由吞吐 | 同机 Rust QPS 至少为 Java 的 1.5 倍；工程目标较 P0 提升至少 30% | 连续两个窗口低于批准容量线 |
| 内存 | 过载时有界；工程目标不超过 P0 RSS +10% | 超预算 10% 或持续增长且不收敛 |
| 恢复 | unregister 零丢弃；expiry index 与 safety scan 一致 | 任一 Full/drop、shadow mismatch 非零或 pending 超预算 |
| KV/配置 | durable 与 applied 收敛后 RPC 才成功 | generation gap 超过一个已完成 batch 或持续 30 秒 |
| TLS | reload 零失败；支持热更新时 1,000 次 RSS 增量目标小于 5 MiB | reload 失败或 native memory 持续增长 |

百分比是待专用同机容量测试验证的工程目标。在没有 P3 实测产物前，不得
把实现完成写成“已达到优化后性能”。

## 当前实测边界

仓库记录的 2026-08-11 Windows smoke（100 Topic、10 Broker、route width 4、
16 连接、2,000 操作）中：Rust P1 为 39,920.40 QPS / p99 3,714 us，
Rust P0 为 34,608.78 QPS / p99 4,513 us，Java 5.5.0 为 12,810.04 QPS /
p99 6,491 us。该结果是真实 E2E smoke，不是生产容量结果，也不是 P3
优化后数据。复现方式见 `rocketmq-namesrv/benches/README.md`。

P2 局部算法实测中，10,000 Broker 内查找 1,000 个过期 Broker：deadline
index 为 14.741 us，full scan 为 175.60 us；heartbeat 维护从 68.420 ns
增加到 221.49 ns。该数据不能外推为 TCP 注册或故障恢复延迟。

## 错误预算与证据留存

正确性、陈旧误删、readiness、安全绕过、KV 持久性和 unregister 丢失的
错误预算为零。压测产物必须同时保存 workload manifest、Git commit、
Java 版本、主机信息、配置、JSON/CSV 原始数据、进程指标、Prometheus
快照和 route digest；smoke、容量测试和 24 小时 soak 必须分开归档。
