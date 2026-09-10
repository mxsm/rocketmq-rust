---
title: "性能调优流程"
---

性能调优从工作负载和实际限制阶段开始。完整流程已整理到[容量与性能](../operations/capacity-performance.md)。

| 需求 | 指南 |
| --- | --- |
| 估算保留存储、副本与追平时间 | [容量计算](../operations/capacity-performance.md) |
| 定位客户端、磁盘、复制或下游瓶颈 | [监控](../operations/monitoring.md) |
| 选择批量、压缩与有界发送 | [生产者 API](../producer/sending-messages.md) |
| 理解消费并行度与完成 | [消费者总览](../consumer/overview.md) |
| 评估刷盘和副本调整 | [存储](../architecture/storage.md)、[HA 设计](../architecture/ha-controller.md) |
| 调整保留策略或重启节点 | [日常维护](../operations/maintenance.md) |

使用实际服务配置 schema 和当前显式注入运行时的客户端 API。每次比较修改一个因素，保留预期持久性约定。示例资源值不是通用调优建议。
