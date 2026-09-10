---
title: "性能问题"
---

## 可以达到多少吞吐量？

本页不提供脱离负载条件的吞吐数字。应报告实际硬件、存储、版本/feature、消息大小、队列布局、客户端模式、刷盘/复制策略、持续时间、错误与消费完成量。生产者短时突发不等于可持续的端到端吞吐。

## 如何规划容量或调优？

[容量与性能](../operations/capacity-performance.md)给出存储、追平计算、瓶颈观察和可重复比较方法。[监控](../operations/monitoring.md)解释定位限制阶段所需信号。

## 增大消息限制或消费者数量能提速吗？

更大的最大消息大小只改变限制，不执行批处理。只有队列分配、顺序和下游容量允许增加完成量时，更多消费者才有帮助。见[发送 API](../producer/sending-messages.md)与[消费模式](../consumer/overview.md)。

## 能否切换刷盘模式降低延迟？

这会影响持久性。把不同确认约定作为性能结果之前，应比较[存储语义](../architecture/storage.md)和 [HA 要求](../architecture/ha-controller.md)。
