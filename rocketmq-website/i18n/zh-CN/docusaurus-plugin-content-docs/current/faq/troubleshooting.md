---
title: "选择诊断流程"
---

操作手册见[按症状排查故障](../operations/troubleshooting.md)，分别处理启动、发现、请求安全、持久化/复制和消费完成。

1. 从[首次诊断](../operations/first-diagnosis.md)开始，记录一个失败操作。
2. 通过[管理查询](../operations/admin.md)比较路由、Broker 状态与消费进度。
3. 通过[监控](../operations/monitoring.md)关联受控日志、指标与追踪。
4. 做出[恢复决策](../operations/backup-recovery.md)前保留数据；计划重启按[日常维护](../operations/maintenance.md)执行。

Rust 进程不提供 JVM 堆、线程转储接口。应定位实际 Rust 进程，使用服务支持的日志过滤与平台观察。收集稳定公开错误和非秘密上下文，不把凭据、消息正文放入报告。
