---
title: "常见运行问题"
---

以下指南提供完整的 Rust 服务命令与解释。

| 问题 | 从这里开始 |
| --- | --- |
| 为什么能访问 NameServer，却无法发送？ | [端点与路由诊断](../operations/troubleshooting.md) |
| 为什么消费者已连接却收不到消息？ | [订阅、分配、位置与完成检查](../operations/troubleshooting.md) |
| 为什么消息可能处理两次？ | [投递、重试与业务幂等](../guides/delivery-and-retry.md) |
| 选择 exporter 或 Proxy 模式后为何启动失败？ | [构建与配置诊断](../operations/troubleshooting.md) |
| 如何使用 Rust CLI 查看集群？ | [管理操作](../operations/admin.md) |
| 路由、复制与存储分别由谁负责？ | [架构总览](../architecture/overview.md) |

首次安装使用[配套本地教程](../getting-started/local-source.md)，当前示例显式注入客户端运行时。不要通过复制无关 Java 启动命令、开放所有防火墙端口、重置偏移量或删除存储状态来修复故障。
