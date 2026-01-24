---
title: 架构概览
sidebar_position: 1
---

# RocketMQ-Rust 架构概览

## 核心组件

### Name Server
负责路由信息管理、 Broker 注册与发现

### Broker
消息存储与转发核心组件

### Producer
消息生产者客户端

### Consumer
消息消费者客户端

## 架构图

```mermaid
graph TB
    Producer[生产者]
    Consumer[消费者]
    Broker[Broker 集群]
    NameServer[Name Server]

    Producer --> Broker
    Consumer --> Broker
    Broker --> NameServer

    style Broker fill:#f9f,stroke:#333,stroke-width:2px
    style NameServer fill:#bbf,stroke:#333,stroke-width:2px
```

## 相关文档

- [消息模型](./message-model)
- [存储架构](./storage)
