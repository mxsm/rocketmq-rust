---
sidebar_position: 1
title: Broker 配置
---

# Broker 配置

配置 RocketMQ Broker，在吞吐、延迟与可靠性之间取得更好的平衡。

## 基础配置

```toml
# broker.conf

# 基础参数
brokerClusterName = DefaultCluster
brokerName = broker-a
brokerId = 0
deleteWhen = 04
fileReservedTime = 48
brokerRole = ASYNC_MASTER
flushDiskType = ASYNC_FLUSH

# 网络参数
namesrvAddr = localhost:9876
listenPort = 10911
```

## 存储配置

### CommitLog 配置

```toml
# CommitLog 文件大小（默认 1GB）
commitLogFileSize = 1073741824

# 刷盘参数
flushCommitLogLeastPages = 4
flushCommitLogThoroughInterval = 10000
flushCommitLogAtCommitOccur = false

# 数据存储路径
storePathRootDir = /data/rocketmq/store
storePathCommitLog = /data/rocketmq/store/commitlog
```

### ConsumeQueue 配置

```toml
# ConsumeQueue 文件大小（默认 30MB）
consumeQueueFileSize = 30000000

# 最小刷盘页数
flushConsumeQueueLeastPages = 2

# 彻底刷盘间隔
flushConsumeQueueThoroughInterval = 60000
```

## 性能配置

### 线程池配置

```toml
# 消息发送/拉取/查询线程池
sendMessageThreadPoolNums = 16
pullMessageThreadPoolNums = 16
queryMessageThreadPoolNums = 8

# 管理线程池
adminBrokerThreadPoolNums = 16
clientManagerThreadPoolNums = 4

# 消费者管理线程池
consumerManagerThreadPoolNums = 4
```

### 缓冲区配置

```toml
# 客户端收发缓冲区（字节）
clientSocketRcvBufSize = 131072
clientSocketSndBufSize = 131072

# OS Page Cache 忙等待超时（毫秒）
osPageCacheBusyTimeOutMills = 1000
```

### 流控配置

```toml
# 最大消息大小（字节）
maxMessageSize = 4194304

# 延迟级别
messageDelayLevel = 1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
```

## 磁盘配置

### 磁盘空间管理

```toml
# 磁盘最大使用率（%）
diskMaxUsedSpaceRatio = 85

# 磁盘告警阈值（%）
diskSpaceWarningLevelRatio = 90

# 磁盘清理阈值（%）
diskSpaceCleanLevelRatio = 85

# 文件保留时长（小时）
fileReservedTime = 72

# 每日删除时间
deleteWhen = 04
```

### 刷盘策略

```toml
# ASYNC_FLUSH（默认）：性能高，异常时可能丢失少量数据
# SYNC_FLUSH：性能较低，可靠性更高
flushDiskType = ASYNC_FLUSH

# CommitLog 最小刷盘页数
flushCommitLogLeastPages = 4

# CommitLog 彻底刷盘间隔（毫秒）
flushCommitLogThoroughInterval = 10000
```

## 高可用配置

### 主从角色配置

```toml
# 主节点
brokerRole = ASYNC_MASTER
brokerId = 0

# 从节点
# brokerRole = SLAVE
# brokerId = 1
```

### 高可用（HA）连接配置

```toml
# HA 监听端口
haListenPort = 10912

# HA 保活间隔（毫秒）
haHousekeepingInterval = 20000

# HA 传输批次大小
haTransferBatchSize = 32768
```

## 安全配置

### ACL

```toml
# 启用 ACL
aclEnable = true
```

### TLS

```toml
# 启用 TLS
tlsEnable = true
tlsTestModeEnable = false
tlsServerCert = /path/to/server.pem
tlsServerKey = /path/to/server.key
tlsServerAuthClient = true
```

## 监控配置

```toml
# 消息轨迹
traceTopicEnable = true
traceTopicName = RMQ_SYS_TRACE_TOPIC

# 统计功能
enableCalcFilterBitMap = true
```

## 最佳实践

1. **优先使用 SSD**：显著提升 CommitLog 写入性能。
2. **按场景选择刷盘模式**：性能与可靠性做取舍。
3. **持续监控磁盘空间**：及时告警，避免触发保护机制。
4. **CommitLog 与 ConsumeQueue 尽量分盘**：降低 I/O 争用。
5. **线程池参数按负载调优**：避免过大或过小。
6. **合理限制消息大小**：防止超大消息影响稳定性。

## 下一步

- [客户端配置](./client-config) - 配置生产者与消费者
- [性能调优](./performance-tuning) - 进一步优化吞吐与延迟
- [存储架构](../architecture/storage) - 理解存储机制细节
