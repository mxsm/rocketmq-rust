---
sidebar_position: 1
title: Broker Configuration
---

# Broker Configuration

Configure RocketMQ brokers for optimal performance and reliability.

## Basic Configuration

```toml
# broker.conf

# Basic settings
brokerClusterName = DefaultCluster
brokerName = broker-a
brokerId = 0
deleteWhen = 04
fileReservedTime = 48
brokerRole = ASYNC_MASTER
flushDiskType = ASYNC_FLUSH

# Network settings
namesrvAddr = localhost:9876
listenPort = 10911
```

## Store Configuration

### CommitLog Settings

```toml
# CommitLog file size (default: 1GB)
commitLogFileSize = 1073741824

# Flush settings
flushCommitLogLeastPages = 4
flushCommitLogThoroughInterval = 10000
flushCommitLogAtCommitOccur = false

# Data storage path
storePathRootDir = /data/rocketmq/store
storePathCommitLog = /data/rocketmq/store/commitlog
```

### ConsumeQueue Settings

```toml
# ConsumeQueue file size (default: 30MB)
consumeQueueFileSize = 30000000

# Flush consume queue least pages
flushConsumeQueueLeastPages = 2

# Flush consume queue thorough interval
flushConsumeQueueThoroughInterval = 60000
```

## Performance Configuration

### Thread Pool Settings

```toml
# Send message thread pool
sendMessageThreadPoolNums = 16
pullMessageThreadPoolNums = 16
queryMessageThreadPoolNums = 8

# Admin broker thread pool
adminBrokerThreadPoolNums = 16
clientManagerThreadPoolNums = 4

# Consumer manager thread pool
consumerManagerThreadPoolNums = 4
```

### Buffer Configuration

```toml
# Client buffer size (bytes)
clientSocketRcvBufSize = 131072
clientSocketSndBufSize = 131072

# OS page cache
osPageCacheBusyTimeOutMills = 1000
osPageCacheBusyTimeOutMills = 1000
```

### Flow Control

```toml
# Max message size (bytes)
maxMessageSize = 4194304

# Message delay level
messageDelayLevel = 1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
```

## Disk Configuration

### Disk Space Management

```toml
# Disk max used space ratio (%)
diskMaxUsedSpaceRatio = 85

# Disk space warning level ratio (%)
diskSpaceWarningLevelRatio = 90

# Disk space clean level ratio (%)
diskSpaceCleanLevelRatio = 85

# File reserved time (hours)
fileReservedTime = 72

# Delete when
deleteWhen = 04
```

### Flush Strategy

```toml
# ASYNC_FLUSH (default) - High performance, may lose data
# SYNC_FLUSH - Low performance, no data loss
flushDiskType = ASYNC_FLUSH

# Flush commit log least pages
flushCommitLogLeastPages = 4

# Flush commit log thorough interval (ms)
flushCommitLogThoroughInterval = 10000
```

## High Availability Configuration

### Master-Slave Configuration

```toml
# Master broker
brokerRole = ASYNC_MASTER
brokerId = 0

# Slave broker
# brokerRole = SLAVE
# brokerId = 1
```

### HA Connection

```toml
# HA listen port
haListenPort = 10912

# HA housekeeping interval (ms)
haHousekeepingInterval = 20000

# HA transfer batch size
haTransferBatchSize = 32768
```

## Security Configuration

### ACL

```toml
# Enable ACL
aclEnable = true
```

### TLS

```toml
# Enable TLS
tlsEnable = true
tlsTestModeEnable = false
tlsServerCert = /path/to/server.pem
tlsServerKey = /path/to/server.key
tlsServerAuthClient = true
```

## Monitoring Configuration

```toml
# Message trace enable
traceTopicEnable = true
traceTopicName = RMQ_SYS_TRACE_TOPIC

# Enable statistics
enableCalcFilterBitMap = true
```

## Best Practices

1. **Use SSDs**: Significantly improves commit log performance
2. **Configure appropriate flush mode**: Balance performance and reliability
3. **Monitor disk usage**: Set up alerts for disk space
4. **Separate commit log and consume queue**: Use different disks
5. **Tune thread pools**: Match to your workload
6. **Set appropriate message size**: Prevent oversized messages

## Next Steps

- [Client Configuration](./client-config) - Configure producers and consumers
- [Performance Tuning](./performance-tuning) - Optimize broker performance
- [Storage](../architecture/storage) - Understand storage mechanism
