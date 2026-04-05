---
sidebar_position: 3
title: 故障排查
---

# 故障排查

本页介绍 RocketMQ-Rust 的高级调试与诊断方法。

## 启用调试日志

```rust
// 设置 RUST_LOG 环境变量
// export RUST_LOG=rocketmq=debug,rocketmq_client=debug

// 或在代码中初始化日志
use log::debug;

fn main() {
    env_logger::init();
    // 你的代码
}
```

## 常见调试场景

### 消息未进入队列

**检查项**：

1. 消息是否发送成功：

    ```rust
    let result = producer.send(message).await?;
    println!("Send result: {:?}", result);
    ```

2. Topic 是否存在：

    ```bash
    sh mqadmin topicList -n localhost:9876
    ```

3. 查看 Broker 日志：

    ```bash
    tail -f ~/logs/rocketmqlogs/broker.log
    ```

### Consumer 不处理消息

**检查项**：

1. Consumer 是否启动：

    ```rust
    consumer.start().await?;
    println!("Consumer started");
    ```

2. 订阅是否正确：

    ```rust
    // 检查 topic 与订阅表达式
    consumer.subscribe("TopicTest", "*").await?;
    ```

3. 查看消费进度：

    ```bash
    # 使用 RocketMQ 管理工具
    sh mqadmin consumerProgress -n localhost:9876 -g my_consumer_group
    ```

## RocketMQ 管理工具

### 查看集群状态

```bash
# 列出所有集群
sh mqadmin clusterList -n localhost:9876

# 列出所有 broker
sh mqadmin brokerList -n localhost:9876

# 列出所有 topic
sh mqadmin topicList -n localhost:9876
```

### 查看 Topic 状态

```bash
# topic 状态
sh mqadmin topicStatus -n localhost:9876 -t TopicTest

# topic 路由
sh mqadmin topicRoute -n localhost:9876 -t TopicTest
```

### 查看消费者状态

```bash
# 消费者进度
sh mqadmin consumerProgress -n localhost:9876 -g my_consumer_group

# 消费者连接
sh mqadmin consumerConnection -n localhost:9876 -g my_consumer_group

# 消费者位点
sh mqadmin getConsumerOffset -n localhost:9876 -g my_consumer_group -t TopicTest
```

### 重置消费位点

```bash
# 重置到指定时间戳
sh mqadmin resetOffsetByTime -n localhost:9876 -g my_consumer_group -t TopicTest -timestamp 1699200000000

# 重置到最早位点
sh mqadmin resetOffsetByTime -n localhost:9876 -g my_consumer_group -t TopicTest -timestamp 0
```

## JMX 监控（Java Broker 场景）

```bash
# 启用 JMX
JAVA_OPT="${JAVA_OPT} -Dcom.sun.management.jmxremote"
JAVA_OPT="${JAVA_OPT} -Dcom.sun.management.jmxremote.port=9999"
JAVA_OPT="${JAVA_OPT} -Dcom.sun.management.jmxremote.authenticate=false"
JAVA_OPT="${JAVA_OPT} -Dcom.sun.management.jmxremote.ssl=false"
```

可通过 JConsole 或 VisualVM 观察：

- 内存使用
- 线程池状态
- 消息队列状态
- 消费堆积情况

## 网络排查

### 连通性测试

```bash
# 测试 Name Server 连接
telnet localhost 9876

# 测试 Broker 连接
telnet localhost 10911

# DNS 解析
nslookup broker_hostname

# 网络时延
ping broker_hostname
```

### 抓包分析

```bash
# 抓取 9876 端口流量
tcpdump -i any port 9876 -w name_server.pcap

# 抓取 10911 端口流量
tcpdump -i any port 10911 -w broker.pcap
```

## 磁盘问题

### 检查磁盘状态

```bash
# 磁盘空间
df -h

# 磁盘 I/O
iostat -x 1

# CommitLog 占用
du -sh /path/to/rocketmq/store/commitlog
```

### 文件句柄监控

```bash
# 查看打开文件数
lsof -p $(pidof java)

# 提升文件句柄限制
ulimit -n 65536
```

## 内存问题

### 检查内存占用

```bash
# 查看 RSS
ps aux | grep java

# 查看 JVM 堆（Java broker）
jmap -heap <pid>

# 排查内存泄漏
jmap -histo:live <pid> | head -20
```

## 线程转储

### 获取线程 Dump

```bash
# Java broker
jstack <pid> > thread_dump.txt

# 检查死锁
jstack <pid> | grep -A 10 "Found one Java-level deadlock"
```

## 常见错误信息

### `No route info of this topic`

**原因**：Topic 不存在或 Broker 未注册。

**解决方案**：

```bash
# 创建 topic
sh mqadmin updateTopic -n localhost:9876 -t TopicTest -c DefaultCluster
```

### `The consumer group not online`

**原因**：Consumer 未启动或已断连。

**解决方案**：

```rust
// 确保 consumer 已启动
consumer.start().await?;
```

### `The broker is not available`

**原因**：Broker 宕机或网络异常。

**解决方案**：

```bash
# 查看 broker 列表
sh mqadmin brokerList -n localhost:9876

# 必要时启动 broker
sh mqbroker -n localhost:9876
```

## 调试工具

### 日志分析

```bash
# 查看 broker 错误日志
grep "ERROR" ~/logs/rocketmqlogs/broker.log | tail -100

# 查看 consumer 错误日志
grep "ERROR" ~/logs/rocketmqfiles/consumer.log | tail -100
```

### 消息追踪

启用消息追踪：

```rust
producer_option.set_enable_msg_trace(true);
consumer_option.set_enable_msg_trace(true);
```

按 ID 或 Key 查询消息：

```bash
sh mqadmin queryMsgById -n localhost:9876 -i <msg_id>

sh mqadmin queryMsgByKey -n localhost:9876 -t TopicTest -k <key>
```

## 性能分析

### Rust 代码性能分析

```bash
# 安装 flamegraph
cargo install flamegraph

# 生成火焰图
cargo flamegraph --bin my_app

# 分析输出
# 输出文件为 flamegraph.svg
```

### 消费处理耗时分析

```rust
use std::time::Instant;

impl MessageListener for MyListener {
    fn consume_message(&self, messages: Vec<MessageExt>) -> ConsumeResult {
        let start = Instant::now();

        for msg in messages {
            process_message(&msg);
        }

        let elapsed = start.elapsed();
        log::debug!("Processed {} messages in {:?}", messages.len(), elapsed);

        ConsumeResult::Success
    }
}
```

## 获取帮助

如果仍无法定位问题：

1. **先看日志**：Broker 与客户端日志是第一手信息。
2. **结合管理命令**：核验 Topic、路由、位点与消费组状态。
3. **搜索 GitHub Issues**：定位是否已有已知问题。
4. **邮件列表求助**：提供最小复现与关键日志。
5. **Stack Overflow 提问**：带上 `rocketmq` 标签。

## 下一步

- [常见问题](./common-issues) - 高频问题速查
- [性能 FAQ](./performance) - 性能问题定位
- [配置概览](../configuration) - 核对配置项
