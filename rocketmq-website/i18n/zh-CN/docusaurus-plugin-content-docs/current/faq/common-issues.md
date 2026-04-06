---
sidebar_position: 1
title: 常见问题
---

# 常见问题

本页汇总 RocketMQ-Rust 使用过程中的高频问题与快速解决方案。

## 连接问题

### 连接被拒绝（Connection Refused）

**问题**：连接 Broker 时出现 `Connection refused`。

**解决方案**：

1. 检查 Broker 是否运行：

    ```bash
    # 查看 broker 进程
    ps aux | grep rocketmq

    # 检查端口监听
    netstat -an | grep 10911
    ```

2. 检查 Name Server 地址：

    ```rust
    use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;

    let producer = DefaultMQProducer::builder()
        .producer_group("check_group")
        .name_server_addr("localhost:9876")
        .build();
    ```

3. 检查防火墙规则：

    ```bash
    # 放行 9876 和 10911
    sudo ufw allow 9876
    sudo ufw allow 10911
    ```

### 无法发现 Broker（Cannot Find Brokers）

**问题**：Producer 或 Consumer 无法发现 Broker。

**解决方案**：

1. 验证 Name Server 连通性：

    ```bash
    # 测试 Name Server 连接
    telnet localhost 9876
    ```

2. 检查 Broker 注册状态：

    ```bash
    # 使用 RocketMQ 管理工具
    sh mqadmin clusterList -n localhost:9876
    ```

3. 确认 Topic 是否存在：

    ```bash
    # 查看 topic 列表
    sh mqadmin topicList -n localhost:9876
    ```

## 消息问题

### 消息未收到（Message Not Received）

**问题**：Consumer 收不到消息。

**解决方案**：

1. 检查订阅表达式：

    ```rust
    // 确保 topic 与订阅表达式匹配
    consumer.subscribe("TopicTest", "*").await?;
    ```

2. 检查消费组：

    ```rust
    // 确保消费组正确
    consumer.set_consumer_group("correct_consumer_group");
    ```

3. 检查消息模型：

    ```rust
    // 确认是集群还是广播模式
    consumer.set_message_model(MessageModel::Clustering);
    ```

4. 检查消费起点：

    ```rust
    // 可能从最新位点开始，导致历史消息不可见
    consumer.set_consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset);
    ```

### 重复消息（Duplicate Messages）

**问题**：收到重复消息。

**说明**：这是 RocketMQ “至少一次投递”语义下的预期行为。

**解决方案**：实现幂等处理：

```rust
use std::collections::HashSet;

struct IdempotentProcessor {
    processed: HashSet<String>,
}

impl IdempotentProcessor {
    fn process(&mut self, msg_id: &str) -> bool {
        if self.processed.contains(msg_id) {
            return false; // 已处理
        }
        self.processed.insert(msg_id.to_string());
        true // 新消息
    }
}
```

## 性能问题

### 吞吐偏低（Low Throughput）

**问题**：发送或消费速率低。

**解决方案**：

1. 增大批量参数：

    ```rust
    producer.set_max_message_size(4 * 1024 * 1024);
    consumer.set_pull_batch_size(64);
    ```

2. 启用压缩：

    ```rust
    producer.set_compress_msg_body_over_howmuch(4 * 1024);
    ```

3. 调整线程池：

    ```rust
    consumer.set_consume_thread_min(10);
    consumer.set_consume_thread_max(20);
    ```

4. 使用异步发送：

    ```rust
    producer.send_with_callback(message, |result, error| {
        if let Some(send_result) = result {
            println!("Sent: {:?}", send_result);
        }
        if let Some(err) = error {
            eprintln!("Send failed: {}", err);
        }
    }).await?;
    ```

### 延迟偏高（High Latency）

**问题**：消息投递耗时过长。

**解决方案**：

1. 检查 Broker 刷盘配置：

    ```toml
    # broker.conf
    flushDiskType = ASYNC_FLUSH
    flushCommitLogLeastPages = 4
    ```

2. 控制消息体大小：

    ```rust
    // 对大消息启用压缩
    producer.set_compress_msg_body_over_howmuch(4 * 1024);
    ```

3. 检查网络延迟：

    ```bash
    ping broker_hostname
    ```

4. 监控系统资源：

    ```bash
    # CPU
    top

    # 磁盘 I/O
    iostat -x 1
    ```

## 内存问题

### 内存不足（Out of Memory）

**问题**：Consumer 进程 OOM 崩溃。

**解决方案**：

1. 限制处理队列阈值：

    ```rust
    consumer.set_pull_threshold_for_topic(10000);
    consumer.set_pull_threshold_for_queue(1000);
    ```

2. 降低拉取批量：

    ```rust
    consumer.set_pull_batch_size(32);
    ```

3. 优化消费逻辑：

    ```rust
    // 优化处理逻辑
    // 尽量采用异步处理
    ```

### 内存泄漏（Memory Leak）

**问题**：内存占用持续上升。

**解决方案**：

1. 检查无界集合：

    ```rust
    // 使用有界 channel
    let (tx, rx) = mpsc::channel(1000);

    // 周期清理已处理数据
    if processed.len() > 10000 {
        processed.clear();
    }
    ```

2. 及时释放消息引用：

    ```rust
    // 处理后不要长期持有消息对象
    consumer.register_message_listener_concurrently(|messages, _ctx| {
        for msg in messages {
                process_message(&msg);
                // 不要把 msg 放入长期存活的数据结构
        }
        Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
    });
    ```

    ```rust
    use rocketmq_client_rust::consumer::listener::consume_concurrently_status::ConsumeConcurrentlyStatus;
    use rocketmq_client_rust::consumer::mq_push_consumer::MQPushConsumer;
    use rocketmq_common::common::message::message_ext::MessageExt;

    fn process_message(_msg: &&MessageExt) {
        // 业务处理
    }
    ```

## 构建问题

### 编译失败（Compilation Errors）

**问题**：`cargo build` 失败。

**解决方案**：

1. 检查 Rust 版本：

    ```bash
    # 需要 Rust 1.70+
    rustc --version
    ```

2. 更新依赖：

    ```bash
    cargo update
    ```

3. 清理后重建：

    ```bash
    cargo clean
    cargo build
    ```

### Windows 链接错误（Link Errors）

**问题**：Windows 平台出现链接错误。

**解决方案**：

1. 安装 C++ 构建工具：

    ```bash
    # 安装 Visual Studio Build Tools
    # 或使用：rustup component add llvm-tools-preview
    ```

2. 使用 MSVC 工具链：

    ```bash
    rustup default stable-x86_64-pc-windows-msvc
    ```

## 排查清单

提问前建议先检查：

- [ ] Broker 是否已启动且可访问
- [ ] Name Server 是否已启动且可访问
- [ ] 客户端配置是否正确
- [ ] Topic 是否存在
- [ ] Consumer Group 是否正确
- [ ] 网络连通性是否正常
- [ ] 防火墙规则是否已配置
- [ ] 磁盘空间是否充足
- [ ] 内存是否充足
- [ ] RocketMQ-Rust 是否为较新版本

## 获取帮助

如果问题仍未解决：

1. 查看 [GitHub Issues](https://github.com/mxsm/rocketmq-rust/issues)
2. 阅读 [架构文档](../architecture/overview)
3. 在 [Stack Overflow](https://stackoverflow.com/questions/tagged/rocketmq) 搜索或提问
4. 加入 [mailing list](https://rocketmq.apache.org/about/contact)

## 下一步

- [性能 FAQ](./performance) - 性能相关问题
- [故障排查](./troubleshooting) - 高级调试方法
- [Broker 配置](../configuration/broker-config) - 查看配置选项
