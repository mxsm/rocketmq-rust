---
title: "客户端协调与请求所有权"
---

Rust 客户端将面向应用的生产、消费 API 与路由发现、Broker 连接、重试和消费协调连接起来。共享机制属于应用提供的 `ClientRuntime`；创建生产者不会隐式建立备用运行时。

## 所有权与共享状态

应用创建 `RuntimeOwner`，将子服务上下文和遥测句柄传给 `ClientRuntime::try_new`，再把返回的 `Arc<ClientRuntime>` 共享给各客户端门面。公开类型从 `rocketmq_client_rust` 或其 `prelude` 导入。内部工厂和处理器模块用于理解实现，不是应用应依赖的导入路径。

`MQClientInstance` 汇集生产者/消费者注册、主题路由缓存、Broker 地址状态、心跳协调、传输回调及定时刷新/再平衡工作。共享运行时所有权不代表每个门面拥有相同的组、订阅或客户端身份。身份和组配置仍决定 Broker 如何观察客户端。

## 从主题到请求

1. 解析配置的 NameServer 端点，按明确选择的发现实现进行可选发现。
2. 获取或刷新主题路由，生成发布/订阅队列视图。
3. 按本次操作的路由策略选择队列，解析其 Broker 端点。
4. 携带剩余截止时间，通过具有准入约束的受管理传输提交请求。
5. 同时解释传输结果和 Broker 结果，更新路由/故障信息，选择允许的重试。

路由缓存减少发现流量，但 Broker 迁移或角色变化后可能过期。路由更新协调器及定时刷新维护本地视图；具体操作仍需处理路由缺失及通告地址不可达。NameServer 连通性和 Broker 连通性是两项不同依赖。

普通生产者重试循环携带共享截止时间，在工作前检查，并将派生出的单次尝试截止时间传入发送核心。它不会为每次尝试重新获得完整超时预算。重试策略结合路由错误、发送状态、剩余次数和通信模式，可以刷新路由或选择其他 Broker。选择器及显式指定队列的 API 有自身语义，不能假设所有发送重载都采用相同故障转移行为。

## 完成具有多个层次

| 观察到的结果 | 应用已知事实 | 仍不确定的内容 |
| --- | --- | --- |
| 发送前本地校验/准入拒绝 | 本次尝试未通过该路径到达 Broker | 之前某次尝试是否成功 |
| 本地写入完成 | 传输完成本地写操作 | Broker 处理和持久化 |
| 有效发送响应 | Broker 返回了特定发送状态 | 业务消费，以及超出该状态的持久性 |
| 分发后响应超时或断连 | 未观察到确定响应 | Broker 是否已接纳或完成请求 |
| 客户端工作取消 | 本地等待/工作按所有者规则停止 | 远端操作是否仍会完成 |

回调、单向发送与等待响应发送的完成契约不同。外层 `Ok` 不总是 `SendOk` 确认；应检查所选 API 的返回或回调路径，详见[发送消息](../producer/sending-messages.md)。

应用重试需要结合幂等性和整体截止时间。在客户端有界重试之外包一层无限循环，会破坏该边界并可能产生重复效果。

## 消费协调

| API | 协调与完成 |
| --- | --- |
| Push 并发消费 | 客户端拉取/接收并调度回调；根据回调结果确认或重试 |
| Push 顺序消费 | 队列所有权/锁及顺序回调约束单队列处理 |
| Lite Pull | 客户端协调订阅队列及轮询；应用选择何时提交已处理进度 |
| Classic Pull 兼容入口 | 调用方通过基于当前客户端实现的已弃用门面指定队列/偏移量 |
| POP 路径 | Broker 投递不可见期和 receipt 确认补充客户端会话/分配状态 |

再平衡改变队列所有权，不会撤销上一任处理器的业务副作用。组内成员应采用兼容的订阅和处理语义。只提交应用可以安全恢复的进度。Lite Pull 本地提交状态和远端持久化是两个步骤，详见 [Pull 消费](../consumer/pull-consumer.md)。

共享消息引用可以在轮询后继续存活并保留内存。除了客户端队列，应用工作也应有界。同步 Push 监听器通过受管理阻塞执行边界运行；耗时业务操作在返回前仍会占用该容量。

## 关闭客户端

停止新增业务工作，在选定截止时间内完成或明确放弃在途业务操作，然后关闭生产者/消费者/Admin 门面。再等待 `ClientRuntime` 关闭并检查报告。在被观测服务结束后关闭遥测，并在运行时的异步执行边界外关闭其所有者。

释放一个门面不能替代关闭共享运行时。同样，取消异步等待也不能强行停止已经开始的阻塞回调。需要同时考虑关闭报告和未完成的业务工作。

## 源码地图

- [客户端入口与 features](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/README.md)。
- [客户端实例](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/factory/mq_client_instance.rs)、[路由协调器](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/factory/route_update.rs)。
- [生产者重试循环](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/producer/producer_impl/default_mq_producer_impl/retry.rs)。
- [运行时所有权](runtime.md)、[顺序消息](../guides/ordered-messages.md)、[POP](../consumer/pop.md)。
