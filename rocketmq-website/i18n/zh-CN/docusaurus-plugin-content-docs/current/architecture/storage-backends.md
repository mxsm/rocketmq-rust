---
title: "存储后端装配与恢复"
---

RocketMQ Rust 提供本地主日志路径、可选 RocksDB 派生存储，以及可选分层二级存储。这些是组合存储中的不同职责，不是三个能够在同一目录上直接切换的等价引擎。

[存储契约](storage.md) 定义追加回执、持久化边界、派生进度及生命周期所有权。本章解释实现如何满足这些契约。

## 各层职责

| 层 | 拥有的职责 | 启用方式 |
| --- | --- | --- |
| `rocketmq-store-api` | 不依赖运行时的读/写/管理/复制契约及结果类型 | 共享依赖，本身不运行存储 |
| `rocketmq-store` | `StoreFactory`、排他 `StorePorts`、Broker 集成、分发和服务装配 | 有效后端配置及编译 features |
| `rocketmq-store-local` | CommitLog、映射文件、恢复、本地 ConsumeQueue/索引、刷盘和 HA 原语 | 由组合存储使用 |
| `rocketmq-store-rocksdb` | 列族、编解码、ConsumeQueue/索引/定时/事务派生状态及快照 | `rocksdb_store` 集成与运行时选择 |
| `rocketmq-tieredstore` | 二级分发/读取、分段、元数据、提供方及保留策略服务 | `tieredstore` 集成与存储策略/提供方 |

## 主日志与派生结构

本地 CommitLog 保存主记录。ConsumeQueue 将队列逻辑偏移量映射到主记录；Key 索引用于查询。分发器根据主日志记录推进派生结构。派生更新本身不证明主记录已持久化。

RocksDB 存储装配保留本地文件 CommitLog。派生状态集成通过 `WalPort` 访问主日志，RocksDB 管理所选队列/索引/定时/事务结构。RocksDB 数据库成功打开，不证明它与当前主日志匹配，也不证明重放已追平。

派生游标标识引擎、源 epoch 及物理进度的排他边界。推进进度必须遵守主日志持久化覆盖及连续处理约束。游标不是消费偏移量，不能为了消除恢复告警而任意重置。

## 打开与加载

`StoreFactory` 创建所选组合；初始化、加载/恢复、启动、关闭仍是不同阶段。配置校验成功后，恢复仍可能因文件系统状态、原生数据库打开或持久化记录不适用而失败。

`RocksDbOpenPlan::from_config` 只校验配置、不打开存储，在禁用或无效时不返回计划。有效计划不是文件系统探测结果。打开操作使用提供的子服务上下文，维护/重建任务仍由该上下文管理。

存储默认 features 包含 `fast-load`。本地原语中，仅启用 `safe-load` 时选择顺序加载；二者同时启用时 `fast-load` 优先，文档中的安全加载环境覆盖项可以强制顺序路径。这些开关改变加载策略，不改变记录和恢复契约。

## 分层存储属于二级路径

分层分发将消息数据、队列/索引信息写入二级分段及元数据。读取在所选策略下支持逻辑队列偏移量、存储时间戳和 Key 查询。内置提供方为 POSIX 文件及内存存储；内存提供方适合测试，无法在进程丢失后保留数据。

恢复会对照元数据和实际分段大小进行协调，包括部分提交的分段。保留策略删除过期分段和对应索引元数据。因此分层存储拥有独立的持久化、清理和恢复工作。添加该层不会自动增强主路径发送确认，也不会自动形成独立备份。

提供 POSIX 后端不能作为内置 S3 兼容提供方的证据。自定义提供方需要自己的持久化及故障契约。

## 故障与维护含义

| 情况 | 正确解释 |
| --- | --- |
| 追加已接纳，刷盘/副本等待超时 | 记录可能已存在；重试前检查类型化结果 |
| 派生存储落后 | 主记录字节与可查询性可能位于不同进度 |
| RocksDB 打开失败 | 有效配置不能消除原生库或文件系统错误 |
| 分层分发失败 | 二级可用性/进度受影响，不能推断主路径已回滚 |
| 清理发现活跃文件租约 | 生命周期契约允许前，仍被引用的文件不能回收 |
| 关闭截止时间耗尽 | 刷盘、任务、租约或回收可能尚未完成 |

备份和迁移需要同时考虑主记录、派生进度、元数据、后端布局及分层提供方状态。切换 `storeType` 不是迁移流程。应在隔离副本上验证恢复兼容性和实际恢复结果。

## 构建与平台约束

虽然 `rocketmq-store-rocksdb` 自身默认 feature 列表为空，它始终依赖原生 RocksDB。构建集成需要原生 C++/绑定工具链。可选 `io_uring` 支持仅用于 Linux，且仍需检查主机能力；编译 feature 不代表实际启用。

应为拥有存储的服务二进制选择 features。单独构建原语 crate，既不会选择 Broker 后端，也不会启动其服务。

## 源码地图

- [存储装配](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-store/README.md)。
- [本地原语](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-store-local/README.md)、[RocksDB 基础层](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-store-rocksdb/README.md)。
- [分层实现](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-tieredstore/README.md)。
- 继续阅读 [HA 持久性](ha-controller.md)及[部署边界](../deployment/overview.md)。
