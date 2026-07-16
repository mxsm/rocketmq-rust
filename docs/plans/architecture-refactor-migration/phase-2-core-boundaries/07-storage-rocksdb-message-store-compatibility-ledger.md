# PR-M06-10 RocksDB MessageStore Adapter 兼容与回滚 Ledger

## 完成目标

本工作包把 RocksDB message-store kernel、Timer/Transaction kernel 和派生状态生命周期交给
`rocketmq-store-rocksdb`，同时保留 `rocketmq_store::message_store::rocksdb_message_store::RocksDBMessageStore`
旧路径、旧 `MessageStore` trait、配置和返回 DTO。Local CommitLog 仍是唯一消息 WAL；RocksDB 只持久 CQ、Index、
Timer、Transaction、checkpoint 和 maintenance 等派生状态。

## 所有权与适配边界

| 能力 | Canonical owner | Store 兼容层 |
|---|---|---|
| 唯一消息 WAL、mapped-file lease、offset correction | `rocketmq-store-local::commit_log` 与 `LocalWalPort` | `StoreLocalWalAdapter` 投影旧 `SelectMappedBufferResult` |
| Rocks CQ/Index、offset/time query、truncate/delete/clean | `rocketmq-store-rocksdb::message_store::RocksDbDerivedStore` | `RocksDBMessageStore` 映射 `StoreError`、`GetMessageStatus` 与旧 trait |
| Local WAL + Rocks derived composition | `RocksDbMessageStoreRoot` 通过 `L: LocalWalPort` 注入读取 | factory 只组装同一个 `LocalFileMessageStore` CommitLog |
| Timer/Transaction build kernel | `rocketmq-store-rocksdb::{timer,transaction}` | Store 只把 `DispatchRequest` 和 legacy config 投影到中立 source trait |
| Rocks lifecycle/flush/health failure | `RocksDbDerivedStore` | Store 保留旧 shutdown/flush sentinel 与 health snapshot 行为 |

依赖方向固定为 `rocketmq-store-rocksdb → rocketmq-store-local → rocketmq-store-api`。Rocks owner 不依赖
Store facade、TieredStore、Broker、Client、Common、Remoting 或 `rocketmq-rust`，也不构造 `CommitLogRoot`、
`MappedFileQueue` 或第二消息日志。

## 兼容决定

- 默认 Rocks 模式不再隐式注册 Local CQ/Index dispatcher，只维护 Rocks CQ/Index 派生路径。
- `rocksdb_cq_double_write_enable=true` 是唯一保留 Local CQ/Index 镜像的显式兼容入口。
- Rocks CQ 成功落盘后单调推进 Local WAL 的 topic/queue allocator；load、restart catch-up 与 truncate 后从 Rocks max
  offset 恢复该表，防止重启补 reput 后重复分配逻辑 offset。
- 普通 key/tag 在消息没有 uniq-key 时使用既有 `IndexRocksDbKey` 的可选 uniq-key 编码；column family、前缀、hour、
  physical offset 和 value bytes 不变。uniq-key 存在时 bytes 与旧 golden 完全一致。
- Timer dispatcher 的 progress 继续使用 physical CommitLog offset；Timer 自身 logical checkpoint 只负责内部去重。
- 同步旧 API 在 Rocks 关闭或读失败时继续返回既有 `0`/`-1` sentinel；fallible API 映射为 `StoreErrorKind::RocksDb`，
  Local WAL port 失败映射为 backend-neutral storage error。
- `get_consume_queue`/`find_consume_queue` 等旧 concrete Local inspection API 在 R0 继续保留；默认 Rocks 模式不承诺
  存在 Local CQ 镜像，调用方应使用 `MessageStore` offset/read capability。

## Parity 与故障语料

- Local/Rocks：Found、OffsetOverflowOne、min/max/next offset 和读取数量一致。
- Restart：已派生 reopen、dirty-tail recovery、未派生 crash/reput catch-up 以及 catch-up 后下一逻辑 offset 连续。
- Query：普通 key 无 uniq-key、uniq-key fallback、Rocks-only index、time lower/upper boundary。
- Failure：关闭后的 offset/read/flush typed mapping、legacy sentinel、health snapshot。
- Dispatch：默认仅 Rocks CQ/Index；显式 double-write 才建立 Local CQ 镜像；Timer/Transaction enable gate 保持。

## 删除条件

Store 兼容 adapter 只有在下一 major 同时满足以下条件时才能删除：所有 Broker/Proxy/Admin/Dashboard consumer 已改用
store-api capability；旧 public path 和 feature alias 的弃用周期结束；Local/Rocks/Tiered parity corpus 在替代 facade 上通过；
磁盘兼容和唯一 WAL 再次签署。

## 回滚点

整体 revert PR-M06-10 即可让 Rocks factory 指回 M06-09 adapter。`rocketmq-store-local`、默认 Local factory、唯一
CommitLog、现有 RocksDB 目录、column family 和消息 bytes 均无需迁移。若只回滚默认派生替换，可临时显式启用
`rocksdb_cq_double_write_enable`，但不得把隐式双写重新定义为默认行为。

## 验证快照

最终验证命令和退出码记录在
[`06-storage-boundary-extraction.md`](06-storage-boundary-extraction.md) 的 “M06-10 RocksDB MessageStore adapter evidence”；
本 ledger 只冻结兼容决定，不用计划命令冒充已执行证据。
