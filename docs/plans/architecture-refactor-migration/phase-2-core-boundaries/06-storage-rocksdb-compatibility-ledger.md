# PR-M06-09 RocksDB Foundation 兼容与所有权 Ledger

本文冻结 `rocketmq-store-rocksdb` 首次成为物理 owner 后的 native 依赖、持久格式、旧 Store 深路径、runtime 边界和后续 adapter 迁移条件。
本包只迁 foundation，不接管 `RocksDBMessageStore`、Timer/Transaction DispatchRequest adapter 或 Store factory；唯一 CommitLog 仍由 Local owner 提供。

## Canonical ownership

| 能力 | canonical owner | `rocketmq-store` 保留面 | 后续删除条件 |
|---|---|---|---|
| native RocksDB dependency、options、error mapping、batch、iterator、snapshot、checkpoint/backup | `rocketmq-store-rocksdb` | `rocksdb::{batch,checkpoint,error,iterator,options,snapshot,store}` 精确 re-export | M06-11 冻结 facade/feature；下一 major 才可移除旧路径 |
| column family、key/value codec 与持久 key/value layout | `rocketmq-store-rocksdb::{column_family,key,value,codec}` | 精确 type/const/function re-export，不复制编码算法 | M06-12 consumer 清单和兼容窗口关闭 |
| normalized Rocks config 与 source port | `rocketmq-store-rocksdb::config` | `MessageStoreConfig` 实现 `RocksDbConfigSource`；旧 `RocksDbConfig::*_from_message_store_config` 调用保持 | legacy config envelope 在下一 major 处理 |
| Rocks CQ kernel、batch writer、offset/truncate/group commit | `rocketmq-store-rocksdb::consume_queue` | `DispatchRequest` source 实现和 `CommitLogDispatcherBuildRocksDbConsumeQueue` adapter | M06-10 adapter parity、M06-11 facade 冻结 |
| Rocks Index kernel、record build queue 与 flush | `rocketmq-store-rocksdb::index` | `DispatchRequest` source 实现和 `CommitLogDispatcherBuildRocksDbIndex` adapter | M06-10 adapter parity、M06-11 facade 冻结 |
| maintenance scheduler 与 blocking/runtime scope | `rocketmq-store-rocksdb::{maintenance,runtime}` | Timer/Transaction legacy service 暂经私有 runtime re-export 调用；Store benchmark/test scope 保持 | M06-10 迁剩余 adapter 后删除私有 bridge |
| `RocksDBMessageStore`、Timer/Transaction dispatcher、backend factory | `rocketmq-store`（暂留） | 完整 legacy adapter | PR-M06-10/M06-11 分别迁移和冻结 |

## Feature and dependency compatibility

- `rocketmq-store-rocksdb` 为 root workspace 第 29 个 package，`default = []`，直接拥有 `rocksdb`/`librocksdb-sys`。
- Store `rocksdb_store` 和旧 alias `rocksdb-store` 语义不变；主 feature 只弱转发 optional `rocketmq-store-rocksdb`，Store manifest 不再直接声明 native `rocksdb`。
- Store default、Store `--no-default-features` 和 `rocketmq-store-local` normal dependency tree 均不包含 `rocketmq-store-rocksdb`、`rocksdb` 或 `librocksdb-sys`。
- Rocks owner 不依赖 Store facade、TieredStore、Broker、Remoting、Common 或 `rocketmq-rust`；source 不出现 `DispatchRequest`、`MessageStoreConfig`、`CommitLogDispatcher` 或第二 CommitLog。

## Persistent and API compatibility

- column family 名称 `default/offset/timer/trans/popState`、CQ key/control bytes、Index `K/T/U` marker、Timer/Transaction key/value、20B CQ value和 checkpoint bytes 不变。
- Store 旧 `rocketmq_store::rocksdb::*` 类型、常量、函数和构造路径继续精确转发；Broker config/POP consumer 无需改 import。
- `RocksDbConfigSource` 与 CQ/Index dispatch source trait 只把 facade DTO 投影为 backend-neutral getter，不把 Common/Broker 类型带入新 owner。
- MSVC 链接 native standalone tests 会超过 PDB 限制，root test profile 仅对 `rocketmq-store-rocksdb` 设置 `debug = 0`；release/dev、其他 package 和运行语义不变。

## Runtime and rollback

- 原 Store Rocks runtime 的 3 个 current-runtime adapter 和 3 个 TaskGroup root 机械迁到新 owner；runtime baseline 仅替换六条路径，指纹后缀和总数不变。
- 回滚时整体 revert PR-M06-09：Store `rocksdb_store` 指回直接 optional native dependency，旧实现文件恢复，root workspace 移除新 crate。default/local 路径、Local CommitLog、磁盘目录和 bytes 无需迁移。
- 不得通过回滚创建第二 CommitLog、改变 column family/key/value bytes 或删除旧 Store public path。

## Validation snapshot

- 新 owner：`cargo check -p rocketmq-store-rocksdb --no-default-features`、`cargo test -p rocketmq-store-rocksdb`、source contract 和 dependency tree isolation 通过。
- legacy 与 Broker：foundation 82/82、Store semantics 5/5、Broker rocks 20/20、pop_consumer 4/4 通过。
- architecture dependency、enforcing runtime audit、ArcMut guard/63 单测/24 fixtures、AGENTS routing、workspace fmt 与 workspace all-target/all-feature Clippy 通过。typed-error guard 在候选分支和未修改的 `origin/main` 均复现相同 Broker/MCP allowlist及缺失治理文档基线，未计为通过且本包没有新增 finding。
