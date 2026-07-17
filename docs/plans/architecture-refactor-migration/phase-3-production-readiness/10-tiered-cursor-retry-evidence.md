# M10-02 Tiered cursor、retry ledger 与背压实施证据

## 结果

M10-02 将 M10-01 的派生进度合同接入真实 Tiered 写路径：CommitLog 仍是唯一 WAL，Tiered 仅持久化
versioned cursor 与不含 payload 的 retry ledger，并通过 count、bytes、age 三类硬上限、WAL pin 和上游背压
阻止无界堆积。该包完成后架构盘点为 **61/82**，剩余 21 个工作包；M10 剩余 3 个，下一包是
PR-M10-03。

## 可追溯性

| 字段 | 值 |
|---|---|
| 工作包 | `PR-M10-02` |
| Issue | [#8262](https://github.com/mxsm/rocketmq-rust/issues/8262) |
| 分支 | `mxsm/architecture-refactor-tiered-cursor-retry-backpressure` |
| Main 基线 | `e43b19a642badcefacf4e60a8b28d4bff7b69181` |
| 冻结代码候选 | `1a5463b143f33a8246d32d82abd05ddacf3b14c6` |
| 运行期证据 | `target/architecture-refactor/M10/tiered-cursor-8262/` |

## 目标与合同结论

- CommitLog 是唯一权威 WAL；retry ledger 只保存
  `(source_epoch, physical_offset, length, topic, queue, retry_state)`，不复制消息体。
- `config/tieredDispatchProgress.bin` 使用 magic、version、payload length、JSON 和 CRC32；临时文件写入、
  sync、原子 rename 后才发布内存状态，损坏或未知版本 fail closed。
- provider 失败先与 cursor 原子持久化到 ledger，之后全局 cursor 才能越过；重启按
  `DerivedRecordId` 从 CommitLog 重建 payload 并幂等重试。
- reader 同时受 channel count 与 byte semaphore 约束；达到 ledger count、bytes 或 age 上限时停止接收、
  `readiness=false`，把背压传回 reput，而不是丢事件或产生无界“已完成但未提交”状态。
- retry 按条目退避并隔离失败；调度由 `ScheduledTaskGroup` 所有，shutdown 可取消阻塞发送，正常关闭则排空
  已跟踪记录。
- 最小未解决记录对应的 CommitLog segment 被 pin；Local 清理和悬挂文件重试都不能越过该 pin，成功重试后
  自动释放。
- primary append receipt、SyncFlush durable watermark 与 Broker ack 不等待 Tiered；派生健康状态独立暴露。
- provider partial write 必须完整续写后才成功，避免短写被误认为 durable commit。

## 正确性语料

| 语料 | 结果 |
|---|---|
| Tiered cursor/retry corpus | 7/7：timeout、partial write、restart、duplicate、ledger full、WAL pin、repair |
| Tiered package | 56 个 library、1 个 POSIX 与 7 个 cursor/retry 集成测试通过 |
| Store Tiered adapter | 5/5：真实 reput 写入、读取 fallback、转换与已有行为兼容 |
| Store Local | 185 个 library 测试及全部 integration/doc-test target 通过，含 pin 生命周期 |
| Store API | 26/26，通过 `DerivedRecordId` 排序扩展后的合同语料 |
| RocksDB 专项 | Store 82 foundation、9 semantics；Broker 20 Rocks 与 4 POP 全部通过 |

RocksDB semantics 首次运行暴露 `rocksdb_query_message_after_dispatch` 中 pending index 为 1。原因是异步批量
默认实现逐条调用 `dispatch_async`，绕过了 RocksDB Index/Timer/Transaction 的 batch flush。修复后默认异步
批量保留各实现的 `dispatch_batch` 语义，仅 Tiered 显式逐条等待异步背压；原测试恢复为 9/9，Tiered 5/5
同步复核通过。该失败是本次发现并修复的回归，不计为成功证据。

## 公共 API 审查

初次签名比较没有删除或重命名公共路径：

- `rocketmq-store` 保持 382 个公共路径且 path hash 不变，仅 trait async 默认实现导致 Rustdoc 原始 hash 变化；
- `rocketmq-store-api` 保持 118 个公共路径且 path hash 不变，仅 `DerivedRecordId` 增加排序派生；
- `rocketmq-tieredstore` 从 105 增至 116 个公共路径，仅新增 `TieredDispatchHealth`、
  `TieredDispatchReadiness` 及其 9 个状态 variant；
- 其余 workspace library 的公共 path count 与 path hash 不变。

基线绑定冻结代码候选 `1a5463b143f33a8246d32d82abd05ddacf3b14c6`。最终提交态比较结果见下方验证记录。

## 验证记录

| 命令/门禁 | 结果 |
|---|---|
| Tiered、Store Tiered、Store API、Store Local focused/package tests | 通过 |
| Store/Broker RocksDB strict Clippy 与规定测试矩阵 | 通过 |
| affected-crate all-target/all-feature strict Clippy | 通过 |
| dependency fixtures/target/baseline 与 release guard | 通过：target 35/35，dev-only 3/3 |
| ArcMut fixtures/direct guard | 通过；5 个异步生命周期 fingerprint 经精确 relocation 审批，identity/occurrence 零增长 |
| runtime enforcing audit | 通过；retry scheduler 使用 `ScheduledTaskGroup`，无新增裸 Tokio task |
| typed-error hygiene 与 AGENTS routing | 通过 |
| MCP consumer check/test/streamable-http strict Clippy/doc | 通过 |
| 根 `cargo fmt --all -- --check` 与 workspace strict Clippy | 通过 |
| public API snapshot | 最终 31/31，differences=0 |
| `git diff --check` | 通过 |

### 已披露的基线失败

`cargo test -p rocketmq-store` **不记录为通过**。单独复跑
`file_store_vs_rocksdb_behavior_parity_after_restart` 仍失败：fixture 在配置为 `StoreType::RocksDB` 时仍构造
`LocalFileMessageStore`，因此伪 Rocks 路径没有逻辑队列。`git diff --exit-code origin/main --
rocketmq-store/tests/commitlog_recovery_tests.rs` 返回零并输出 `BASELINE_FIXTURE_UNCHANGED`，证明该测试及工厂未被
M10-02 修改。真实 RocksDB foundation/semantics 和 Broker 专项矩阵全部通过；剩余风险仅为该既有测试工厂错误。

## 审查与回滚

`[REV]` 结论：cursor/retry 不参与 Broker ack；ledger 不含 payload；count/bytes/age、队列 bytes 与 WAL pin
均有硬边界；失败 worker 不阻塞其他条目；无第二 WAL。M10-02 最终 material finding 为 0。

回滚时停止 Tiered reader、设置 `readiness=false`，冻结已提交 cursor 与 retry ledger并保留/pin 对应 WAL。
不得删除或越过未解决记录；修复后从已验证 checkpoint 重放。只有通过相同 cursor/retry/kill-restart corpus 的
上一实现才允许受控切换。
