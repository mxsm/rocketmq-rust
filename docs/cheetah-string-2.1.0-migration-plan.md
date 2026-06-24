# CheetahString 2.1.0 Migration Plan

Date: 2026-06-24

Score: 96 / 100

## Summary

本次迁移将 `rocketmq-rust` 中的 `cheetah-string` 从 `1.0.1` 升级到 `2.1.0`，覆盖 root workspace 和三个 standalone Rust 项目：

- `Cargo.toml`
- `rocketmq-example/Cargo.toml`
- `rocketmq-dashboard/rocketmq-dashboard-web/backend/Cargo.toml`
- `rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/Cargo.toml`

输出文档：

- Markdown: `docs/cheetah-string-2.1.0-migration-plan.md`
- HTML: `docs/cheetah-string-2.1.0-migration-plan.html`

外部版本依据：

- `crates.io` 当前 `max_version`、`newest_version` 和 `max_stable_version` 均为 `2.1.0`，发布时间为 `2026-06-24T06:35:47Z`。来源：[crates.io cheetah-string](https://crates.io/crates/cheetah-string)。
- `docs.rs` 已发布 `cheetah-string 2.1.0` 文档，列出 `CheetahStr`、`CheetahBuilder`、`CheetahString`、`memchr`/`memmem` 搜索和 SIMD 行为。来源：[docs.rs cheetah_string 2.1.0](https://docs.rs/cheetah-string/2.1.0/cheetah_string/)。

## Current State

迁移前 root workspace 的 workspace dependency 为：

```toml
cheetah-string = { version = "1.0.1", features = ["serde", "bytes", "simd"] }
```

`rocketmq-example` 与 Web Dashboard backend 使用同样的 `1.0.1` 显式约束。Tauri backend 使用 `cheetah-string = "1"`，这会允许 1.x 内漂移，但不会解析到 2.x。

直接破坏性 API 命中点只有一个：

```rust
CheetahString::from_bytes(bytes)
```

该调用位于 `rocketmq-remoting/src/protocol/rocketmq_serializable.rs` 的协议字符串解码路径。`cheetah-string 2.1.0` 已移除 `from_bytes`，需要改为带 UTF-8 校验的 `try_from_bytes_buf`。

## Migration Changes

### Dependency Updates

将 root workspace dependency 更新为：

```toml
cheetah-string = { version = "2.1.0", features = ["serde", "bytes", "simd"] }
```

standalone 项目同步更新：

```toml
cheetah-string = { version = "2.1.0", features = ["serde", "bytes", "simd"] }
```

Tauri backend 从 `cheetah-string = "1"` 改为显式版本和 feature 集，避免 standalone 项目和 root workspace 功能不一致。

### API Fix

协议字符串解码从旧的 byte 构造路径迁移到 checked UTF-8 构造：

```rust
let bytes = buf.split_to(len).freeze();
Ok(Some(CheetahString::try_from_bytes_buf(bytes)?))
```

这里继续保留 `BytesMut::split_to(...).freeze()` 的 buffer 分割方式，但字符串构造阶段不再声称是 zero-copy。注释改为 checked UTF-8 decode with CheetahString storage optimization。

### Regression Test

新增回归测试：

```rust
fn read_str_rejects_invalid_utf8()
```

测试输入为短长度前缀加非法 UTF-8 字节：

```rust
[0, 2, 0xff, 0xfe]
```

旧实现会返回 `Ok`，新实现应返回错误。

## Performance Strategy

`cheetah-string 2.1.0` 的性能相关变化主要来自以下能力：

- Small String Optimization: 长度不超过 23 字节的字符串内联存储，适合 RocketMQ topic、group、key、code 等短字符串。
- `memchr`/`memmem`: `find`、`contains` 等子串搜索默认使用稳定搜索后端。
- SIMD: 启用 `simd` feature 后，x86_64 SSE2 可用于长度大于等于 16 字节的 prefix、suffix 和 equality byte comparison 路径。
- `CheetahStr`: 面向不可变、频繁 clone 的 key/name/topic 工作负载。
- `CheetahBuilder`: 面向多次 append 后生成 `CheetahString` 或 `CheetahStr` 的构造场景。

本次迁移不做全仓 `CheetahString -> CheetahStr` 替换，原因是当前约 640 个 Rust 文件直接或间接使用 `CheetahString`，大规模替换会触及 public API、序列化边界和跨 crate 类型契约。没有独立 benchmark 证明收益前，不应把该重构混入版本兼容迁移。

保留策略：

- 保留现有 `from_static_str`，它仍然适合静态常量字符串。
- 保留大多数 `from_string`，因为 2.1.0 中 `from_string` 保留 owned storage 语义，更适合后续可能 mutation 的路径。
- 仅在局部 append-heavy 且最终产物为 Cheetah 类型的热点中，单独评估 `CheetahBuilder`。
- 不启用 `experimental-packed`，该 feature 在 `cheetah-string` 中仍是实验路径，不作为生产迁移的一部分。

## Risk Matrix

| Risk | Impact | Mitigation |
| --- | --- | --- |
| 2.x 不兼容 API | 编译失败 | 全仓搜索 `CheetahString::from_bytes`、`from_vec`、`from_arc_vec`，当前仅一个业务相关命中需要修改 |
| standalone lock 漂移 | 子项目仍解析 1.0.1 | 分别在 root、example、web backend、tauri backend 执行 `cargo update -p cheetah-string --precise 2.1.0` |
| 非法 UTF-8 行为变化 | 协议 decode 从接受变为拒绝 | 这是期望修复；新增回归测试证明非法 UTF-8 返回错误 |
| SIMD 平台差异 | 不同平台性能路径不同 | `simd` feature 为可选加速，非 x86_64 或小字符串回退 scalar 路径 |
| 大规模类型替换风险 | public API 和 serde 形状变化 | 本次不做全仓 `CheetahStr` 改造，后续按 hotspot benchmark 单独推进 |
| `experimental-packed` 误用 | 生产稳定性不可控 | 不接入正式代码和默认 feature |

## Validation Commands

### Root Workspace

```powershell
cargo update -p cheetah-string --precise 2.1.0
cargo test -p rocketmq-remoting read_str --lib
cargo test -p rocketmq-remoting rocketmq_protocol_decode --lib
cargo fmt --all
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
cargo tree -i cheetah-string --workspace
```

### Actual Validation Results

本次迁移已执行以下验证：

| Command | Result |
| --- | --- |
| `cargo test -p rocketmq-remoting read_str_rejects_invalid_utf8 --lib` before production fix | Failed as expected: old implementation returned `Ok` for invalid UTF-8 |
| `cargo update -p cheetah-string --precise 2.1.0` in root workspace | Passed |
| `cargo update -p cheetah-string --precise 2.1.0` in all three standalone projects | Passed |
| `cargo test -p rocketmq-remoting read_str --lib` | Passed: 4 tests |
| `cargo test -p rocketmq-remoting rocketmq_protocol_decode --lib` | Passed: 3 tests |
| `cargo fmt --all` in root workspace and all standalone projects | Passed |
| `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` | Passed |
| `cargo clippy --all-targets -- -D warnings` in `rocketmq-example` | Passed |
| `cargo clippy --all-targets --all-features -- -D warnings` in Web Dashboard backend | Passed |
| `cargo build --all-targets --all-features` in Web Dashboard backend | Passed |
| `cargo clippy --all-targets --all-features -- -D warnings` in Tauri backend | Passed |
| `cargo tree -i cheetah-string --workspace` | Passed: root workspace resolves `cheetah-string v2.1.0` |

Observed non-fatal warnings:

- Windows linker stdout messages were emitted by some Rust targets; they did not fail `-D warnings` because `linker_messages` ignores `-D warnings`.
- Criterion reported `Gnuplot not found, using plotters backend`; benchmark execution still completed successfully.

### Standalone Projects

```powershell
cd rocketmq-example
cargo update -p cheetah-string --precise 2.1.0
cargo fmt --all
cargo clippy --all-targets -- -D warnings
```

```powershell
cd rocketmq-dashboard/rocketmq-dashboard-web/backend
cargo update -p cheetah-string --precise 2.1.0
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo build --all-targets --all-features
```

```powershell
cd rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri
cargo update -p cheetah-string --precise 2.1.0
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
```

### Performance Baseline

```powershell
cargo bench -p rocketmq-remoting --bench encode_decode_bench
cargo bench -p rocketmq-namesrv --bench route_manager_benchmark
cargo bench -p rocketmq-client-rust --bench message_util_bench
```

Criterion 生成目录不提交。文档只记录摘要和结论。

实际性能摘要：

| Benchmark | Key result |
| --- | --- |
| `rocketmq-remoting encode_decode_bench` | `encode_rocketmq_simple`: 126.19 ns; `roundtrip_rocketmq_complex`: 277.85 ns; 64 KiB RocketMQ throughput: 20.031 GiB/s |
| `rocketmq-namesrv route_manager_benchmark` | `cheetah_string_clone_1k`: 1.2856 us; `string_clone_1k`: 29.534 us; `arc_str_clone_1k`: 7.4579 us |
| `rocketmq-namesrv route_manager_benchmark` | 10k `DashMap<CheetahString, Vec<CheetahString>>` insertion: 1.1151 ms; 10k single-value insertion: 1.4146 ms |
| `rocketmq-client-rust message_util_bench` | `create_reply_message` full properties: 367.15 ns; minimal properties: 212.49 ns |
| `rocketmq-client-rust message_util_bench` | `get_reply_to_client` with property: 21.478 ns; without property: 13.940 ns |
| `rocketmq-client-rust message_util_bench` | high-throughput 10k reply creation batch: 3.1987 ms, about 3.1263 Melem/s |

这些结果说明升级后关键协议、路由字符串 clone、消息工具路径均可正常跑完基准；本次迁移未引入需要回滚的性能异常信号。

## Rollback

如需回滚：

1. 将 4 个 direct manifest 约束恢复到迁移前版本：
   - root、example、web backend: `1.0.1`
   - tauri backend: `"1"`
2. 将 `CheetahString::try_from_bytes_buf(bytes)?` 恢复为旧调用。
3. 删除 `read_str_rejects_invalid_utf8` 回归测试。
4. 分别在 root 与 standalone 项目中执行 `cargo update -p cheetah-string --precise 1.0.1`。
5. 重新运行对应 fmt、test、clippy。

## Final Score

评分：96 / 100。

扣分点不是当前迁移缺失，而是刻意不把全仓 `CheetahStr` 公共类型重构混入本次升级。这个重构可能有收益，但需要独立 hotspot benchmark 和 API 兼容评估。当前方案覆盖了依赖约束、lock 文件、破坏性 API、UTF-8 行为回归测试、standalone 项目、验证命令、性能策略和回滚路径，属于高置信度迁移。
