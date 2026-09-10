---
title: "离线存储工具"
---

# 离线存储工具

`rocketmq-store-inspect` 包构建 `rocketmq-cli-rust` 可执行文件，用于读取或转换本地存储文件，不连接在线 RocketMQ 集群。按状态影响选择子命令，并将其结果与完整恢复或回退决策分开。

## 选择操作

| 命令 | 输入 | 输出 | 状态影响与限制 |
| --- | --- | --- | --- |
| `read-message-log` | 单个 CommitLog 段 | 文件大小及消息/客户端 ID 表格 | 只读扫描，不获取 Broker Store 独占锁，不提供全面完整性报告 |
| `downgrade-preflight` | 规范 Broker TOML 和目标版本 | 结构化兼容报告，输出到 stdout 或所选文件 | 获取 Store 独占锁，检查选定持久格式但不转换 |
| `consolidate-multipath` | 源 CommitLog 根目录、新目标、段大小及实际 Store 根目录 | 新合并目录和 JSON 报告 | 在 Store 锁保护下复制/发布新文件，保留源文件 |

从仓库根目录执行：

```bash
cargo build -p rocketmq-store-inspect --bin rocketmq-cli-rust
cargo run -p rocketmq-store-inspect --bin rocketmq-cli-rust -- --help
```

调试构建后使用 `target/debug/rocketmq-cli-rust`，Windows 加 `.exe`。包名和可执行文件名不同，也不同于执行在线管理的 `rocketmq-admin-cli`。`--verbose` 增加受控诊断字段，不是暴露原始存储内容或凭据的开关。

## 准备一致输入

简单扫描使用稳定副本或已停止存储。预检或合并前停止所属 Broker，使用其实际 Store 根目录。无关空目录的锁不能保护 Broker 数据。

记录主 CommitLog 根目录、映射段大小、元数据路径及活动存储配置组合。保留可恢复源副本和足够操作空间。检查配置使用明确绝对路径，避免工具检查默认用户目录或从错误工作目录解析的存储。

下列示例含部署专属路径，应有意识替换，不表示可检查或修改任意在线主机的数据。

## 读取消息标识

```bash
target/debug/rocketmq-cli-rust read-message-log -c /data/commitlog/00000000000000000000 -f 0 -t 2
```

尽管参数名为 `-c / --config`，在本子命令中它表示段文件。`-f / --from`、`-t / --to` 是记录计数，不是字节或逻辑队列偏移量。当前实现先递增计数再过滤，`from=0` 和 `from=1` 均包含第一条记录，`from=2` 从第二条开始。`to=2` 将扫描限制在前两条记录。

读取器打印 `message_id`、`client_message_id` 并跳过消息体。较短表格不能证明整个段有效，截断或无效帧大小可能使扫描结束而没有完整损坏报告。它适合检查标识，不用于证明 CRC 覆盖、恢复缺失记录或推断消费完成。

## 检查拟执行降级

停止 Broker 后运行：

```bash
target/debug/rocketmq-cli-rust downgrade-preflight --target-version 0.9.0 --config /etc/rocketmq-rust/broker.toml --output downgrade-report.json
```

`0.9.0` 是目标值示例，不是建议安装或降级到该发行版。选择实际目标版本，并使用能够理解源格式的当前检查工具。命令读取规范 TOML，评估 Rust 自有布局，包括多路径、POP、定时、压缩主题和分层状态。

阅读 `allowed`、逐项检查及要求操作。拒绝结果以 `2` 退出，其他失败使用类型化 CLI 错误码。未指定 `--output` 时报告输出到 stdout，指定后应保存在合适输出位置。未获得锁或配置解析失败，不能解释为允许降级。

允许报告只针对已检查格式，不证明 Controller 成员兼容、消息覆盖、应用兼容或整个集群回退已经验证。不要修改格式标记来消除拒绝，应选择实际支持的转换或兼容恢复源。参见[升级与回退](./upgrade-rollback.md)。

## 合并受支持的多路径段

对已停止 Broker 的实际布局运行：

```bash
target/debug/rocketmq-cli-rust consolidate-multipath --source-root /data-a/commitlog --source-root /data-b/commitlog --target /data-consolidated/commitlog --mapped-file-size 1073741824 --store-root /var/lib/rocketmq-rust/store
```

目标目录必须不存在，其父目录和 Store 根目录必须已存在。段大小必须匹配源布局；`1073741824` 是示例的一 GiB 大小，不表示可据此重新解释较小段。

```mermaid
flowchart LR
  S[已停止的源根目录] --> L[获取实际 Store 锁]
  L --> V[检查归属、连续性、帧和空间]
  V --> C[复制到暂存区并比较字节]
  C --> F[同步文件并通过重命名发布目标]
  F --> R[读取报告并规划配置切换]
```

工具验证受支持段的归属、连续性和帧结构，检查空间，复制到暂存区、比较字节、同步文件，再通过重命名发布新目标。源文件保持可用。新目标是复制结果，不代表已自动更新 Broker 配置。

成功后检查报告，仅在选定转换流程中将目标配置指向合并后的 CommitLog；若需降级，再执行适用格式预检。保持其他 Store 元数据及派生状态恢复要求一致。不要仅因目标目录存在就删除源根目录。

失败时保留源和错误/报告上下文。重试前检查暂存和目标状态，不通过删除未知目录来强制重试。合并不能修复缺失段，不能合并任意重叠历史，也不能转换所有其他持久格式。

## 在正确层面理解完成

| 结果 | 仍需完成的工作 |
| --- | --- |
| 已打印 ID | 评估扫描是否覆盖预期记录，以及是否需要其他诊断 |
| 降级报告允许 | 执行版本专属集群/配置转换及恢复观察 |
| 合并目录已发布 | 修改目标配置，并按选定流程验证启动/恢复 |
| 转换后 Broker 已启动 | 检查路由/写权限、存储进度、应用重放及业务完成 |

通过[备份与恢复](./backup-recovery.md)保留恢复路径，通过[存储后端](../architecture/storage-backends.md)理解主数据与派生状态。本文记录源码定义的命令行为；本次写作没有针对真实存储执行数据扫描、合并或降级。

来源：[CLI 参数](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-tools/rocketmq-store-inspect/src/command_line.rs)、[读取器](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-tools/rocketmq-store-inspect/src/content_show.rs)、[预检](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-tools/rocketmq-store-inspect/src/downgrade_preflight.rs)、[合并](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-tools/rocketmq-store-inspect/src/multipath_consolidate.rs)、[工具 README](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-tools/rocketmq-store-inspect/README.md)。
