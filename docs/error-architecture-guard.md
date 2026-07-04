# Error Architecture Guard

`scripts/error_architecture_guard.py` 用于阻止 error 重构中已经收口的架构约束回退。它是轻量静态检查，不编译 Rust 代码，失败时输出 `path:line`。

## 本地运行

Windows:

```powershell
py scripts\error_architecture_guard.py
```

Linux/macOS:

```bash
python3 scripts/error_architecture_guard.py
```

## CI 入口

- Root workspace：`.github/workflows/rocketmq-rust-ci.yaml` 的 `Error architecture guard` step。
- Dashboard web backend：`.github/workflows/dashboard-web-ci.yml` 的 `Error architecture guard` step。

dashboard web CI 也会在 `scripts/error_architecture_guard.py` 变更时触发，因为 guard 已覆盖 dashboard backend HTTP boundary 与 sensitive output。

## 覆盖范围

当前 guard 覆盖：

- legacy error alias/public `anyhow` 回归。
- broker/namesrv processor 通用 response code 新增。
- remoting/proxy/dashboard 外部 adapter 是否读取中心 spec。
- callback/retry boundary 是否回退到 display string、downcast 或局部猜测。
- `ErrorKind` 与 `ErrorSpec` 合约完整性。
- source 字符串化 allowlist。
- sensitive Debug/Display/API output redaction。

## allowlist 规则

新增 allowlist 前先判断是否能改成 typed error、source-preserving error 或 redacted context。只有满足下面任一条件才允许登记：

- 外部 trait 或协议要求固定错误类型，无法携带原 source。
- 兼容 Java remoting response code，且替换会改变 wire contract。
- binary、build script、test、example 或 process outer boundary 保留 `anyhow`。
- 字段名看起来敏感，但实际是算法、开关、元数据等非 secret 值。

每条 allowlist 必须写清楚边界原因，不能只写“temporary”或“legacy”。新增或修改 allowlist 后必须运行 guard，并为对应行为补 focused regression test。
