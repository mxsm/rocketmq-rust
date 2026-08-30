# Remoting Processor API 与测试布局清理方案

## 1. 背景与目标

当前 Remoting Processor 相关实现尚未正式发布，因此不需要通过 `V1`、`V2`、
`v1`、`v2` 或 `v2_only` 等名称区分内部架构代际。本次清理直接选择唯一实现作为正式实现，
删除过渡层和重复实现，并将对外名称、模块名、文件名统一为不带版本后缀的正常命名。

本次清理同时解决测试代码分散在 `src`、测试模块命名暴露历史 Issue 编号、
重复结构测试过多等问题。目标如下：

- Remoting Processor 只保留一套生产 API 和一套执行路径。
- 内部架构类型、模块、文件和文档不再使用版本后缀。
- 不改变已经存在的线协议、持久化格式或 RocketMQ 领域协议版本语义。
- 可独立测试的代码放入对应 crate 的 `tests` 目录。
- 必须访问私有实现的测试放入 `tests/unit`，由生产模块使用 `#[cfg(test)]` 和
  `#[path = "..."]` 引入；无法合理外置的小型白盒测试可以留在对应生产文件底部。
- 删除仅验证符号存在、trait bound、源码字符串或已删除兼容层的低价值测试。
- 保留能够验证故障语义、并发所有权、终止状态和安全边界的回归测试。

## 2. 命名边界

### 2.1 必须移除版本后缀的对象

- 尚未发布的内部 Processor API、Dispatcher、Server、Client、Session Registry。
- 只为迁移期存在的 facade、adapter、alias 和 re-export。
- 文件名、模块名、测试名及文档名中的架构代际标记。
- 注释和基准测试中将当前实现描述为新旧两个版本的内容。

统一后的名称应直接表达职责，例如：

- `RequestProcessor`
- `AuthorizedCommandDispatcher`
- `TransportServer`
- `SessionRegistry`
- `NetworkSession`
- `processor_api.rs`
- `authorized_dispatch.rs`

### 2.2 必须保留版本含义的对象

以下名称不属于内部架构代际，不能为了统一命名而删除：

- RocketMQ 线协议中的请求码、请求头和响应头版本，例如发送消息协议的第二版格式。
- 已持久化记录或外部兼容格式中具有真实语义的版本字段。
- 第三方协议、TLS、HTTP 或 OpenTelemetry 等规范自身的版本标识。
- 已公开且需要兼容决策才能修改的外部协议名称。

判断原则是：删除名称是否会改变节点互通、磁盘数据读取或外部客户端兼容性。
如果会改变，则必须保留或单独提出兼容迁移方案。

## 3. 清理步骤

### 步骤一：建立清单并确定唯一实现

1. 扫描生产源码、测试、示例、基准测试和文档中的版本后缀。
2. 将命中项分类为内部架构命名、真实协议版本或历史文档。
3. 对每组重复实现选择当前功能完整、所有权模型明确且测试覆盖更好的实现。
4. 在删除前确认所有调用方、feature 组合和独立项目消费者。

### 步骤二：收敛生产 API

1. 将选定实现重命名为无版本后缀的正式名称。
2. 删除旧实现、迁移 facade、兼容 alias 和重复 re-export。
3. 将 `api::v1`、`api::v2` 等内部层级收敛为单一 `api`。
4. 统一 Processor、Dispatcher、Server、Session 和 Response 生命周期接口。
5. 响应完成通知只保留一个观察入口，确保一次响应只产生一次完成回调。
6. 对网络响应的 session、owner 和 command identity 执行 fail-closed 校验，
   不允许缺失身份时回退到可选的原始请求信息。

### 步骤三：迁移所有消费者

1. 依次迁移 Transport、Broker、NameServer、Controller、Client、Proxy、Auth、
   Admin Core、MCP、示例和基准测试。
2. 删除调用侧仅为兼容旧 API 存在的适配代码。
3. 更新公共 API 意图文件和结构快照，使其只描述唯一正式 API。
4. 检查 standalone 项目的路径依赖并执行各自的验证流程。

### 步骤四：整理测试代码

按以下优先级处理测试：

1. 只依赖公共 API 的行为测试移动到 `<crate>/tests/`。
2. 需要访问私有类型或内部 seam 的测试移动到 `<crate>/tests/unit/`，由对应生产模块通过
   `#[cfg(test)]` 和 `#[path = "..."]` 引入。
3. 无法外置且与单个实现强绑定的小型白盒测试放在对应生产文件底部的
   `#[cfg(test)] mod tests` 中。
4. 删除只验证类型存在、trait bound、常量值、源码文本或已删除兼容层的测试。
5. 删除与更高层行为测试完全重复、没有额外边界覆盖的测试。
6. 保留故障注入、并发竞态、所有权、deadline、终止状态和协议兼容回归测试。

`issue_9754_tests.rs` 不应按文件名整体删除。其有效回归覆盖应按行为重命名并保留：

- 连接写失败语义移动为 `tests/unit/connection/write_failure_semantics.rs`。
- Writer queue 失败语义移动为
  `tests/unit/writer_runtime/queue_failure_semantics.rs`。
- 删除重复的 API 存在性、trait bound 和源码字符串断言。

### 步骤五：清理文档和基线

1. 删除只用于旧版到新版迁移的 ledger 和 migration 文档。
2. 将仍有价值的性能基线和架构说明改为无版本后缀名称。
3. 新增唯一正式 Remoting Processor API 的使用说明。
4. 更新公共 API 意图和结构快照，但不借本次修改重写无关的维护性基线。

### 步骤六：分层验证

1. 对受影响 crate 执行 package-scoped `cargo fmt -- --check`。
2. 执行 workspace `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings`。
3. 执行 Transport 全功能测试和 Broker library 测试。
4. 覆盖 Observability 的 metrics、traces、logs 及组合 feature 矩阵。
5. 执行公共 API intent guard 和结构快照检查。
6. 执行 MCP standalone 的格式、检查、测试、Clippy、文档和只读边界检查。
7. 执行 runtime ownership、error hygiene 和 maintainability guard，并区分本次回归与
   仓库既有基线问题。
8. 执行 `git diff --check` 和最终命名/测试布局扫描。

## 4. 验收标准

| 类别 | 验收标准 |
|---|---|
| 单一实现 | Remoting Processor 的生产请求处理只存在一套正式 API 和一条执行路径。 |
| 命名 | 内部生产源码、测试文件和当前文档不存在架构代际版本后缀；真实线协议和持久化版本名称除外。 |
| 兼容层 | 不存在仅用于未发布旧接口迁移的 facade、adapter、alias 或重复 re-export。 |
| 响应语义 | 每个响应只触发一次完成观察；网络响应身份缺失或不匹配时拒绝发送。 |
| 测试布局 | `src` 下不存在独立的 `*_tests.rs`、`acceptance_tests` 或 Issue 编号测试文件；允许对应生产文件内的小型 `#[cfg(test)]` 模块。 |
| 测试价值 | 删除纯符号、trait bound、源码字符串和重复测试；保留故障、竞态、所有权、deadline、终止状态及协议兼容覆盖。 |
| Issue 回归 | 原 `issue_9754` 的独特失败语义测试以行为名称保留，并可由正常测试命令执行。 |
| 编译与静态检查 | 受影响项目格式检查通过，workspace Clippy 在 `-D warnings` 下通过。 |
| 行为测试 | Transport 全功能测试、Broker library 测试、Observability feature 矩阵和 MCP standalone 测试通过。 |
| 公共 API | Public API intent guard 和 structural snapshot 检查无差异。 |
| 文档 | 只描述唯一正式 API，清理步骤、命名边界和验收标准可从仓库内直接查阅。 |
| 差异质量 | `git diff --check` 通过，不包含本地绝对路径、构建产物或运行日志。 |

## 5. 本次实施结果

- Transport 的内部版本化 API 已收敛为单一正式 API，所有 workspace 消费者同步迁移。
- Broker、Transport 中可外置的私有测试已迁移到 `tests/unit`，并由对应生产模块引入。
- 原 `issue_9754` 中 22 个独特回归测试按失败行为重命名后保留，冗余结构测试已删除。
- 版本化迁移文档已删除，性能基线及正式 API 文档已改为正常命名。
- Public API intent 当前记录 Transport 219 个意图项；结构快照覆盖 26 个包、50 个 profile，
  检查差异为 0。
- Transport 全功能测试、Broker library 测试、Observability feature 矩阵、workspace Clippy
  和 MCP standalone 验证均已通过。

## 6. 已知仓库基线问题

以下检查仍报告既有或全仓库基线问题，不能标记为通过：

- Runtime audit 在未修改的 Controller 测试文件中仍有 1 个 current-runtime-adapter 发现。
- Error hygiene guard 仍报告仓库现有的通用响应、错误字符串化和敏感 Debug 约束问题。
- Module maintainability guard 仍包含大量历史发现以及本次有意的路径和公共表面重置；
  本次不全局重写该基线。

这些结果必须在 Pull Request 中如实列出，并由后续独立工作处理；它们不应通过放宽检查、
写入宽泛基线或恢复已删除兼容层来掩盖。
