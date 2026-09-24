# Runtime 栈安全优化方案与实现

## 问题与目标

原有修复在 `TaskGroup` 提交任务时，对超过 16 KiB 的 Future 提前装箱。
这一修复应当保留，但它只覆盖任务提交边界。业务 Future 在到达该边界前，
仍可能被 operation、调度器、超时和阻塞执行包装层按值持有或移动。
深任务树的关闭及祖先释放还存在另一类风险：调用栈随树深度增长。

目标是消除已复现的运行时包装和树遍历导致的栈放大，同时保留任务归属、
取消、排空、截止时间和关闭报告契约。实现不提高默认线程栈大小、不增加树深度限制，
也不要求所有业务调用方自行装箱。

## 具体实现

| 路径 | 优化措施 | 成本与约束 |
| --- | --- | --- |
| 普通任务、可取消服务、operation、draining operation | 在生命周期及取消包装前，按原始 Future 大小选择内联或 `Box::pin` | 统一使用内部 16 KiB 阈值；小 Future 保留内联路径 |
| 固定延迟、固定频率、受控、operation 和 bounded 调度 | 注册时分别判断工厂闭包与单次运行 Future 的大小；大工厂先装箱，大运行先装箱再进入调度/超时包装 | 大工厂每次注册分配；大 Future 每次运行分配；使用具体泛型类型，无 `dyn Future` |
| 阻塞执行 | 同步入口装箱闭包，再返回执行 admission 和等待的 Future | 非零大小闭包增加一次分配，避免闭包大小进入层层异步状态机；零大小类型无需实际分配 |
| 阻塞返回值 | 大结果在内部以 `Box<R>` 穿过 JoinHandle 和等待路径，在公开返回边界解箱 | 仅大结果增加一次分配；调用方仍接收 `R` |
| 关闭任务树 | 迭代封闭 admission、保留所有后代、传播最早截止时间；在同一层并发轮询每个节点的关闭逻辑 | 显式遍历存储随节点数增长；不按兄弟节点顺序逐个等待 |
| 子树关闭报告 | 每个节点继续使用自己的 `OnceCell`；父节点等待子报告通知，不递归轮询子树 Future | 支持父子并发关闭、关闭 Future 被丢弃后重试，以及截止时间提前 |
| 立即关闭、祖先释放、报告复制与健康检查 | 分别采用后序迭代组装、迭代拆解祖先 Arc、迭代复制与遍历 | 保留报告树结构和现有计数含义 |

### 为什么不只扩大栈或统一装箱所有异步任务

扩大栈只能推迟故障边界，并随线程数增加内存成本。对最终包装好的 Future 装箱，
也无法撤销此前已经发生的大对象移动，或缩小调度运行过程中的嵌套 poll 栈帧。
因此，装箱边界必须在相应包装层之前。

异步任务和调度采用大小分支，保留常见小任务的内联路径。阻塞入口则统一装箱闭包：
如果把内联闭包与装箱闭包放进同一个返回 Future 的不同分支，该 Future 的布局仍需
容纳较大的内联分支。同步装箱使闭包大小不再影响 admission/wait 的 Future 布局。
这是一项明确的栈安全与分配成本取舍，不代表已经证明吞吐性能最优。

### 兼容性说明

- `spawn*().await` 的使用方式与返回结果保持一致。阻塞接口由 `async fn` 改为
  同步返回 `impl Future + Send`，闭包装箱和 `name.into()` 在调用时发生。
  admission、配额获取和执行提交仍在 Future 被轮询后发生；创建后立即丢弃不会启动工作。
- 不可中断的阻塞闭包仍持有容量直到执行完成；超时只停止等待，不撤销已发生的副作用。
- operation 注册、取消及 draining 的差异保持不变。调度模式、重叠规则、运行超时和指标保持原语义。
- 关闭先封闭并保留整棵已接受的子树，再取消任务，避免快速结束的叶节点让报告丢失祖先。
  封闭 admission 后重新传播截止时间，覆盖首次传播期间并发创建的子节点。
- 公共报告字段与序列化结构不变。报告缓存仍属于原有任务节点，没有新建运行时或后台关闭任务。

## 回归验证

`tests/task_submission_stack.rs` 将可能触发进程终止的场景放在子进程中，显式设置
1 MiB 提交线程栈；新增执行场景同时设置 1 MiB Tokio 工作线程栈。测试验证任务实际完成、
返回值、关闭报告及树深度，不能仅以提交成功代替执行成功。

| 场景 | 输入 |
| --- | --- |
| 普通任务与既有调度提交 | 原有 16 KiB 回归 |
| operation、draining operation、可取消服务 | 64 KiB Future |
| 单次调度运行与全部调度适配入口 | 32 KiB Future，包含运行超时包装 |
| 大调度工厂 | 64 KiB 捕获数据、小 Future |
| bounded 调度工厂与运行同时较大 | 64 KiB 工厂捕获与 32 KiB Future |
| 阻塞捕获与返回值 | 64 KiB 捕获、32 KiB 返回值 |
| 优雅关闭、立即关闭、空闲祖先释放 | 分别为 512、1024、4096 层 |

`tests/task_group_shutdown_tree.rs` 另外覆盖兄弟报告依赖、父子同时关闭、关闭调用被取消后重试，
以及已在等待的子节点接收更早的根截止时间。既有生命周期、operation 完成、阻塞所有权和
scope admission 测试继续验证行为兼容。

复现命令：

```text
cargo test -p rocketmq-runtime --test task_submission_stack --test task_group_shutdown_tree
cargo test -p rocketmq-runtime --release --test task_submission_stack --test task_group_shutdown_tree
cargo test -p rocketmq-runtime --lib --test runtime_model --test runtime_resource_ownership --test task_completion --test critical_failure --test blocking_ownership --test blocking_scope_admission
cargo fmt -p rocketmq-runtime -- --check
cargo clippy -p rocketmq-runtime --features async_fs --all-targets --no-deps -- -D warnings
```

## 边界与后续决策

这些回归锁定的是特定输入和构建配置下的已知故障，不是任意大小、任意树深度的安全承诺：

- `Box::pin(future)` 和 `Box::new(operation())` 仍可能先在栈上构造原始值。业务 Future
  自身的 poll、闭包调用、大返回值在调用方的移动，以及业务类型递归析构，都可能使用大量栈。
  更大的固定数组应由业务直接采用 `Vec` 或其他堆分配容器构造。
- `ShutdownReport` 保留公开的递归 `children: Vec<ShutdownReport>` 结构；其派生 Debug、
  Serde 序列化和自动析构仍可能递归。任务诊断的递归扫描也未在此次修改中重写。
  已通过的深度测试不应解释为所有诊断、格式化和销毁路径均支持无限深度。
- 每个节点缓存完整子树报告的既有设计，在链式任务树上仍可能产生平方级报告存储/复制总量。
  若业务确实需要极深动态层级，应另行评估扁平报告和有界诊断接口，不能只继续增大测试深度。
- 16 KiB 阈值沿用既有策略。调整阈值、采用小对象内联容器或评估分配器影响，应由任务大小分布、
  分配次数、吞吐和延迟基准决定。当前修复不宣称性能最优，也不等价于生产负载验证。

本次本地验证平台为 Windows x86_64 / MSVC；其他平台仍需相应 CI 验证。

## 本次验证记录（2026-09-24）

- Rust 1.95.0、Tokio 1.53.1；默认 feature 的 298 项 Debug 测试通过，包含
  203 项单元测试、82 项既有生命周期/所有权回归和上述 13 项针对性测试。
- 上述 13 项针对性测试的 Release 构建通过。
- 原始独立 Release 复现程序重新链接修复后的 crate，8 个场景通过，包括
  128 KiB operation、draining operation 和调度 Future，以及阻塞和深树场景。
  64 KiB 阻塞闭包的公开 Future 从 262800 字节缩小到 664 字节。
- Client、Broker、NameServer 的默认 feature 编译检查通过，覆盖共享运行时的直接调用方。
- `async_fs` feature 下所有 target 的 Clippy 检查通过（`-D warnings`）；包级格式检查通过。
  此项是编译/静态验证，不表示运行了该 feature 下的全部测试。
- 没有执行 Linux 验证、生产流量验证或分配/吞吐/延迟性能基准。
