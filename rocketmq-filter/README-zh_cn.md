# rocketmq-filter

[English](README.md) | [简体中文](README-zh_cn.md)

[RocketMQ-Rust](../README-zh_cn.md) 的 SQL92 消息过滤、可插拔 Filter SPI、表达式求值和 Bloom Filter 工具 crate。

`rocketmq-filter` 提供 RocketMQ-Rust broker 侧和客户端订阅过滤路径所需的过滤基础能力。它可以将 SQL92 风格的订阅表达式
编译为可复用的表达式树，基于消息属性上下文进行求值，提供线程安全的过滤器注册表，并包含与 RocketMQ consume queue
过滤元数据兼容的 Bloom Filter 基础组件。

该 crate 不是独立运行的 RocketMQ 服务。它为上层 crate 提供可复用的过滤、表达式和 bitset 工具。

## 能力边界

| 领域 | 提供能力 |
|------|----------|
| SQL92 过滤 | `SqlFilter` 将 SQL92 风格的属性表达式编译为可复用的 `Expression` 对象。 |
| Filter SPI | `Filter`、`FilterSpi`、`FilterError` 和 `FilterFactory` 提供可插拔的过滤器注册与查找模型。 |
| 表达式运行时 | `Expression`、`Value`、`EvaluationContext` 和 `MessageEvaluationContext` 用于基于消息属性求值并返回类型化结果。 |
| 布尔表达式 | 提供 `AlwaysTrueExpression`、`AlwaysFalseExpression`、`PropertyEqualsExpression`、`AndExpression`、`OrExpression` 和 `NotExpression` 等基础表达式。 |
| Bloom Filter 工具 | 基于 byte 对齐 bit array 和 MurmurHash3 double hashing，生成并校验与 Java RocketMQ 兼容的 Bloom filter 数据。 |
| 热路径 benchmark | 使用 Criterion 覆盖 SQL 表达式编译、表达式求值和 Bloom filter byte slice 命中检查。 |

## 架构

```text
subscription expression
        |
        v
FilterFactory -> SqlFilter -> SQL runtime parser -> Expression tree
                                                  |
message properties -> MessageEvaluationContext ---+
                                                  |
                                                  v
                                           Value::Boolean / Value::Null / error

consumer filter metadata -> BloomFilterData -> BloomFilter -> BitsArray / raw bytes
```

`FilterFactory` 在静态初始化时注册默认 `SQL92` filter，并允许按类型注册额外 filter。`SqlFilter` 将解析工作委托给
SQL runtime，返回 object-safe 的表达式实例，调用方可以缓存编译后的表达式。`MessageEvaluationContext` 提供消息属性，
运行时会在 SQL 表达式需要时对字符串属性进行数字和布尔类型转换。

## Crate 结构

| 路径 | 职责 |
|------|------|
| [`src/lib.rs`](src/lib.rs) | 常量、表达式、过滤器和工具模块的公共导出。 |
| [`src/filter.rs`](src/filter.rs) | Filter 模块 facade 和公共导出。 |
| [`src/filter/filter_spi.rs`](src/filter/filter_spi.rs) | 核心 `Filter` trait、Java 兼容的 `FilterSpi` alias 和 `FilterError`。 |
| [`src/filter/filter_factory.rs`](src/filter/filter_factory.rs) | 基于 `DashMap` 的全局 filter registry，并默认注册 `SQL92`。 |
| [`src/filter/filter_sql_filter.rs`](src/filter/filter_sql_filter.rs) | 无状态 SQL92 filter 实现。 |
| [`src/filter/sql_runtime.rs`](src/filter/sql_runtime.rs) | SQL 表达式 lexer、parser、evaluator 和三值逻辑求值行为。 |
| [`src/expression.rs`](src/expression.rs) | 表达式 trait、值类型、求值错误和表达式导出。 |
| [`src/expression/evaluation_context.rs`](src/expression/evaluation_context.rs) | 带 serde 支持的消息属性求值上下文。 |
| [`src/expression/boolean_expression.rs`](src/expression/boolean_expression.rs) | 简单过滤路径和测试使用的布尔表达式基础组件。 |
| [`src/utils/bits_array.rs`](src/utils/bits_array.rs) | 支持 bitwise 操作和边界检查访问的 bit array wrapper。 |
| [`src/utils/bloom_filter.rs`](src/utils/bloom_filter.rs) | 与 RocketMQ 过滤元数据兼容的 Bloom filter 实现。 |
| [`src/utils/bloom_filter_data.rs`](src/utils/bloom_filter_data.rs) | 可序列化的 Bloom filter 元数据模型。 |
| [`benches/sql_filter_benchmark.rs`](benches/sql_filter_benchmark.rs) | SQL 和 Bloom filter 热路径 Criterion benchmark。 |

## 环境要求

- Rust `1.85.0` 或更新版本。
- 使用仓库中的 [`../rust-toolchain.toml`](../rust-toolchain.toml) 工具链。
- 本地测试、表达式求值和 Bloom filter 使用不需要运行 RocketMQ broker 或 NameServer。

## 安装

在当前 workspace 内使用：

```toml
[dependencies]
rocketmq-filter = { path = "../rocketmq-filter" }
```

外部项目使用：

```toml
[dependencies]
rocketmq-filter = "1.0.0"
```

## 快速开始

编译并执行 SQL92 风格的订阅表达式：

```rust
use rocketmq_filter::expression::{MessageEvaluationContext, Value};
use rocketmq_filter::filter::{Filter, FilterFactory};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let filter = FilterFactory::get_sql_filter();
    let expression = filter.compile("color = 'blue' AND retries >= 3")?;

    let mut context = MessageEvaluationContext::new();
    context.put("color", "blue");
    context.put("retries", "5");

    let result = expression.evaluate(&context)?;
    assert_eq!(result, Value::Boolean(true));

    Ok(())
}
```

使用 Bloom filter 元数据进行候选消息预检查：

```rust
use rocketmq_filter::utils::bits_array::BitsArray;
use rocketmq_filter::utils::bloom_filter::BloomFilter;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bloom_filter = BloomFilter::create_by_fn(10, 100)?;
    let filter_data = bloom_filter.generate("GroupA#TopicA");
    let mut bits = BitsArray::create(bloom_filter.m() as usize);

    bloom_filter.hash_to(&filter_data, &mut bits)?;

    assert!(bloom_filter.is_hit(&filter_data, &bits)?);
    assert!(bloom_filter.is_hit_bytes(&filter_data, bits.bytes())?);

    Ok(())
}
```

## SQL92 表达式支持

当前 SQL runtime 支持：

| 类别 | 支持语法 |
|------|----------|
| 比较 | `=`、`!=`、`<>`、`>`、`>=`、`<`、`<=` |
| 逻辑操作 | `AND`、`OR`、`NOT`、括号 |
| Null 判断 | `IS NULL`、`IS NOT NULL` |
| 集合与范围 | `IN`、`NOT IN`、`BETWEEN`、`NOT BETWEEN` |
| 字符串匹配 | `CONTAINS`、`NOT CONTAINS`、`STARTSWITH`、`ENDSWITH` |
| 字面量 | 单引号字符串、通过 `''` 转义单引号、整数、浮点数、`TRUE`、`FALSE`、`NULL`、`NOW` |
| 属性名 | ASCII identifier，首字符之后允许字母、数字、`_`、`$` 和 `.` |

消息属性以字符串形式传入。求值器会在比较需要时进行数字和布尔转换。缺失属性会求值为 `NULL`，因此直接使用 SQL 表达式时，
调用方需要处理 `Value::Null`。

## Bloom Filter 工具

`BloomFilter` 尽量贴近 Java RocketMQ 设计：

- `create_by_fn(f, n)` 根据 false-positive 百分比 `f` 和预期元素数量 `n` 推导 hash 数量和 byte 对齐的 bit 长度。
- `generate` 生成包含 bit positions 和 bit count 的 `BloomFilterData`。
- `hash_to`、`hash_to_str` 和 `hash_to_positions` 用于写入 Bloom filter bit。
- `is_hit`、`is_hit_str`、`is_hit_positions` 和 `is_hit_bytes` 用于检查候选命中。
- `is_hit_bytes` 可在 consume queue 热路径上避免临时构造 `BitsArray`。

## 验证

该 crate 的聚焦检查：

```bash
cargo test -p rocketmq-filter --lib
cargo test -p rocketmq-filter --benches --no-run
```

如果修改 Rust 代码，需要在仓库根目录执行 workspace 级验证：

```bash
cargo fmt --all
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
```

## Benchmark

在 workspace 根目录运行 filter benchmark：

```bash
cargo bench -p rocketmq-filter --bench sql_filter_benchmark
```

Criterion 报告输出到 `target/criterion-filter`。对比 SQL 解析、表达式求值或 Bloom filter 命中检查的性能变化时，
应保持相同的工具链、feature set 和硬件环境。

## License

RocketMQ-Rust 使用 Apache License 2.0。详见 [../LICENSE-APACHE](../LICENSE-APACHE)。
