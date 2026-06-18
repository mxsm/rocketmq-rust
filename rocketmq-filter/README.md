# rocketmq-filter

[English](README.md) | [简体中文](README-zh_cn.md)

SQL92 message filtering, pluggable filter SPI, expression evaluation, and Bloom filter utilities for
[RocketMQ-Rust](../README.md).

`rocketmq-filter` provides the filtering foundation used by RocketMQ-Rust broker-side and client-facing message
selection paths. It compiles SQL92-style subscription expressions into reusable expression trees, evaluates them against
message property contexts, exposes a thread-safe filter registry, and includes Bloom filter primitives compatible with
RocketMQ consume-queue filtering metadata.

The crate is intentionally focused: it does not run a RocketMQ service by itself. It supplies reusable filtering,
expression, and bitset utilities for higher-level crates.

## Capabilities

| Area | What it provides |
|------|------------------|
| SQL92 filtering | `SqlFilter` compiles SQL92-style property expressions into reusable `Expression` objects. |
| Filter SPI | `Filter`, `FilterSpi`, `FilterError`, and `FilterFactory` provide a pluggable filter registration and lookup model. |
| Expression runtime | `Expression`, `Value`, `EvaluationContext`, and `MessageEvaluationContext` evaluate message properties with typed results. |
| Boolean expressions | Ready-to-use boolean primitives such as `AlwaysTrueExpression`, `AlwaysFalseExpression`, `PropertyEqualsExpression`, `AndExpression`, `OrExpression`, and `NotExpression`. |
| Bloom filter utilities | Java-compatible Bloom filter generation and hit checks using byte-aligned bit arrays and MurmurHash3-based double hashing. |
| Hot-path benchmarks | Criterion benchmarks for SQL expression compilation, expression evaluation, and Bloom filter byte-slice hit checks. |

## Architecture

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

`FilterFactory` registers the default `SQL92` filter at static initialization time and allows additional filters to be
registered by type. `SqlFilter` delegates parsing to the SQL runtime and returns object-safe expression instances that
can be cached by callers. `MessageEvaluationContext` supplies string properties for evaluation, while the runtime
coerces numeric and boolean literals where the SQL expression requires them.

## Crate Layout

| Path | Purpose |
|------|---------|
| [`src/lib.rs`](src/lib.rs) | Public module exports for constants, expressions, filters, and utilities. |
| [`src/filter.rs`](src/filter.rs) | Filter module facade and public exports. |
| [`src/filter/filter_spi.rs`](src/filter/filter_spi.rs) | Core `Filter` trait, Java-compatible `FilterSpi` alias, and `FilterError`. |
| [`src/filter/filter_factory.rs`](src/filter/filter_factory.rs) | Global filter registry backed by `DashMap`, with default `SQL92` registration. |
| [`src/filter/filter_sql_filter.rs`](src/filter/filter_sql_filter.rs) | Stateless SQL92 filter implementation. |
| [`src/filter/sql_runtime.rs`](src/filter/sql_runtime.rs) | SQL expression lexer, parser, evaluator, and three-valued evaluation behavior. |
| [`src/expression.rs`](src/expression.rs) | Expression traits, values, evaluation errors, and expression exports. |
| [`src/expression/evaluation_context.rs`](src/expression/evaluation_context.rs) | Message property evaluation context with serde support. |
| [`src/expression/boolean_expression.rs`](src/expression/boolean_expression.rs) | Boolean expression primitives used by simple filtering paths and tests. |
| [`src/utils/bits_array.rs`](src/utils/bits_array.rs) | Bit array wrapper with bitwise operations and bounds-checked access. |
| [`src/utils/bloom_filter.rs`](src/utils/bloom_filter.rs) | Bloom filter implementation compatible with RocketMQ filtering metadata. |
| [`src/utils/bloom_filter_data.rs`](src/utils/bloom_filter_data.rs) | Serializable Bloom filter metadata model. |
| [`benches/sql_filter_benchmark.rs`](benches/sql_filter_benchmark.rs) | Criterion benchmarks for SQL and Bloom filter hot paths. |

## Requirements

- Rust `1.85.0` or newer.
- The repository toolchain from [`../rust-toolchain.toml`](../rust-toolchain.toml).
- No running RocketMQ broker or NameServer is required for local tests, expression evaluation, or Bloom filter usage.

## Installation

Inside this workspace:

```toml
[dependencies]
rocketmq-filter = { path = "../rocketmq-filter" }
```

For external consumers:

```toml
[dependencies]
rocketmq-filter = "1.0.0"
```

## Quick Start

Compile and evaluate a SQL92-style subscription expression:

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

Use Bloom filter metadata to pre-check candidate messages:

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

## SQL92 Expression Support

The current SQL runtime supports:

| Category | Supported syntax |
|----------|------------------|
| Comparison | `=`, `!=`, `<>`, `>`, `>=`, `<`, `<=` |
| Logical operators | `AND`, `OR`, `NOT`, parentheses |
| Null checks | `IS NULL`, `IS NOT NULL` |
| Set and range checks | `IN`, `NOT IN`, `BETWEEN`, `NOT BETWEEN` |
| String matching | `CONTAINS`, `NOT CONTAINS`, `STARTSWITH`, `ENDSWITH` |
| Literals | Single-quoted strings, escaped quotes with `''`, integers, floating-point numbers, `TRUE`, `FALSE`, `NULL`, `NOW` |
| Property names | ASCII identifiers with letters, digits, `_`, `$`, and `.` after the first character |

Message properties are supplied as strings. The evaluator coerces numeric and boolean values when needed for comparison.
Missing properties evaluate as `NULL`, so callers should handle `Value::Null` when using SQL expressions directly.

## Bloom Filter Utilities

`BloomFilter` follows the RocketMQ Java design closely:

- `create_by_fn(f, n)` derives the hash count and byte-aligned bit length from false-positive percentage `f` and
  expected element count `n`.
- `generate` produces `BloomFilterData` with calculated bit positions and bit count.
- `hash_to`, `hash_to_str`, and `hash_to_positions` set Bloom filter bits.
- `is_hit`, `is_hit_str`, `is_hit_positions`, and `is_hit_bytes` check candidate hits.
- `is_hit_bytes` avoids building a temporary `BitsArray` on hot consume-queue paths.

## Validation

Focused checks for this crate:

```bash
cargo test -p rocketmq-filter --lib
cargo test -p rocketmq-filter --benches --no-run
```

Workspace-level Rust validation is run from the repository root when Rust code changes:

```bash
cargo fmt --all
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
```

## Benchmarks

Run the filter benchmark suite from the workspace root:

```bash
cargo bench -p rocketmq-filter --bench sql_filter_benchmark
```

Criterion reports are written under `target/criterion-filter`. Keep benchmark comparisons on the same toolchain,
feature set, and hardware when evaluating changes to SQL parsing, expression evaluation, or Bloom filter hit checks.

## License

RocketMQ-Rust is licensed under the Apache License 2.0. See [../LICENSE-APACHE](../LICENSE-APACHE).
