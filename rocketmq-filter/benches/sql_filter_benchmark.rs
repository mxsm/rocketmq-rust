use std::collections::HashMap;
use std::hint::black_box;
use std::path::Path;

use ahash::RandomState;
use cheetah_string::CheetahString;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use rocketmq_filter::expression::MessageEvaluationContext;
use rocketmq_filter::filter::Filter;
use rocketmq_filter::filter::SqlFilter;
use rocketmq_filter::utils::bits_array::BitsArray;
use rocketmq_filter::utils::bloom_filter::BloomFilter;

fn benchmark_sql_compile(c: &mut Criterion) {
    let mut group = c.benchmark_group("sql_compile");
    let filter = SqlFilter::new();

    for expression in [
        "color = 'blue'",
        "color = 'blue' AND retries >= 3 AND region IN ('hz', 'sh')",
        "color = 'blue' AND retries >= 3 AND region IN ('hz', 'sh') AND name CONTAINS 'rocket'",
    ] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("compile", expression.len()),
            &expression,
            |b, expression| {
                b.iter(|| {
                    let compiled = filter
                        .compile(black_box(expression))
                        .expect("benchmark expression should compile");
                    black_box(compiled);
                });
            },
        );
    }

    group.finish();
}

fn benchmark_sql_evaluate(c: &mut Criterion) {
    let mut group = c.benchmark_group("sql_evaluate");
    let filter = SqlFilter::new();
    let expression = filter
        .compile("color = 'blue' AND retries >= 3 AND region IN ('hz', 'sh') AND enabled = TRUE")
        .expect("benchmark expression should compile");
    let mut properties = HashMap::with_hasher(RandomState::default());
    properties.insert(CheetahString::from_slice("color"), CheetahString::from_slice("blue"));
    properties.insert(CheetahString::from_slice("retries"), CheetahString::from_slice("5"));
    properties.insert(CheetahString::from_slice("region"), CheetahString::from_slice("hz"));
    properties.insert(CheetahString::from_slice("enabled"), CheetahString::from_slice("true"));
    let context = MessageEvaluationContext::from_properties(properties);

    group.bench_function("evaluate_match", |b| {
        b.iter(|| {
            let value = expression
                .evaluate(black_box(&context))
                .expect("expression should evaluate");
            black_box(value);
        });
    });

    group.finish();
}

fn benchmark_bloom_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("bloom_filter");
    let bloom_filter = BloomFilter::create_by_fn(20, 32).expect("benchmark bloom filter");
    let filter_data = bloom_filter.generate("GroupBench#TopicBench");
    let mut bits = BitsArray::create(bloom_filter.m() as usize);
    bloom_filter
        .hash_to(&filter_data, &mut bits)
        .expect("bloom hash should succeed");

    group.bench_function("is_hit_bytes", |b| {
        b.iter(|| {
            let is_hit = bloom_filter
                .is_hit_bytes(&filter_data, black_box(bits.bytes()))
                .expect("bloom hit check should succeed");
            black_box(is_hit);
        });
    });

    group.finish();
}

criterion_group! {
    name = sql_filter_benches;
    config = Criterion::default()
        .sample_size(20)
        .output_directory(Path::new("target/criterion-filter"));
    targets =
        benchmark_sql_compile,
        benchmark_sql_evaluate,
        benchmark_bloom_filter
}
criterion_main!(sql_filter_benches);
