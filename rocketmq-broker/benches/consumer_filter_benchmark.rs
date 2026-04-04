use std::hint::black_box;
use std::path::Path;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BatchSize;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use rocketmq_broker::bench_support::ConsumerFilterBenchHarness;

fn bench_consumer_filter_resolve(c: &mut Criterion) {
    let mut group = c.benchmark_group("consumer_filter_resolve");
    let expression = "color = 'blue' AND retries >= 3 AND region IN ('hz', 'sh')";

    group.bench_function("cache_hit", |b| {
        let harness = ConsumerFilterBenchHarness::new();
        assert!(harness.resolve_sql("TopicBench", "GroupBench", expression, 1));

        b.iter(|| {
            let resolved = harness.resolve_sql("TopicBench", "GroupBench", black_box(expression), 1);
            black_box(resolved);
        });
    });

    for input_size in [16usize, 128, 512] {
        group.throughput(Throughput::Elements(input_size as u64));
        group.bench_with_input(
            BenchmarkId::new("working_set_unique_expressions", input_size),
            &input_size,
            |b, &size| {
                let harness = ConsumerFilterBenchHarness::new();
                let expressions = (0..size)
                    .map(|index| format!("color = 'blue' AND retries >= {index}"))
                    .collect::<Vec<_>>();
                let mut next = 0usize;

                b.iter(|| {
                    let expression = &expressions[next % expressions.len()];
                    next += 1;
                    let resolved = harness.resolve_sql("TopicBench", "GroupBench", black_box(expression), next as u64);
                    black_box(resolved);
                });
            },
        );
    }

    group.bench_function("generated_miss_end_to_end", |b| {
        let harness = ConsumerFilterBenchHarness::new();
        let mut next = 0u64;

        b.iter_batched(
            || {
                let current = next;
                next += 1;
                (format!("color = 'blue' AND retries >= {current}"), current + 1)
            },
            |(expression, version)| {
                let resolved = harness.resolve_sql("TopicBench", "GroupBench", black_box(&expression), version);
                black_box(resolved);
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_consumer_filter_stats_snapshot(c: &mut Criterion) {
    c.bench_function("consumer_filter_stats_snapshot", |b| {
        let harness = ConsumerFilterBenchHarness::new();
        assert!(harness.resolve_sql("TopicBench", "GroupBench", "color = 'blue'", 1));
        assert!(harness.resolve_sql("TopicBench", "GroupBench", "color = 'blue'", 2));

        b.iter(|| black_box(harness.stats_snapshot()));
    });
}

criterion_group! {
    name = consumer_filter_benches;
    config = Criterion::default()
        .sample_size(20)
        .output_directory(Path::new("target/criterion-broker-filter"));
    targets =
        bench_consumer_filter_resolve,
        bench_consumer_filter_stats_snapshot
}
criterion_main!(consumer_filter_benches);
