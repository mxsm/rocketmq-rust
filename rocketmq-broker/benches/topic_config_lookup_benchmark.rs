use std::collections::HashMap;
use std::hint::black_box;
use std::path::Path;
use std::sync::Arc;

use arc_swap::ArcSwap;
use cheetah_string::CheetahString;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use dashmap::DashMap;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_rust::ArcMut;

type TopicConfigMap = DashMap<CheetahString, ArcMut<TopicConfig>>;
type TopicConfigSnapshot = ArcSwap<HashMap<CheetahString, ArcMut<TopicConfig>>>;

fn topic_name(index: usize) -> CheetahString {
    CheetahString::from_string(format!("TopicConfigLookup{index}"))
}

fn build_topic_config_maps(topic_count: usize) -> (TopicConfigMap, TopicConfigSnapshot, Vec<CheetahString>) {
    let dash_map = DashMap::with_capacity(topic_count);
    let mut snapshot_map = HashMap::with_capacity(topic_count);
    let mut topics = Vec::with_capacity(topic_count);

    for index in 0..topic_count {
        let topic = topic_name(index);
        let config = ArcMut::new(TopicConfig::with_queues(topic.clone(), 4, 4));
        dash_map.insert(topic.clone(), config.clone());
        snapshot_map.insert(topic.clone(), config);
        topics.push(topic);
    }

    (dash_map, ArcSwap::from(Arc::new(snapshot_map)), topics)
}

fn bench_topic_config_lookup(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("topic_config_lookup");

    for topic_count in [1_024usize, 10_000, 100_000] {
        let (dash_map, snapshot, topics) = build_topic_config_maps(topic_count);
        let hot_topic = &topics[topic_count / 2];
        group.throughput(Throughput::Elements(1));

        group.bench_with_input(
            BenchmarkId::new("dashmap_get_clone", topic_count),
            hot_topic,
            |bencher, topic| {
                bencher.iter(|| {
                    let config = dash_map.get(black_box(topic)).as_deref().cloned();
                    black_box(config);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("snapshot_hashmap_get_clone", topic_count),
            hot_topic,
            |bencher, topic| {
                bencher.iter(|| {
                    let snapshot = snapshot.load();
                    let config = snapshot.get(black_box(topic)).cloned();
                    black_box(config);
                });
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = topic_config_lookup_benches;
    config = Criterion::default()
        .sample_size(20)
        .output_directory(Path::new("target/criterion-broker-topic-config-lookup"));
    targets = bench_topic_config_lookup
}
criterion_main!(topic_config_lookup_benches);
