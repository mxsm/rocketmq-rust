use std::collections::HashMap;
use std::hint::black_box;
use std::path::Path;

use cheetah_string::CheetahString;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::utils::message_utils;
use rocketmq_common::MessageDecoder::message_properties_to_string;
use rocketmq_common::MessageDecoder::string_to_message_properties;

fn encoded_properties(property_count: usize) -> CheetahString {
    let mut properties = HashMap::new();
    properties.insert(
        CheetahString::from_static_str(MessageConst::PROPERTY_KEYS),
        CheetahString::from_static_str("bench-key"),
    );
    properties.insert(
        CheetahString::from_static_str(MessageConst::PROPERTY_TAGS),
        CheetahString::from_static_str("TagA"),
    );
    properties.insert(
        CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
        CheetahString::from_static_str("bench-uniq"),
    );
    properties.insert(
        CheetahString::from_static_str(MessageConst::PROPERTY_POP_CK),
        CheetahString::from_static_str("reserved-broker-state"),
    );

    for index in 0..property_count {
        properties.insert(
            CheetahString::from_string(format!("userKey{index}")),
            CheetahString::from_string(format!("userValue{index}")),
        );
    }

    message_properties_to_string(&properties)
}

fn baseline_double_parse(
    properties_string: &CheetahString,
    region_id: &str,
    trace_on: bool,
) -> HashMap<CheetahString, CheetahString> {
    let mut properties = string_to_message_properties(Some(properties_string));
    properties.insert(
        CheetahString::from_static_str(MessageConst::PROPERTY_MSG_REGION),
        CheetahString::from_slice(region_id),
    );
    properties.insert(
        CheetahString::from_static_str(MessageConst::PROPERTY_TRACE_SWITCH),
        CheetahString::from_string(trace_on.to_string()),
    );

    let properties_string = message_properties_to_string(&properties);
    let properties_string = message_utils::delete_property(properties_string.as_str(), MessageConst::PROPERTY_POP_CK);
    string_to_message_properties(Some(&CheetahString::from_string(properties_string)))
}

fn reused_single_parse(
    properties_string: &CheetahString,
    region_id: &str,
    trace_on: bool,
) -> HashMap<CheetahString, CheetahString> {
    let mut properties = string_to_message_properties(Some(properties_string));
    properties.insert(
        CheetahString::from_static_str(MessageConst::PROPERTY_MSG_REGION),
        CheetahString::from_slice(region_id),
    );
    properties.insert(
        CheetahString::from_static_str(MessageConst::PROPERTY_TRACE_SWITCH),
        CheetahString::from_string(trace_on.to_string()),
    );
    properties.remove(MessageConst::PROPERTY_POP_CK);
    properties
}

fn bench_send_message_properties(c: &mut Criterion) {
    let mut group = c.benchmark_group("send_message_properties_single_parse");

    for property_count in [4usize, 16, 64] {
        let properties = encoded_properties(property_count);
        group.throughput(Throughput::Elements(property_count as u64));

        group.bench_with_input(
            BenchmarkId::new("baseline_double_parse", property_count),
            &properties,
            |b, properties| {
                b.iter(|| {
                    black_box(baseline_double_parse(black_box(properties), "DefaultRegion", true));
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("reused_single_parse", property_count),
            &properties,
            |b, properties| {
                b.iter(|| {
                    black_box(reused_single_parse(black_box(properties), "DefaultRegion", true));
                });
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = send_message_properties_benches;
    config = Criterion::default()
        .sample_size(20)
        .output_directory(Path::new("target/criterion-broker-send-message-properties"));
    targets = bench_send_message_properties
}
criterion_main!(send_message_properties_benches);
