// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeSet;
use std::collections::HashSet;

use rocketmq_observability::metrics::catalog::MetricDescriptor;
use rocketmq_observability::metrics::catalog::MetricKind;
use rocketmq_observability::metrics::catalog::MetricSource;
use rocketmq_observability::metrics::catalog::JAVA_METRICS;
use rocketmq_observability::metrics::catalog::RUST_METRICS;

const FIXTURE: &str = include_str!("fixtures/metric_catalog_descriptors.tsv");
const JAVA_DESCRIPTOR_COUNT: usize = 94;
const RUST_DESCRIPTOR_COUNT: usize = 116;
const TOTAL_DESCRIPTOR_COUNT: usize = JAVA_DESCRIPTOR_COUNT + RUST_DESCRIPTOR_COUNT;
const DISTINCT_LABEL_SEQUENCE_COUNT: usize = 56;
const DISTINCT_SOURCE_COUNT: usize = 14;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CatalogPartition {
    Java,
    Rust,
}

#[derive(Debug, PartialEq, Eq)]
struct FixtureDescriptor {
    catalog: CatalogPartition,
    index: usize,
    tuple: DescriptorTuple,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DescriptorTuple {
    name: String,
    kind: String,
    unit: String,
    labels: Vec<String>,
    source: String,
}

#[test]
fn metric_catalog_matches_literal_contract_fixture() {
    let expected = parse_fixture();

    assert_partition_matches(
        CatalogPartition::Java,
        JAVA_METRICS,
        &expected[..JAVA_DESCRIPTOR_COUNT],
        0,
    );
    assert_partition_matches(
        CatalogPartition::Rust,
        RUST_METRICS,
        &expected[JAVA_DESCRIPTOR_COUNT..],
        JAVA_DESCRIPTOR_COUNT,
    );
}

#[test]
fn extra_actual_tail_descriptor_reports_the_first_divergent_tuple() {
    let expected_tuple = test_tuple("expected-tail");
    let actual_tail = test_tuple("extra-actual-tail");
    let expected = [FixtureDescriptor {
        catalog: CatalogPartition::Rust,
        index: 95,
        tuple: expected_tuple.clone(),
    }];
    let actual = [expected_tuple, actual_tail.clone()];

    let message = partition_mismatch_message(CatalogPartition::Rust, &actual, &expected, 94);

    assert_diagnostic_contains(
        &message,
        [
            "partition=Rust".to_owned(),
            "global_index=95".to_owned(),
            "partition_index=1".to_owned(),
            "field=descriptor".to_owned(),
            "expected tuple: <missing>".to_owned(),
            format!("actual tuple: {actual_tail:?}"),
        ],
    );
}

#[test]
fn missing_actual_tail_descriptor_reports_the_first_divergent_tuple() {
    let expected_tail = test_tuple("missing-actual-tail");
    let actual_tuple = test_tuple("actual-tail");
    let expected = [
        FixtureDescriptor {
            catalog: CatalogPartition::Rust,
            index: 94,
            tuple: actual_tuple.clone(),
        },
        FixtureDescriptor {
            catalog: CatalogPartition::Rust,
            index: 95,
            tuple: expected_tail.clone(),
        },
    ];
    let actual = [actual_tuple];

    let message = partition_mismatch_message(CatalogPartition::Rust, &actual, &expected, 94);

    assert_diagnostic_contains(
        &message,
        [
            "partition=Rust".to_owned(),
            "global_index=95".to_owned(),
            "partition_index=1".to_owned(),
            "field=descriptor".to_owned(),
            format!("expected tuple: {expected_tail:?}"),
            "actual tuple: <missing>".to_owned(),
        ],
    );
}

fn parse_fixture() -> Vec<FixtureDescriptor> {
    assert!(
        FIXTURE.ends_with('\n'),
        "metric catalog fixture must end with a newline"
    );

    let mut seen_indices = BTreeSet::new();
    let descriptors = FIXTURE
        .lines()
        .enumerate()
        .map(|(line_offset, line)| {
            let line_number = line_offset + 1;
            let fields = line.split('\t').collect::<Vec<_>>();
            assert_eq!(
                fields.len(),
                7,
                "fixture line {line_number} must contain exactly seven TSV fields"
            );

            let catalog = parse_catalog(fields[0], line_number);
            let index = fields[1]
                .parse::<usize>()
                .unwrap_or_else(|error| panic!("fixture line {line_number} has an invalid index: {error}"));
            assert!(
                seen_indices.insert(index),
                "fixture line {line_number} duplicates global index {index}"
            );

            FixtureDescriptor {
                catalog,
                index,
                tuple: DescriptorTuple {
                    name: parse_json_string(fields[2], line_number, "name"),
                    kind: parse_kind(fields[3], line_number).to_owned(),
                    unit: parse_json_string(fields[4], line_number, "unit"),
                    labels: parse_json_labels(fields[5], line_number),
                    source: parse_source(fields[6], line_number).to_owned(),
                },
            }
        })
        .collect::<Vec<_>>();

    assert_eq!(
        descriptors.len(),
        TOTAL_DESCRIPTOR_COUNT,
        "fixture must contain exactly {TOTAL_DESCRIPTOR_COUNT} descriptor rows"
    );
    for index in 0..TOTAL_DESCRIPTOR_COUNT {
        assert!(seen_indices.contains(&index), "fixture is missing global index {index}");
    }

    let (java, rust) = descriptors.split_at(JAVA_DESCRIPTOR_COUNT);
    assert_eq!(
        java.len(),
        JAVA_DESCRIPTOR_COUNT,
        "fixture must reserve the first {JAVA_DESCRIPTOR_COUNT} rows for Java descriptors"
    );
    assert_eq!(
        rust.len(),
        RUST_DESCRIPTOR_COUNT,
        "fixture must reserve the final {RUST_DESCRIPTOR_COUNT} rows for Rust descriptors"
    );
    assert_partition_fixture(CatalogPartition::Java, java, 0);
    assert_partition_fixture(CatalogPartition::Rust, rust, JAVA_DESCRIPTOR_COUNT);

    let label_sequences = descriptors
        .iter()
        .map(|descriptor| descriptor.tuple.labels.clone())
        .collect::<HashSet<_>>();
    assert_eq!(
        label_sequences.len(),
        DISTINCT_LABEL_SEQUENCE_COUNT,
        "fixture must preserve every current resolved label sequence"
    );

    let sources = descriptors
        .iter()
        .map(|descriptor| descriptor.tuple.source.as_str())
        .collect::<HashSet<_>>();
    assert_eq!(
        sources.len(),
        DISTINCT_SOURCE_COUNT,
        "fixture must preserve every current metric source"
    );

    descriptors
}

fn parse_catalog(value: &str, line_number: usize) -> CatalogPartition {
    match value {
        "java" => CatalogPartition::Java,
        "rust" => CatalogPartition::Rust,
        _ => panic!("fixture line {line_number} has unknown catalog partition {value:?}"),
    }
}

fn parse_json_string(value: &str, line_number: usize, field: &str) -> String {
    serde_json::from_str(value)
        .unwrap_or_else(|error| panic!("fixture line {line_number} has an invalid JSON {field} string: {error}"))
}

fn parse_json_labels(value: &str, line_number: usize) -> Vec<String> {
    serde_json::from_str(value)
        .unwrap_or_else(|error| panic!("fixture line {line_number} has invalid JSON labels: {error}"))
}

fn parse_kind(value: &str, line_number: usize) -> &'static str {
    match value {
        "Counter" => "Counter",
        "Gauge" => "Gauge",
        "Histogram" => "Histogram",
        "ObservableGauge" => "ObservableGauge",
        "UpDownCounter" => "UpDownCounter",
        _ => panic!("fixture line {line_number} has unknown metric kind {value:?}"),
    }
}

fn parse_source(value: &str, line_number: usize) -> &'static str {
    match value {
        "Broker" => "Broker",
        "Client" => "Client",
        "NameServer" => "NameServer",
        "Pop" => "Pop",
        "Remoting" => "Remoting",
        "Store" => "Store",
        "Timer" => "Timer",
        "RocksDb" => "RocksDb",
        "TieredStore" => "TieredStore",
        "Proxy" => "Proxy",
        "Controller" => "Controller",
        "Observability" => "Observability",
        "Mcp" => "Mcp",
        "Runtime" => "Runtime",
        _ => panic!("fixture line {line_number} has unknown metric source {value:?}"),
    }
}

fn assert_partition_fixture(catalog: CatalogPartition, descriptors: &[FixtureDescriptor], first_index: usize) {
    for (partition_index, descriptor) in descriptors.iter().enumerate() {
        assert_eq!(
            descriptor.catalog, catalog,
            "fixture global index {} has an out-of-order partition marker",
            descriptor.index
        );
        assert_eq!(
            descriptor.index,
            first_index + partition_index,
            "fixture global indexes must be contiguous within the {catalog:?} partition"
        );
    }
}

fn assert_partition_matches(
    catalog: CatalogPartition,
    actual: &[MetricDescriptor],
    expected: &[FixtureDescriptor],
    first_index: usize,
) {
    let actual = actual.iter().map(descriptor_tuple).collect::<Vec<_>>();
    assert_partition_tuple_matches(catalog, &actual, expected, first_index);
}

fn assert_partition_tuple_matches(
    catalog: CatalogPartition,
    actual: &[DescriptorTuple],
    expected: &[FixtureDescriptor],
    first_index: usize,
) {
    for partition_index in 0..expected.len().max(actual.len()) {
        let expected = expected.get(partition_index);
        let actual = actual.get(partition_index);
        match (expected, actual) {
            (Some(expected), Some(actual)) if expected.tuple != *actual => {
                let fields = differing_fields(&expected.tuple, actual);
                panic!(
                    "metric catalog mismatch at partition={catalog:?}, global_index={}, partition_index={partition_index}, field={}\nexpected tuple: {:?}\nactual tuple: {:?}",
                    first_index + partition_index,
                    fields.join(", "),
                    expected.tuple,
                    actual,
                );
            }
            (Some(expected), None) => {
                panic!(
                    "metric catalog mismatch at partition={catalog:?}, global_index={}, partition_index={partition_index}, field=descriptor\nexpected tuple: {:?}\nactual tuple: <missing>",
                    first_index + partition_index,
                    expected.tuple,
                );
            }
            (None, Some(actual)) => {
                panic!(
                    "metric catalog mismatch at partition={catalog:?}, global_index={}, partition_index={partition_index}, field=descriptor\nexpected tuple: <missing>\nactual tuple: {:?}",
                    first_index + partition_index,
                    actual,
                );
            }
            (Some(_), Some(_)) => {}
            (None, None) => unreachable!("partition comparison range is derived from both lengths"),
        }
    }

    assert_eq!(
        actual.len(),
        expected.len(),
        "{catalog:?} catalog descriptor count differs from its fixture partition"
    );
}

fn partition_mismatch_message(
    catalog: CatalogPartition,
    actual: &[DescriptorTuple],
    expected: &[FixtureDescriptor],
    first_index: usize,
) -> String {
    let panic = std::panic::catch_unwind(|| {
        assert_partition_tuple_matches(catalog, actual, expected, first_index);
    })
    .expect_err("synthetic tail mismatch must panic");

    if let Some(message) = panic.downcast_ref::<String>() {
        return message.clone();
    }
    if let Some(message) = panic.downcast_ref::<&str>() {
        return (*message).to_owned();
    }
    panic!("tail mismatch did not produce a string panic message");
}

fn assert_diagnostic_contains<I>(message: &str, required_context: I)
where
    I: IntoIterator<Item = String>,
{
    for expected in required_context {
        assert!(
            message.contains(&expected),
            "diagnostic did not contain {expected:?}: {message}"
        );
    }
}

fn test_tuple(name: &str) -> DescriptorTuple {
    DescriptorTuple {
        name: name.to_owned(),
        kind: "Counter".to_owned(),
        unit: String::new(),
        labels: Vec::new(),
        source: "Runtime".to_owned(),
    }
}

fn descriptor_tuple(descriptor: &MetricDescriptor) -> DescriptorTuple {
    DescriptorTuple {
        name: descriptor.name.to_owned(),
        kind: metric_kind(descriptor.kind).to_owned(),
        unit: descriptor.unit.to_owned(),
        labels: descriptor.labels.iter().map(|label| (*label).to_owned()).collect(),
        source: metric_source(descriptor.source).to_owned(),
    }
}

fn differing_fields(expected: &DescriptorTuple, actual: &DescriptorTuple) -> Vec<&'static str> {
    let mut fields = Vec::new();
    if expected.name != actual.name {
        fields.push("name");
    }
    if expected.kind != actual.kind {
        fields.push("kind");
    }
    if expected.unit != actual.unit {
        fields.push("unit");
    }
    if expected.labels != actual.labels {
        fields.push("labels");
    }
    if expected.source != actual.source {
        fields.push("source");
    }
    fields
}

fn metric_kind(kind: MetricKind) -> &'static str {
    match kind {
        MetricKind::Counter => "Counter",
        MetricKind::Gauge => "Gauge",
        MetricKind::Histogram => "Histogram",
        MetricKind::ObservableGauge => "ObservableGauge",
        MetricKind::UpDownCounter => "UpDownCounter",
    }
}

fn metric_source(source: MetricSource) -> &'static str {
    match source {
        MetricSource::Broker => "Broker",
        MetricSource::Client => "Client",
        MetricSource::NameServer => "NameServer",
        MetricSource::Pop => "Pop",
        MetricSource::Remoting => "Remoting",
        MetricSource::Store => "Store",
        MetricSource::Timer => "Timer",
        MetricSource::RocksDb => "RocksDb",
        MetricSource::TieredStore => "TieredStore",
        MetricSource::Proxy => "Proxy",
        MetricSource::Controller => "Controller",
        MetricSource::Observability => "Observability",
        MetricSource::Mcp => "Mcp",
        MetricSource::Runtime => "Runtime",
    }
}
