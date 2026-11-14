// Copyright 2026 The RocketMQ Rust Authors
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

use super::*;

fn histogram(node: &str, first_populated: usize, counts: &[f64], times: &[i64], increase: f64) -> LatencyHistogram {
    LatencyHistogram {
        node_id: node.to_owned(),
        buckets: LATENCY_GRID
            .iter()
            .enumerate()
            .map(|(index, bound)| LatencyBucket {
                upper_bound_millis: *bound,
                samples: times
                    .iter()
                    .zip(counts)
                    .map(|(time, count)| LatencyCounterSample {
                        at_millis: *time,
                        count: if index >= first_populated { *count } else { 0.0 },
                    })
                    .collect(),
                increase: if index >= first_populated { increase } else { 0.0 },
            })
            .collect(),
    }
}

fn baseline() -> LatencyHistogram {
    histogram("broker-a:0", 7, &[0.0, 100.0, 200.0], &[1, 50, 99], 200.0)
}

#[test]
fn send_latency_interpolates_a_finite_bucket_without_calling_counts_milliseconds() {
    let result = assess_latency_histograms(0, 100, &[baseline()]).unwrap();
    assert_eq!(result.state, LatencyWindowState::Available);
    assert_eq!(result.observed_increments, 200.0);
    assert_eq!(result.scrape_samples, 3);
    let quantile = result.quantile.unwrap();
    assert_eq!(quantile.estimate_millis, 248.5);
    assert_eq!(quantile.lower_bound_millis, 100.0);
    assert_eq!(quantile.upper_bound_millis, Some(250.0));
}

#[test]
fn send_latency_sums_valid_increases_then_interpolates_instead_of_summing_quantiles() {
    let second = histogram("broker-b:0", 8, &[0.0, 50.0, 100.0], &[1, 50, 99], 100.0);
    let result = assess_latency_histograms(0, 100, &[baseline(), second]).unwrap();
    assert_eq!(result.quantile.unwrap().estimate_millis, 492.5);
    assert_eq!(result.estimated_increments, 300.0);
    assert_eq!(result.observed_nodes, 2);
}

#[test]
fn send_latency_empty_is_missing_but_constant_complete_counters_are_no_recorded_traffic() {
    assert_eq!(
        assess_latency_histograms(0, 100, &[]),
        Err(LatencyWindowIssue::MissingHistogram)
    );
    let quiet = histogram("a", 7, &[12.0, 12.0, 12.0], &[1, 50, 99], 0.0);
    let result = assess_latency_histograms(0, 100, &[quiet]).unwrap();
    assert_eq!(result.state, LatencyWindowState::NoRecordedTraffic);
    assert_eq!(result.quantile, None);
}

#[test]
fn send_latency_observed_reset_is_counted_once_per_node_not_once_per_bucket() {
    let reset = histogram("a", 7, &[100.0, 5.0, 150.0], &[1, 50, 99], 150.0);
    let result = assess_latency_histograms(0, 100, &[reset]).unwrap();
    assert_eq!(result.observed_resets, 1);
    assert_eq!(result.observed_increments, 150.0);
    assert_eq!(result.quantile.unwrap().estimate_millis, 248.5);
}

#[test]
fn send_latency_missing_interior_or_unknown_grid_fails_even_when_every_node_agrees() {
    let mut incomplete = baseline();
    incomplete.buckets.remove(3);
    let mut another = incomplete.clone();
    another.node_id = "b".to_owned();
    assert_eq!(
        assess_latency_histograms(0, 100, &[incomplete, another]),
        Err(LatencyWindowIssue::UnrecognizedGrid)
    );
    let mut unknown = baseline();
    unknown.buckets[1].upper_bound_millis = Some(6.0);
    assert_eq!(
        assess_latency_histograms(0, 100, &[unknown]),
        Err(LatencyWindowIssue::UnrecognizedGrid)
    );
    let mut no_tail = baseline();
    no_tail.buckets[15].upper_bound_millis = Some(20000.0);
    assert_eq!(
        assess_latency_histograms(0, 100, &[no_tail]),
        Err(LatencyWindowIssue::UnrecognizedGrid)
    );
}

#[test]
fn send_latency_invalid_numbers_counter_shape_or_cross_bucket_times_cannot_produce_p99() {
    for invalid in [-1.0, f64::NAN, f64::INFINITY, 9_007_199_254_740_992.0, 0.5] {
        let mut sample = baseline();
        sample.buckets[7].samples[1].count = invalid;
        assert_eq!(
            assess_latency_histograms(0, 100, &[sample]),
            Err(LatencyWindowIssue::InvalidDistribution)
        );
    }
    let mut bad_increase = baseline();
    bad_increase.buckets[8].increase = 199.0;
    assert_eq!(
        assess_latency_histograms(0, 100, &[bad_increase]),
        Err(LatencyWindowIssue::InvalidDistribution)
    );
    let mut bad_clock = baseline();
    bad_clock.buckets[8].samples[1].at_millis = 51;
    assert_eq!(
        assess_latency_histograms(0, 100, &[bad_clock]),
        Err(LatencyWindowIssue::InvalidDistribution)
    );
}

#[test]
fn send_latency_uses_actual_scrapes_and_explicit_span_gap_boundaries() {
    let one = histogram("a", 7, &[200.0], &[99], 200.0);
    assert_eq!(
        assess_latency_histograms(0, 100, &[one]),
        Err(LatencyWindowIssue::InsufficientSamples)
    );
    let equal = histogram("a", 7, &[0.0, 200.0], &[10, 90], 200.0);
    assert_eq!(
        assess_latency_histograms(0, 100, &[equal]).unwrap().state,
        LatencyWindowState::Available
    );
    let short = histogram("a", 7, &[0.0, 200.0], &[10, 89], 200.0);
    assert_eq!(
        assess_latency_histograms(0, 100, &[short]),
        Err(LatencyWindowIssue::IncompleteSpan)
    );
    let equal_gap = histogram("a", 7, &[0.0, 100.0, 200.0], &[1, 60_001, 74_999], 200.0);
    assert!(assess_latency_histograms(0, 75_000, &[equal_gap]).is_ok());
    let bad_gap = histogram("a", 7, &[0.0, 100.0, 200.0], &[1, 60_002, 74_999], 200.0);
    assert_eq!(
        assess_latency_histograms(0, 75_000, &[bad_gap]),
        Err(LatencyWindowIssue::ScrapeGap)
    );
}

#[test]
fn send_latency_left_open_membership_and_duplicate_scrapes_are_strict() {
    for times in [[0, 50, 99], [1, 50, 101], [1, 50, 50], [1, 99, 50]] {
        let sample = histogram("a", 7, &[0.0, 100.0, 200.0], &times, 200.0);
        assert_eq!(
            assess_latency_histograms(0, 100, &[sample]),
            Err(LatencyWindowIssue::InvalidSampleTime)
        );
    }
    let sample = histogram("a", 7, &[0.0, 100.0, 200.0], &[1, 50, 100], 200.0);
    assert!(assess_latency_histograms(0, 100, &[sample]).is_ok());
    assert_eq!(
        assess_latency_histograms(0, 100, &[baseline(), baseline()]),
        Err(LatencyWindowIssue::AmbiguousPopulation)
    );
}

#[test]
fn send_latency_zero_recorded_milliseconds_is_real_traffic_while_infinite_tail_is_resolution_limited() {
    let zero = histogram("a", 0, &[0.0, 100.0, 200.0], &[1, 50, 99], 200.0);
    let result = assess_latency_histograms(0, 100, &[zero]).unwrap();
    assert_eq!(result.quantile.unwrap().estimate_millis, 0.0);
    assert_eq!(result.state, LatencyWindowState::Available);
    let tail = histogram("a", 15, &[0.0, 100.0, 200.0], &[1, 50, 99], 200.0);
    let result = assess_latency_histograms(0, 100, &[tail]).unwrap();
    assert_eq!(result.state, LatencyWindowState::TailLimited);
    assert_eq!(result.quantile.unwrap().upper_bound_millis, None);
}

#[test]
fn send_latency_sparse_observed_events_are_not_qualified_by_a_large_fractional_estimate() {
    let sparse = histogram("a", 7, &[0.0, 0.0, 1.0], &[1, 50, 99], 100.25);
    let result = assess_latency_histograms(0, 100, &[sparse]).unwrap();
    assert_eq!(result.observed_increments, 1.0);
    assert_eq!(result.estimated_increments, 100.25);
    assert_eq!(result.state, LatencyWindowState::InsufficientPopulation);
}

#[test]
fn send_latency_oldest_last_population_is_not_replaced_by_a_newer_node() {
    let newer = histogram("a", 7, &[0.0, 100.0, 200.0], &[1, 50, 99], 200.0);
    let older = histogram("b", 7, &[0.0, 100.0, 200.0], &[1, 45, 85], 200.0);
    let result = assess_latency_histograms(0, 100, &[newer, older]).unwrap();
    assert_eq!(result.data_last_millis, 99);
    assert_eq!(result.oldest_last_millis, 85);
    assert_eq!(
        assess_latency_histograms(i64::MIN, i64::MAX, &[baseline()]),
        Err(LatencyWindowIssue::UnsupportedWindow)
    );
}
