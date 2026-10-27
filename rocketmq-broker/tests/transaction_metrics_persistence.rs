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

use std::fs;

use rocketmq_broker::test_support::TransactionMetricsProbe;

#[test]
fn persists_and_recovers_pending_counts_per_topic() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let checkpoint = directory.path().join("transactionMetrics");
    let metrics = TransactionMetricsProbe::open(&checkpoint).expect("new metrics store");

    assert_eq!(metrics.add_and_get("orders", 2), 2);
    assert_eq!(metrics.add_and_get("payments", 1), 1);
    assert_eq!(metrics.add_and_get("orders", -1), 1);
    metrics.persist().expect("persist transaction metrics");

    let recovered = TransactionMetricsProbe::open(&checkpoint).expect("recover transaction metrics");
    assert_eq!(recovered.count("orders"), 1);
    assert_eq!(recovered.count("payments"), 1);
    assert_eq!(recovered.snapshot(), vec![("orders".into(), 1), ("payments".into(), 1)]);
}

#[test]
fn falls_back_to_the_previous_complete_generation() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let checkpoint = directory.path().join("transactionMetrics");
    let metrics = TransactionMetricsProbe::open(&checkpoint).expect("new metrics store");

    metrics.add_and_get("orders", 1);
    metrics.persist().expect("first checkpoint");
    metrics.add_and_get("orders", 1);
    metrics.persist().expect("second checkpoint");
    fs::write(&checkpoint, b"{truncated").expect("corrupt current generation");

    let recovered = TransactionMetricsProbe::open(&checkpoint).expect("recover backup generation");
    assert_eq!(recovered.count("orders"), 1);
    assert!(recovered.recovered_from_backup());
}

#[test]
fn opens_the_empty_checkpoint_written_by_a_java_broker() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let checkpoint = directory.path().join("transactionMetrics");
    fs::write(
        &checkpoint,
        br#"{"dataVersion":{"counter":0,"stateVersion":0,"timestamp":1793057488361},"transactionCount":{}}"#,
    )
    .expect("write Java checkpoint");

    let recovered = TransactionMetricsProbe::open(&checkpoint).expect("open Java checkpoint");
    assert!(recovered.snapshot().is_empty());
    assert!(!recovered.recovered_from_backup());
}

#[test]
fn recovers_java_topic_counts_and_persists_them_in_the_native_layout() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let checkpoint = directory.path().join("transactionMetrics");
    fs::write(
        &checkpoint,
        br#"{"dataVersion":{"counter":3,"stateVersion":0,"timestamp":1793057488361},"transactionCount":{"orders":{"count":2,"timeStamp":1793057488361},"payments":{"count":0,"timeStamp":1793057487000}}}"#,
    )
    .expect("write Java checkpoint");

    let recovered = TransactionMetricsProbe::open(&checkpoint).expect("open Java checkpoint");
    assert_eq!(recovered.snapshot(), vec![("orders".into(), 2), ("payments".into(), 0)]);
    assert_eq!(recovered.add_and_get("orders", 1), 3);
    recovered.persist().expect("persist recovered metrics");

    let reopened = TransactionMetricsProbe::open(&checkpoint).expect("reopen native checkpoint");
    assert_eq!(reopened.snapshot(), vec![("orders".into(), 3), ("payments".into(), 0)]);
    assert!(!reopened.recovered_from_backup());
}

#[test]
fn rejects_an_unknown_checkpoint_version_without_mutating_it() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let checkpoint = directory.path().join("transactionMetrics");
    let body = br#"{"version":99,"generation":7,"topics":{}}"#;
    fs::write(&checkpoint, body).expect("write unsupported checkpoint");

    let error = TransactionMetricsProbe::open(&checkpoint).expect_err("unknown version must fail closed");
    assert_eq!(error, "core.configuration.invalid: Configuration value is invalid");
    assert_eq!(fs::read(&checkpoint).expect("checkpoint remains readable"), body);
}
