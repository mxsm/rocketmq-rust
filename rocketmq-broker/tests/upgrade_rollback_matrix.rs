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

#[path = "../src/transaction/transaction_metrics.rs"]
mod transaction_metrics;

use rocketmq_broker::config::java_properties::JavaBrokerProperties;
use rocketmq_model::common::pop_retry_policy::PopRetryPolicy;
use rocketmq_store_rocksdb::profile_marker::PopConsumerProfileMarker;
use rocketmq_store_rocksdb::profile_marker::POP_CONSUMER_PROFILE_COLUMN_FAMILY;
use rocketmq_store_rocksdb::profile_marker::POP_CONSUMER_PROFILE_MARKER_KEY;
use rocketmq_store_rocksdb::read_only::PopConsumerProfileState;
use rocketmq_store_rocksdb::read_only::ReadOnlyRocksDb;
use rocketmq_store_rocksdb::store::KeyValueStore;
use rocketmq_store_rocksdb::RocksDbStore;
use transaction_metrics::TransactionMetrics;

#[test]
fn pop_profile_and_transaction_checkpoint_survive_a_restart_boundary() {
    let temp = tempfile::tempdir().expect("tempdir");
    let store = open_profile_store(temp.path().join("kvStore"));
    let marker = PopConsumerProfileMarker::new(3);
    store
        .put_cf(
            POP_CONSUMER_PROFILE_COLUMN_FAMILY,
            POP_CONSUMER_PROFILE_MARKER_KEY,
            &marker.encode().expect("marker"),
        )
        .expect("persist profile marker");
    store.flush().expect("flush profile marker");
    store.close();
    drop(store);

    let reopened = ReadOnlyRocksDb::open_existing(temp.path().join("kvStore"))
        .expect("read-only reopen")
        .expect("profile database");
    assert_eq!(
        reopened.inspect_pop_consumer_profile(true).expect("inspect marker"),
        PopConsumerProfileState::PresentValid(marker)
    );
    assert_eq!(PopRetryPolicy::dual_read_v2_write(0).write_version.number(), 2);

    let checkpoint = temp.path().join("transactionMetrics");
    let metrics = TransactionMetrics::open(&checkpoint).expect("transaction metrics");
    metrics.add_and_get("UpgradeTopic", 2);
    metrics.persist().expect("persist transaction metrics");
    let recovered = TransactionMetrics::open(&checkpoint).expect("recover transaction metrics");
    assert_eq!(recovered.count("UpgradeTopic"), 2);
    assert_eq!(recovered.snapshot(), vec![("UpgradeTopic".to_owned(), 2)]);
    assert!(!recovered.persist_if_dirty().expect("clean checkpoint"));
    assert!(!recovered.recovered_from_backup());
}

#[test]
fn java_properties_conversion_is_idempotent_across_restart_input() {
    let input = include_str!("fixtures/config/java-broker.conf");
    let first = JavaBrokerProperties::parse(input).expect("first conversion");
    let second = JavaBrokerProperties::parse(input).expect("second conversion");

    assert_eq!(
        first.config().broker().get_properties(),
        second.config().broker().get_properties()
    );
    assert_eq!(
        first.config().store().get_properties(),
        second.config().store().get_properties()
    );
}

fn open_profile_store(path: std::path::PathBuf) -> RocksDbStore {
    let mut config = rocketmq_store_rocksdb::RocksDbConfig {
        enabled: true,
        path,
        ..rocketmq_store_rocksdb::RocksDbConfig::default()
    };
    let mut profile = rocketmq_store_rocksdb::config::RocksDbColumnFamilyConfig::consume_queue_default();
    profile.name = POP_CONSUMER_PROFILE_COLUMN_FAMILY.to_owned();
    config.column_families.push(profile);
    RocksDbStore::open(config).expect("open profile store")
}
