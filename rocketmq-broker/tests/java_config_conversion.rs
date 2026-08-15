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

use rocketmq_broker::config::java_properties::JavaBrokerProperties;
use rocketmq_store::StoreType;

#[test]
fn converts_java_broker_and_store_properties_atomically() {
    let input = include_str!("fixtures/config/java-broker.conf");
    let conversion = JavaBrokerProperties::parse(input).expect("Java properties should convert");

    assert_eq!(conversion.config().broker().broker_identity.broker_name, "broker-a");
    assert_eq!(conversion.config().broker().listen_port, 11911);
    assert_eq!(conversion.config().store().store_type, StoreType::LocalFile);
    assert_eq!(conversion.config().store().max_message_size, 0);
    assert!(!conversion.config().broker().enable_property_filter);
    assert_eq!(
        conversion.entries().len(),
        input.lines().filter(|line| line.contains('=')).count()
    );
    assert!(conversion.entries().iter().all(|entry| entry.status().is_mapped()));
}

#[test]
fn java_store_type_alias_is_ascii_case_insensitive_but_canonical() {
    for alias in ["default", "DEFAULT", "DeFaUlT"] {
        let conversion =
            JavaBrokerProperties::parse(&format!("storeType={alias}")).expect("default alias should convert");
        assert_eq!(conversion.config().store().store_type, StoreType::LocalFile);
    }
    for alias in ["defaultRocksDB", "DEFAULTROCKSDB", "DefaultRocksDb"] {
        let conversion =
            JavaBrokerProperties::parse(&format!("storeType={alias}")).expect("defaultRocksDB alias should convert");
        assert_eq!(conversion.config().store().store_type, StoreType::RocksDB);
    }
    assert!(JavaBrokerProperties::parse("storeType=rocksdb").is_err());
}

#[test]
fn accepts_java_properties_whitespace_separator() {
    let conversion = JavaBrokerProperties::parse("brokerName broker-from-java\nlistenPort : 10919")
        .expect("Java whitespace and colon separators should convert");

    assert_eq!(
        conversion.config().broker().broker_identity.broker_name,
        "broker-from-java"
    );
    assert_eq!(conversion.config().broker().listen_port, 10919);
}

#[test]
fn converts_java_rocksdb_role_and_flush_fixture() {
    let conversion = JavaBrokerProperties::parse(include_str!("fixtures/config/java-rocksdb.conf"))
        .expect("Java RocksDB properties should convert");

    assert_eq!(conversion.config().store().store_type, StoreType::RocksDB);
    assert_eq!(
        conversion.config().store().broker_role,
        rocketmq_model::common::broker::broker_role::BrokerRole::AsyncMaster
    );
    assert_eq!(
        conversion.config().store().flush_disk_type,
        rocketmq_store::FlushDiskType::AsyncFlush
    );
}

#[test]
fn duplicate_unknown_and_dledger_properties_fail_closed() {
    assert!(JavaBrokerProperties::parse("listenPort=10911\nlistenPort=10912").is_err());
    assert!(JavaBrokerProperties::parse("notARealRocketMqProperty=true").is_err());
    let error =
        JavaBrokerProperties::parse("enableDLedgerCommitLog=true").expect_err("DLedger configuration must be rejected");
    assert!(error.to_string().contains("enableDLedgerCommitLog"), "{error}");
    assert!(error.to_string().contains("DLedger"), "{error}");
}

#[test]
fn conversion_report_redacts_sensitive_values_and_preserves_empty_strings() {
    let conversion = JavaBrokerProperties::parse("authConfigPath=\naclFile=/secret/plain_acl.yml")
        .expect("auth paths should map to broker configuration");
    assert!(conversion.config().broker().auth_config_path.is_empty());

    let report = conversion.report_json().expect("conversion report should serialize");
    assert!(!report.contains("/secret/plain_acl.yml"));
    assert!(report.contains("configured"));
    assert!(conversion.warnings().iter().any(|warning| warning.key() == "aclFile"));
}
