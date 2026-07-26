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

use std::collections::HashMap;
use std::path::PathBuf;

use cheetah_string::CheetahString;
use rocketmq_broker::config::broker_config::BrokerConfig;
use rocketmq_broker::config::error::BrokerConfigError;
use rocketmq_broker::config::error::ConfigSection;
use rocketmq_broker::config::raw::RawBrokerConfig;
use rocketmq_broker::config::transaction::ConfigUpdateTransaction;
use rocketmq_broker::config::validated::ConfigGeneration;
use rocketmq_broker::config::validated::ValidatedBrokerConfig;
use rocketmq_runtime::MemoryLimitSource;
use rocketmq_store::MessageStoreConfig;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("config")
        .join(name)
}

fn assert_invalid_section(result: Result<ValidatedBrokerConfig, BrokerConfigError>, expected: ConfigSection) {
    match result {
        Err(BrokerConfigError::Invalid { section, .. }) => assert_eq!(section, expected),
        Err(error) => panic!("expected an invalid {expected} configuration, got {error}"),
        Ok(_) => panic!("expected an invalid {expected} configuration"),
    }
}

#[test]
fn canonical_fixture_crosses_the_raw_and_validated_boundary() {
    let raw = RawBrokerConfig::load(fixture("valid.toml")).expect("canonical fixture should deserialize");
    let validated = ValidatedBrokerConfig::try_from(raw).expect("canonical fixture should validate");
    let sections = validated.sections();

    assert_eq!(sections.identity().broker_name(), "config-contract-broker");
    assert_eq!(sections.identity().cluster_name(), "ConfigContract");
    assert_eq!(sections.identity().broker_id(), 0);
    assert_eq!(sections.network().advertised_address(), "broker.example.internal");
    assert_eq!(sections.network().listen_port(), 10911);
    assert_eq!(sections.network().fast_listen_port(), 10909);
    assert_eq!(sections.high_availability().listen_port(), 10912);
    assert_eq!(
        sections.storage().store_root(),
        std::path::Path::new("./target/config-contract/store")
    );
    assert!(sections.security().authentication_enabled());
    assert!(sections.security().authorization_enabled());
    assert!(sections.resources().max_client_events() > 0);
    assert!((0.0..=1.0).contains(&sections.telemetry().metrics_sample_ratio()));
    assert!(!sections.telemetry().trace_exporter().is_enable());
    assert_eq!(validated.logging().logging.filter.as_deref(), Some("info"));
    assert!(validated.logging().logging.reload.enabled);
}

#[test]
fn unknown_nested_field_is_rejected_during_deserialization() {
    let error = RawBrokerConfig::load(fixture("unknown-field.toml"))
        .expect_err("unknown identity field must fail before validation");

    match error {
        BrokerConfigError::Load { source, .. } => assert!(
            source.to_string().contains("brokerColour"),
            "load error should identify the unknown field: {source}"
        ),
        error => panic!("expected a typed load error, got {error}"),
    }
}

#[test]
fn unknown_top_level_section_is_rejected_during_deserialization() {
    let error = RawBrokerConfig::load(fixture("unknown-section.toml"))
        .expect_err("unknown top-level section must fail before validation");

    match error {
        BrokerConfigError::Load { source, .. } => assert!(
            source.to_string().contains("experimental"),
            "load error should identify the unknown section: {source}"
        ),
        error => panic!("expected a typed load error, got {error}"),
    }
}

#[test]
fn derived_fields_are_rejected_when_configured_under_the_wrong_owner() {
    for (name, expected_section, expected_field) in [
        (
            "misplaced-listener.toml",
            ConfigSection::Network,
            "broker.brokerServerConfig.listenPort",
        ),
        (
            "misplaced-controller-mode.toml",
            ConfigSection::HighAvailability,
            "store.enableControllerMode",
        ),
        (
            "misplaced-duplication.toml",
            ConfigSection::HighAvailability,
            "store.duplicationEnable",
        ),
    ] {
        let error = RawBrokerConfig::load(fixture(name))
            .expect_err("derived field must be configured only under its canonical owner");
        match error {
            BrokerConfigError::Invalid { section, field, .. } => {
                assert_eq!(section, expected_section);
                assert_eq!(field, expected_field);
            }
            error => panic!("expected a typed misplaced-field error, got {error}"),
        }
    }
}

#[test]
fn every_validated_section_rejects_an_invalid_candidate() {
    let mut broker = BrokerConfig::default();
    broker.broker_identity.broker_name = " ".into();
    assert_invalid_section(
        ValidatedBrokerConfig::try_from_parts(broker, MessageStoreConfig::default()),
        ConfigSection::Identity,
    );

    let broker = BrokerConfig {
        broker_ip1: "two hosts".into(),
        ..BrokerConfig::default()
    };
    assert_invalid_section(
        ValidatedBrokerConfig::try_from_parts(broker, MessageStoreConfig::default()),
        ConfigSection::Network,
    );

    let broker = BrokerConfig::default();
    let store = MessageStoreConfig {
        ha_listen_port: broker.listen_port as usize,
        ..MessageStoreConfig::default()
    };
    assert_invalid_section(
        ValidatedBrokerConfig::try_from_parts(broker, store),
        ConfigSection::HighAvailability,
    );

    let broker = BrokerConfig::default();
    let store = MessageStoreConfig {
        mapped_file_size_commit_log: 0,
        ..MessageStoreConfig::default()
    };
    assert_invalid_section(
        ValidatedBrokerConfig::try_from_parts(broker, store),
        ConfigSection::Storage,
    );

    let raw = RawBrokerConfig::load(fixture("invalid-security.toml"))
        .expect("invalid security fixture should still deserialize");
    assert_invalid_section(ValidatedBrokerConfig::try_from(raw), ConfigSection::Security);

    let broker = BrokerConfig {
        max_client_event_count: 0,
        ..BrokerConfig::default()
    };
    assert_invalid_section(
        ValidatedBrokerConfig::try_from_parts(broker, MessageStoreConfig::default()),
        ConfigSection::Resources,
    );

    let broker = BrokerConfig {
        trace_sample_ratio: 1.5,
        ..BrokerConfig::default()
    };
    assert_invalid_section(
        ValidatedBrokerConfig::try_from_parts(broker, MessageStoreConfig::default()),
        ConfigSection::Telemetry,
    );
}

#[test]
fn resource_budget_is_derived_from_the_validated_process_hard_limit() {
    const HARD_LIMIT: u64 = 512 * 1024 * 1024;
    let broker = BrokerConfig {
        process_memory_limit_bytes: HARD_LIMIT,
        ..BrokerConfig::default()
    };

    let validated = ValidatedBrokerConfig::try_from_parts(broker, MessageStoreConfig::default())
        .expect("explicit process memory hard limit should validate");
    let resources = validated.sections().resources();

    assert_eq!(resources.process_memory_limit_bytes(), HARD_LIMIT);
    assert_eq!(resources.process_memory_limit_source(), MemoryLimitSource::Configured);
    assert_eq!(resources.managed_memory_bytes(), HARD_LIMIT / 4);
    assert_eq!(resources.control_reserve_bytes(), HARD_LIMIT / 4 / 20);
    assert_eq!(
        resources.max_pop_polling_requests(),
        BrokerConfig::default().max_pop_polling_size
    );
    let root = resources
        .budget_tree()
        .expect("validated resources produce a budget tree")
        .root();
    let expected_item_capacity = resources
        .max_lite_subscriptions()
        .saturating_mul(2)
        .saturating_add(resources.max_pop_polling_requests())
        .saturating_add(65_536);
    assert_eq!(
        root.limit().capacity.count,
        usize::try_from(expected_item_capacity).unwrap_or(usize::MAX)
    );
    assert_eq!(root.limit().capacity.bytes, (HARD_LIMIT / 4) as usize);
    assert_eq!(root.limit().control_reserve.bytes, (HARD_LIMIT / 4 / 20) as usize);
}

#[test]
fn ipv6_advertised_address_uses_socket_address_brackets() {
    let broker = BrokerConfig {
        broker_ip1: "::1".into(),
        ..BrokerConfig::default()
    };

    let validated = ValidatedBrokerConfig::try_from_parts(broker, MessageStoreConfig::default())
        .expect("IPv6 advertised address should validate");

    assert_eq!(validated.broker().get_broker_addr(), "[::1]:10911");
}

#[test]
fn dynamic_patch_is_validated_and_static_fields_require_restart() {
    let current = ValidatedBrokerConfig::default();
    let dynamic = HashMap::from([(
        CheetahString::from_static_str("maxClientEventCount"),
        CheetahString::from_static_str("101"),
    )]);
    let transaction = ConfigUpdateTransaction::from_broker_patch(ConfigGeneration::INITIAL, &current, &dynamic)
        .expect("supported dynamic patch should validate");
    assert_eq!(transaction.expected_generation(), ConfigGeneration::INITIAL);

    let static_field = HashMap::from([(
        CheetahString::from_static_str("listenPort"),
        CheetahString::from_static_str("20911"),
    )]);
    let error = match ConfigUpdateTransaction::from_broker_patch(ConfigGeneration::INITIAL, &current, &static_field) {
        Ok(_) => panic!("listenPort must not be published at runtime"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        BrokerConfigError::RestartRequired { fields } if fields == "listenPort"
    ));
}

#[test]
fn runtime_composition_accepts_only_validated_configuration() {
    let builder_source = include_str!("../src/broker_bootstrap.rs");
    let composition_source = include_str!("../src/broker_runtime/composition.rs");

    assert!(builder_source.contains("validated_config: ValidatedBrokerConfig"));
    assert!(builder_source.contains("with_validated_config"));
    assert!(!builder_source.contains("with_broker_config"));
    assert!(!builder_source.contains("with_message_store_config"));
    assert!(composition_source.contains("new_with_validated_config"));
    assert!(!composition_source.contains("pub fn new_with_broker_config"));
}

#[test]
fn rendered_deployment_sources_use_the_canonical_section_layout() {
    let helm = include_str!("../../distribution/helm/rocketmq-rust/templates/configmaps.yaml");
    let kubernetes = include_str!("../../distribution/kubernetes/base/manifest.yaml");

    for source in [helm, kubernetes] {
        assert!(source.contains("    [broker]"));
        assert!(source.contains("    [broker.brokerIdentity]"));
        assert!(source.contains("    [store]"));
    }
}
