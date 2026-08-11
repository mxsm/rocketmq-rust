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
use std::io::Write;
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
use rocketmq_store::FlushDiskType;
use rocketmq_store::MessageStoreConfig;
use rocketmq_store_api::TimerStoreMode;

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

fn load_inline_config(source: &str) -> Result<RawBrokerConfig, Box<BrokerConfigError>> {
    let mut file = tempfile::Builder::new()
        .suffix(".toml")
        .tempfile()
        .expect("temporary configuration file");
    file.write_all(source.as_bytes())
        .expect("write temporary configuration");
    RawBrokerConfig::load(file.path()).map_err(Box::new)
}

#[test]
fn timer_extended_capability_configuration_survives_validation_unchanged() {
    let store = MessageStoreConfig {
        timer_store_mode: TimerStoreMode::ExtendedTimeline,
        timer_extended_shadow_enable: false,
        timer_extended_admission_enable: true,
        timer_extended_activation_epoch: 11,
        timer_extended_admission_horizon_days: 366,
        ..MessageStoreConfig::default()
    };
    let validated = ValidatedBrokerConfig::try_from_parts(BrokerConfig::default(), store)
        .expect("Extended Timer configuration should cross the validated boundary");

    assert_eq!(validated.store().timer_store_mode, TimerStoreMode::ExtendedTimeline);
    assert!(validated.store().timer_extended_admission_enable);
    assert_eq!(validated.store().timer_extended_activation_epoch, 11);
    assert_eq!(validated.store().timer_extended_admission_horizon_days, 366);
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
    assert_eq!(validated.store().max_recovery_commit_log_files, 30);
    assert_eq!(validated.store().flush_consume_queue_least_pages, 2);
}

#[test]
fn omitted_store_fields_use_the_production_defaults() {
    let raw = load_inline_config("[store]\n").expect("minimal store config should deserialize");
    let validated = ValidatedBrokerConfig::try_from(raw).expect("production defaults should validate");
    let store = validated.store();

    assert_eq!(store.flush_disk_type, FlushDiskType::AsyncFlush);
    assert_eq!(store.max_recovery_commit_log_files, 30);
    assert_eq!(store.flush_commit_log_least_pages, 4);
    assert_eq!(store.commit_commit_log_least_pages, 4);
    assert_eq!(store.flush_consume_queue_least_pages, 2);
    assert_eq!(store.flush_consume_queue_thorough_interval, 60_000);
    assert_eq!(store.slave_timeout, 3_000);
    assert_eq!(store.transient_store_pool_size, 5);
    assert_eq!(store.min_in_sync_replicas, 1);
    assert_eq!(store.ha_max_time_slave_not_catchup, 15_000);
}

#[test]
fn fast_failure_pending_budget_defaults_and_explicit_values_cross_validation() {
    let default = ValidatedBrokerConfig::try_from_parts(BrokerConfig::default(), MessageStoreConfig::default())
        .expect("default fast-failure pending budget should validate");
    assert_eq!(default.broker().broker_fast_failure_pending_max_count, 4_096);
    assert_eq!(default.broker().broker_fast_failure_pending_max_bytes, 64 * 1024 * 1024);

    let raw = load_inline_config(
        "[broker]\nbrokerFastFailurePendingMaxCount = 17\nbrokerFastFailurePendingMaxBytes = 65537\n",
    )
    .expect("explicit fast-failure budget should deserialize");
    let explicit = ValidatedBrokerConfig::try_from(raw).expect("explicit fast-failure budget should validate");
    assert_eq!(explicit.broker().broker_fast_failure_pending_max_count, 17);
    assert_eq!(explicit.broker().broker_fast_failure_pending_max_bytes, 65_537);
}

#[test]
fn zero_fast_failure_pending_budget_fails_before_broker_startup() {
    for broker in [
        BrokerConfig {
            broker_fast_failure_pending_max_count: 0,
            ..BrokerConfig::default()
        },
        BrokerConfig {
            broker_fast_failure_pending_max_bytes: 0,
            ..BrokerConfig::default()
        },
    ] {
        assert_invalid_section(
            ValidatedBrokerConfig::try_from_parts(broker, MessageStoreConfig::default()),
            ConfigSection::Resources,
        );
    }
}

#[test]
fn explicit_store_overrides_remain_authoritative() {
    let raw = load_inline_config("[store]\nflushDiskType = \"SYNC_FLUSH\"\nallAckInSyncStateSet = true\n")
        .expect("explicit durability overrides should deserialize");
    let validated = ValidatedBrokerConfig::try_from(raw).expect("explicit durability overrides should validate");

    assert_eq!(validated.store().flush_disk_type, FlushDiskType::SyncFlush);
    assert!(validated.store().all_ack_in_sync_state_set);
}

#[test]
fn unsafe_explicit_zero_is_rejected() {
    let raw = load_inline_config("[store]\nflushCommitLogLeastPages = 0\n").expect("explicit zero should deserialize");

    assert_invalid_section(ValidatedBrokerConfig::try_from(raw), ConfigSection::Storage);
}

#[test]
fn compatibility_profile_is_rejected_as_an_unknown_field() {
    let error = load_inline_config("[store]\ncompatibilityProfile = \"JAVA_5_5\"\n")
        .expect_err("removed profile key must fail before validation");

    assert!(error.to_string().contains("compatibilityProfile"));
}

#[test]
fn dledger_configuration_is_rejected_at_the_validated_boundary() {
    let store = MessageStoreConfig {
        enable_dledger_commit_log: true,
        ..MessageStoreConfig::default()
    };

    assert_invalid_section(
        ValidatedBrokerConfig::try_from_parts(BrokerConfig::default(), store),
        ConfigSection::Storage,
    );
}

#[test]
fn enabled_transient_pool_requires_at_least_one_buffer() {
    let store = MessageStoreConfig {
        transient_store_pool_enable: true,
        transient_store_pool_size: 0,
        ..MessageStoreConfig::default()
    };

    assert_invalid_section(
        ValidatedBrokerConfig::try_from_parts(BrokerConfig::default(), store),
        ConfigSection::Storage,
    );
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
        assert!(!source.contains("compatibilityProfile"));
    }
}

#[test]
fn deterministic_config_cases_preserve_defaults_and_reject_unknown_fields() {
    const SEED: u64 = 0x524d_5143_4f4e_4647;
    let mut state = SEED;

    for case in 0..16 {
        state = state
            .wrapping_mul(2_862_933_555_777_941_757)
            .wrapping_add(3_037_000_493);
        let listen_port = 12_000 + (state % 20_000) as u32;
        let max_client_events = 1 + state % 16_384;
        let source = format!(
            "[broker]\n\
             listenPort = {listen_port}\n\
             maxClientEventCount = {max_client_events}\n\
             storePathRootDir = \"./target/property-config/broker-{case}\"\n\
             [broker.brokerIdentity]\n\
             brokerName = \"property-broker-{case}\"\n\
             brokerClusterName = \"PropertyCluster\"\n\
             brokerId = 0\n\
             [store]\n\
             haListenPort = {}\n\
             storePathRootDir = \"./target/property-config/store-{case}\"\n",
            listen_port + 1
        );
        let raw = load_inline_config(&source)
            .unwrap_or_else(|error| panic!("seed={SEED:#018x} case={case} failed to deserialize: {error}"));
        let validated = ValidatedBrokerConfig::try_from(raw)
            .unwrap_or_else(|error| panic!("seed={SEED:#018x} case={case} failed to validate: {error}"));
        assert_eq!(
            validated.sections().network().listen_port(),
            listen_port as u16,
            "seed={SEED:#018x} case={case}"
        );
        assert_eq!(
            validated.sections().resources().max_client_events(),
            max_client_events as i32,
            "seed={SEED:#018x} case={case}"
        );

        let unknown = source.replace("[broker]\n", &format!("[broker]\nunknownProperty{case} = true\n"));
        assert!(
            load_inline_config(&unknown).is_err(),
            "seed={SEED:#018x} case={case} accepted an unknown field"
        );
    }
}
