use std::collections::HashMap;

use cheetah_string::CheetahString;
use rocketmq_admin_core::core::export_data::filter_export_broker_properties;
use rocketmq_admin_core::core::export_data::ExportConfigsRequest;
use rocketmq_admin_core::core::export_data::ExportMetadataInRocksDbConfigType;
use rocketmq_admin_core::core::export_data::ExportMetadataInRocksDbRequest;
use rocketmq_admin_core::core::export_data::ExportMetadataInRocksDbResult;
use rocketmq_admin_core::core::export_data::ExportMetadataRequest;
use rocketmq_admin_core::core::export_data::ExportMetadataScope;
use rocketmq_admin_core::core::export_data::ExportMetadataTarget;
use rocketmq_admin_core::core::export_data::ExportPopRecordRequest;
use rocketmq_admin_core::core::export_data::ExportPopRecordTarget;
use rocketmq_admin_core::core::export_data::ExportService;

#[test]
fn export_configs_request_trims_cluster_and_namesrv() {
    let request = ExportConfigsRequest::try_new(" DefaultCluster ")
        .unwrap()
        .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".to_string()));

    assert_eq!(request.cluster_name().as_str(), "DefaultCluster");
    assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
}

#[test]
fn export_configs_request_rejects_blank_cluster() {
    assert!(ExportConfigsRequest::try_new(" ").is_err());
}

#[test]
fn filter_export_broker_properties_keeps_only_exportable_keys() {
    let mut properties = HashMap::new();
    properties.insert(
        CheetahString::from_static_str("brokerName"),
        CheetahString::from_static_str("broker-a"),
    );
    properties.insert(
        CheetahString::from_static_str("brokerRole"),
        CheetahString::from_static_str("ASYNC_MASTER"),
    );
    properties.insert(
        CheetahString::from_static_str("internalOnly"),
        CheetahString::from_static_str("hidden"),
    );

    let filtered = filter_export_broker_properties(&properties);

    assert_eq!(filtered.get("brokerName").map(String::as_str), Some("broker-a"));
    assert_eq!(filtered.get("brokerRole").map(String::as_str), Some("ASYNC_MASTER"));
    assert!(!filtered.contains_key("internalOnly"));
}

#[test]
fn export_metadata_request_trims_broker_topic_target() {
    let request = ExportMetadataRequest::try_new(None, Some(" 127.0.0.1:10911 ".to_string()), true, false, false)
        .unwrap()
        .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".to_string()));

    assert_eq!(
        request.target(),
        &ExportMetadataTarget::Broker("127.0.0.1:10911".into())
    );
    assert_eq!(request.scope(), ExportMetadataScope::Topic);
    assert!(!request.special_topic());
    assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
}

#[test]
fn export_metadata_request_defaults_cluster_scope_to_all() {
    let request =
        ExportMetadataRequest::try_new(Some(" DefaultCluster ".to_string()), None, false, false, true).unwrap();

    assert_eq!(
        request.target(),
        &ExportMetadataTarget::Cluster("DefaultCluster".into())
    );
    assert_eq!(request.scope(), ExportMetadataScope::All);
    assert!(request.special_topic());
}

#[test]
fn export_metadata_request_rejects_invalid_targets() {
    assert!(ExportMetadataRequest::try_new(None, None, false, false, false).is_err());
    assert!(ExportMetadataRequest::try_new(
        Some("DefaultCluster".to_string()),
        Some("127.0.0.1:10911".to_string()),
        true,
        false,
        false
    )
    .is_err());
    assert!(ExportMetadataRequest::try_new(None, Some("127.0.0.1:10911".to_string()), false, false, false).is_err());
}

#[test]
fn export_pop_record_request_trims_broker_target() {
    let request = ExportPopRecordRequest::try_new(None, Some(" 127.0.0.1:10911 ".to_string()), true)
        .unwrap()
        .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".to_string()));

    assert_eq!(
        request.target(),
        &ExportPopRecordTarget::Broker("127.0.0.1:10911".into())
    );
    assert!(request.dry_run());
    assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
}

#[test]
fn export_pop_record_request_trims_cluster_target() {
    let request = ExportPopRecordRequest::try_new(Some(" DefaultCluster ".to_string()), None, false).unwrap();

    assert_eq!(
        request.target(),
        &ExportPopRecordTarget::Cluster("DefaultCluster".into())
    );
    assert!(!request.dry_run());
}

#[test]
fn export_pop_record_request_rejects_invalid_targets() {
    assert!(ExportPopRecordRequest::try_new(None, None, false).is_err());
    assert!(ExportPopRecordRequest::try_new(
        Some("DefaultCluster".to_string()),
        Some("127.0.0.1:10911".to_string()),
        false,
    )
    .is_err());
}

#[test]
fn export_metadata_in_rocksdb_request_trims_fields() {
    let request = ExportMetadataInRocksDbRequest::new(" /tmp/metadata ", " topics ", true);

    assert_eq!(request.path().to_string_lossy(), "/tmp/metadata");
    assert_eq!(request.config_type(), "topics");
    assert!(request.json_enable());
    assert_eq!(
        request.normalized_config_type(),
        Some(ExportMetadataInRocksDbConfigType::Topics)
    );
}

#[test]
fn export_metadata_in_rocksdb_request_recognizes_subscription_groups() {
    let request = ExportMetadataInRocksDbRequest::new("/tmp/metadata", "subscriptionGroups", false);

    assert_eq!(
        request.normalized_config_type(),
        Some(ExportMetadataInRocksDbConfigType::SubscriptionGroups)
    );
}

#[test]
fn export_metadata_in_rocksdb_invalid_path_returns_structured_result() {
    let request = ExportMetadataInRocksDbRequest::new("Z:/path/that/does/not/exist", "topics", false);

    let result = ExportService::export_metadata_in_rocksdb_by_request(&request).unwrap();

    assert!(matches!(result, ExportMetadataInRocksDbResult::InvalidPath));
}
