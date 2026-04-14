use std::collections::HashMap;

use cheetah_string::CheetahString;
use rocketmq_admin_core::core::export_data::filter_export_broker_properties;
use rocketmq_admin_core::core::export_data::ExportConfigsRequest;
use rocketmq_admin_core::core::export_data::ExportMetadataRequest;
use rocketmq_admin_core::core::export_data::ExportMetadataScope;
use rocketmq_admin_core::core::export_data::ExportMetadataTarget;

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
