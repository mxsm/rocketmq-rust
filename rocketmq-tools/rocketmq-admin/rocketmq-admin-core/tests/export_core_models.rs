use std::collections::HashMap;
use std::fs;
use std::time::SystemTime;

use cheetah_string::CheetahString;
use rocketmq_admin_core::core::export_data::filter_export_broker_properties;
use rocketmq_admin_core::core::export_data::ExportConfigsRequest;
use rocketmq_admin_core::core::export_data::ExportConfigsResult;
use rocketmq_admin_core::core::export_data::ExportFileOverwritePolicy;
use rocketmq_admin_core::core::export_data::ExportFileWriteRequest;
use rocketmq_admin_core::core::export_data::ExportMetadataInRocksDbConfigType;
use rocketmq_admin_core::core::export_data::ExportMetadataInRocksDbRequest;
use rocketmq_admin_core::core::export_data::ExportMetadataInRocksDbResult;
use rocketmq_admin_core::core::export_data::ExportMetadataRequest;
use rocketmq_admin_core::core::export_data::ExportMetadataScope;
use rocketmq_admin_core::core::export_data::ExportMetadataTarget;
use rocketmq_admin_core::core::export_data::ExportMetricsRequest;
use rocketmq_admin_core::core::export_data::ExportMetricsTotals;
use rocketmq_admin_core::core::export_data::ExportPopRecordRequest;
use rocketmq_admin_core::core::export_data::ExportPopRecordTarget;
use rocketmq_admin_core::core::export_data::ExportRocksDbConfigRpcRequest;
use rocketmq_admin_core::core::export_data::ExportRocksDbConfigRpcTarget;
use rocketmq_admin_core::core::export_data::ExportService;
use rocketmq_remoting::protocol::body::broker_stats_item::BrokerStatsItem;
use rocketmq_remoting::protocol::body::kv_table::KVTable;
use rocketmq_remoting::protocol::subscription::broker_stats_data::BrokerStatsData;

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
fn export_metrics_request_trims_cluster_and_namesrv() {
    let request = ExportMetricsRequest::try_new(" DefaultCluster ")
        .unwrap()
        .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".to_string()))
        .with_timeout_millis(5000);

    assert_eq!(request.cluster_name().as_str(), "DefaultCluster");
    assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
    assert_eq!(request.timeout_millis(), 5000);
}

#[test]
fn export_metrics_report_parses_runtime_quota_like_java() {
    let mut runtime_stats = KVTable::default();
    runtime_stats.table.insert(
        CheetahString::from_static_str("totalMemKBytes"),
        CheetahString::from_static_str("4096"),
    );
    runtime_stats.table.insert(
        CheetahString::from_static_str("commitLogDiskRatio"),
        CheetahString::from_static_str("0.25"),
    );
    runtime_stats.table.insert(
        CheetahString::from_static_str("consumeQueueDiskRatio"),
        CheetahString::from_static_str("0.10"),
    );
    runtime_stats.table.insert(
        CheetahString::from_static_str("putTps"),
        CheetahString::from_static_str("123.5 1 1"),
    );
    runtime_stats.table.insert(
        CheetahString::from_static_str("getTransferredTps"),
        CheetahString::from_static_str("45.25 1 1"),
    );
    runtime_stats.table.insert(
        CheetahString::from_static_str("msgPutTotalYesterdayMorning"),
        CheetahString::from_static_str("100"),
    );
    runtime_stats.table.insert(
        CheetahString::from_static_str("msgPutTotalTodayMorning"),
        CheetahString::from_static_str("160"),
    );
    runtime_stats.table.insert(
        CheetahString::from_static_str("msgGetTotalYesterdayMorning"),
        CheetahString::from_static_str("30"),
    );
    runtime_stats.table.insert(
        CheetahString::from_static_str("msgGetTotalTodayMorning"),
        CheetahString::from_static_str("80"),
    );
    runtime_stats.table.insert(
        CheetahString::from_static_str("putMessageAverageSize"),
        CheetahString::from_static_str("512"),
    );

    let broker_config = HashMap::from([(
        CheetahString::from_static_str("clientCallbackExecutorThreads"),
        CheetahString::from_static_str("8"),
    )]);
    let trans_stats = BrokerStatsData::new(
        BrokerStatsItem::new(10, 1.5, 0.0),
        BrokerStatsItem::default(),
        BrokerStatsItem::default(),
    );
    let schedule_stats = BrokerStatsData::new(
        BrokerStatsItem::new(20, 2.5, 0.0),
        BrokerStatsItem::default(),
        BrokerStatsItem::default(),
    );

    let report = ExportService::build_export_metrics_broker_report(
        &runtime_stats,
        &broker_config,
        12,
        4,
        Some(&trans_stats),
        Some(&schedule_stats),
        vec!["JAVA%V5_1_0".to_string()],
    );
    let mut totals = ExportMetricsTotals::default();
    totals.accumulate(&report.runtime_quota);

    assert_eq!(report.runtime_env.cpu_num.as_deref(), Some("8"));
    assert_eq!(report.runtime_env.total_mem_kbytes.as_deref(), Some("4096"));
    assert_eq!(report.runtime_quota.tps.normal_in_tps, 123.5);
    assert_eq!(report.runtime_quota.tps.normal_out_tps, 45.25);
    assert_eq!(report.runtime_quota.tps.trans_in_tps, 1.5);
    assert_eq!(report.runtime_quota.tps.schedule_in_tps, 2.5);
    assert_eq!(report.runtime_quota.one_day_num.normal_one_day_in_num, 60);
    assert_eq!(report.runtime_quota.one_day_num.normal_one_day_out_num, 50);
    assert_eq!(report.runtime_quota.one_day_num.trans_one_day_in_num, 10);
    assert_eq!(report.runtime_quota.one_day_num.schedule_one_day_in_num, 20);
    assert_eq!(report.runtime_quota.topic_size, 12);
    assert_eq!(report.runtime_quota.group_size, 4);
    assert_eq!(report.runtime_version.client_info, vec!["JAVA%V5_1_0"]);
    assert_eq!(totals.total_tps.total_normal_in_tps, 123.5);
    assert_eq!(totals.total_one_day_num.normal_one_day_out_num, 50);
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
fn export_metadata_in_rocksdb_request_recognizes_consumer_offsets() {
    let request = ExportMetadataInRocksDbRequest::new("/tmp/metadata", "consumerOffsets", true);

    assert_eq!(
        request.normalized_config_type(),
        Some(ExportMetadataInRocksDbConfigType::ConsumerOffsets)
    );
    assert_eq!(
        request.full_path().to_string_lossy().replace('\\', "/"),
        "/tmp/metadata/consumerOffsets"
    );
    assert_eq!(
        ExportMetadataInRocksDbConfigType::ConsumerOffsets.table_key(),
        "offsetTable"
    );
}

#[test]
fn export_rocksdb_config_rpc_request_trims_target_and_config_types() {
    let request = ExportRocksDbConfigRpcRequest::try_new(
        Some(" DefaultCluster ".to_string()),
        None,
        " topics;consumerOffsets; ",
        Some(5000),
    )
    .unwrap()
    .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".to_string()));

    assert_eq!(
        request.target(),
        &ExportRocksDbConfigRpcTarget::Cluster("DefaultCluster".into())
    );
    assert_eq!(
        request.config_types(),
        &[
            ExportMetadataInRocksDbConfigType::Topics,
            ExportMetadataInRocksDbConfigType::ConsumerOffsets
        ]
    );
    assert_eq!(
        request.config_type_names(),
        vec![CheetahString::from("topics"), CheetahString::from("consumerOffsets")]
    );
    assert_eq!(request.timeout_millis(), 5000);
    assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
}

#[test]
fn export_rocksdb_config_rpc_request_rejects_invalid_target_or_config_type() {
    assert!(ExportRocksDbConfigRpcRequest::try_new(None, None, "topics", None).is_err());
    assert!(ExportRocksDbConfigRpcRequest::try_new(
        Some("DefaultCluster".to_string()),
        Some("127.0.0.1:10911".to_string()),
        "topics",
        None
    )
    .is_err());
    assert!(ExportRocksDbConfigRpcRequest::try_new(None, Some("127.0.0.1:10911".to_string()), "bad", None).is_err());
}

#[test]
fn export_metadata_in_rocksdb_consumer_offsets_extracts_offset_table() {
    let entries = vec![rocketmq_admin_core::core::export_data::ExportMetadataInRocksDbEntry {
        key: "GroupA@TopicA".to_string(),
        value: r#"{"offsetTable":{"0":12,"1":34}}"#.to_string(),
    }];

    let entries =
        ExportService::convert_rocksdb_metadata_entries(ExportMetadataInRocksDbConfigType::ConsumerOffsets, entries)
            .unwrap();

    assert_eq!(entries[0].key, "GroupA@TopicA");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&entries[0].value).unwrap(),
        serde_json::json!({"0": 12, "1": 34})
    );
}

#[test]
fn export_metadata_in_rocksdb_invalid_path_returns_structured_result() {
    let request = ExportMetadataInRocksDbRequest::new("Z:/path/that/does/not/exist", "topics", false);

    let result = ExportService::export_metadata_in_rocksdb_by_request(&request).unwrap();

    assert!(matches!(result, ExportMetadataInRocksDbResult::InvalidPath));
}

#[test]
fn export_file_write_request_validates_path_and_overwrite_policy() {
    assert!(ExportFileWriteRequest::try_new(" ", ExportFileOverwritePolicy::CreateNew).is_err());

    let path = unique_temp_export_path("existing-no-overwrite");
    fs::write(&path, b"existing").unwrap();

    let request =
        ExportFileWriteRequest::try_new(path.to_string_lossy(), ExportFileOverwritePolicy::CreateNew).unwrap();
    let result = ExportService::write_json_export_file(&request, &sample_export_configs_result());
    assert!(result.is_err());

    let request =
        ExportFileWriteRequest::try_new(path.to_string_lossy(), ExportFileOverwritePolicy::Overwrite).unwrap();
    let result = ExportService::write_json_export_file(&request, &sample_export_configs_result()).unwrap();
    assert!(result.overwritten());
    assert!(result.bytes_written() > 0);

    let _ = fs::remove_file(path);
}

#[test]
fn export_file_writer_creates_pretty_json_output() {
    let path = unique_temp_export_path("new-output");
    let _ = fs::remove_file(&path);

    let request =
        ExportFileWriteRequest::try_new(path.to_string_lossy(), ExportFileOverwritePolicy::CreateNew).unwrap();
    let result = ExportService::write_json_export_file(&request, &sample_export_configs_result()).unwrap();

    assert_eq!(result.output_path(), path.as_path());
    assert!(!result.overwritten());
    assert!(result.bytes_written() > 0);

    let written = fs::read_to_string(&path).unwrap();
    assert!(written.contains("\"broker-a\""));
    assert!(written.contains('\n'));

    let _ = fs::remove_file(path);
}

fn sample_export_configs_result() -> ExportConfigsResult {
    ExportConfigsResult {
        name_servers: vec!["127.0.0.1:9876".to_string()],
        broker_configs: HashMap::from([(
            "broker-a".to_string(),
            HashMap::from([("brokerName".to_string(), "broker-a".to_string())]),
        )]),
        master_broker_size: 1,
        slave_broker_size: 0,
    }
}

fn unique_temp_export_path(label: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "rocketmq-admin-core-{label}-{}-{nanos}.json",
        std::process::id()
    ))
}
