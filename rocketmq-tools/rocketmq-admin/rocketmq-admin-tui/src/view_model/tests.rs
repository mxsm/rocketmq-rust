use rocketmq_admin_core::core::auth::AuthOperationResult;
use rocketmq_admin_core::core::broker::BrokerConsumeStatsResult;
use rocketmq_admin_core::core::broker::BrokerConsumeStatsRow;
use rocketmq_admin_core::core::broker::BrokerOperationFailure;
use rocketmq_admin_core::core::broker::BrokerOperationResult;
use rocketmq_admin_core::core::cluster::ClusterBaseInfoRow;
use rocketmq_admin_core::core::cluster::ClusterListMode;
use rocketmq_admin_core::core::cluster::ClusterListQueryResult;
use rocketmq_admin_core::core::cluster::ClusterSendMessageRtResult;
use rocketmq_admin_core::core::cluster::ClusterSendMessageRtRow;
use rocketmq_admin_core::core::connection::ConsumerConnectionQueryResult;
use rocketmq_admin_core::core::connection::ProducerConnectionQueryResult;
use rocketmq_admin_core::core::consumer::ConsumerGroupProgressResult;
use rocketmq_admin_core::core::consumer::ConsumerOperationFailure;
use rocketmq_admin_core::core::consumer::ConsumerOperationResult;
use rocketmq_admin_core::core::consumer::ConsumerProgressResult;
use rocketmq_admin_core::core::consumer::ConsumerProgressRow;
use rocketmq_admin_core::core::consumer::ConsumerRunningInfoItem;
use rocketmq_admin_core::core::consumer::ConsumerRunningInfoResult;
use rocketmq_admin_core::core::consumer::MonitoringEvent;
use rocketmq_admin_core::core::consumer::MonitoringResult;
use rocketmq_admin_core::core::controller::ControllerElectMasterResult;
use rocketmq_admin_core::core::export_data::ExportConfigsResult;
use rocketmq_admin_core::core::export_data::ExportMetadataInRocksDbConfigType;
use rocketmq_admin_core::core::export_data::ExportMetadataInRocksDbEntry;
use rocketmq_admin_core::core::export_data::ExportMetadataInRocksDbResult;
use rocketmq_admin_core::core::export_data::ExportMetricsBrokerReport;
use rocketmq_admin_core::core::export_data::ExportMetricsDiskRatio;
use rocketmq_admin_core::core::export_data::ExportMetricsOneDayNum;
use rocketmq_admin_core::core::export_data::ExportMetricsResult;
use rocketmq_admin_core::core::export_data::ExportMetricsRuntimeEnv;
use rocketmq_admin_core::core::export_data::ExportMetricsRuntimeQuota;
use rocketmq_admin_core::core::export_data::ExportMetricsRuntimeVersion;
use rocketmq_admin_core::core::export_data::ExportMetricsTotalTps;
use rocketmq_admin_core::core::export_data::ExportMetricsTotals;
use rocketmq_admin_core::core::export_data::ExportMetricsTps;
use rocketmq_admin_core::core::export_data::ExportPopRecordResult;
use rocketmq_admin_core::core::export_data::ExportPopRecordTargetResult;
use rocketmq_admin_core::core::export_data::ExportRocksDbConfigRpcResult;
use rocketmq_admin_core::core::export_data::ExportRocksDbConfigRpcTargetResult;
use rocketmq_admin_core::core::message::DecodeMessageIdEntry;
use rocketmq_admin_core::core::message::DecodeMessageIdOutcome;
use rocketmq_admin_core::core::message::DecodeMessageIdResult;
use rocketmq_admin_core::core::message::DirectConsumeMessageResult;
use rocketmq_admin_core::core::message::DirectConsumeMessageResultDetail;
use rocketmq_admin_core::core::message::DirectConsumeMessageStatus;
use rocketmq_admin_core::core::message::MessagePullEvent;
use rocketmq_admin_core::core::message::MessageTraceView;
use rocketmq_admin_core::core::message::MessageTrackEntry;
use rocketmq_admin_core::core::message::MessageTrackOutcome;
use rocketmq_admin_core::core::message::MessageTrackResult;
use rocketmq_admin_core::core::message::MessageTrackRow;
use rocketmq_admin_core::core::message::QueryMessageByIdEntry;
use rocketmq_admin_core::core::message::QueryMessageByIdOutcome;
use rocketmq_admin_core::core::message::QueryMessageByIdResult;
use rocketmq_admin_core::core::message::QueryMessageByKeyResult;
use rocketmq_admin_core::core::message::QueryMessageByKeyRow;
use rocketmq_admin_core::core::message::QueryMessageByOffsetResult;
use rocketmq_admin_core::core::message::QueryMessageByUniqueKeyResult;
use rocketmq_admin_core::core::message::UniqueKeyDirectStatus;
use rocketmq_admin_core::core::producer::ProducerInfoQueryResult;
use rocketmq_admin_core::core::producer::SendMessageResult;
use rocketmq_admin_core::core::producer::SendMessageResultRow;
use rocketmq_admin_core::core::producer::SendMessageStatusResult;
use rocketmq_admin_core::core::producer::SendMessageStatusRow;
use rocketmq_admin_core::core::queue::CheckRocksdbCqWriteProgressEntry;
use rocketmq_admin_core::core::queue::CheckRocksdbCqWriteProgressResult;
use rocketmq_admin_core::core::queue::QueryConsumeQueueResult;
use rocketmq_admin_core::core::queue::QueueOperationFailure;
use rocketmq_admin_core::core::stats::StatsAllQueryResult;
use rocketmq_admin_core::core::stats::StatsAllRow;
use rocketmq_admin_core::core::stats::StatsAllTopicFailure;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_remoting::protocol::body::broker_body::broker_member_group::BrokerMemberGroup;
use rocketmq_remoting::protocol::body::check_rocksdb_cqwrite_progress_response_body::CheckRocksdbCqWriteResult;
use rocketmq_remoting::protocol::body::connection::Connection;
use rocketmq_remoting::protocol::body::consume_queue_data::ConsumeQueueData;
use rocketmq_remoting::protocol::body::consume_status::ConsumeStatus;
use rocketmq_remoting::protocol::body::consumer_connection::ConsumerConnection;
use rocketmq_remoting::protocol::body::consumer_running_info::ConsumerRunningInfo;
use rocketmq_remoting::protocol::body::process_queue_info::ProcessQueueInfo;
use rocketmq_remoting::protocol::body::producer_connection::ProducerConnection;
use rocketmq_remoting::protocol::body::producer_info::ProducerInfo;
use rocketmq_remoting::protocol::body::producer_table_info::ProducerTableInfo;
use rocketmq_remoting::protocol::body::query_consume_queue_response_body::QueryConsumeQueueResponseBody;
use rocketmq_remoting::protocol::header::elect_master_response_header::ElectMasterResponseHeader;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_remoting::protocol::LanguageCode;
use rocketmq_rust::ArcMut;

use std::collections::BTreeMap;
use std::collections::HashMap;

use crate::admin_facade::MessagePullCapture;

use super::CommandResultViewModel;
use super::KeyValueViewModel;
use super::OperationSummaryViewModel;
use super::TableViewModel;

#[test]
fn result_view_models_render_text_bodies() {
    let table = CommandResultViewModel::Table(TableViewModel {
        title: "table".to_string(),
        headers: vec!["a".to_string(), "b".to_string()],
        rows: vec![vec!["1".to_string(), "2".to_string()]],
    });
    assert!(table.text_body().contains("a | b"));

    let key_value = CommandResultViewModel::KeyValue(KeyValueViewModel {
        title: "kv".to_string(),
        rows: vec![("key".to_string(), "value".to_string())],
    });
    assert_eq!(key_value.text_body(), "key: value");

    let summary = CommandResultViewModel::OperationSummary(OperationSummaryViewModel {
        title: "summary".to_string(),
        success_count: 1,
        failure_count: 1,
        targets: vec!["topic-a".to_string()],
        errors: vec!["failed".to_string()],
    });
    assert!(summary.text_body().contains("topic-a"));
}

#[test]
fn table_viewport_slices_columns_by_horizontal_scroll() {
    let table = TableViewModel {
        title: "table".to_string(),
        headers: vec![
            "cluster".to_string(),
            "broker".to_string(),
            "addr".to_string(),
            "status".to_string(),
        ],
        rows: vec![vec![
            "cluster-a".to_string(),
            "broker-a".to_string(),
            "127.0.0.1:10911".to_string(),
            "online".to_string(),
        ]],
    };

    let viewport = table.viewport(1, 18);

    assert!(viewport.hidden_left);
    assert!(viewport.hidden_right);
    assert_eq!(viewport.headers, vec!["broker"]);
    assert_eq!(viewport.rows[0], vec!["broker-a"]);
    assert_eq!(viewport.widths.len(), 1);
}

#[test]
fn table_viewport_clamps_out_of_range_scroll_to_last_column() {
    let table = TableViewModel {
        title: "table".to_string(),
        headers: vec!["a".to_string(), "b".to_string()],
        rows: vec![vec!["1".to_string(), "2".to_string()]],
    };

    let viewport = table.viewport(99, 80);

    assert!(viewport.hidden_left);
    assert!(!viewport.hidden_right);
    assert_eq!(viewport.headers, vec!["b"]);
    assert_eq!(viewport.rows[0], vec!["2"]);
}

#[test]
fn key_value_view_model_sorts_rows_by_key_then_value() {
    let key_value = KeyValueViewModel::sorted(
        "kv",
        vec![
            ("z".to_string(), "last".to_string()),
            ("a".to_string(), "second".to_string()),
            ("a".to_string(), "first".to_string()),
        ],
    );

    assert_eq!(
        key_value.rows,
        vec![
            ("a".to_string(), "first".to_string()),
            ("a".to_string(), "second".to_string()),
            ("z".to_string(), "last".to_string()),
        ]
    );
}

#[test]
fn phase_two_cluster_and_diagnostics_results_render_as_tables() {
    let cluster = CommandResultViewModel::cluster_list(
        "Cluster List",
        &ClusterListQueryResult {
            mode: ClusterListMode::Base,
            base_rows: vec![ClusterBaseInfoRow {
                cluster_name: "DefaultCluster".to_string(),
                broker_name: "broker-a".to_string(),
                broker_id: 0,
                broker_addr: "127.0.0.1:10911".into(),
                version: "5.0.0".to_string(),
                in_tps: "12.5".to_string(),
                out_tps: "9.5".to_string(),
                timer_progress: "0".to_string(),
                page_cache_lock_time_millis: "1".to_string(),
                hour: "10".to_string(),
                space: "1G".to_string(),
                broker_active: true,
            }],
            more_stats_rows: Vec::new(),
        },
    );
    assert_table(
        &cluster,
        &[
            "Cluster",
            "Broker",
            "Broker ID",
            "Address",
            "Version",
            "In TPS",
            "Out TPS",
            "Active",
        ],
        &[
            "DefaultCluster",
            "broker-a",
            "0",
            "127.0.0.1:10911",
            "5.0.0",
            "12.5",
            "9.5",
            "true",
        ],
    );

    let send_rt = CommandResultViewModel::cluster_send_message_rt(
        "Send RT",
        &ClusterSendMessageRtResult {
            missing_clusters: vec!["missing".to_string()],
            rows: vec![ClusterSendMessageRtRow {
                cluster_name: "DefaultCluster".to_string(),
                broker_name: "broker-a".to_string(),
                rt: 3.25,
                success_count: 4,
                fail_count: 1,
            }],
        },
    );
    assert_table(
        &send_rt,
        &["Cluster", "Broker", "RT", "Success", "Failed"],
        &["DefaultCluster", "broker-a", "3.25", "4", "1"],
    );

    let elect_master = CommandResultViewModel::controller_elect_master(
        "Elect Master",
        &ControllerElectMasterResult {
            response_header: ElectMasterResponseHeader {
                master_broker_id: Some(1),
                master_address: Some("127.0.0.1:10912".into()),
                master_epoch: Some(3),
                sync_state_set_epoch: Some(4),
            },
            broker_member_group: BrokerMemberGroup {
                cluster: "DefaultCluster".into(),
                broker_name: "broker-a".into(),
                broker_addrs: HashMap::from([(1, "127.0.0.1:10912".into()), (0, "127.0.0.1:10911".into())]),
            },
        },
    );
    assert_table(
        &elect_master,
        &["Kind", "Broker ID", "Address", "Value"],
        &["cluster", "", "", "DefaultCluster"],
    );
    assert!(elect_master.text_body().contains("broker | 0 | 127.0.0.1:10911"));

    let consume_stats = CommandResultViewModel::broker_consume_stats(
        "Broker Consume Stats",
        &BrokerConsumeStatsResult {
            broker_addr: Some("127.0.0.1:10911".into()),
            total_diff: 10,
            total_inflight_diff: 2,
            rows: vec![BrokerConsumeStatsRow {
                topic: "TopicA".into(),
                group: "GroupA".into(),
                broker_name: "broker-a".into(),
                queue_id: 1,
                broker_offset: 100,
                consumer_offset: 90,
                diff: 10,
                last_timestamp: 1234,
            }],
        },
    );
    assert_table(
        &consume_stats,
        &[
            "Topic",
            "Group",
            "Broker",
            "Queue ID",
            "Broker Offset",
            "Consumer Offset",
            "Diff",
            "Last Timestamp",
        ],
        &["TopicA", "GroupA", "broker-a", "1", "100", "90", "10", "1234"],
    );
}

#[test]
fn phase_two_consumer_and_message_results_render_as_tables() {
    let progress = CommandResultViewModel::consumer_progress(
        "Consumer Progress",
        &ConsumerProgressResult::Group(ConsumerGroupProgressResult {
            rows: vec![ConsumerProgressRow {
                topic: "TopicA".into(),
                broker_name: "broker-a".into(),
                queue_id: 1,
                broker_offset: 100,
                consumer_offset: 95,
                client_ip: Some("127.0.0.1".to_string()),
                diff: 5,
                inflight: 1,
                last_timestamp: 1234,
            }],
            consume_tps: 7.5,
            diff_total: 5,
            inflight_total: 1,
            show_client_ip: true,
        }),
    );
    assert_table(
        &progress,
        &[
            "Topic",
            "Broker",
            "Queue ID",
            "Broker Offset",
            "Consumer Offset",
            "Client IP",
            "Diff",
            "Inflight",
            "Last Timestamp",
        ],
        &["TopicA", "broker-a", "1", "100", "95", "127.0.0.1", "5", "1", "1234"],
    );

    let by_key = CommandResultViewModel::message_query_by_key(
        "Query Message By Key",
        &QueryMessageByKeyResult {
            rows: vec![QueryMessageByKeyRow {
                message_id: "msg-1".into(),
                queue_id: 2,
                queue_offset: 42,
                index_key: Some("order-1".to_string()),
            }],
        },
    );
    assert_table(
        &by_key,
        &["Message ID", "Queue ID", "Queue Offset", "Index Key"],
        &["msg-1", "2", "42", "order-1"],
    );

    let decoded = CommandResultViewModel::decoded_message_ids(
        "Decode Message ID",
        &DecodeMessageIdResult {
            entries: vec![
                DecodeMessageIdEntry {
                    message_id: "msg-1".into(),
                    outcome: DecodeMessageIdOutcome::Decoded {
                        broker_ip: "127.0.0.1".to_string(),
                        broker_port: 10911,
                        commit_log_offset: 64,
                        offset_hex: "40".to_string(),
                    },
                },
                DecodeMessageIdEntry {
                    message_id: "bad".into(),
                    outcome: DecodeMessageIdOutcome::Invalid {
                        error: "invalid id".to_string(),
                    },
                },
            ],
        },
    );
    assert_table(
        &decoded,
        &[
            "Message ID",
            "Status",
            "Broker",
            "CommitLog Offset",
            "Offset Hex",
            "Error",
        ],
        &["msg-1", "decoded", "127.0.0.1:10911", "64", "40", ""],
    );
    assert!(decoded.text_body().contains("invalid id"));

    let trace = CommandResultViewModel::message_trace(
        "Query Message Trace",
        &[MessageTraceView {
            msg_type: "Pub".to_string(),
            group_name: "GroupA".to_string(),
            client_host: "127.0.0.1".to_string(),
            time_stamp: 1234,
            cost_time: 8,
            status: "SUCCESS".to_string(),
            topic: Some("TopicA".to_string()),
            tags: Some("TagA".to_string()),
            keys: Some("order-1".to_string()),
            store_host: Some("127.0.0.1:10911".to_string()),
        }],
    );
    assert_table(
        &trace,
        &[
            "Type",
            "Group",
            "Client Host",
            "Timestamp",
            "Cost",
            "Status",
            "Topic",
            "Tags",
            "Keys",
            "Store Host",
        ],
        &[
            "Pub",
            "GroupA",
            "127.0.0.1",
            "1234",
            "8",
            "SUCCESS",
            "TopicA",
            "TagA",
            "order-1",
            "127.0.0.1:10911",
        ],
    );
}

#[test]
fn phase_two_message_detail_results_render_as_tables() {
    let message = sample_message("msg-1");

    let by_offset = CommandResultViewModel::message_query_by_offset(
        "Query Message By Offset",
        &QueryMessageByOffsetResult {
            pull_status: Default::default(),
            message: Some(ArcMut::new(message.clone())),
        },
    );
    assert_table(
        &by_offset,
        &[
            "Message ID",
            "Status",
            "Topic",
            "Broker",
            "Queue ID",
            "Queue Offset",
            "CommitLog Offset",
            "Tags",
            "Keys",
            "Born Host",
            "Born Time",
            "Store Host",
            "Store Time",
            "Body Bytes",
            "Body Preview",
            "Properties",
            "Note",
        ],
        &[
            "msg-1",
            "FOUND",
            "TopicA",
            "broker-a",
            "3",
            "42",
            "9001",
            "TagA",
            "KeyA",
            "127.0.0.1:10001",
            "1700000000000",
            "127.0.0.1:10911",
            "1700000001000",
            "7",
            "payload",
            "KEYS=KeyA; TAGS=TagA",
            "",
        ],
    );

    let by_id = CommandResultViewModel::message_query_by_id(
        "Query Message By ID",
        &QueryMessageByIdResult {
            entries: vec![
                QueryMessageByIdEntry {
                    message_id: "msg-1".into(),
                    outcome: QueryMessageByIdOutcome::Found {
                        message: Box::new(message.clone()),
                        broker_addr: "127.0.0.1:10911".to_string(),
                        query_time_ms: 7,
                    },
                },
                QueryMessageByIdEntry {
                    message_id: "missing".into(),
                    outcome: QueryMessageByIdOutcome::NotFound {
                        reason: "not found".to_string(),
                        query_time_ms: 3,
                    },
                },
            ],
        },
    );
    assert_table(
        &by_id,
        &[
            "Message ID",
            "Status",
            "Topic",
            "Broker",
            "Queue ID",
            "Queue Offset",
            "CommitLog Offset",
            "Tags",
            "Keys",
            "Born Host",
            "Born Time",
            "Store Host",
            "Store Time",
            "Body Bytes",
            "Body Preview",
            "Properties",
            "Note",
        ],
        &[
            "msg-1",
            "found",
            "TopicA",
            "broker-a",
            "3",
            "42",
            "9001",
            "TagA",
            "KeyA",
            "127.0.0.1:10001",
            "1700000000000",
            "127.0.0.1:10911",
            "1700000001000",
            "7",
            "payload",
            "KEYS=KeyA; TAGS=TagA",
            "broker_addr=127.0.0.1:10911; query_time_ms=7",
        ],
    );
    assert!(by_id.text_body().contains("not-found"));

    let unique = CommandResultViewModel::message_query_by_unique_key(
        "Query Message By Unique Key",
        &QueryMessageByUniqueKeyResult::DirectStatus(UniqueKeyDirectStatus::NotPushConsumer {
            client_id: "client-a".into(),
        }),
    );
    assert_table(
        &unique,
        &[
            "Message ID",
            "Status",
            "Topic",
            "Broker",
            "Queue ID",
            "Queue Offset",
            "CommitLog Offset",
            "Tags",
            "Keys",
            "Born Host",
            "Born Time",
            "Store Host",
            "Store Time",
            "Body Bytes",
            "Body Preview",
            "Properties",
            "Note",
        ],
        &[
            "",
            "direct-status",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "client_id=client-a; not a push consumer",
        ],
    );
}

#[test]
fn phase_two_queue_results_render_as_tables() {
    let consume_queue = CommandResultViewModel::consume_queue(
        "Query Consume Queue",
        &QueryConsumeQueueResult {
            broker_addr: "127.0.0.1:10911".into(),
            response_body: QueryConsumeQueueResponseBody {
                subscription_data: None,
                filter_data: Some("filter-data".into()),
                queue_data: Some(vec![ConsumeQueueData {
                    physic_offset: 123,
                    physic_size: 4,
                    tags_code: 7,
                    extend_data_json: Some("{\"a\":1}".into()),
                    bit_map: None,
                    eval: true,
                    msg: Some("hello".into()),
                }]),
                max_queue_index: 10,
                min_queue_index: 1,
            },
        },
    );
    assert_table(
        &consume_queue,
        &[
            "Broker Addr",
            "Physical Offset",
            "Physical Size",
            "Tags Code",
            "Ext",
            "Bitmap",
            "Eval",
            "Message",
        ],
        &["127.0.0.1:10911", "123", "4", "7", "{\"a\":1}", "", "true", "hello"],
    );

    let progress = CommandResultViewModel::rocksdb_cq_progress(
        "RocksDB CQ Write Progress",
        &CheckRocksdbCqWriteProgressResult {
            cluster_found: true,
            entries: vec![CheckRocksdbCqWriteProgressEntry {
                broker_name: "broker-a".into(),
                broker_addr: "127.0.0.1:10911".into(),
                result: CheckRocksdbCqWriteResult {
                    check_result: Some("ok".into()),
                    check_status: 0,
                },
            }],
            failures: vec![QueueOperationFailure {
                broker_name: "broker-b".into(),
                broker_addr: "127.0.0.1:10912".into(),
                error: "timeout".to_string(),
            }],
        },
    );
    assert_table(
        &progress,
        &["Broker", "Address", "Status", "Check Result", "Error"],
        &["broker-a", "127.0.0.1:10911", "CheckOk", "ok", ""],
    );
    assert!(progress.text_body().contains("timeout"));
}

#[test]
fn phase_two_connection_and_running_info_results_render_as_tables() {
    let mut connection = Connection::new();
    connection.set_client_id("client-a".into());
    connection.set_client_addr("127.0.0.1:10001".into());
    connection.set_language(LanguageCode::RUST);
    connection.set_version(501);

    let mut consumer_connection = ConsumerConnection::new();
    consumer_connection.insert_connection(connection.clone());
    consumer_connection.set_consume_type(ConsumeType::ConsumePassively);
    consumer_connection.set_message_model(MessageModel::Clustering);
    consumer_connection
        .get_subscription_table_mut()
        .insert("TopicA".into(), subscription("TopicA", "TagA"));

    let result = CommandResultViewModel::consumer_connection(
        "Consumer Connection",
        &ConsumerConnectionQueryResult {
            connection: consumer_connection,
        },
    );
    assert_table(
        &result,
        &[
            "Kind",
            "Client ID",
            "Address",
            "Language",
            "Version",
            "Topic",
            "Sub Expression",
            "Consume Type",
            "Message Model",
        ],
        &[
            "Connection",
            "client-a",
            "127.0.0.1:10001",
            "RUST",
            "501",
            "",
            "",
            "ConsumePassively",
            "Clustering",
        ],
    );
    assert!(result.text_body().contains("Subscription"));

    let mut producer_connection = ProducerConnection::new();
    producer_connection.connection_set_mut().insert(connection);
    let result = CommandResultViewModel::producer_connection(
        "Producer Connection",
        &ProducerConnectionQueryResult {
            connection: producer_connection,
        },
    );
    assert_table(
        &result,
        &["Client ID", "Address", "Language", "Version"],
        &["client-a", "127.0.0.1:10001", "RUST", "501"],
    );

    let mut running_info = ConsumerRunningInfo::new();
    running_info.consume_type = ConsumeType::ConsumePassively;
    running_info.consume_orderly = true;
    running_info.prop_consumer_start_timestamp = 1234;
    running_info.subscription_set.insert(subscription("TopicA", "TagA"));
    running_info.mq_table.insert(
        MessageQueue::from_parts("TopicA", "broker-a", 1),
        ProcessQueueInfo {
            commit_offset: 100,
            cached_msg_min_offset: 90,
            cached_msg_max_offset: 110,
            cached_msg_count: 3,
            cached_msg_size_in_mib: 1,
            transaction_msg_min_offset: 0,
            transaction_msg_max_offset: 0,
            transaction_msg_count: 0,
            locked: true,
            try_unlock_times: 0,
            last_lock_timestamp: 1,
            droped: false,
            last_pull_timestamp: 2,
            last_consume_timestamp: 3,
        },
    );
    running_info.status_table.insert(
        "TopicA".to_string(),
        ConsumeStatus {
            pull_rt: 1.0,
            pull_tps: 2.0,
            consume_rt: 3.0,
            consume_ok_tps: 4.0,
            consume_failed_tps: 5.0,
            consume_failed_msgs: 6,
        },
    );
    running_info
        .user_consumer_info
        .insert("key".to_string(), "value".to_string());

    let result = CommandResultViewModel::consumer_running_info(
        "Consumer Running Info",
        &ConsumerRunningInfoResult {
            items: vec![ConsumerRunningInfoItem {
                client_id: "client-a".into(),
                version: 501,
                running_info,
            }],
            subscription_consistent: Some(true),
            process_queue_analysis: vec!["all good".to_string()],
        },
    );
    assert_table(
        &result,
        &[
            "Client ID",
            "Version",
            "Consume Type",
            "Orderly",
            "Start Timestamp",
            "Subscriptions",
            "Queues",
            "POP Queues",
            "Status Rows",
            "User Info",
            "Subscription Consistent",
            "Analysis",
        ],
        &[
            "client-a",
            "501",
            "ConsumePassively",
            "true",
            "1234",
            "1",
            "1",
            "0",
            "1",
            "1",
            "true",
            "all good",
        ],
    );
}

#[test]
fn phase_two_producer_info_and_stats_results_render_as_tables() {
    let mut producer_groups = HashMap::new();
    producer_groups.insert(
        "ProducerGroupA".to_string(),
        vec![ProducerInfo::new(
            "producer-client-a",
            "127.0.0.1",
            LanguageCode::RUST,
            501,
            1234,
        )],
    );

    let result = CommandResultViewModel::producer_info(
        "Producer Info",
        &ProducerInfoQueryResult {
            producer_table_info: ProducerTableInfo::new(producer_groups),
        },
    );
    assert_table(
        &result,
        &["Group", "Client ID", "Remote IP", "Language", "Version", "Last Update"],
        &[
            "ProducerGroupA",
            "producer-client-a",
            "127.0.0.1",
            "RUST",
            "501",
            "1234",
        ],
    );

    let result = CommandResultViewModel::stats_all(
        "Stats All",
        &StatsAllQueryResult {
            rows: vec![StatsAllRow {
                topic: "TopicA".into(),
                consumer_group: Some("GroupA".into()),
                accumulation: 10,
                in_tps: 1.5,
                out_tps: Some(1.25),
                in_msg_count_24h: 100,
                out_msg_count_24h: Some(90),
            }],
            failures: vec![StatsAllTopicFailure {
                topic: "TopicB".into(),
                error: "route missing".to_string(),
            }],
        },
    );
    assert_table(
        &result,
        &[
            "Topic",
            "Consumer Group",
            "Accumulation",
            "In TPS",
            "Out TPS",
            "In 24h",
            "Out 24h",
            "Error",
        ],
        &["TopicA", "GroupA", "10", "1.5", "1.25", "100", "90", ""],
    );
    assert!(result.text_body().contains("route missing"));
}

#[test]
fn phase_two_producer_diagnostics_results_render_as_tables() {
    let send_message = CommandResultViewModel::send_message(
        "Send Message",
        &SendMessageResult {
            row: SendMessageResultRow {
                broker_name: "broker-a".to_string(),
                queue_id: "1".to_string(),
                send_status: "SEND_OK".to_string(),
                msg_id: "MSGID".to_string(),
            },
        },
    );
    assert_table(
        &send_message,
        &["Broker", "Queue ID", "Status", "Message ID"],
        &["broker-a", "1", "SEND_OK", "MSGID"],
    );

    let result = CommandResultViewModel::send_message_status(
        "Send Message Status",
        &SendMessageStatusResult {
            rows: vec![SendMessageStatusRow {
                rt_millis: 12,
                send_result: "SEND_OK".to_string(),
            }],
        },
    );

    assert_table(&result, &["Attempt", "RT", "Result"], &["1", "12", "SEND_OK"]);
}

#[test]
fn phase_three_operation_results_render_partial_failures_as_summary() {
    let auth = CommandResultViewModel::auth_operation(
        "Create User",
        &AuthOperationResult {
            broker_addrs: vec!["127.0.0.1:10911".into()],
        },
    );
    assert_summary(&auth, 1, 0, &["127.0.0.1:10911"], &[]);

    let broker = CommandResultViewModel::broker_operation(
        "Cold Data Flow Control",
        &BrokerOperationResult {
            broker_addrs: vec!["127.0.0.1:10911".into()],
            failures: vec![BrokerOperationFailure {
                broker_addr: "127.0.0.1:10912".into(),
                error: "timeout".to_string(),
            }],
        },
    );
    assert_summary(&broker, 1, 1, &["127.0.0.1:10911"], &["127.0.0.1:10912: timeout"]);

    let consumer = CommandResultViewModel::consumer_operation(
        "Update Subscription Group",
        &ConsumerOperationResult {
            broker_addrs: vec!["127.0.0.1:10911".into()],
            failures: vec![ConsumerOperationFailure {
                broker_addr: "127.0.0.1:10912".into(),
                error: "rejected".to_string(),
            }],
            warnings: vec!["retry topic cleanup failed".to_string()],
        },
    );
    assert_summary(
        &consumer,
        1,
        1,
        &["127.0.0.1:10911", "warning: retry topic cleanup failed"],
        &["127.0.0.1:10912: rejected"],
    );
}

#[test]
fn phase_four_export_and_message_workflow_results_render_structured_output() {
    let export_configs = CommandResultViewModel::export_configs(
        "Export Configs",
        &ExportConfigsResult {
            name_servers: vec!["127.0.0.1:9876".to_string()],
            broker_configs: HashMap::from([(
                "broker-a".to_string(),
                HashMap::from([("brokerRole".to_string(), "ASYNC_MASTER".to_string())]),
            )]),
            master_broker_size: 1,
            slave_broker_size: 0,
        },
    );
    assert_table(
        &export_configs,
        &["Type", "Target", "Key", "Value"],
        &["name-server", "", "address", "127.0.0.1:9876"],
    );

    let export_metrics = CommandResultViewModel::export_metrics(
        "Export Metrics",
        &ExportMetricsResult {
            evaluate_report: BTreeMap::from([(
                "broker-a".to_string(),
                ExportMetricsBrokerReport {
                    runtime_env: ExportMetricsRuntimeEnv {
                        cpu_num: Some("8".to_string()),
                        total_mem_kbytes: Some("4096".to_string()),
                    },
                    runtime_quota: ExportMetricsRuntimeQuota {
                        disk_ratio: ExportMetricsDiskRatio {
                            commit_log_disk_ratio: Some("0.25".to_string()),
                            consume_queue_disk_ratio: Some("0.10".to_string()),
                        },
                        tps: ExportMetricsTps {
                            normal_in_tps: 123.5,
                            normal_out_tps: 45.25,
                            trans_in_tps: 1.5,
                            schedule_in_tps: 2.5,
                        },
                        one_day_num: ExportMetricsOneDayNum {
                            normal_one_day_in_num: 60,
                            normal_one_day_out_num: 50,
                            trans_one_day_in_num: 10,
                            schedule_one_day_in_num: 20,
                        },
                        message_average_size: Some("512".to_string()),
                        topic_size: 12,
                        group_size: 4,
                    },
                    runtime_version: ExportMetricsRuntimeVersion {
                        rocketmq_version: "V5_1_0".to_string(),
                        client_info: vec!["JAVA%V5_1_0".to_string()],
                    },
                },
            )]),
            total_data: ExportMetricsTotals {
                total_tps: ExportMetricsTotalTps {
                    total_normal_in_tps: 123.5,
                    total_normal_out_tps: 45.25,
                    total_trans_in_tps: 1.5,
                    total_schedule_in_tps: 2.5,
                },
                total_one_day_num: ExportMetricsOneDayNum {
                    normal_one_day_in_num: 60,
                    normal_one_day_out_num: 50,
                    trans_one_day_in_num: 10,
                    schedule_one_day_in_num: 20,
                },
            },
        },
    );
    assert_table(
        &export_metrics,
        &[
            "Broker",
            "CPU",
            "Memory KB",
            "CommitLog",
            "ConsumeQueue",
            "Normal In TPS",
            "Normal Out TPS",
            "Trans In TPS",
            "Schedule In TPS",
            "Normal 24h In",
            "Normal 24h Out",
            "Trans 24h In",
            "Schedule 24h In",
            "Avg Size",
            "Topics",
            "Groups",
            "Version",
            "Clients",
        ],
        &[
            "broker-a",
            "8",
            "4096",
            "0.25",
            "0.10",
            "123.50",
            "45.25",
            "1.50",
            "2.50",
            "60",
            "50",
            "10",
            "20",
            "512",
            "12",
            "4",
            "V5_1_0",
            "JAVA%V5_1_0",
        ],
    );

    let rocksdb = CommandResultViewModel::export_metadata_rocksdb(
        "Export Metadata In RocksDB",
        &ExportMetadataInRocksDbResult::Data {
            config_type: ExportMetadataInRocksDbConfigType::Topics,
            json_enable: true,
            entries: vec![ExportMetadataInRocksDbEntry {
                key: "TopicA".to_string(),
                value: "{\"readQueueNums\":4}".to_string(),
            }],
        },
    );
    assert_table(
        &rocksdb,
        &["Config Type", "Format", "Key", "Value"],
        &["Topics", "json", "TopicA", "{\"readQueueNums\":4}"],
    );

    let consumer_offsets = CommandResultViewModel::export_metadata_rocksdb(
        "Export Metadata In RocksDB",
        &ExportMetadataInRocksDbResult::Data {
            config_type: ExportMetadataInRocksDbConfigType::ConsumerOffsets,
            json_enable: true,
            entries: vec![ExportMetadataInRocksDbEntry {
                key: "GroupA@TopicA".to_string(),
                value: "{\"0\":12}".to_string(),
            }],
        },
    );
    assert_table(
        &consumer_offsets,
        &["Config Type", "Format", "Key", "Value"],
        &["ConsumerOffsets", "json", "GroupA@TopicA", "{\"0\":12}"],
    );

    let rocksdb_rpc = CommandResultViewModel::export_rocksdb_config_rpc(
        "Export RocksDB Config RPC",
        &ExportRocksDbConfigRpcResult {
            targets: vec![
                ExportRocksDbConfigRpcTargetResult {
                    broker_name: "broker-a".to_string(),
                    broker_addr: "127.0.0.1:10911".to_string(),
                    config_types: vec![ExportMetadataInRocksDbConfigType::Topics],
                    exported: true,
                    error: None,
                },
                ExportRocksDbConfigRpcTargetResult {
                    broker_name: "broker-b".to_string(),
                    broker_addr: "127.0.0.1:10912".to_string(),
                    config_types: vec![ExportMetadataInRocksDbConfigType::ConsumerOffsets],
                    exported: false,
                    error: Some("timeout".to_string()),
                },
            ],
        },
    );
    assert_summary(
        &rocksdb_rpc,
        1,
        1,
        &["broker-a 127.0.0.1:10911 [Topics]"],
        &["broker-b 127.0.0.1:10912: timeout"],
    );

    let pop_records = CommandResultViewModel::export_pop_records(
        "Export Pop Records",
        &ExportPopRecordResult {
            targets: vec![
                ExportPopRecordTargetResult {
                    broker_name: "broker-a".to_string(),
                    broker_addr: "127.0.0.1:10911".to_string(),
                    dry_run: false,
                    exported: true,
                    error: None,
                },
                ExportPopRecordTargetResult {
                    broker_name: "broker-b".to_string(),
                    broker_addr: "127.0.0.1:10912".to_string(),
                    dry_run: false,
                    exported: false,
                    error: Some("timeout".to_string()),
                },
            ],
        },
    );
    assert_summary(
        &pop_records,
        1,
        1,
        &["broker-a 127.0.0.1:10911"],
        &["broker-b 127.0.0.1:10912: timeout"],
    );

    let pull_events = CommandResultViewModel::message_pull_events(
        "Print Messages",
        &MessagePullCapture {
            events: vec![
                MessagePullEvent::QueueRange {
                    mq: MessageQueue::from_parts("TopicA", "broker-a", 1),
                    min_offset: 10,
                    max_offset: 20,
                },
                MessagePullEvent::CountLimit {
                    message_number: 10,
                    queue_id: Some(1),
                },
            ],
            event_limit: 2,
            truncated: true,
        },
    );
    assert_table(
        &pull_events,
        &["Event", "Topic", "Broker", "Queue", "Offset", "Status", "Detail"],
        &["queue-range", "TopicA", "broker-a", "1", "10..20", "", ""],
    );
    assert!(pull_events.text_body().contains("truncated after 2 events"));
}

#[test]
fn phase_five_monitoring_events_render_as_table() {
    let result = CommandResultViewModel::consumer_monitoring(
        "Start Monitoring",
        &MonitoringResult {
            events: vec![
                MonitoringEvent::BeginRound {
                    round: 1,
                    timestamp_millis: 1234,
                },
                MonitoringEvent::UndoneMsgs {
                    round: 1,
                    consumer_group: "GroupA".to_string(),
                    topic: "TopicA".to_string(),
                    undone_msgs_total: 42,
                    undone_msgs_single_mq: 40,
                    undone_msgs_delay_time_millis: None,
                },
                MonitoringEvent::ConsumerRunningInfo {
                    round: 1,
                    consumer_group: "GroupA".to_string(),
                    client_count: 2,
                    subscription_consistent: Some(false),
                    process_queue_analysis: vec!["client-a stalled".to_string()],
                },
            ],
            rounds_completed: 1,
            groups_scanned: 1,
            error_count: 0,
            truncated: true,
        },
    );

    assert_table(
        &result,
        &["Event", "Round", "Group", "Topic", "Metric", "Detail"],
        &["begin-round", "1", "", "", "timestamp_millis", "1234"],
    );
    assert!(result.text_body().contains("undone-msgs"));
    assert!(result.text_body().contains("client-a stalled"));
    assert!(result.text_body().contains("max events reached"));
}

#[test]
fn phase_five_direct_consume_result_renders_as_table() {
    let result = CommandResultViewModel::direct_consume_message(
        "Direct Consume Message",
        &DirectConsumeMessageResult {
            topic: "TopicA".into(),
            msg_id: "MSGID".into(),
            consumer_group: "GroupA".into(),
            client_id: "client-a".into(),
            status: DirectConsumeMessageStatus::Consumed(DirectConsumeMessageResultDetail {
                order: false,
                auto_commit: true,
                consume_result: Some("CR_SUCCESS".to_string()),
                remark: Some("ok".to_string()),
                spent_time_millis: 12,
            }),
        },
    );

    assert_table(
        &result,
        &[
            "Consumer Group",
            "Client ID",
            "Topic",
            "Message ID",
            "Status",
            "Consume Result",
            "Order",
            "Auto Commit",
            "Spent Time",
            "Remark",
        ],
        &[
            "GroupA",
            "client-a",
            "TopicA",
            "MSGID",
            "consumed",
            "CR_SUCCESS",
            "false",
            "true",
            "12",
            "ok",
        ],
    );
}

#[test]
fn phase_five_message_track_result_renders_as_table() {
    let result = CommandResultViewModel::message_track(
        "Track Message By ID",
        &MessageTrackResult {
            entries: vec![MessageTrackEntry {
                message_id: "MSGID".into(),
                outcome: MessageTrackOutcome::Found {
                    broker_addr: "127.0.0.1:10911".to_string(),
                    query_time_ms: 7,
                    tracks: vec![MessageTrackRow {
                        consumer_group: "GroupA".to_string(),
                        track_type: Some("CONSUMED".to_string()),
                        exception_desc: String::new(),
                    }],
                },
            }],
        },
    );

    assert_table(
        &result,
        &[
            "Message ID",
            "Status",
            "Consumer Group",
            "Track Type",
            "Exception",
            "Broker",
            "Query Time",
            "Note",
        ],
        &["MSGID", "found", "GroupA", "CONSUMED", "", "127.0.0.1:10911", "7", ""],
    );
}

fn assert_table(result: &CommandResultViewModel, headers: &[&str], first_row: &[&str]) {
    let CommandResultViewModel::Table(table) = result else {
        panic!("expected table result, got {result:?}");
    };
    assert_eq!(
        table.headers,
        headers.iter().map(|header| (*header).to_string()).collect::<Vec<_>>()
    );
    assert_eq!(
        table.rows.first(),
        Some(&first_row.iter().map(|cell| (*cell).to_string()).collect::<Vec<_>>())
    );
}

fn assert_summary(
    result: &CommandResultViewModel,
    success_count: usize,
    failure_count: usize,
    targets: &[&str],
    errors: &[&str],
) {
    let CommandResultViewModel::OperationSummary(summary) = result else {
        panic!("expected operation summary result, got {result:?}");
    };
    assert_eq!(summary.success_count, success_count);
    assert_eq!(summary.failure_count, failure_count);
    assert_eq!(
        summary.targets,
        targets.iter().map(|value| (*value).to_string()).collect::<Vec<_>>()
    );
    assert_eq!(
        summary.errors,
        errors.iter().map(|value| (*value).to_string()).collect::<Vec<_>>()
    );
}

fn sample_message(message_id: &str) -> MessageExt {
    let message = Message::builder()
        .topic("TopicA")
        .body_slice(b"payload")
        .build()
        .expect("sample message should build");
    let mut message_ext = MessageExt::default();
    message_ext.set_message_inner(message);
    message_ext.set_msg_id(message_id.into());
    message_ext.set_broker_name("broker-a".into());
    message_ext.set_queue_id(3);
    message_ext.set_queue_offset(42);
    message_ext.set_commit_log_offset(9001);
    message_ext.set_born_host("127.0.0.1:10001".parse().expect("born host"));
    message_ext.set_store_host("127.0.0.1:10911".parse().expect("store host"));
    message_ext.set_born_timestamp(1_700_000_000_000);
    message_ext.set_store_timestamp(1_700_000_001_000);
    message_ext.put_property(MessageConst::PROPERTY_TAGS.into(), "TagA".into());
    message_ext.put_property(MessageConst::PROPERTY_KEYS.into(), "KeyA".into());
    message_ext
}

fn subscription(topic: &str, sub_string: &str) -> SubscriptionData {
    SubscriptionData {
        topic: topic.into(),
        sub_string: sub_string.into(),
        ..Default::default()
    }
}
