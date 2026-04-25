use std::fmt::Debug;

use rocketmq_admin_core::core::auth::AuthOperationResult;
use rocketmq_admin_core::core::auth::CopyAclResult;
use rocketmq_admin_core::core::auth::CopyUsersResult;
use rocketmq_admin_core::core::broker::BrokerBooleanOperationResult;
use rocketmq_admin_core::core::broker::BrokerConsumeStatsResult;
use rocketmq_admin_core::core::broker::BrokerOperationResult;
use rocketmq_admin_core::core::broker::CleanExpiredConsumeQueueReport;
use rocketmq_admin_core::core::broker::CommitLogReadAheadResult;
use rocketmq_admin_core::core::cluster::ClusterListMode;
use rocketmq_admin_core::core::cluster::ClusterListQueryResult;
use rocketmq_admin_core::core::cluster::ClusterSendMessageRtResult;
use rocketmq_admin_core::core::connection::ConsumerConnectionQueryResult;
use rocketmq_admin_core::core::connection::ProducerConnectionQueryResult;
use rocketmq_admin_core::core::consumer::ConsumerOperationResult;
use rocketmq_admin_core::core::consumer::ConsumerProgressResult;
use rocketmq_admin_core::core::consumer::ConsumerRunningInfoResult;
use rocketmq_admin_core::core::consumer::MonitoringEvent;
use rocketmq_admin_core::core::consumer::MonitoringResult;
use rocketmq_admin_core::core::controller::ControllerElectMasterResult;
use rocketmq_admin_core::core::export_data::ExportConfigsResult;
use rocketmq_admin_core::core::export_data::ExportFileWriteResult;
use rocketmq_admin_core::core::export_data::ExportMetadataInRocksDbConfigType;
use rocketmq_admin_core::core::export_data::ExportMetadataInRocksDbResult;
use rocketmq_admin_core::core::export_data::ExportMetadataResult;
use rocketmq_admin_core::core::export_data::ExportMetadataScope;
use rocketmq_admin_core::core::export_data::ExportMetricsResult;
use rocketmq_admin_core::core::export_data::ExportPopRecordResult;
use rocketmq_admin_core::core::export_data::ExportRocksDbConfigRpcResult;
use rocketmq_admin_core::core::lite::TriggerLiteDispatchResult;
use rocketmq_admin_core::core::message::DecodeMessageIdOutcome;
use rocketmq_admin_core::core::message::DecodeMessageIdResult;
use rocketmq_admin_core::core::message::DirectConsumeMessageResult;
use rocketmq_admin_core::core::message::DirectConsumeMessageStatus;
use rocketmq_admin_core::core::message::DumpCompactionLogResult;
use rocketmq_admin_core::core::message::MessagePullEvent;
use rocketmq_admin_core::core::message::MessageTraceView;
use rocketmq_admin_core::core::message::MessageTrackOutcome;
use rocketmq_admin_core::core::message::MessageTrackResult;
use rocketmq_admin_core::core::message::QueryMessageByIdOutcome;
use rocketmq_admin_core::core::message::QueryMessageByIdResult;
use rocketmq_admin_core::core::message::QueryMessageByKeyResult;
use rocketmq_admin_core::core::message::QueryMessageByOffsetResult;
use rocketmq_admin_core::core::message::QueryMessageByUniqueKeyResult;
use rocketmq_admin_core::core::message::UniqueKeyDirectStatus;
use rocketmq_admin_core::core::producer::CheckMessageSendRtResult;
use rocketmq_admin_core::core::producer::ProducerInfoQueryResult;
use rocketmq_admin_core::core::producer::SendMessageResult;
use rocketmq_admin_core::core::producer::SendMessageStatusResult;
use rocketmq_admin_core::core::queue::CheckRocksdbCqWriteProgressResult;
use rocketmq_admin_core::core::queue::QueryConsumeQueueResult;
use rocketmq_admin_core::core::stats::StatsAllQueryResult;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::MessageConst;
use serde::Serialize;

use super::CommandResultViewModel;
use super::KeyValueViewModel;
use super::OperationSummaryViewModel;
use super::TableViewModel;
use crate::admin_facade::MessagePullCapture;

impl CommandResultViewModel {
    pub fn from_debug(title: impl Into<String>, value: &impl Debug) -> Self {
        Self::Text {
            title: title.into(),
            body: format!("{value:#?}"),
        }
    }

    pub fn from_serializable(title: impl Into<String>, value: &impl Serialize) -> Self {
        let body = serde_json::to_string_pretty(value)
            .unwrap_or_else(|error| format!("Failed to serialize result as JSON: {error}"));
        Self::Json {
            title: title.into(),
            body,
        }
    }

    pub fn operation_success(title: impl Into<String>, targets: Vec<String>) -> Self {
        Self::OperationSummary(OperationSummaryViewModel {
            title: title.into(),
            success_count: targets.len(),
            failure_count: 0,
            targets,
            errors: Vec::new(),
        })
    }

    pub fn operation_summary(
        title: impl Into<String>,
        success_count: usize,
        failure_count: usize,
        targets: Vec<String>,
        errors: Vec<String>,
    ) -> Self {
        Self::OperationSummary(OperationSummaryViewModel {
            title: title.into(),
            success_count,
            failure_count,
            targets,
            errors,
        })
    }

    pub fn export_file_written(title: impl Into<String>, result: &ExportFileWriteResult) -> Self {
        Self::operation_success(
            title,
            vec![format!(
                "{} ({} bytes, overwritten: {})",
                result.output_path().display(),
                result.bytes_written(),
                result.overwritten()
            )],
        )
    }

    pub fn error(title: impl Into<String>, body: impl Into<String>) -> Self {
        Self::Text {
            title: title.into(),
            body: body.into(),
        }
    }

    pub fn key_value_sorted(title: impl Into<String>, rows: Vec<(String, String)>) -> Self {
        Self::KeyValue(KeyValueViewModel::sorted(title, rows))
    }

    pub fn auth_operation(title: impl Into<String>, result: &AuthOperationResult) -> Self {
        let targets = result.broker_addrs.iter().map(ToString::to_string).collect::<Vec<_>>();
        Self::operation_summary(title, targets.len(), 0, targets, Vec::new())
    }

    pub fn auth_copy_users(title: impl Into<String>, result: &CopyUsersResult) -> Self {
        let mut targets = result
            .copied_usernames
            .iter()
            .map(|username| format!("copied user: {username}"))
            .collect::<Vec<_>>();
        targets.extend(
            result
                .skipped_usernames
                .iter()
                .map(|username| format!("skipped user: {username}")),
        );
        let errors = result
            .failures
            .iter()
            .map(|failure| format!("{}: {}", failure.broker_addr, failure.error))
            .collect::<Vec<_>>();
        Self::operation_summary(
            title,
            result.copied_usernames.len(),
            result.failures.len(),
            targets,
            errors,
        )
    }

    pub fn auth_copy_acl(title: impl Into<String>, result: &CopyAclResult) -> Self {
        let mut targets = result
            .copied_subjects
            .iter()
            .map(|subject| format!("copied subject: {subject}"))
            .collect::<Vec<_>>();
        targets.extend(
            result
                .skipped_subjects
                .iter()
                .map(|subject| format!("skipped subject: {subject}")),
        );
        let errors = result
            .failures
            .iter()
            .map(|failure| format!("{}: {}", failure.broker_addr, failure.error))
            .collect::<Vec<_>>();
        Self::operation_summary(
            title,
            result.copied_subjects.len(),
            result.failures.len(),
            targets,
            errors,
        )
    }

    pub fn broker_operation(title: impl Into<String>, result: &BrokerOperationResult) -> Self {
        let targets = result.broker_addrs.iter().map(ToString::to_string).collect::<Vec<_>>();
        let errors = result
            .failures
            .iter()
            .map(|failure| format!("{}: {}", failure.broker_addr, failure.error))
            .collect::<Vec<_>>();
        Self::operation_summary(title, result.broker_addrs.len(), result.failures.len(), targets, errors)
    }

    pub fn broker_boolean_operation(
        title: impl Into<String>,
        target: impl Into<String>,
        result: &BrokerBooleanOperationResult,
    ) -> Self {
        let target = target.into();
        let errors = if result.success {
            Vec::new()
        } else {
            vec![format!("{target}: operation returned false")]
        };
        Self::operation_summary(
            title,
            usize::from(result.success),
            usize::from(!result.success),
            vec![target],
            errors,
        )
    }

    pub fn clean_expired_consume_queue(title: impl Into<String>, result: &CleanExpiredConsumeQueueReport) -> Self {
        let mut targets = result
            .targets
            .iter()
            .map(|target| {
                if result.dry_run {
                    format!("dry-run target: {target}")
                } else {
                    target.to_string()
                }
            })
            .collect::<Vec<_>>();
        targets.push(format!("scanned brokers: {}", result.scanned_brokers));
        targets.push(format!("cleanup invocations: {}", result.cleanup_invocations));
        let false_results = result
            .target_results
            .iter()
            .filter(|target| !target.success)
            .map(|target| format!("{}: operation returned false", target.broker_addr))
            .collect::<Vec<_>>();
        let mut errors = false_results;
        errors.extend(
            result
                .failures
                .iter()
                .map(|failure| format!("{}: {}", failure.broker_addr, failure.error)),
        );
        Self::operation_summary(
            title,
            result.cleanup_successes,
            result.cleanup_false_results + result.failures.len(),
            targets,
            errors,
        )
    }

    pub fn commit_log_read_ahead(title: impl Into<String>, result: &CommitLogReadAheadResult) -> Self {
        let targets = result
            .sections
            .iter()
            .map(|section| {
                let status = if section.applied { "applied" } else { "inspected" };
                format!("{}: {status}", section.broker_addr)
            })
            .collect::<Vec<_>>();
        let errors = result
            .failures
            .iter()
            .map(|failure| format!("{}: {}", failure.broker_addr, failure.error))
            .collect::<Vec<_>>();
        Self::operation_summary(title, result.sections.len(), result.failures.len(), targets, errors)
    }

    pub fn consumer_operation(title: impl Into<String>, result: &ConsumerOperationResult) -> Self {
        let mut targets = result.broker_addrs.iter().map(ToString::to_string).collect::<Vec<_>>();
        targets.extend(result.warnings.iter().map(|warning| format!("warning: {warning}")));
        let errors = result
            .failures
            .iter()
            .map(|failure| format!("{}: {}", failure.broker_addr, failure.error))
            .collect::<Vec<_>>();
        Self::operation_summary(title, result.broker_addrs.len(), result.failures.len(), targets, errors)
    }

    pub fn lite_trigger_dispatch(title: impl Into<String>, result: &TriggerLiteDispatchResult) -> Self {
        let targets = result
            .entries
            .iter()
            .filter(|entry| entry.dispatched)
            .map(|entry| entry.broker_name.to_string())
            .collect::<Vec<_>>();
        let errors = result
            .entries
            .iter()
            .filter(|entry| !entry.dispatched)
            .map(|entry| {
                let error = entry.error.as_deref().unwrap_or("dispatch returned false");
                format!("{}: {error}", entry.broker_name)
            })
            .collect::<Vec<_>>();
        Self::operation_summary(title, targets.len(), errors.len(), targets, errors)
    }

    pub fn cluster_list(title: impl Into<String>, result: &ClusterListQueryResult) -> Self {
        match &result.mode {
            ClusterListMode::Base => Self::table(
                title,
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
                result
                    .base_rows
                    .iter()
                    .map(|row| {
                        vec![
                            row.cluster_name.clone(),
                            row.broker_name.clone(),
                            row.broker_id.to_string(),
                            row.broker_addr.to_string(),
                            row.version.clone(),
                            row.in_tps.clone(),
                            row.out_tps.clone(),
                            row.broker_active.to_string(),
                        ]
                    })
                    .collect(),
            ),
            ClusterListMode::MoreStats => Self::table(
                title,
                &[
                    "Cluster",
                    "Broker",
                    "In Yesterday",
                    "Out Yesterday",
                    "In Today",
                    "Out Today",
                ],
                result
                    .more_stats_rows
                    .iter()
                    .map(|row| {
                        vec![
                            row.cluster_name.clone(),
                            row.broker_name.clone(),
                            row.in_total_yesterday.to_string(),
                            row.out_total_yesterday.to_string(),
                            row.in_total_today.to_string(),
                            row.out_total_today.to_string(),
                        ]
                    })
                    .collect(),
            ),
        }
    }

    pub fn cluster_send_message_rt(title: impl Into<String>, result: &ClusterSendMessageRtResult) -> Self {
        let mut rows = result
            .rows
            .iter()
            .map(|row| {
                vec![
                    row.cluster_name.clone(),
                    row.broker_name.clone(),
                    row.rt.to_string(),
                    row.success_count.to_string(),
                    row.fail_count.to_string(),
                ]
            })
            .collect::<Vec<_>>();

        rows.extend(result.missing_clusters.iter().map(|cluster| {
            vec![
                cluster.clone(),
                "(missing)".to_string(),
                String::new(),
                String::new(),
                String::new(),
            ]
        }));

        Self::table(title, &["Cluster", "Broker", "RT", "Success", "Failed"], rows)
    }

    pub fn controller_elect_master(title: impl Into<String>, result: &ControllerElectMasterResult) -> Self {
        let header = &result.response_header;
        let group = &result.broker_member_group;
        let mut rows = vec![
            vec![
                "cluster".to_string(),
                String::new(),
                String::new(),
                group.cluster.to_string(),
            ],
            vec![
                "broker-name".to_string(),
                String::new(),
                String::new(),
                group.broker_name.to_string(),
            ],
            vec![
                "master".to_string(),
                optional_text(&header.master_broker_id),
                optional_text(&header.master_address),
                String::new(),
            ],
            vec![
                "master-epoch".to_string(),
                String::new(),
                String::new(),
                optional_text(&header.master_epoch),
            ],
            vec![
                "sync-state-set-epoch".to_string(),
                String::new(),
                String::new(),
                optional_text(&header.sync_state_set_epoch),
            ],
        ];
        let mut broker_addrs = group.broker_addrs.iter().collect::<Vec<_>>();
        broker_addrs.sort_by_key(|(broker_id, _)| *broker_id);
        rows.extend(broker_addrs.into_iter().map(|(broker_id, broker_addr)| {
            vec![
                "broker".to_string(),
                broker_id.to_string(),
                broker_addr.to_string(),
                String::new(),
            ]
        }));

        Self::table(title, &["Kind", "Broker ID", "Address", "Value"], rows)
    }

    pub fn broker_consume_stats(title: impl Into<String>, result: &BrokerConsumeStatsResult) -> Self {
        Self::table(
            title,
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
            result
                .rows
                .iter()
                .map(|row| {
                    vec![
                        row.topic.to_string(),
                        row.group.to_string(),
                        row.broker_name.to_string(),
                        row.queue_id.to_string(),
                        row.broker_offset.to_string(),
                        row.consumer_offset.to_string(),
                        row.diff.to_string(),
                        row.last_timestamp.to_string(),
                    ]
                })
                .collect(),
        )
    }

    pub fn consumer_progress(title: impl Into<String>, result: &ConsumerProgressResult) -> Self {
        match result {
            ConsumerProgressResult::Group(progress) => {
                let headers = if progress.show_client_ip {
                    vec![
                        "Topic",
                        "Broker",
                        "Queue ID",
                        "Broker Offset",
                        "Consumer Offset",
                        "Client IP",
                        "Diff",
                        "Inflight",
                        "Last Timestamp",
                    ]
                } else {
                    vec![
                        "Topic",
                        "Broker",
                        "Queue ID",
                        "Broker Offset",
                        "Consumer Offset",
                        "Diff",
                        "Inflight",
                        "Last Timestamp",
                    ]
                };
                let rows = progress
                    .rows
                    .iter()
                    .map(|row| {
                        let mut cells = vec![
                            row.topic.to_string(),
                            row.broker_name.to_string(),
                            row.queue_id.to_string(),
                            row.broker_offset.to_string(),
                            row.consumer_offset.to_string(),
                        ];
                        if progress.show_client_ip {
                            cells.push(optional_text(&row.client_ip));
                        }
                        cells.extend([
                            row.diff.to_string(),
                            row.inflight.to_string(),
                            row.last_timestamp.to_string(),
                        ]);
                        cells
                    })
                    .collect();

                Self::table(title, &headers, rows)
            }
            ConsumerProgressResult::All(groups) => Self::table(
                title,
                &[
                    "Group",
                    "Version",
                    "Count",
                    "Consume Type",
                    "Message Model",
                    "Consume TPS",
                    "Diff Total",
                ],
                groups
                    .iter()
                    .map(|group| {
                        vec![
                            group.group.clone(),
                            group.version.to_string(),
                            group.count.to_string(),
                            format!("{:?}", group.consume_type),
                            format!("{:?}", group.message_model),
                            group.consume_tps.to_string(),
                            group.diff_total.to_string(),
                        ]
                    })
                    .collect(),
            ),
        }
    }

    pub fn consumer_monitoring(title: impl Into<String>, result: &MonitoringResult) -> Self {
        let mut rows = result.events.iter().map(monitoring_event_row).collect::<Vec<_>>();
        if result.truncated {
            rows.push(vec![
                "truncated".to_string(),
                result.rounds_completed.to_string(),
                String::new(),
                String::new(),
                "max events reached".to_string(),
                format!("groups: {}, errors: {}", result.groups_scanned, result.error_count),
            ]);
        }
        Self::table(title, &["Event", "Round", "Group", "Topic", "Metric", "Detail"], rows)
    }

    pub fn message_query_by_key(title: impl Into<String>, result: &QueryMessageByKeyResult) -> Self {
        Self::table(
            title,
            &["Message ID", "Queue ID", "Queue Offset", "Index Key"],
            result
                .rows
                .iter()
                .map(|row| {
                    vec![
                        row.message_id.to_string(),
                        row.queue_id.to_string(),
                        row.queue_offset.to_string(),
                        optional_text(&row.index_key),
                    ]
                })
                .collect(),
        )
    }

    pub fn message_query_by_offset(title: impl Into<String>, result: &QueryMessageByOffsetResult) -> Self {
        let status = result.pull_status.to_string();
        let rows = result
            .message
            .as_ref()
            .map(|message| vec![message_detail_row(&status, message.as_ref(), "")])
            .unwrap_or_else(|| vec![empty_message_detail_row("", &status, "no message returned")]);

        Self::table(title, &MESSAGE_DETAIL_HEADERS, rows)
    }

    pub fn message_query_by_id(title: impl Into<String>, result: &QueryMessageByIdResult) -> Self {
        Self::table(
            title,
            &MESSAGE_DETAIL_HEADERS,
            result
                .entries
                .iter()
                .map(|entry| match &entry.outcome {
                    QueryMessageByIdOutcome::Found {
                        message,
                        broker_addr,
                        query_time_ms,
                    } => message_detail_row(
                        "found",
                        message,
                        &format!("broker_addr={broker_addr}; query_time_ms={query_time_ms}"),
                    ),
                    QueryMessageByIdOutcome::NotFound { reason, query_time_ms } => empty_message_detail_row(
                        entry.message_id.as_str(),
                        "not-found",
                        &format!("reason={}; query_time_ms={query_time_ms}", sanitize_cell(reason)),
                    ),
                    QueryMessageByIdOutcome::Failed { error, query_time_ms } => empty_message_detail_row(
                        entry.message_id.as_str(),
                        "failed",
                        &format!("error={}; query_time_ms={query_time_ms}", sanitize_cell(error)),
                    ),
                    QueryMessageByIdOutcome::TimedOut => {
                        empty_message_detail_row(entry.message_id.as_str(), "timed-out", "query timed out")
                    }
                })
                .collect(),
        )
    }

    pub fn message_query_by_unique_key(title: impl Into<String>, result: &QueryMessageByUniqueKeyResult) -> Self {
        let rows = match result {
            QueryMessageByUniqueKeyResult::Messages(messages) => messages
                .iter()
                .map(|message| message_detail_row("found", message, ""))
                .collect(),
            QueryMessageByUniqueKeyResult::DirectStatus(status) => {
                let note = match status {
                    UniqueKeyDirectStatus::PushConsumerUnsupported { client_id } => {
                        format!("client_id={client_id}; push consumer direct consume is unsupported")
                    }
                    UniqueKeyDirectStatus::NotPushConsumer { client_id } => {
                        format!("client_id={client_id}; not a push consumer")
                    }
                    UniqueKeyDirectStatus::RunningInfoFailed { client_id } => {
                        format!("client_id={client_id}; running info query failed")
                    }
                };
                vec![empty_message_detail_row("", "direct-status", &note)]
            }
        };

        Self::table(title, &MESSAGE_DETAIL_HEADERS, rows)
    }

    pub fn direct_consume_message(title: impl Into<String>, result: &DirectConsumeMessageResult) -> Self {
        let mut row = vec![
            result.consumer_group.to_string(),
            result.client_id.to_string(),
            result.topic.to_string(),
            result.msg_id.to_string(),
        ];

        match &result.status {
            DirectConsumeMessageStatus::Consumed(detail) => {
                row.extend([
                    "consumed".to_string(),
                    detail.consume_result.clone().unwrap_or_default(),
                    detail.order.to_string(),
                    detail.auto_commit.to_string(),
                    detail.spent_time_millis.to_string(),
                    detail.remark.clone().unwrap_or_default(),
                ]);
            }
            DirectConsumeMessageStatus::NotPushConsumer => {
                row.extend([
                    "not-push-consumer".to_string(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    "target client is not a push consumer".to_string(),
                ]);
            }
            DirectConsumeMessageStatus::RunningInfoFailed { error } => {
                row.extend([
                    "running-info-failed".to_string(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    sanitize_cell(error),
                ]);
            }
            DirectConsumeMessageStatus::Failed { error } => {
                row.extend([
                    "failed".to_string(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    sanitize_cell(error),
                ]);
            }
        }

        Self::table(
            title,
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
            vec![row],
        )
    }

    pub fn message_track(title: impl Into<String>, result: &MessageTrackResult) -> Self {
        let mut rows = Vec::new();
        for entry in &result.entries {
            match &entry.outcome {
                MessageTrackOutcome::Found {
                    broker_addr,
                    query_time_ms,
                    tracks,
                } if tracks.is_empty() => rows.push(vec![
                    entry.message_id.to_string(),
                    "found".to_string(),
                    String::new(),
                    String::new(),
                    String::new(),
                    broker_addr.clone(),
                    query_time_ms.to_string(),
                    "no consumer".to_string(),
                ]),
                MessageTrackOutcome::Found {
                    broker_addr,
                    query_time_ms,
                    tracks,
                } => {
                    rows.extend(tracks.iter().map(|track| {
                        vec![
                            entry.message_id.to_string(),
                            "found".to_string(),
                            track.consumer_group.clone(),
                            track.track_type.clone().unwrap_or_default(),
                            sanitize_cell(&track.exception_desc),
                            broker_addr.clone(),
                            query_time_ms.to_string(),
                            String::new(),
                        ]
                    }));
                }
                MessageTrackOutcome::NotFound { reason, query_time_ms } => rows.push(vec![
                    entry.message_id.to_string(),
                    "not-found".to_string(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    query_time_ms.to_string(),
                    sanitize_cell(reason),
                ]),
                MessageTrackOutcome::Failed { error, query_time_ms } => rows.push(vec![
                    entry.message_id.to_string(),
                    "failed".to_string(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    query_time_ms.to_string(),
                    sanitize_cell(error),
                ]),
                MessageTrackOutcome::TimedOut => rows.push(vec![
                    entry.message_id.to_string(),
                    "timed-out".to_string(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    "track query timed out".to_string(),
                ]),
            }
        }

        Self::table(
            title,
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
            rows,
        )
    }

    pub fn decoded_message_ids(title: impl Into<String>, result: &DecodeMessageIdResult) -> Self {
        Self::table(
            title,
            &[
                "Message ID",
                "Status",
                "Broker",
                "CommitLog Offset",
                "Offset Hex",
                "Error",
            ],
            result
                .entries
                .iter()
                .map(|entry| match &entry.outcome {
                    DecodeMessageIdOutcome::Decoded {
                        broker_ip,
                        broker_port,
                        commit_log_offset,
                        offset_hex,
                    } => vec![
                        entry.message_id.to_string(),
                        "decoded".to_string(),
                        format!("{broker_ip}:{broker_port}"),
                        commit_log_offset.to_string(),
                        offset_hex.to_string(),
                        String::new(),
                    ],
                    DecodeMessageIdOutcome::Invalid { error } => vec![
                        entry.message_id.to_string(),
                        "invalid".to_string(),
                        String::new(),
                        String::new(),
                        String::new(),
                        error.to_string(),
                    ],
                })
                .collect(),
        )
    }

    pub fn send_message_status(title: impl Into<String>, result: &SendMessageStatusResult) -> Self {
        Self::table(
            title,
            &["Attempt", "RT", "Result"],
            result
                .rows
                .iter()
                .enumerate()
                .map(|(index, row)| {
                    vec![
                        (index + 1).to_string(),
                        row.rt_millis.to_string(),
                        row.send_result.clone(),
                    ]
                })
                .collect(),
        )
    }

    pub fn send_message(title: impl Into<String>, result: &SendMessageResult) -> Self {
        Self::table(
            title,
            &["Broker", "Queue ID", "Status", "Message ID"],
            vec![vec![
                result.row.broker_name.clone(),
                result.row.queue_id.clone(),
                result.row.send_status.clone(),
                result.row.msg_id.clone(),
            ]],
        )
    }

    pub fn check_message_send_rt(title: impl Into<String>, result: &CheckMessageSendRtResult) -> Self {
        let mut rows = result
            .rows
            .iter()
            .enumerate()
            .map(|(index, row)| {
                vec![
                    (index + 1).to_string(),
                    row.broker_name.clone(),
                    row.queue_id.to_string(),
                    row.send_success.to_string(),
                    row.rt_millis.to_string(),
                    result.avg_rt.to_string(),
                ]
            })
            .collect::<Vec<_>>();

        if rows.is_empty() {
            rows.push(vec![
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                result.avg_rt.to_string(),
            ]);
        }

        Self::table(
            title,
            &["Attempt", "Broker", "Queue ID", "Success", "RT", "Avg RT"],
            rows,
        )
    }

    pub fn message_trace(title: impl Into<String>, traces: &[MessageTraceView]) -> Self {
        Self::table(
            title,
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
            traces
                .iter()
                .map(|trace| {
                    vec![
                        trace.msg_type.clone(),
                        trace.group_name.clone(),
                        trace.client_host.clone(),
                        trace.time_stamp.to_string(),
                        trace.cost_time.to_string(),
                        trace.status.clone(),
                        optional_text(&trace.topic),
                        optional_text(&trace.tags),
                        optional_text(&trace.keys),
                        optional_text(&trace.store_host),
                    ]
                })
                .collect(),
        )
    }

    pub fn dump_compaction_log(title: impl Into<String>, result: &DumpCompactionLogResult) -> Self {
        if result.missing_file_name {
            return Self::operation_summary(title, 0, 1, Vec::new(), vec!["file name is required".to_string()]);
        }

        Self::table(
            title,
            &MESSAGE_DETAIL_HEADERS,
            result
                .messages
                .iter()
                .map(|message| message_detail_row("decoded", message, "compaction log"))
                .collect(),
        )
    }

    pub fn message_pull_events(title: impl Into<String>, result: &MessagePullCapture) -> Self {
        let mut rows = result.events.iter().map(message_pull_event_row).collect::<Vec<_>>();
        if result.truncated {
            rows.push(vec![
                "truncated".to_string(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                format!("truncated after {} events", result.event_limit),
            ]);
        }
        Self::table(
            title,
            &["Event", "Topic", "Broker", "Queue", "Offset", "Status", "Detail"],
            rows,
        )
    }

    pub fn export_configs(title: impl Into<String>, result: &ExportConfigsResult) -> Self {
        let mut rows = result
            .name_servers
            .iter()
            .map(|name_server| {
                vec![
                    "name-server".to_string(),
                    String::new(),
                    "address".to_string(),
                    name_server.clone(),
                ]
            })
            .collect::<Vec<_>>();
        rows.push(vec![
            "summary".to_string(),
            String::new(),
            "master_broker_size".to_string(),
            result.master_broker_size.to_string(),
        ]);
        rows.push(vec![
            "summary".to_string(),
            String::new(),
            "slave_broker_size".to_string(),
            result.slave_broker_size.to_string(),
        ]);
        for (broker_name, configs) in &result.broker_configs {
            let mut entries = configs.iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(right.0));
            rows.extend(entries.into_iter().map(|(key, value)| {
                vec![
                    "broker-config".to_string(),
                    broker_name.clone(),
                    key.clone(),
                    value.clone(),
                ]
            }));
        }
        Self::table(title, &["Type", "Target", "Key", "Value"], rows)
    }

    pub fn export_metrics(title: impl Into<String>, result: &ExportMetricsResult) -> Self {
        let mut rows = result
            .evaluate_report
            .iter()
            .map(|(broker_name, report)| {
                let quota = &report.runtime_quota;
                vec![
                    broker_name.clone(),
                    optional_text(&report.runtime_env.cpu_num),
                    optional_text(&report.runtime_env.total_mem_kbytes),
                    optional_text(&quota.disk_ratio.commit_log_disk_ratio),
                    optional_text(&quota.disk_ratio.consume_queue_disk_ratio),
                    fixed_f64(quota.tps.normal_in_tps),
                    fixed_f64(quota.tps.normal_out_tps),
                    fixed_f64(quota.tps.trans_in_tps),
                    fixed_f64(quota.tps.schedule_in_tps),
                    quota.one_day_num.normal_one_day_in_num.to_string(),
                    quota.one_day_num.normal_one_day_out_num.to_string(),
                    quota.one_day_num.trans_one_day_in_num.to_string(),
                    quota.one_day_num.schedule_one_day_in_num.to_string(),
                    optional_text(&quota.message_average_size),
                    quota.topic_size.to_string(),
                    quota.group_size.to_string(),
                    report.runtime_version.rocketmq_version.clone(),
                    report.runtime_version.client_info.join(","),
                ]
            })
            .collect::<Vec<_>>();

        let totals = &result.total_data;
        rows.push(vec![
            "TOTAL".to_string(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            fixed_f64(totals.total_tps.total_normal_in_tps),
            fixed_f64(totals.total_tps.total_normal_out_tps),
            fixed_f64(totals.total_tps.total_trans_in_tps),
            fixed_f64(totals.total_tps.total_schedule_in_tps),
            totals.total_one_day_num.normal_one_day_in_num.to_string(),
            totals.total_one_day_num.normal_one_day_out_num.to_string(),
            totals.total_one_day_num.trans_one_day_in_num.to_string(),
            totals.total_one_day_num.schedule_one_day_in_num.to_string(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
        ]);

        Self::table(
            title,
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
            rows,
        )
    }

    pub fn export_metadata(title: impl Into<String>, result: &ExportMetadataResult) -> Self {
        match result {
            ExportMetadataResult::BrokerTopic { wrapper } => {
                let topic_count = wrapper
                    .topic_config_table()
                    .map(|table| table.len())
                    .unwrap_or_default();
                Self::operation_summary(
                    title,
                    topic_count,
                    0,
                    vec![format!("broker topics: {topic_count}")],
                    Vec::new(),
                )
            }
            ExportMetadataResult::BrokerSubscriptionGroup { wrapper } => {
                let group_count = wrapper.get_subscription_group_table().len();
                Self::operation_summary(
                    title,
                    group_count,
                    0,
                    vec![format!("broker subscription groups: {group_count}")],
                    Vec::new(),
                )
            }
            ExportMetadataResult::Cluster {
                scope,
                topic_config_table,
                subscription_group_table,
                export_time_millis,
            } => Self::operation_summary(
                title,
                topic_config_table.len() + subscription_group_table.len(),
                0,
                vec![
                    format!("scope: {}", export_metadata_scope_name(*scope)),
                    format!("topics: {}", topic_config_table.len()),
                    format!("subscription groups: {}", subscription_group_table.len()),
                    format!("export_time_millis: {export_time_millis}"),
                ],
                Vec::new(),
            ),
        }
    }

    pub fn export_metadata_rocksdb(title: impl Into<String>, result: &ExportMetadataInRocksDbResult) -> Self {
        match result {
            ExportMetadataInRocksDbResult::InvalidPath => Self::operation_summary(
                title,
                0,
                1,
                Vec::new(),
                vec!["RocksDB path is empty or does not exist".to_string()],
            ),
            ExportMetadataInRocksDbResult::InvalidConfigType { config_type } => Self::operation_summary(
                title,
                0,
                1,
                Vec::new(),
                vec![format!("invalid config type: {config_type}")],
            ),
            ExportMetadataInRocksDbResult::Data {
                config_type,
                json_enable,
                entries,
            } => Self::table(
                title,
                &["Config Type", "Format", "Key", "Value"],
                entries
                    .iter()
                    .map(|entry| {
                        vec![
                            export_rocksdb_config_type_name(*config_type).to_string(),
                            if *json_enable { "json" } else { "raw" }.to_string(),
                            entry.key.clone(),
                            entry.value.clone(),
                        ]
                    })
                    .collect(),
            ),
        }
    }

    pub fn export_rocksdb_config_rpc(title: impl Into<String>, result: &ExportRocksDbConfigRpcResult) -> Self {
        let targets = result
            .targets
            .iter()
            .filter(|target| target.exported)
            .map(|target| {
                format!(
                    "{} {} [{}]",
                    target.broker_name,
                    target.broker_addr,
                    target
                        .config_types
                        .iter()
                        .map(|config_type| export_rocksdb_config_type_name(*config_type))
                        .collect::<Vec<_>>()
                        .join(",")
                )
            })
            .collect::<Vec<_>>();
        let errors = result
            .targets
            .iter()
            .filter_map(|target| {
                target
                    .error
                    .as_ref()
                    .map(|error| format!("{} {}: {error}", target.broker_name, target.broker_addr))
            })
            .collect::<Vec<_>>();
        Self::operation_summary(title, targets.len(), errors.len(), targets, errors)
    }

    pub fn export_pop_records(title: impl Into<String>, result: &ExportPopRecordResult) -> Self {
        let targets = result
            .targets
            .iter()
            .filter(|target| target.exported || target.dry_run)
            .map(|target| format!("{} {}", target.broker_name, target.broker_addr))
            .collect::<Vec<_>>();
        let errors = result
            .targets
            .iter()
            .filter_map(|target| {
                target
                    .error
                    .as_ref()
                    .map(|error| format!("{} {}: {error}", target.broker_name, target.broker_addr))
            })
            .collect::<Vec<_>>();
        Self::operation_summary(title, targets.len(), errors.len(), targets, errors)
    }

    pub fn consume_queue(title: impl Into<String>, result: &QueryConsumeQueueResult) -> Self {
        let rows = result
            .response_body
            .queue_data
            .as_deref()
            .map(|queue_data| {
                queue_data
                    .iter()
                    .map(|row| {
                        vec![
                            result.broker_addr.to_string(),
                            row.physic_offset.to_string(),
                            row.physic_size.to_string(),
                            row.tags_code.to_string(),
                            optional_text(&row.extend_data_json),
                            optional_text(&row.bit_map),
                            row.eval.to_string(),
                            optional_text(&row.msg),
                        ]
                    })
                    .collect()
            })
            .unwrap_or_default();

        Self::table(
            title,
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
            rows,
        )
    }

    pub fn rocksdb_cq_progress(title: impl Into<String>, result: &CheckRocksdbCqWriteProgressResult) -> Self {
        let mut rows = result
            .entries
            .iter()
            .map(|entry| {
                vec![
                    entry.broker_name.to_string(),
                    entry.broker_addr.to_string(),
                    format!("{:?}", entry.result.get_check_status()),
                    optional_text(&entry.result.check_result),
                    String::new(),
                ]
            })
            .collect::<Vec<_>>();

        rows.extend(result.failures.iter().map(|failure| {
            vec![
                failure.broker_name.to_string(),
                failure.broker_addr.to_string(),
                "Failed".to_string(),
                String::new(),
                failure.error.clone(),
            ]
        }));

        Self::table(title, &["Broker", "Address", "Status", "Check Result", "Error"], rows)
    }

    pub fn consumer_connection(title: impl Into<String>, result: &ConsumerConnectionQueryResult) -> Self {
        let consume_type = result
            .connection
            .get_consume_type()
            .map(|value| format!("{value:?}"))
            .unwrap_or_default();
        let message_model = result
            .connection
            .get_message_model()
            .map(|value| format!("{value:?}"))
            .unwrap_or_default();

        let mut connection_rows = result
            .connection
            .get_connection_set()
            .iter()
            .map(|connection| {
                vec![
                    "Connection".to_string(),
                    connection.get_client_id().to_string(),
                    connection.get_client_addr().to_string(),
                    connection.get_language().to_string(),
                    connection.get_version().to_string(),
                    String::new(),
                    String::new(),
                    consume_type.clone(),
                    message_model.clone(),
                ]
            })
            .collect::<Vec<_>>();
        connection_rows.sort_by(|left, right| left[1].cmp(&right[1]).then_with(|| left[2].cmp(&right[2])));

        let mut subscription_rows = result
            .connection
            .get_subscription_table()
            .values()
            .map(|subscription| {
                vec![
                    "Subscription".to_string(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    subscription.topic.to_string(),
                    subscription.sub_string.to_string(),
                    consume_type.clone(),
                    message_model.clone(),
                ]
            })
            .collect::<Vec<_>>();
        subscription_rows.sort_by(|left, right| left[5].cmp(&right[5]).then_with(|| left[6].cmp(&right[6])));

        connection_rows.extend(subscription_rows);

        Self::table(
            title,
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
            connection_rows,
        )
    }

    pub fn producer_connection(title: impl Into<String>, result: &ProducerConnectionQueryResult) -> Self {
        let mut rows = result
            .connection
            .connection_set()
            .iter()
            .map(|connection| {
                vec![
                    connection.get_client_id().to_string(),
                    connection.get_client_addr().to_string(),
                    connection.get_language().to_string(),
                    connection.get_version().to_string(),
                ]
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| left[0].cmp(&right[0]).then_with(|| left[1].cmp(&right[1])));

        Self::table(title, &["Client ID", "Address", "Language", "Version"], rows)
    }

    pub fn consumer_running_info(title: impl Into<String>, result: &ConsumerRunningInfoResult) -> Self {
        let subscription_consistent = result
            .subscription_consistent
            .map(|value| value.to_string())
            .unwrap_or_default();
        let analysis = result.process_queue_analysis.join("; ");
        let mut rows = result
            .items
            .iter()
            .map(|item| {
                vec![
                    item.client_id.to_string(),
                    item.version.to_string(),
                    format!("{:?}", item.running_info.consume_type),
                    item.running_info.consume_orderly.to_string(),
                    item.running_info.prop_consumer_start_timestamp.to_string(),
                    item.running_info.subscription_set.len().to_string(),
                    item.running_info.mq_table.len().to_string(),
                    item.running_info.mq_pop_table.len().to_string(),
                    item.running_info.status_table.len().to_string(),
                    item.running_info.user_consumer_info.len().to_string(),
                    subscription_consistent.clone(),
                    analysis.clone(),
                ]
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| left[0].cmp(&right[0]));

        Self::table(
            title,
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
            rows,
        )
    }

    pub fn producer_info(title: impl Into<String>, result: &ProducerInfoQueryResult) -> Self {
        let mut rows = result
            .producer_table_info
            .data()
            .iter()
            .flat_map(|(group, producers)| {
                producers.iter().map(|producer| {
                    vec![
                        group.clone(),
                        producer.client_id().to_string(),
                        producer.remote_ip().to_string(),
                        producer.language().to_string(),
                        producer.version().to_string(),
                        producer.last_update_timestamp().to_string(),
                    ]
                })
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| left[0].cmp(&right[0]).then_with(|| left[1].cmp(&right[1])));

        Self::table(
            title,
            &["Group", "Client ID", "Remote IP", "Language", "Version", "Last Update"],
            rows,
        )
    }

    pub fn stats_all(title: impl Into<String>, result: &StatsAllQueryResult) -> Self {
        let mut rows = result
            .rows
            .iter()
            .map(|row| {
                vec![
                    row.topic.to_string(),
                    optional_text(&row.consumer_group),
                    row.accumulation.to_string(),
                    row.in_tps.to_string(),
                    optional_text(&row.out_tps),
                    row.in_msg_count_24h.to_string(),
                    optional_text(&row.out_msg_count_24h),
                    String::new(),
                ]
            })
            .collect::<Vec<_>>();

        rows.extend(result.failures.iter().map(|failure| {
            vec![
                failure.topic.to_string(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                failure.error.clone(),
            ]
        }));

        Self::table(
            title,
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
            rows,
        )
    }

    fn table(title: impl Into<String>, headers: &[&str], rows: Vec<Vec<String>>) -> Self {
        Self::Table(TableViewModel {
            title: title.into(),
            headers: headers.iter().map(|header| (*header).to_string()).collect(),
            rows,
        })
    }

    pub fn title(&self) -> &str {
        match self {
            Self::Table(value) => &value.title,
            Self::KeyValue(value) => &value.title,
            Self::Json { title, .. } | Self::Text { title, .. } => title,
            Self::OperationSummary(value) => &value.title,
        }
    }

    pub fn text_body(&self) -> String {
        match self {
            Self::Table(table) => {
                let mut lines = Vec::new();
                lines.push(table.headers.join(" | "));
                lines.extend(table.rows.iter().map(|row| row.join(" | ")));
                lines.join("\n")
            }
            Self::KeyValue(key_values) => key_values
                .rows
                .iter()
                .map(|(key, value)| format!("{key}: {value}"))
                .collect::<Vec<_>>()
                .join("\n"),
            Self::Json { body, .. } | Self::Text { body, .. } => body.clone(),
            Self::OperationSummary(summary) => {
                let mut lines = vec![
                    format!("success: {}", summary.success_count),
                    format!("failed: {}", summary.failure_count),
                ];
                if !summary.targets.is_empty() {
                    lines.push("targets:".to_string());
                    lines.extend(summary.targets.iter().map(|target| format!("  {target}")));
                }
                if !summary.errors.is_empty() {
                    lines.push("errors:".to_string());
                    lines.extend(summary.errors.iter().map(|error| format!("  {error}")));
                }
                lines.join("\n")
            }
        }
    }
}

fn optional_text<T: ToString>(value: &Option<T>) -> String {
    value.as_ref().map(ToString::to_string).unwrap_or_default()
}

fn fixed_f64(value: f64) -> String {
    format!("{value:.2}")
}

const MESSAGE_DETAIL_HEADERS: [&str; 17] = [
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
];

fn message_detail_row(status: &str, message: &MessageExt, note: &str) -> Vec<String> {
    let body = message.body();
    vec![
        message.msg_id().to_string(),
        status.to_string(),
        message.topic().to_string(),
        message.broker_name().to_string(),
        message.queue_id().to_string(),
        message.queue_offset().to_string(),
        message.commit_log_offset().to_string(),
        optional_text(&message.get_tags()),
        message_property(message, MessageConst::PROPERTY_KEYS),
        message.born_host().to_string(),
        message.born_timestamp().to_string(),
        message.store_host().to_string(),
        message.store_timestamp().to_string(),
        body.as_ref().map(|body| body.len().to_string()).unwrap_or_default(),
        body.as_ref()
            .map(|body| truncate_cell(&sanitize_cell(String::from_utf8_lossy(body.as_ref()).as_ref()), 120))
            .unwrap_or_default(),
        message_properties_summary(message),
        note.to_string(),
    ]
}

fn empty_message_detail_row(message_id: &str, status: &str, note: &str) -> Vec<String> {
    vec![
        message_id.to_string(),
        status.to_string(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        note.to_string(),
    ]
}

fn monitoring_event_row(event: &MonitoringEvent) -> Vec<String> {
    match event {
        MonitoringEvent::BeginRound {
            round,
            timestamp_millis,
        } => vec![
            "begin-round".to_string(),
            round.to_string(),
            String::new(),
            String::new(),
            "timestamp_millis".to_string(),
            timestamp_millis.to_string(),
        ],
        MonitoringEvent::UndoneMsgs {
            round,
            consumer_group,
            topic,
            undone_msgs_total,
            undone_msgs_single_mq,
            undone_msgs_delay_time_millis,
        } => vec![
            "undone-msgs".to_string(),
            round.to_string(),
            consumer_group.clone(),
            topic.clone(),
            format!("total={undone_msgs_total}, single_mq={undone_msgs_single_mq}"),
            undone_msgs_delay_time_millis
                .map(|delay| format!("delay_ms={delay}"))
                .unwrap_or_else(|| "delay_ms=not-sampled".to_string()),
        ],
        MonitoringEvent::ConsumerRunningInfo {
            round,
            consumer_group,
            client_count,
            subscription_consistent,
            process_queue_analysis,
        } => vec![
            "running-info".to_string(),
            round.to_string(),
            consumer_group.clone(),
            String::new(),
            format!("clients={client_count}"),
            format!(
                "subscription_consistent={}, analysis={}",
                subscription_consistent
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unknown".to_string()),
                process_queue_analysis.join("; ")
            ),
        ],
        MonitoringEvent::Error {
            round,
            consumer_group,
            operation,
            error,
        } => vec![
            "error".to_string(),
            round.to_string(),
            consumer_group.clone().unwrap_or_default(),
            String::new(),
            operation.clone(),
            error.clone(),
        ],
        MonitoringEvent::EndRound {
            round,
            elapsed_millis,
            groups_scanned,
            events_emitted,
        } => vec![
            "end-round".to_string(),
            round.to_string(),
            String::new(),
            String::new(),
            format!("groups={groups_scanned}, events={events_emitted}"),
            format!("elapsed_ms={elapsed_millis}"),
        ],
    }
}

fn message_pull_event_row(event: &MessagePullEvent) -> Vec<String> {
    match event {
        MessagePullEvent::QueueRange {
            mq,
            min_offset,
            max_offset,
        } => vec![
            "queue-range".to_string(),
            mq.topic().to_string(),
            mq.broker_name().to_string(),
            mq.queue_id().to_string(),
            format!("{min_offset}..{max_offset}"),
            String::new(),
            String::new(),
        ],
        MessagePullEvent::Messages { messages } => {
            let first_message_id = messages
                .first()
                .map(|message| message.msg_id().to_string())
                .unwrap_or_default();
            vec![
                "messages".to_string(),
                messages
                    .first()
                    .map(|message| message.topic().to_string())
                    .unwrap_or_default(),
                messages
                    .first()
                    .map(|message| message.broker_name().to_string())
                    .unwrap_or_default(),
                messages
                    .first()
                    .map(|message| message.queue_id().to_string())
                    .unwrap_or_default(),
                String::new(),
                messages.len().to_string(),
                first_message_id,
            ]
        }
        MessagePullEvent::ConsumeOk => empty_message_event_row("consume-ok", "", "consume status is OK"),
        MessagePullEvent::CountLimit {
            message_number,
            queue_id,
        } => vec![
            "count-limit".to_string(),
            String::new(),
            String::new(),
            queue_id.map(|queue_id| queue_id.to_string()).unwrap_or_default(),
            String::new(),
            String::new(),
            format!("message_number={message_number}"),
        ],
        MessagePullEvent::OffsetNotMatched { mq, offset } => vec![
            "offset-not-matched".to_string(),
            mq.topic().to_string(),
            mq.broker_name().to_string(),
            mq.queue_id().to_string(),
            offset.to_string(),
            String::new(),
            String::new(),
        ],
        MessagePullEvent::NoMatched { mq, status, offset } => vec![
            "no-matched".to_string(),
            mq.topic().to_string(),
            mq.broker_name().to_string(),
            mq.queue_id().to_string(),
            offset.to_string(),
            status.to_string(),
            String::new(),
        ],
        MessagePullEvent::Finished { mq, status, offset } => vec![
            "finished".to_string(),
            mq.topic().to_string(),
            mq.broker_name().to_string(),
            mq.queue_id().to_string(),
            offset.to_string(),
            status.to_string(),
            String::new(),
        ],
        MessagePullEvent::PullError { error } => empty_message_event_row("pull-error", "", error),
        MessagePullEvent::Separator => empty_message_event_row("separator", "", ""),
        MessagePullEvent::TagCounts(tag_counts) => empty_message_event_row(
            "tag-counts",
            &tag_counts.len().to_string(),
            &tag_counts
                .iter()
                .map(|(tag, count)| format!("{tag}={count}"))
                .collect::<Vec<_>>()
                .join("; "),
        ),
    }
}

fn empty_message_event_row(event: &str, status: &str, detail: &str) -> Vec<String> {
    vec![
        event.to_string(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        status.to_string(),
        detail.to_string(),
    ]
}

fn export_metadata_scope_name(scope: ExportMetadataScope) -> &'static str {
    match scope {
        ExportMetadataScope::Topic => "topic",
        ExportMetadataScope::SubscriptionGroup => "subscription-group",
        ExportMetadataScope::All => "all",
    }
}

fn export_rocksdb_config_type_name(config_type: ExportMetadataInRocksDbConfigType) -> &'static str {
    match config_type {
        ExportMetadataInRocksDbConfigType::Topics => "Topics",
        ExportMetadataInRocksDbConfigType::SubscriptionGroups => "SubscriptionGroups",
        ExportMetadataInRocksDbConfigType::ConsumerOffsets => "ConsumerOffsets",
    }
}

fn message_property(message: &MessageExt, key: &str) -> String {
    message
        .properties()
        .iter()
        .find_map(|(property_key, value)| (property_key.as_str() == key).then(|| value.to_string()))
        .unwrap_or_default()
}

fn message_properties_summary(message: &MessageExt) -> String {
    let mut properties = message
        .properties()
        .iter()
        .map(|(key, value)| format!("{}={}", key.as_str(), sanitize_cell(value.as_str())))
        .collect::<Vec<_>>();
    properties.sort();
    properties.join("; ")
}

fn sanitize_cell(value: &str) -> String {
    value.replace('\r', "\\r").replace('\n', "\\n")
}

fn truncate_cell(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let truncated = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}
