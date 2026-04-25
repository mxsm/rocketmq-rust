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

use crate::admin_facade::MessagePullCapture;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableViewModel {
    pub title: String,
    pub headers: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableViewport {
    pub headers: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub widths: Vec<u16>,
    pub hidden_left: bool,
    pub hidden_right: bool,
}

impl TableViewModel {
    const MIN_COLUMN_WIDTH: u16 = 8;
    const MAX_COLUMN_WIDTH: u16 = 32;

    pub fn viewport(&self, first_column: usize, available_width: u16) -> TableViewport {
        if self.headers.is_empty() {
            return TableViewport {
                headers: Vec::new(),
                rows: Vec::new(),
                widths: Vec::new(),
                hidden_left: false,
                hidden_right: false,
            };
        }

        let first_column = first_column.min(self.headers.len() - 1);
        let available_width = available_width.max(Self::MIN_COLUMN_WIDTH);
        let mut used_width = 0_u16;
        let mut columns = Vec::new();

        for column in first_column..self.headers.len() {
            let width = self.column_width(column).min(available_width);
            let separator_width = u16::from(!columns.is_empty());
            if !columns.is_empty() && used_width.saturating_add(separator_width).saturating_add(width) > available_width
            {
                break;
            }
            used_width = used_width.saturating_add(separator_width).saturating_add(width);
            columns.push((column, width));
        }

        let headers = columns
            .iter()
            .map(|(column, _)| self.headers[*column].clone())
            .collect::<Vec<_>>();
        let rows = self
            .rows
            .iter()
            .map(|row| {
                columns
                    .iter()
                    .map(|(column, _)| row.get(*column).cloned().unwrap_or_default())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let hidden_right = columns
            .last()
            .map(|(column, _)| column + 1 < self.headers.len())
            .unwrap_or(false);

        TableViewport {
            headers,
            rows,
            widths: columns.into_iter().map(|(_, width)| width).collect(),
            hidden_left: first_column > 0,
            hidden_right,
        }
    }

    fn column_width(&self, column: usize) -> u16 {
        let header_width = self
            .headers
            .get(column)
            .map(|header| header.chars().count())
            .unwrap_or_default();
        let cell_width = self
            .rows
            .iter()
            .filter_map(|row| row.get(column))
            .map(|cell| cell.chars().count())
            .max()
            .unwrap_or_default();
        let width = header_width.max(cell_width).saturating_add(2);
        width.clamp(Self::MIN_COLUMN_WIDTH as usize, Self::MAX_COLUMN_WIDTH as usize) as u16
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyValueViewModel {
    pub title: String,
    pub rows: Vec<(String, String)>,
}

impl KeyValueViewModel {
    pub fn sorted(title: impl Into<String>, mut rows: Vec<(String, String)>) -> Self {
        rows.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
        Self {
            title: title.into(),
            rows,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationSummaryViewModel {
    pub title: String,
    pub success_count: usize,
    pub failure_count: usize,
    pub targets: Vec<String>,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandResultViewModel {
    Table(TableViewModel),
    KeyValue(KeyValueViewModel),
    Json { title: String, body: String },
    Text { title: String, body: String },
    OperationSummary(OperationSummaryViewModel),
}

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

#[cfg(test)]
mod tests {
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
}
