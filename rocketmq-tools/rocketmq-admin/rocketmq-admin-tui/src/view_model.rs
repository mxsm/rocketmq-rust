use std::fmt::Debug;

use rocketmq_admin_core::core::broker::BrokerConsumeStatsResult;
use rocketmq_admin_core::core::cluster::ClusterListMode;
use rocketmq_admin_core::core::cluster::ClusterListQueryResult;
use rocketmq_admin_core::core::cluster::ClusterSendMessageRtResult;
use rocketmq_admin_core::core::consumer::ConsumerProgressResult;
use rocketmq_admin_core::core::message::DecodeMessageIdOutcome;
use rocketmq_admin_core::core::message::DecodeMessageIdResult;
use rocketmq_admin_core::core::message::MessageTraceView;
use rocketmq_admin_core::core::message::QueryMessageByKeyResult;
use rocketmq_admin_core::core::queue::CheckRocksdbCqWriteProgressResult;
use rocketmq_admin_core::core::queue::QueryConsumeQueueResult;
use serde::Serialize;

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

    pub fn error(title: impl Into<String>, body: impl Into<String>) -> Self {
        Self::Text {
            title: title.into(),
            body: body.into(),
        }
    }

    pub fn key_value_sorted(title: impl Into<String>, rows: Vec<(String, String)>) -> Self {
        Self::KeyValue(KeyValueViewModel::sorted(title, rows))
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

#[cfg(test)]
mod tests {
    use rocketmq_admin_core::core::broker::BrokerConsumeStatsResult;
    use rocketmq_admin_core::core::broker::BrokerConsumeStatsRow;
    use rocketmq_admin_core::core::cluster::ClusterBaseInfoRow;
    use rocketmq_admin_core::core::cluster::ClusterListMode;
    use rocketmq_admin_core::core::cluster::ClusterListQueryResult;
    use rocketmq_admin_core::core::cluster::ClusterSendMessageRtResult;
    use rocketmq_admin_core::core::cluster::ClusterSendMessageRtRow;
    use rocketmq_admin_core::core::consumer::ConsumerGroupProgressResult;
    use rocketmq_admin_core::core::consumer::ConsumerProgressResult;
    use rocketmq_admin_core::core::consumer::ConsumerProgressRow;
    use rocketmq_admin_core::core::message::DecodeMessageIdEntry;
    use rocketmq_admin_core::core::message::DecodeMessageIdOutcome;
    use rocketmq_admin_core::core::message::DecodeMessageIdResult;
    use rocketmq_admin_core::core::message::MessageTraceView;
    use rocketmq_admin_core::core::message::QueryMessageByKeyResult;
    use rocketmq_admin_core::core::message::QueryMessageByKeyRow;
    use rocketmq_admin_core::core::queue::CheckRocksdbCqWriteProgressEntry;
    use rocketmq_admin_core::core::queue::CheckRocksdbCqWriteProgressResult;
    use rocketmq_admin_core::core::queue::QueryConsumeQueueResult;
    use rocketmq_admin_core::core::queue::QueueOperationFailure;
    use rocketmq_remoting::protocol::body::check_rocksdb_cqwrite_progress_response_body::CheckRocksdbCqWriteResult;
    use rocketmq_remoting::protocol::body::consume_queue_data::ConsumeQueueData;
    use rocketmq_remoting::protocol::body::query_consume_queue_response_body::QueryConsumeQueueResponseBody;

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
}
