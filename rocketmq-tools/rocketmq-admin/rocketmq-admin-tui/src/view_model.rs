use std::fmt::Debug;

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

#[cfg(test)]
mod tests {
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
}
