use std::fmt::Debug;

use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableViewModel {
    pub title: String,
    pub headers: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyValueViewModel {
    pub title: String,
    pub rows: Vec<(String, String)>,
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
}
