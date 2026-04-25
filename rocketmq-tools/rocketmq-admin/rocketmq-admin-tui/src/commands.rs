use crate::state::CommandFormState;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CommandCategory {
    Topic,
    NameServer,
    Auth,
    Broker,
    Cluster,
    Controller,
    Container,
    Connection,
    Consumer,
    Offset,
    Queue,
    Ha,
    Stats,
    Producer,
    Lite,
    Message,
    Export,
    StaticTopic,
}

impl CommandCategory {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Topic => "Topic",
            Self::NameServer => "NameServer",
            Self::Auth => "Auth",
            Self::Broker => "Broker",
            Self::Cluster => "Cluster",
            Self::Controller => "Controller",
            Self::Container => "Container",
            Self::Connection => "Connection",
            Self::Consumer => "Consumer",
            Self::Offset => "Offset",
            Self::Queue => "Queue",
            Self::Ha => "HA",
            Self::Stats => "Stats",
            Self::Producer => "Producer",
            Self::Lite => "Lite",
            Self::Message => "Message",
            Self::Export => "Export",
            Self::StaticTopic => "Static Topic",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RiskLevel {
    Safe,
    Mutating,
    Dangerous,
}

impl RiskLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Safe => "safe",
            Self::Mutating => "mutating",
            Self::Dangerous => "dangerous",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultViewKind {
    Table,
    KeyValue,
    Json,
    Text,
    OperationSummary,
}

impl ResultViewKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::KeyValue => "key-value",
            Self::Json => "json",
            Self::Text => "text",
            Self::OperationSummary => "operation-summary",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandExecutor {
    Facade,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArgKind {
    String {
        placeholder: &'static str,
    },
    OptionalString {
        placeholder: &'static str,
    },
    Number {
        default: Option<i64>,
        min: Option<i64>,
    },
    Bool {
        default: bool,
    },
    Enum {
        values: &'static [&'static str],
        default: &'static str,
    },
    KeyValueMap,
    TimestampMillis,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArgSpec {
    pub name: &'static str,
    pub label: &'static str,
    pub help: &'static str,
    pub required: bool,
    pub kind: ArgKind,
}

impl ArgSpec {
    pub fn default_value(&self) -> String {
        match &self.kind {
            ArgKind::String { .. } | ArgKind::OptionalString { .. } | ArgKind::KeyValueMap => String::new(),
            ArgKind::Number { default, .. } => default.map(|value| value.to_string()).unwrap_or_default(),
            ArgKind::Bool { default } => default.to_string(),
            ArgKind::Enum { default, .. } => (*default).to_string(),
            ArgKind::TimestampMillis => String::new(),
        }
    }

    pub fn placeholder(&self) -> &'static str {
        match &self.kind {
            ArgKind::String { placeholder } | ArgKind::OptionalString { placeholder } => placeholder,
            ArgKind::Number { .. } => "number",
            ArgKind::Bool { .. } => "true/false",
            ArgKind::Enum { values, .. } => values.first().copied().unwrap_or("value"),
            ArgKind::KeyValueMap => "key=value, one per line",
            ArgKind::TimestampMillis => "timestamp millis",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandSpec {
    pub id: &'static str,
    pub category: CommandCategory,
    pub title: &'static str,
    pub description: &'static str,
    pub risk_level: RiskLevel,
    pub args: Vec<ArgSpec>,
    pub executor: CommandExecutor,
    pub result_view_kind: ResultViewKind,
    pub confirmation_field: Option<&'static str>,
}

impl CommandSpec {
    pub fn matches_query(&self, query: &str) -> bool {
        let query = query.trim().to_ascii_lowercase();
        if query.is_empty() {
            return true;
        }
        self.id.to_ascii_lowercase().contains(&query)
            || self.title.to_ascii_lowercase().contains(&query)
            || self.description.to_ascii_lowercase().contains(&query)
            || self.category.as_str().to_ascii_lowercase().contains(&query)
    }

    pub fn expected_confirmation(&self, form: &CommandFormState) -> Option<String> {
        match self.risk_level {
            RiskLevel::Safe => None,
            RiskLevel::Mutating => Some("confirm".to_string()),
            RiskLevel::Dangerous => self
                .confirmation_field
                .and_then(|field| form.raw_value(field))
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .or_else(|| Some("confirm".to_string())),
        }
    }
}

mod catalog;
mod executor;

pub use catalog::command_catalog;
pub use executor::execute_command_with_progress;

#[cfg(test)]
mod tests;
