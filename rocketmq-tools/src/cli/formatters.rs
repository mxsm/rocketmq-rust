// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! CLI output formatters
//!
//! Provides multiple output formats: JSON, Table, YAML

mod json_formatter;
mod table_formatter;
mod yaml_formatter;

pub use json_formatter::JsonFormatter;
use serde::Serialize;
pub use table_formatter::TableFormatter;
pub use yaml_formatter::YamlFormatter;

/// Output format enum
#[derive(Debug, Clone, Copy, Default)]
pub enum OutputFormat {
    #[default]
    Table,
    Json,
    Yaml,
}

impl From<&str> for OutputFormat {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "json" => Self::Json,
            "yaml" | "yml" => Self::Yaml,
            _ => Self::Table,
        }
    }
}

/// Formatter trait for output formatting
pub trait Formatter {
    /// Format data to string
    fn format<T: Serialize>(&self, data: &T) -> String;
}

/// Formatter enum that holds concrete implementations
pub enum FormatterType {
    Json(JsonFormatter),
    Table(TableFormatter),
    Yaml(YamlFormatter),
}

impl FormatterType {
    pub fn format<T: Serialize>(&self, data: &T) -> String {
        match self {
            Self::Json(f) => f.format(data),
            Self::Table(f) => f.format(data),
            Self::Yaml(f) => f.format(data),
        }
    }
}

impl From<OutputFormat> for FormatterType {
    fn from(format: OutputFormat) -> Self {
        match format {
            OutputFormat::Json => Self::Json(JsonFormatter),
            OutputFormat::Table => Self::Table(TableFormatter),
            OutputFormat::Yaml => Self::Yaml(YamlFormatter),
        }
    }
}

/// Get formatter by format type
pub fn get_formatter(format: OutputFormat) -> FormatterType {
    FormatterType::from(format)
}
