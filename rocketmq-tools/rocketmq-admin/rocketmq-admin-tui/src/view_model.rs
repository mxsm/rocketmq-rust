// Copyright 2026 The RocketMQ Rust Authors
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
    pub(crate) fn retained_bytes(&self) -> usize {
        match self {
            Self::Table(table) => {
                table.title.len()
                    + table.headers.iter().map(String::len).sum::<usize>()
                    + table
                        .rows
                        .iter()
                        .flat_map(|row| row.iter())
                        .map(String::len)
                        .sum::<usize>()
            }
            Self::KeyValue(values) => {
                values.title.len()
                    + values
                        .rows
                        .iter()
                        .map(|(key, value)| key.len().saturating_add(value.len()))
                        .sum::<usize>()
            }
            Self::Json { title, body } | Self::Text { title, body } => title.len().saturating_add(body.len()),
            Self::OperationSummary(summary) => {
                summary.title.len()
                    + summary.targets.iter().map(String::len).sum::<usize>()
                    + summary.errors.iter().map(String::len).sum::<usize>()
            }
        }
    }
}

mod conversions;

#[cfg(test)]
mod tests;
