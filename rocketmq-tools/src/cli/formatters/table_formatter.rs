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

//! Table formatter using tabled

use serde::Serialize;
use tabled::settings::Style;
use tabled::Table;
use tabled::Tabled;

use super::Formatter;

pub struct TableFormatter;

impl Formatter for TableFormatter {
    fn format<T: Serialize>(&self, data: &T) -> String {
        // For tabled to work, T must implement Tabled
        // This is a simplified version - convert to JSON first for display
        serde_json::to_string_pretty(data).unwrap_or_else(|e| format!("Error: {}", e))
    }
}

impl TableFormatter {
    /// Format data that implements Tabled trait
    pub fn format_tabled<T: Tabled>(data: &[T]) -> String {
        let mut table = Table::new(data);
        table.with(Style::extended());
        table.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Tabled)]
    struct TestData {
        name: String,
        value: i32,
    }

    #[test]
    fn test_table_formatter() {
        let data = vec![
            TestData {
                name: "test1".to_string(),
                value: 1,
            },
            TestData {
                name: "test2".to_string(),
                value: 2,
            },
        ];

        let output = TableFormatter::format_tabled(&data);
        assert!(output.contains("name"));
        assert!(output.contains("test1"));
    }
}
