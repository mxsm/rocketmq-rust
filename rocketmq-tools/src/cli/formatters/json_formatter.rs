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

//! JSON formatter

use serde::Serialize;

use super::Formatter;

pub struct JsonFormatter;

impl Formatter for JsonFormatter {
    fn format<T: Serialize>(&self, data: &T) -> String {
        serde_json::to_string_pretty(data).unwrap_or_else(|e| format!("{{\"error\": \"{}\"}}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_formatter() {
        let formatter = JsonFormatter;
        let data = serde_json::json!({"name": "test", "value": 123});
        let output = formatter.format(&data);
        assert!(output.contains("\"name\""));
        assert!(output.contains("\"test\""));
    }
}
