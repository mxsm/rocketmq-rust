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

use std::fmt::Display;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum TraceType {
    #[default]
    Pub,
    SubBefore,
    SubAfter,
    EndTransaction,
}

impl Display for TraceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TraceType::Pub => write!(f, "Pub"),
            TraceType::SubBefore => write!(f, "SubBefore"),
            TraceType::SubAfter => write!(f, "SubAfter"),
            TraceType::EndTransaction => write!(f, "EndTransaction"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Write;

    use super::*;

    #[test]
    fn trace_type_default_value() {
        let trace_type = TraceType::default();
        assert_eq!(trace_type, TraceType::Pub);
    }

    #[test]
    fn trace_type_display_pub() {
        let trace_type = TraceType::Pub;
        let mut output = String::new();
        write!(&mut output, "{}", trace_type).unwrap();
        assert_eq!(output, "Pub");
    }

    #[test]
    fn trace_type_display_sub_before() {
        let trace_type = TraceType::SubBefore;
        let mut output = String::new();
        write!(&mut output, "{}", trace_type).unwrap();
        assert_eq!(output, "SubBefore");
    }

    #[test]
    fn trace_type_display_sub_after() {
        let trace_type = TraceType::SubAfter;
        let mut output = String::new();
        write!(&mut output, "{}", trace_type).unwrap();
        assert_eq!(output, "SubAfter");
    }

    #[test]
    fn trace_type_display_end_transaction() {
        let trace_type = TraceType::EndTransaction;
        let mut output = String::new();
        write!(&mut output, "{}", trace_type).unwrap();
        assert_eq!(output, "EndTransaction");
    }
}
