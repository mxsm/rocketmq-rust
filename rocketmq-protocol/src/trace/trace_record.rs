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

use cheetah_string::CheetahString;

use super::TraceType;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TraceRecord {
    pub trace_type: TraceType,
    pub fields: Vec<CheetahString>,
}

impl TraceRecord {
    pub fn new(trace_type: TraceType, fields: Vec<CheetahString>) -> Self {
        Self { trace_type, fields }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fields() -> Vec<CheetahString> {
        vec![
            CheetahString::from_static_str("1710000000000"),
            CheetahString::from_static_str("region"),
            CheetahString::from_static_str("topic"),
        ]
    }

    #[test]
    fn new_stores_trace_type_and_fields_in_order() {
        let record = TraceRecord::new(TraceType::SubAfter, fields());
        assert_eq!(record.trace_type, TraceType::SubAfter);
        assert_eq!(record.fields, fields());
    }

    #[test]
    fn records_built_from_same_inputs_are_equal() {
        let left = TraceRecord::new(TraceType::Pub, fields());
        let right = TraceRecord::new(TraceType::Pub, fields());
        assert_eq!(left, right);
        assert_eq!(left.clone(), right);
    }

    #[test]
    fn records_differ_when_trace_type_or_fields_differ() {
        let base = TraceRecord::new(TraceType::Pub, fields());
        assert_ne!(base, TraceRecord::new(TraceType::Recall, fields()));
        assert_ne!(base, TraceRecord::new(TraceType::Pub, Vec::new()));
    }
}
