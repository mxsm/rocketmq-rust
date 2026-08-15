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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TransactionPendingSample {
    pub topic: String,
    pub count: i64,
}

impl TransactionPendingSample {
    pub(crate) fn new(topic: impl Into<String>, count: i64) -> Self {
        Self {
            topic: topic.into(),
            count,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TransactionPendingSample;

    #[test]
    fn transaction_pending_sample_keeps_java_pending_semantics() {
        let sample = TransactionPendingSample::new("orders", 7);

        assert_eq!(sample.topic, "orders");
        assert_eq!(sample.count, 7);
    }
}
