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

use crate::config::backend::LocalQueryConfig;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexQuerySafety {
    pub safe: bool,
    pub safe_offset: i64,
    pub confirm_offset: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryPolicy {
    message_index_enabled: bool,
}

impl QueryPolicy {
    pub const fn new(config: LocalQueryConfig) -> Self {
        Self {
            message_index_enabled: config.message_index_enabled,
        }
    }

    pub fn index_safety(self, safe_offset: i64, confirm_offset: i64) -> IndexQuerySafety {
        let confirm_offset = confirm_offset.max(0);
        IndexQuerySafety {
            safe: !self.message_index_enabled || safe_offset >= confirm_offset,
            safe_offset,
            confirm_offset,
        }
    }

    pub const fn should_record_degradation(self, result_is_empty: bool, safety: IndexQuerySafety) -> bool {
        result_is_empty && !safety.safe
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_index_is_always_safe() {
        let policy = QueryPolicy::new(LocalQueryConfig {
            message_index_enabled: false,
        });
        assert!(policy.index_safety(0, 100).safe);
    }

    #[test]
    fn empty_result_is_degraded_only_while_index_lags() {
        let policy = QueryPolicy::new(LocalQueryConfig {
            message_index_enabled: true,
        });
        let lagging = policy.index_safety(99, 100);
        assert!(policy.should_record_degradation(true, lagging));
        assert!(!policy.should_record_degradation(false, lagging));
        assert!(!policy.should_record_degradation(true, policy.index_safety(100, 100)));
    }
}
