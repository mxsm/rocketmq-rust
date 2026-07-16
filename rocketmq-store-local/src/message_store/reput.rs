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

use crate::config::backend::LocalReputConfig;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReputPolicy {
    read_uncommitted: bool,
}

impl ReputPolicy {
    pub const fn new(config: LocalReputConfig) -> Self {
        Self {
            read_uncommitted: config.read_uncommitted,
        }
    }

    pub const fn end_offset(self, confirm_offset: i64, max_offset: i64) -> i64 {
        if self.read_uncommitted {
            max_offset
        } else {
            confirm_offset
        }
    }

    pub const fn is_available(self, reput_offset: i64, confirm_offset: i64, max_offset: i64) -> bool {
        reput_offset < self.end_offset(confirm_offset, max_offset)
    }

    pub const fn has_unconfirmed(self, reput_offset: i64, confirm_offset: i64, max_offset: i64) -> bool {
        !self.read_uncommitted
            && reput_offset < max_offset
            && !self.is_available(reput_offset, confirm_offset, max_offset)
    }

    pub const fn behind_bytes(self, reput_offset: i64, confirm_offset: i64, max_offset: i64) -> u64 {
        let behind = self.end_offset(confirm_offset, max_offset).saturating_sub(reput_offset);
        if behind > 0 {
            behind as u64
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn committed_policy_stops_at_confirm_offset() {
        let policy = ReputPolicy::new(LocalReputConfig {
            read_uncommitted: false,
        });
        assert_eq!(policy.end_offset(80, 100), 80);
        assert!(!policy.is_available(80, 80, 100));
        assert!(policy.has_unconfirmed(80, 80, 100));
        assert_eq!(policy.behind_bytes(70, 80, 100), 10);
    }

    #[test]
    fn uncommitted_policy_dispatches_to_max_offset() {
        let policy = ReputPolicy::new(LocalReputConfig { read_uncommitted: true });
        assert_eq!(policy.end_offset(80, 100), 100);
        assert!(policy.is_available(80, 80, 100));
        assert!(!policy.has_unconfirmed(80, 80, 100));
    }
}
