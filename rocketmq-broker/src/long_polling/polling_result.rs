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

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default, Hash)]
pub enum PollingResult {
    #[default]
    PollingSuc,
    PollingFull,
    PollingTimeout,
    NotPolling,
}

impl Display for PollingResult {
    //compatible with the toString method in java
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PollingResult::PollingSuc => write!(f, "POLLING_SUC"),
            PollingResult::PollingFull => write!(f, "POLLING_FULL"),
            PollingResult::PollingTimeout => write!(f, "POLLING_TIMEOUT"),
            PollingResult::NotPolling => write!(f, "NOT_POLLING"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn polling_result_display_polling_suc() {
        let result = PollingResult::PollingSuc;
        assert_eq!(format!("{}", result), "POLLING_SUC");
    }

    #[test]
    fn polling_result_display_polling_full() {
        let result = PollingResult::PollingFull;
        assert_eq!(format!("{}", result), "POLLING_FULL");
    }

    #[test]
    fn polling_result_display_polling_timeout() {
        let result = PollingResult::PollingTimeout;
        assert_eq!(format!("{}", result), "POLLING_TIMEOUT");
    }

    #[test]
    fn polling_result_display_not_polling() {
        let result = PollingResult::NotPolling;
        assert_eq!(format!("{}", result), "NOT_POLLING");
    }

    #[test]
    fn polling_result_default() {
        let result: PollingResult = Default::default();
        assert_eq!(result, PollingResult::PollingSuc);
    }
}
