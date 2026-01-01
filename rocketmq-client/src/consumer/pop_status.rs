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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PopStatus {
    /// Founded
    #[default]
    Found,
    /// No new message can be pulled after polling timeout
    NoNewMsg,
    /// Polling pool is full, do not try again immediately
    PollingFull,
    /// Polling timeout but no message found
    PollingNotFound,
}

impl Display for PopStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PopStatus::Found => write!(f, "FOUND"),
            PopStatus::NoNewMsg => write!(f, "NO_NEW_MSG"),
            PopStatus::PollingFull => write!(f, "POLLING_FULL"),
            PopStatus::PollingNotFound => write!(f, "POLLING_NOT_FOUND"),
        }
    }
}

impl From<PopStatus> for i32 {
    fn from(status: PopStatus) -> i32 {
        match status {
            PopStatus::Found => 0,
            PopStatus::NoNewMsg => 1,
            PopStatus::PollingFull => 2,
            PopStatus::PollingNotFound => 3,
        }
    }
}

impl From<i32> for PopStatus {
    fn from(status: i32) -> Self {
        match status {
            0 => PopStatus::Found,
            1 => PopStatus::NoNewMsg,
            2 => PopStatus::PollingFull,
            3 => PopStatus::PollingNotFound,
            _ => PopStatus::Found,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pop_status_display_found() {
        assert_eq!(PopStatus::Found.to_string(), "FOUND");
    }

    #[test]
    fn pop_status_display_no_new_msg() {
        assert_eq!(PopStatus::NoNewMsg.to_string(), "NO_NEW_MSG");
    }

    #[test]
    fn pop_status_display_polling_full() {
        assert_eq!(PopStatus::PollingFull.to_string(), "POLLING_FULL");
    }

    #[test]
    fn pop_status_display_polling_not_found() {
        assert_eq!(PopStatus::PollingNotFound.to_string(), "POLLING_NOT_FOUND");
    }

    #[test]
    fn pop_status_from_i32_found() {
        assert_eq!(PopStatus::from(0), PopStatus::Found);
    }

    #[test]
    fn pop_status_from_i32_no_new_msg() {
        assert_eq!(PopStatus::from(1), PopStatus::NoNewMsg);
    }

    #[test]
    fn pop_status_from_i32_polling_full() {
        assert_eq!(PopStatus::from(2), PopStatus::PollingFull);
    }

    #[test]
    fn pop_status_from_i32_polling_not_found() {
        assert_eq!(PopStatus::from(3), PopStatus::PollingNotFound);
    }

    #[test]
    fn pop_status_from_i32_default() {
        assert_eq!(PopStatus::from(999), PopStatus::Found);
    }

    #[test]
    fn pop_status_into_i32_found() {
        let status: i32 = PopStatus::Found.into();
        assert_eq!(status, 0);
    }

    #[test]
    fn pop_status_into_i32_no_new_msg() {
        let status: i32 = PopStatus::NoNewMsg.into();
        assert_eq!(status, 1);
    }

    #[test]
    fn pop_status_into_i32_polling_full() {
        let status: i32 = PopStatus::PollingFull.into();
        assert_eq!(status, 2);
    }

    #[test]
    fn pop_status_into_i32_polling_not_found() {
        let status: i32 = PopStatus::PollingNotFound.into();
        assert_eq!(status, 3);
    }
}
