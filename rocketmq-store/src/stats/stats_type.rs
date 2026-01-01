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

#[derive(Debug, PartialEq, Copy, Clone, Default)]
pub enum StatsType {
    #[default]
    SendSuccess,
    SendFailure,
    RcvSuccess,
    RcvEpolls,
    SendBack,
    SendBackToDlq,
    SendOrder,
    SendTimer,
    SendTransaction,
    PermFailure,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default() {
        assert_eq!(StatsType::default(), StatsType::SendSuccess);
    }

    #[test]
    fn test_send_success() {
        assert_eq!(StatsType::SendSuccess, StatsType::SendSuccess);
    }

    #[test]
    fn test_send_failure() {
        assert_eq!(StatsType::SendFailure, StatsType::SendFailure);
    }

    #[test]
    fn test_rcv_success() {
        assert_eq!(StatsType::RcvSuccess, StatsType::RcvSuccess);
    }

    #[test]
    fn test_rcv_epolls() {
        assert_eq!(StatsType::RcvEpolls, StatsType::RcvEpolls);
    }

    #[test]
    fn test_send_back() {
        assert_eq!(StatsType::SendBack, StatsType::SendBack);
    }

    #[test]
    fn test_send_back_to_dlq() {
        assert_eq!(StatsType::SendBackToDlq, StatsType::SendBackToDlq);
    }

    #[test]
    fn test_send_order() {
        assert_eq!(StatsType::SendOrder, StatsType::SendOrder);
    }

    #[test]
    fn test_send_timer() {
        assert_eq!(StatsType::SendTimer, StatsType::SendTimer);
    }

    #[test]
    fn test_send_transaction() {
        assert_eq!(StatsType::SendTransaction, StatsType::SendTransaction);
    }

    #[test]
    fn test_perm_failure() {
        assert_eq!(StatsType::PermFailure, StatsType::PermFailure);
    }
}
