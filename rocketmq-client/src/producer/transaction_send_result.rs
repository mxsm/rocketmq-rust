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

use serde::Deserialize;
use serde::Serialize;

use crate::producer::local_transaction_state::LocalTransactionState;
use crate::producer::send_result::SendResult;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TransactionSendResult {
    pub local_transaction_state: Option<LocalTransactionState>,

    #[serde(flatten)]
    pub send_result: Option<SendResult>,
}

impl Display for TransactionSendResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TransactionSendResult {{ local_transaction_state: {:?}, send_result: {:?} }}",
            self.local_transaction_state, self.send_result
        )
    }
}
