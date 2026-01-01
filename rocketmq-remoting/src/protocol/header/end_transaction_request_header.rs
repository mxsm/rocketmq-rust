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

use cheetah_string::CheetahString;
use rocketmq_macros::RequestHeaderCodecV2;
use serde::Deserialize;
use serde::Serialize;

use crate::rpc::rpc_request_header::RpcRequestHeader;

#[derive(Clone, Debug, Serialize, Deserialize, Default, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct EndTransactionRequestHeader {
    #[required]
    pub topic: CheetahString,

    #[required]
    pub producer_group: CheetahString,

    //ConsumeQueue Offset
    #[required]
    pub tran_state_table_offset: u64,

    // Offset of the message in the CommitLog
    #[required]
    pub commit_log_offset: u64,

    //TRANSACTION_COMMIT_TYPE,TRANSACTION_ROLLBACK_TYPE,TRANSACTION_NOT_TYPE
    #[required]
    pub commit_or_rollback: i32,

    //Whether the check-back is initiated by the Broker
    #[required]
    pub from_transaction_check: bool,

    #[required]
    pub msg_id: CheetahString,

    pub transaction_id: Option<CheetahString>,

    #[serde(flatten)]
    pub rpc_request_header: RpcRequestHeader,
}
