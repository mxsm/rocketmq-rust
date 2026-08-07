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
use rocketmq_macros::RequestHeaderCodecV3;
use serde::Deserialize;
use serde::Serialize;

use crate::rpc::rpc_request_header::RpcRequestHeader;

#[derive(Clone, Debug, Serialize, Deserialize, RequestHeaderCodecV3)]
#[header(
    type_id = "rocketmq_protocol::protocol::header::check_rocksdb_cq_write_progress_request_header::CheckRocksdbCqWriteProgressRequestHeader",
    java_class = "org.apache.rocketmq.remoting.protocol.header.CheckRocksdbCqWriteProgressRequestHeader"
)]
#[serde(rename_all = "camelCase")]
pub struct CheckRocksdbCqWriteProgressRequestHeader {
    #[header(required)]
    pub topic: CheetahString,

    #[serde(default)]
    #[header(default, default_semantic = "literal:0")]
    pub check_store_time: i64,

    #[serde(flatten)]
    #[header(flatten, presence = "always")]
    pub rpc: Option<RpcRequestHeader>,
}
