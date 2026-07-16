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

use bytes::Bytes;
use rocketmq_model::result::PullStatus;

/// Broker-local transport response retained only until pull bytes are decoded.
pub(super) struct BrokerPullResponse {
    pub(super) status: PullStatus,
    pub(super) next_begin_offset: u64,
    pub(super) min_offset: u64,
    pub(super) max_offset: u64,
    pub(super) message_binary: Option<Bytes>,
    pub(super) offset_delta: Option<i64>,
}
