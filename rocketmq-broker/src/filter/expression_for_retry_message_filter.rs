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

use std::collections::HashMap;

use cheetah_string::CheetahString;
use rocketmq_store::consume_queue::cq_ext_unit::CqExtUnit;
use rocketmq_store::filter::MessageFilter;

pub struct ExpressionForRetryMessageFilter;

#[allow(unused_variables)]
impl MessageFilter for ExpressionForRetryMessageFilter {
    fn is_matched_by_consume_queue(&self, tags_code: Option<i64>, cq_ext_unit: Option<&CqExtUnit>) -> bool {
        true
    }

    fn is_matched_by_commit_log(
        &self,
        msg_buffer: Option<&[u8]>,
        properties: Option<&HashMap<CheetahString, CheetahString>>,
    ) -> bool {
        true
    }
}
