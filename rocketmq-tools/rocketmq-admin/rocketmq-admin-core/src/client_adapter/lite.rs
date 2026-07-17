// Copyright 2026 The RocketMQ Rust Authors
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
use rocketmq_client_rust::MQAdminExt;

use crate::client_adapter::lifecycle::AdminSession;
use crate::core::lite::GetLiteBrokerInfoRequest;
use crate::core::lite::LiteAdmin;
use crate::core::lite::LiteBrokerInfo;
use crate::core::AdminError;
use crate::core::AdminFuture;

impl LiteAdmin for AdminSession {
    fn get_lite_broker_info<'a>(
        &'a mut self,
        request: &'a GetLiteBrokerInfoRequest,
    ) -> AdminFuture<'a, LiteBrokerInfo> {
        Box::pin(async move {
            self.ensure_open()?;
            let info = self
                .inner
                .get_broker_lite_info(CheetahString::from(request.broker_addr.as_str()))
                .await
                .map_err(|error| AdminError::backend("get_broker_lite_info", error.to_string()))?;
            Ok(LiteBrokerInfo {
                store_type: info.get_store_type().map(ToString::to_string),
                max_lmq_num: info.get_max_lmq_num(),
                current_lmq_num: info.get_current_lmq_num(),
                lite_subscription_count: info.get_lite_subscription_count(),
                order_info_count: info.get_order_info_count(),
                consume_queue_count: info.get_cq_table_size(),
                offset_count: info.get_offset_table_size(),
                event_count: info.get_event_map_size(),
            })
        })
    }
}
