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

use rocketmq_model::common::mix_all::MASTER_ID;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::protocol::header::reset_master_flush_offset_header::ResetMasterFlushOffsetHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_store::BrokerAdminStore;

use crate::broker::broker_admin_runtime::BrokerAdminRuntime;

pub struct ResetMasterFlushOffsetHandler;

impl ResetMasterFlushOffsetHandler {
    pub const fn new() -> Self {
        Self
    }

    pub async fn reset_master_flush_offset<MS: BrokerAdminStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let broker_id = broker_runtime_inner.broker_config().broker_identity.broker_id;
        if broker_id != MASTER_ID {
            let request_header = request.decode_required_header::<ResetMasterFlushOffsetHeader>(
                "decode reset-master-flush-offset request header",
            )?;

            if let Some(maset_flush_offset) = request_header.master_flush_offset {
                if let Some(message_store) = broker_runtime_inner.message_store() {
                    message_store.set_master_flushed_offset(maset_flush_offset);
                }
            }
        }

        Ok(Some(RemotingCommand::create_success_response_command()))
    }
}
