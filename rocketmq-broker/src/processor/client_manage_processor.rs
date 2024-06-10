/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::collections::HashMap;
use std::sync::Arc;

use rocketmq_common::common::mix_all::IS_SUB_CHANGE;
use rocketmq_common::common::mix_all::IS_SUPPORT_HEART_BEAT_V2;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::protocol::header::unregister_client_request_header::UnregisterClientRequestHeader;
use rocketmq_remoting::protocol::heartbeat::heartbeat_data::HeartbeatData;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::runtime::server::ConnectionHandlerContext;

use crate::client::client_channel_info::ClientChannelInfo;
use crate::client::manager::producer_manager::ProducerManager;

#[derive(Default, Clone)]
pub struct ClientManageProcessor {
    consumer_group_heartbeat_table:
        HashMap<String /* ConsumerGroup */, i32 /* HeartbeatFingerprint */>,
    producer_manager: Arc<ProducerManager>,
}

impl ClientManageProcessor {
    pub fn new(producer_manager: Arc<ProducerManager>) -> Self {
        Self {
            consumer_group_heartbeat_table: HashMap::new(),
            producer_manager,
        }
    }
}

impl ClientManageProcessor {
    pub async fn process_request(
        &self,
        ctx: ConnectionHandlerContext<'_>,
        request_code: RequestCode,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        match request_code {
            RequestCode::HeartBeat => self.heart_beat(ctx, request),
            RequestCode::UnregisterClient => self.unregister_client(ctx, request),
            RequestCode::CheckClientConfig => {
                unimplemented!("CheckClientConfig")
            }
            _ => {
                unimplemented!("CheckClientConfig")
            }
        }
    }
}

impl ClientManageProcessor {
    fn unregister_client(
        &self,
        ctx: ConnectionHandlerContext<'_>,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        let request_header = request
            .decode_command_custom_header::<UnregisterClientRequestHeader>()
            .unwrap();

        let client_channel_info = ClientChannelInfo::new(
            ctx.remoting_address().to_string(),
            request_header.client_id.clone(),
            request.language(),
            request.version(),
        );

        if let Some(ref group) = request_header.producer_group {
            self.producer_manager
                .unregister_producer(group, &client_channel_info);
        }

        if let Some(ref _group) = request_header.consumer_group {
            unimplemented!()
        }

        Some(RemotingCommand::create_response_command())
    }

    fn heart_beat(
        &self,
        ctx: ConnectionHandlerContext<'_>,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        let heartbeat_data =
            HeartbeatData::decode(request.body().as_ref().map(|v| v.as_ref()).unwrap());
        let client_channel_info = ClientChannelInfo::new(
            ctx.remoting_address().to_string(),
            heartbeat_data.client_id.clone(),
            request.language(),
            request.version(),
        );
        if heartbeat_data.heartbeat_fingerprint != 0 {
            return self.heart_beat_v2(ctx, heartbeat_data, client_channel_info);
        }

        //do consumer data handle

        //do producer data handle
        for producer_data in heartbeat_data.producer_data_set.iter() {
            self.producer_manager
                .register_producer(&producer_data.group_name, &client_channel_info);
        }
        let mut response_command = RemotingCommand::create_response_command();
        response_command.add_ext_field(IS_SUPPORT_HEART_BEAT_V2.to_string(), true.to_string());
        response_command.add_ext_field(IS_SUB_CHANGE.to_string(), true.to_string());
        Some(response_command)
    }

    fn heart_beat_v2(
        &self,
        _ctx: ConnectionHandlerContext<'_>,
        heartbeat_data: HeartbeatData,
        client_channel_info: ClientChannelInfo,
    ) -> Option<RemotingCommand> {
        let is_sub_change = false;
        //handle consumer data

        //handle producer data
        for producer_data in heartbeat_data.producer_data_set.iter() {
            self.producer_manager
                .register_producer(&producer_data.group_name, &client_channel_info);
        }
        let mut response_command = RemotingCommand::create_response_command();
        response_command.add_ext_field(IS_SUPPORT_HEART_BEAT_V2.to_string(), true.to_string());
        response_command.add_ext_field(IS_SUB_CHANGE.to_string(), is_sub_change.to_string());
        Some(response_command)
    }
}
