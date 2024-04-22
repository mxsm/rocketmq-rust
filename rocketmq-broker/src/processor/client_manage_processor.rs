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

use rocketmq_remoting::{
    code::request_code::RequestCode,
    protocol::{
        header::namesrv::broker_request::UnRegisterBrokerRequestHeader,
        remoting_command::RemotingCommand,
    },
    runtime::server::ConnectionHandlerContext,
};

#[derive(Default)]
pub struct ClientManageProcessor {
    consumer_group_heartbeat_table:
        HashMap<String /* ConsumerGroup */, i32 /* HeartbeatFingerprint */>,
}

impl ClientManageProcessor {
    pub fn new() -> Self {
        Self {
            consumer_group_heartbeat_table: HashMap::new(),
        }
    }
}

impl ClientManageProcessor {
    pub async fn process_request(
        &self,
        ctx: ConnectionHandlerContext<'_>,
        request_code: RequestCode,
        request: RemotingCommand,
    ) -> RemotingCommand {
        let response = match request_code {
            RequestCode::HeartBeat => {
                unimplemented!()
            }
            RequestCode::UnregisterClient => self.unregister_client(ctx, request),
            RequestCode::CheckClientConfig => {
                unimplemented!()
            }
            _ => {
                unimplemented!()
            }
        };
        response.unwrap()
    }
}

impl ClientManageProcessor {
    fn unregister_client(
        &self,
        ctx: ConnectionHandlerContext<'_>,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        Some(RemotingCommand::create_response_command())
    }
}
