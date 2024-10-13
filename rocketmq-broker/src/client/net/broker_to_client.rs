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
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;

use crate::error::BrokerError::BrokerClientError;
use crate::Result;

#[derive(Default, Clone)]
pub struct Broker2Client;

impl Broker2Client {
    pub async fn call_client(
        &mut self,
        channel: &mut Channel,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> Result<RemotingCommand> {
        match channel.send_wait_response(request, timeout_millis).await {
            Ok(value) => Ok(value),
            Err(e) => Err(BrokerClientError(e)),
        }
    }
}
