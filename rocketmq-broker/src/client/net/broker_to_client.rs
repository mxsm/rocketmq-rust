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
use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::MessageDecoder;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::check_transaction_state_request_header::CheckTransactionStateRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;

use crate::broker_error::BrokerError::BrokerCommonError;
use crate::broker_error::BrokerError::BrokerRemotingError;
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
        match channel.upgrade() {
            None => {
                unimplemented!("channel is closed");
            }
            Some(mut channel) => match channel.send_wait_response(request, timeout_millis).await {
                Ok(value) => Ok(value),
                Err(e) => Err(BrokerRemotingError(e)),
            },
        }
    }

    pub async fn check_producer_transaction_state(
        &self,
        _group: &CheetahString,
        channel: &mut Channel,
        request_header: CheckTransactionStateRequestHeader,
        message_ext: MessageExt,
    ) -> Result<()> {
        let mut request = RemotingCommand::create_request_command(
            RequestCode::CheckTransactionState,
            request_header,
        );
        match MessageDecoder::encode(&message_ext, false) {
            Ok(body) => {
                request.set_body_mut_ref(body);
            }
            Err(e) => {
                return Err(BrokerCommonError(e));
            }
        }
        match channel.upgrade() {
            None => {
                unimplemented!("channel is closed");
            }
            Some(channel) => match channel.send_one_way(request, 100).await {
                Ok(_) => Ok(()),
                Err(e) => Err(BrokerRemotingError(e)),
            },
        }
    }
}
