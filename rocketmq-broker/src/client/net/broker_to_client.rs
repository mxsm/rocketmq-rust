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

#[derive(Default, Clone)]
pub struct Broker2Client;

#[allow(unused)]
impl Broker2Client {
    pub async fn call_client(
        &mut self,
        channel: &mut Channel,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        channel
            .channel_inner_mut()
            .send_wait_response(request, timeout_millis)
            .await
    }

    pub async fn check_producer_transaction_state(
        &self,
        group: &CheetahString,
        channel: &mut Channel,
        request_header: CheckTransactionStateRequestHeader,
        message_ext: MessageExt,
    ) -> rocketmq_error::RocketMQResult<()> {
        let mut request = RemotingCommand::create_request_command(
            RequestCode::CheckTransactionState,
            request_header,
        );
        match MessageDecoder::encode(&message_ext, false) {
            Ok(body) => {
                request.set_body_mut_ref(body);
            }
            Err(e) => {
                return Err(e);
            }
        }
        channel
            .channel_inner_mut()
            .send_one_way(request, 100)
            .await?;
        Ok(())
    }
}
