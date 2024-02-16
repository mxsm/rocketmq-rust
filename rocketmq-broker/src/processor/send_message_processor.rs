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
use rocketmq_remoting::{
    code::request_code::RequestCode,
    protocol::{
        header::message_operation_header::send_message_request_header::parse_request_header,
        remoting_command::RemotingCommand,
    },
    runtime::{processor::RequestProcessor, server::ConnectionHandlerContext},
};

#[derive(Default)]
pub struct SendMessageProcessor {
    inner: SendMessageProcessorInner,
}

impl RequestProcessor for SendMessageProcessor {
    fn process_request(
        &mut self,
        ctx: ConnectionHandlerContext,
        request: RemotingCommand,
    ) -> RemotingCommand {
        let request_code = RequestCode::from(request.code());
        match request_code {
            RequestCode::ConsumerSendMsgBack => self.inner.consumer_send_msg_back(&ctx, &request),
            _ => {
                let _request_header = parse_request_header(&request).unwrap();
            }
        }
        RemotingCommand::create_response_command()
    }
}

#[derive(Default)]
struct SendMessageProcessorInner {}

impl SendMessageProcessorInner {
    fn consumer_send_msg_back(
        &mut self,
        _ctx: &ConnectionHandlerContext,
        _request: &RemotingCommand,
    ) {
        todo!()
    }
}
