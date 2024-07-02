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

use std::sync::Arc;

use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_remoting::runtime::server::ConnectionHandlerContext;
use tracing::info;

pub use self::client_request_processor::ClientRequestProcessor;
use crate::processor::default_request_processor::DefaultRequestProcessor;

mod client_request_processor;
pub mod default_request_processor;

pub struct NameServerRequestProcessor {
    pub(crate) client_request_processor: Arc<ClientRequestProcessor>,
    pub(crate) default_request_processor: Arc<DefaultRequestProcessor>,
}

impl Clone for NameServerRequestProcessor {
    fn clone(&self) -> Self {
        Self {
            client_request_processor: self.client_request_processor.clone(),
            default_request_processor: self.default_request_processor.clone(),
        }
    }
}

impl RequestProcessor for NameServerRequestProcessor {
    async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        let request_code = RequestCode::from(request.code());
        info!("process_request: {:?}", request_code);
        match request_code {
            RequestCode::GetRouteinfoByTopic => self
                .client_request_processor
                .process_request(channel, ctx, request),
            _ => self
                .default_request_processor
                .process_request(channel, ctx, request),
        }
    }
}
