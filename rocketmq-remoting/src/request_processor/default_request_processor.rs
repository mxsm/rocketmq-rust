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

use crate::net::channel::Channel;
use crate::protocol::remoting_command::RemotingCommand;
use crate::runtime::processor::RequestProcessor;
use crate::runtime::server::ConnectionHandlerContext;
use crate::Result;

#[derive(Clone)]
pub struct DefaultRemotingRequestProcessor;

impl RequestProcessor for DefaultRemotingRequestProcessor {
    async fn process_request(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: RemotingCommand,
    ) -> Result<Option<RemotingCommand>> {
        Ok(Some(request))
    }
}
