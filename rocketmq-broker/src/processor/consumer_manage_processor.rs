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

use bytes::Bytes;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::body::get_consumer_listby_group_response_body::GetConsumerListByGroupResponseBody;
use rocketmq_remoting::protocol::header::get_consumer_listby_group_request_header::GetConsumerListByGroupRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::runtime::server::ConnectionHandlerContext;
use tracing::warn;

use crate::client::manager::consumer_manager::ConsumerManager;

#[derive(Clone)]
pub struct ConsumerManageProcessor {
    consumer_manager: Arc<ConsumerManager>,
}

impl ConsumerManageProcessor {
    pub fn new(consumer_manager: Arc<ConsumerManager>) -> Self {
        Self { consumer_manager }
    }
}

#[allow(unused_variables)]
impl ConsumerManageProcessor {
    pub async fn process_request(
        &mut self,
        ctx: ConnectionHandlerContext<'_>,
        request_code: RequestCode,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        match request_code {
            RequestCode::GetConsumerListByGroup => {
                self.get_consumer_list_by_group(ctx, request).await
            }
            RequestCode::UpdateConsumerOffset => self.update_consumer_offset(ctx, request).await,
            RequestCode::QueryConsumerOffset => self.query_consumer_offset(ctx, request).await,
            _ => None,
        }
    }

    pub async fn get_consumer_list_by_group(
        &mut self,
        ctx: ConnectionHandlerContext<'_>,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        let response = RemotingCommand::create_response_command();
        let request_header = request
            .decode_command_custom_header::<GetConsumerListByGroupRequestHeader>()
            .unwrap();
        let consumer_group_info = self
            .consumer_manager
            .get_consumer_group_info(request_header.consumer_group.as_str());

        match consumer_group_info {
            None => {
                warn!(
                    "getConsumerGroupInfo failed, {} {}",
                    request_header.consumer_group,
                    ctx.remoting_address()
                );
            }
            Some(info) => {
                let client_ids = info.get_all_client_ids();
                if !client_ids.is_empty() {
                    let body = GetConsumerListByGroupResponseBody {
                        consumer_id_list: client_ids,
                    };
                    return Some(
                        response
                            .set_body(Some(Bytes::from(body.encode())))
                            .set_code(ResponseCode::Success),
                    );
                } else {
                    warn!(
                        "getAllClientId failed, {} {}",
                        request_header.consumer_group,
                        ctx.remoting_address()
                    )
                }
            }
        }
        Some(
            response
                .set_remark(Some(format!(
                    "no consumer for this group, {}",
                    request_header.consumer_group
                )))
                .set_code(ResponseCode::SystemError),
        )
    }

    pub async fn update_consumer_offset(
        &mut self,
        ctx: ConnectionHandlerContext<'_>,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        unimplemented!()
    }

    pub async fn query_consumer_offset(
        &mut self,
        ctx: ConnectionHandlerContext<'_>,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        unimplemented!()
    }
}
