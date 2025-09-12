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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use cheetah_string::CheetahString;
use rocketmq_common::common::FAQUrl;
use rocketmq_common::TimeUtils;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::RemotingSysResponseCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::client_request_header::GetRouteInfoRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_rust::ArcMut;
use tracing::info;
use tracing::warn;

use crate::bootstrap::NameServerRuntimeInner;
use crate::processor::NAMESPACE_ORDER_TOPIC_CONFIG;

pub struct ClientRequestProcessor {
    name_server_runtime_inner: ArcMut<NameServerRuntimeInner>,
    need_check_namesrv_ready: AtomicBool,
    startup_time_millis: u64,
}

impl RequestProcessor for ClientRequestProcessor {
    #[inline]
    async fn process_request(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_code = RequestCode::from(request.code());
        info!(
            "Name server ClientRequestProcessor Received request code: {:?}",
            request_code
        );

        self.get_route_info_by_topic(request)
    }
}

impl ClientRequestProcessor {
    pub(crate) fn new(name_server_runtime_inner: ArcMut<NameServerRuntimeInner>) -> Self {
        Self {
            need_check_namesrv_ready: AtomicBool::new(true),
            startup_time_millis: TimeUtils::get_current_millis(),
            name_server_runtime_inner,
        }
    }

    fn get_route_info_by_topic(
        &self,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<GetRouteInfoRequestHeader>()?;
        let wait_seconds = self
            .name_server_runtime_inner
            .name_server_config()
            .wait_seconds_for_service as u64;
        let elapsed_millis =
            TimeUtils::get_current_millis().saturating_sub(self.startup_time_millis);
        let namesrv_ready = self.need_check_namesrv_ready.load(Ordering::Relaxed)
            && elapsed_millis >= wait_seconds * 1000;

        let need_wait_for_service = self
            .name_server_runtime_inner
            .name_server_config()
            .need_wait_for_service;
        if need_wait_for_service && !namesrv_ready {
            warn!("name server not ready. request code {} ", request.code());
            return Ok(Some(
                RemotingCommand::create_response_command_with_code(
                    RemotingSysResponseCode::SystemError,
                )
                .set_remark("name server not ready"),
            ));
        }
        match self
            .name_server_runtime_inner
            .route_info_manager()
            .pickup_topic_route_data(request_header.topic.as_ref())
        {
            None => Ok(Some(
                RemotingCommand::create_response_command_with_code(ResponseCode::TopicNotExist)
                    .set_remark(format!(
                        "No topic route info in name server for the topic:{}{}",
                        request_header.topic,
                        FAQUrl::suggest_todo(FAQUrl::APPLY_TOPIC_URL)
                    )),
            )),
            Some(mut topic_route_data) => {
                if self.need_check_namesrv_ready.load(Ordering::Acquire) {
                    self.need_check_namesrv_ready
                        .store(false, Ordering::Release);
                }
                if self
                    .name_server_runtime_inner
                    .name_server_config()
                    .order_message_enable
                {
                    //get kv config
                    let order_topic_config = self
                        .name_server_runtime_inner
                        .kvconfig_manager()
                        .get_kvconfig(
                            &CheetahString::from_static_str(NAMESPACE_ORDER_TOPIC_CONFIG),
                            &request_header.topic,
                        );
                    topic_route_data.order_topic_conf = order_topic_config;
                };
                //Rust only support standard JSON serialization
                let content = topic_route_data.encode()?;
                Ok(Some(
                    RemotingCommand::create_response_command_with_code(ResponseCode::Success)
                        .set_body(content),
                ))
            }
        }
    }
}
