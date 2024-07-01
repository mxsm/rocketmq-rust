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

use std::cell::SyncUnsafeCell;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Weak;
use std::time::Duration;

use bytes::Bytes;
use rocketmq_common::common::namesrv::namesrv_config::NamesrvConfig;
use rocketmq_common::common::FAQUrl;
use rocketmq_common::TimeUtils;
use rocketmq_remoting::code::response_code::RemotingSysResponseCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::client_request_header::GetRouteInfoRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::runtime::server::ConnectionHandlerContext;
use tracing::warn;

use crate::kvconfig::kvconfig_mananger::KVConfigManager;
use crate::route::route_info_manager::RouteInfoManager;

pub struct ClientRequestProcessor {
    route_info_manager: Arc<parking_lot::RwLock<RouteInfoManager>>,
    namesrv_config: Arc<NamesrvConfig>,
    need_check_namesrv_ready: AtomicBool,
    startup_time_millis: u64,
    kvconfig_manager: Arc<parking_lot::RwLock<KVConfigManager>>,
}

impl ClientRequestProcessor {
    pub fn new(
        route_info_manager: Arc<parking_lot::RwLock<RouteInfoManager>>,
        namesrv_config: Arc<NamesrvConfig>,
        kvconfig_manager: Arc<parking_lot::RwLock<KVConfigManager>>,
    ) -> Self {
        Self {
            route_info_manager,
            namesrv_config,
            need_check_namesrv_ready: AtomicBool::new(true),
            startup_time_millis: TimeUtils::get_current_millis(),
            kvconfig_manager,
        }
    }

    fn get_route_info_by_topic(&self, request: RemotingCommand) -> RemotingCommand {
        let request_header = request
            .decode_command_custom_header::<GetRouteInfoRequestHeader>()
            .unwrap();
        let namesrv_ready = self.need_check_namesrv_ready.load(Ordering::Relaxed)
            && TimeUtils::get_current_millis() - self.startup_time_millis
                >= Duration::from_secs(self.namesrv_config.wait_seconds_for_service as u64)
                    .as_millis() as u64;
        if self.namesrv_config.need_wait_for_service && !namesrv_ready {
            warn!("name server not ready. request code {} ", request.code());
            return RemotingCommand::create_response_command_with_code(
                RemotingSysResponseCode::SystemError,
            )
            .set_remark(Some(String::from("name server not ready")));
        }
        match self
            .route_info_manager
            .read()
            .pickup_topic_route_data(request_header.topic.as_str())
        {
            None => RemotingCommand::create_response_command_with_code(ResponseCode::TopicNotExist)
                .set_remark(Some(format!(
                    "No topic route info in name server for the topic:{}{}",
                    request_header.topic,
                    FAQUrl::suggest_todo(FAQUrl::APPLY_TOPIC_URL)
                ))),
            Some(mut topic_route_data) => {
                if self.need_check_namesrv_ready.load(Ordering::Relaxed) {
                    self.need_check_namesrv_ready.store(false, Ordering::SeqCst);
                }
                if self.namesrv_config.order_message_enable {
                    //get kv config
                    let order_topic_config = self
                        .kvconfig_manager
                        .read()
                        .get_kvconfig("ORDER_TOPIC_CONFIG", request_header.topic.clone());
                    topic_route_data.order_topic_conf = order_topic_config;
                };
                /*let standard_json_only = request_header.accept_standard_json_only.unwrap_or(false);
                let content = if request.version() >= RocketMqVersion::into(RocketMqVersion::V494)
                    || standard_json_only
                {
                    //topic_route_data.encode()
                    topic_route_data.encode()
                } else {
                    topic_route_data.encode()
                };*/
                let content = topic_route_data.encode();
                RemotingCommand::create_response_command_with_code(RemotingSysResponseCode::Success)
                    .set_body(Some(Bytes::from(content)))
            }
        }
    }
}

impl ClientRequestProcessor {
    pub fn process_request(
        &self,
        _channel: Channel,
        _ctx: Weak<SyncUnsafeCell<ConnectionHandlerContext>>,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        Some(self.get_route_info_by_topic(request))
    }
}
