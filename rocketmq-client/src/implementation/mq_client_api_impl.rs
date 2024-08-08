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

use rocketmq_common::common::mix_all;
use rocketmq_common::common::namesrv::default_top_addressing::DefaultTopAddressing;
use rocketmq_common::common::namesrv::name_server_update_callback::NameServerUpdateCallback;
use rocketmq_common::common::namesrv::top_addressing::TopAddressing;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_remoting::clients::rocketmq_default_impl::RocketmqDefaultClient;
use rocketmq_remoting::clients::RemotingClient;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::header::client_request_header::GetRouteInfoRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_remoting::protocol::RemotingDeserializable;
use rocketmq_remoting::remoting::RemotingService;
use rocketmq_remoting::runtime::config::client_config::TokioClientConfig;
use rocketmq_remoting::runtime::RPCHook;
use tracing::warn;

use crate::base::client_config::ClientConfig;
use crate::error::MQClientError;
use crate::implementation::client_remoting_processor::ClientRemotingProcessor;
use crate::Result;

pub struct MQClientAPIImpl {
    remoting_client: RocketmqDefaultClient,
    top_addressing: Box<dyn TopAddressing>,
    client_remoting_processor: ClientRemotingProcessor,
    name_srv_addr: Option<String>,
    client_config: ClientConfig,
}

impl NameServerUpdateCallback for MQClientAPIImpl {
    fn on_name_server_address_changed(&self, namesrv_address: Option<String>) -> String {
        unimplemented!("on_name_server_address_changed")
    }
}

impl MQClientAPIImpl {
    pub fn new(
        tokio_client_config: Arc<TokioClientConfig>,
        client_remoting_processor: ClientRemotingProcessor,
        rpc_hook: Option<Arc<Box<dyn RPCHook>>>,
        client_config: ClientConfig,
    ) -> Self {
        let mut default_client = RocketmqDefaultClient::new(tokio_client_config);
        if let Some(hook) = rpc_hook {
            default_client.register_rpc_hook(hook);
        }

        MQClientAPIImpl {
            remoting_client: default_client,
            top_addressing: Box::new(DefaultTopAddressing::new(
                mix_all::get_ws_addr(),
                client_config.unit_name.clone(),
            )),
            client_remoting_processor,
            name_srv_addr: None,
            client_config,
        }
    }

    pub async fn start(&self) {
        self.remoting_client.start().await;
    }

    pub async fn fetch_name_server_addr(&mut self) -> Option<String> {
        let addrs = self.top_addressing.fetch_ns_addr();
        if addrs.is_some() && !addrs.as_ref().unwrap().is_empty() {
            let mut notify = false;
            if let Some(addr) = self.name_srv_addr.as_mut() {
                let addrs = addrs.unwrap();
                if addr != addrs.as_str() {
                    *addr = addrs.clone();
                    notify = true;
                }
            }
            if notify {
                let name_srv = self.name_srv_addr.as_ref().unwrap().as_str();
                self.update_name_server_address_list(name_srv).await;
                return Some(name_srv.to_string());
            }
        }

        self.name_srv_addr.clone()
    }

    pub async fn update_name_server_address_list(&self, addrs: &str) {
        let addr_vec = addrs
            .split(";")
            .map(|s| s.to_string())
            .collect::<Vec<String>>();
        self.remoting_client
            .update_name_server_address_list(addr_vec)
            .await;
    }

    #[inline]
    pub async fn get_default_topic_route_info_from_name_server(
        &self,
        timeout_millis: u64,
    ) -> Result<Option<TopicRouteData>> {
        self.get_topic_route_info_from_name_server_detail(
            TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC,
            timeout_millis,
            false,
        )
        .await
    }

    #[inline]
    pub async fn get_topic_route_info_from_name_server(
        &self,
        topic: &str,
        timeout_millis: u64,
    ) -> Result<Option<TopicRouteData>> {
        self.get_topic_route_info_from_name_server_detail(topic, timeout_millis, true)
            .await
    }

    #[inline]
    pub async fn get_topic_route_info_from_name_server_detail(
        &self,
        topic: &str,
        timeout_millis: u64,
        allow_topic_not_exist: bool,
    ) -> Result<Option<TopicRouteData>> {
        let request_header = GetRouteInfoRequestHeader {
            topic: topic.to_string(),
            accept_standard_json_only: None,
            topic_request_header: None,
        };
        let request = RemotingCommand::create_request_command(
            RequestCode::GetRouteinfoByTopic,
            request_header,
        );
        let response = self
            .remoting_client
            .invoke_async(None, request, timeout_millis)
            .await;
        match response {
            Ok(result) => {
                let code = result.code();
                let response_code = ResponseCode::from(code);
                match response_code {
                    ResponseCode::Success => {
                        let body = result.body();
                        if body.is_some() && !body.as_ref().unwrap().is_empty() {
                            let route_data =
                                TopicRouteData::decode(body.as_ref().unwrap().as_ref());
                            if let Ok(data) = route_data {
                                return Ok(Some(data));
                            }
                        }
                    }
                    ResponseCode::TopicNotExist => {
                        if allow_topic_not_exist {
                            warn!(
                                "get Topic [{}] RouteInfoFromNameServer is not exist value",
                                topic
                            );
                        }
                    }
                    _ => {
                        return Err(MQClientError::MQClientException(
                            code,
                            result.remark().cloned().unwrap_or_default(),
                        ))
                    }
                }
                return Err(MQClientError::MQClientException(
                    code,
                    result.remark().cloned().unwrap_or_default(),
                ));
            }
            Err(err) => Err(MQClientError::RemotingException(err)),
        }
    }

    pub fn get_name_server_address_list(&self) -> Vec<String> {
        self.remoting_client.get_name_server_address_list()
    }
}
