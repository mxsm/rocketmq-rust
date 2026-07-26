// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;

pub struct RouteClient<'a> {
    api: &'a MQClientAPIImpl,
}

impl RouteClient<'_> {
    pub async fn topic_route_info(
        &self,
        topic: &CheetahString,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<Option<TopicRouteData>> {
        self.api
            .get_topic_route_info_from_name_server(topic, timeout_millis)
            .await
    }
}

impl MQClientAPIImpl {
    #[must_use]
    pub fn route_client(&self) -> RouteClient<'_> {
        RouteClient { api: self }
    }
}

impl MQClientAPIImpl {
    #[inline]
    pub async fn get_default_topic_route_info_from_name_server(
        &self,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<Option<TopicRouteData>> {
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
    ) -> rocketmq_error::RocketMQResult<Option<TopicRouteData>> {
        self.get_topic_route_info_from_name_server_detail(topic, timeout_millis, true)
            .await
    }

    #[inline]
    pub async fn get_topic_route_info_from_name_server_detail(
        &self,
        topic: &str,
        timeout_millis: u64,
        allow_topic_not_exist: bool,
    ) -> rocketmq_error::RocketMQResult<Option<TopicRouteData>> {
        let request_header = GetRouteInfoRequestHeader {
            topic: CheetahString::from_slice(topic),
            accept_standard_json_only: None,
            topic_request_header: None,
        };
        let request = RemotingCommand::create_request_command(RequestCode::GetRouteinfoByTopic, request_header);
        let response = self.remoting_client.invoke_request(None, request, timeout_millis).await;
        match response {
            Ok(mut result) => {
                let code = result.code();
                let response_code = ResponseCode::from(code);
                match response_code {
                    ResponseCode::Success => {
                        let body = result.take_body();
                        if let Some(body_inner) = body {
                            let route_data = TopicRouteData::decode(body_inner.as_ref())?;
                            return Ok(Some(route_data));
                        }
                    }
                    ResponseCode::TopicNotExist => {
                        if allow_topic_not_exist {
                            warn!("get Topic [{}] RouteInfoFromNameServer is not exist value", topic);
                        }
                    }
                    _ => {
                        return Err(mq_client_err!(
                            code,
                            result.remark().cloned().unwrap_or_default().to_string()
                        ))
                    }
                }
                Err(mq_client_err!(
                    code,
                    result.remark().cloned().unwrap_or_default().to_string()
                ))
            }
            Err(err) => Err(err),
        }
    }

    pub fn get_name_server_address_list(&self) -> Vec<CheetahString> {
        self.remoting_client.get_name_server_address_list()
    }
}
