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
#[cfg(feature = "admin-full")]
impl MqClientAdminInner for MQClientAPIImpl {
    async fn query_message(
        &self,
        address: &str,
        unique_key_flag: bool,
        decompress_body: bool,
        request_header: QueryMessageRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<Vec<MessageExt>> {
        let topic = request_header.topic.clone();
        let key = request_header.key.clone();
        let mut request = self.create_request_command(RequestCode::QueryMessage, request_header);
        request.ensure_ext_fields_initialized();
        request.add_ext_field(
            mix_all::UNIQUE_MSG_QUERY_FLAG,
            CheetahString::from_static_str(if unique_key_flag { "true" } else { "false" }),
        );

        let outcome = self.invoke_admin_request(address, request, timeout_millis).await;
        let mut response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(admin_request_error("query_message", RetryInput::Rejected(rejection)));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(admin_request_error("query_message", RetryInput::Contract(contract)));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                let Some(mut body) = response.take_body() else {
                    return Err(mq_client_err!("query_message response body is empty"));
                };
                Ok(MessageDecoder::decodes_batch(&mut body, true, decompress_body)
                    .into_iter()
                    .filter(|msg| admin_message_matches_query(&topic, &key, msg, unique_key_flag))
                    .collect())
            }
            ResponseCode::QueryNotFound => Ok(Vec::new()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |s| s.to_string())
            )),
        }
    }

    async fn get_topic_stats_info(
        &self,
        address: &str,
        request_header: GetTopicStatsInfoRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<TopicStatsTable> {
        let request = self.create_request_command(RequestCode::GetTopicStatsInfo, request_header);
        let outcome = self.invoke_admin_request(address, request, timeout_millis).await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(admin_request_error(
                    "get_topic_stats_info",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(admin_request_error(
                    "get_topic_stats_info",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return TopicStatsTable::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    async fn query_consume_time_span(
        &self,
        address: &str,
        request_header: QueryConsumeTimeSpanRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<Vec<QueueTimeSpan>> {
        let request = self.create_request_command(RequestCode::QueryConsumeTimeSpan, request_header);
        let outcome = self.invoke_admin_request(address, request, timeout_millis).await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(admin_request_error(
                    "query_consume_time_span",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(admin_request_error(
                    "query_consume_time_span",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                let body: QueryConsumeTimeSpanBody = super::decode_admin_json(body.as_ref())?;
                return Ok(body.consume_time_span_set);
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    #[cfg(feature = "admin-mutation")]
    async fn update_or_create_topic(
        &self,
        address: &str,
        request_header: CreateTopicRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request = self.create_request_command(RequestCode::UpdateAndCreateTopic, request_header);
        let outcome = self.invoke_admin_request(address, request, timeout_millis).await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(admin_request_error(
                    "update_or_create_topic",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(admin_request_error(
                    "update_or_create_topic",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(());
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    #[cfg(feature = "admin-mutation")]
    async fn update_or_create_subscription_group(
        &self,
        address: &str,
        config: SubscriptionGroupConfig,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request = self
            .create_request_command(RequestCode::UpdateAndCreateSubscriptionGroup, EmptyHeader {})
            .set_body(config.encode()?);
        let outcome = self.invoke_admin_request(address, request, timeout_millis).await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(admin_request_error(
                    "update_or_create_subscription_group",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(admin_request_error(
                    "update_or_create_subscription_group",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(());
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    #[cfg(feature = "admin-mutation")]
    async fn delete_topic_in_broker(
        &self,
        address: &str,
        request_header: DeleteTopicRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request = self.create_request_command(RequestCode::DeleteTopicInBroker, request_header);
        let outcome = self.invoke_admin_request(address, request, timeout_millis).await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(admin_request_error(
                    "delete_topic_in_broker",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(admin_request_error(
                    "delete_topic_in_broker",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(());
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    #[cfg(feature = "admin-mutation")]
    async fn delete_topic_in_broker_list(
        &self,
        address: &str,
        topic_list: Vec<CheetahString>,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request = delete_topic_list_request(&self.command_factory, topic_list)?;
        let outcome = self.invoke_admin_request(address, request, timeout_millis).await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(admin_request_error(
                    "delete_topic_in_broker_list",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(admin_request_error(
                    "delete_topic_in_broker_list",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string())
            )),
        }
    }

    #[cfg(feature = "admin-mutation")]
    async fn delete_topic_in_nameserver(
        &self,
        address: &str,
        request_header: DeleteTopicFromNamesrvRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request = self.create_request_command(RequestCode::DeleteTopicInNamesrv, request_header);
        let outcome = self.invoke_admin_request(address, request, timeout_millis).await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(admin_request_error(
                    "delete_topic_in_nameserver",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(admin_request_error(
                    "delete_topic_in_nameserver",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(());
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    #[cfg(feature = "admin-mutation")]
    async fn delete_kv_config(
        &self,
        address: &str,
        request_header: DeleteKVConfigRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request = self.create_request_command(RequestCode::DeleteKvConfig, request_header);
        let outcome = self.invoke_admin_request(address, request, timeout_millis).await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(admin_request_error("delete_kv_config", RetryInput::Rejected(rejection)));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(admin_request_error("delete_kv_config", RetryInput::Contract(contract)));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(());
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    #[cfg(feature = "admin-mutation")]
    async fn delete_subscription_group(
        &self,
        address: &str,
        request_header: DeleteSubscriptionGroupRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request = self.create_request_command(RequestCode::DeleteSubscriptionGroup, request_header);
        let outcome = self.invoke_admin_request(address, request, timeout_millis).await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(admin_request_error(
                    "delete_subscription_group",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(admin_request_error(
                    "delete_subscription_group",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(());
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    #[cfg(feature = "admin-mutation")]
    async fn delete_subscription_group_list(
        &self,
        address: &str,
        group_name_list: Vec<CheetahString>,
        clean_offset: bool,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request = delete_subscription_group_list_request(&self.command_factory, group_name_list, clean_offset)?;
        let outcome = self.invoke_admin_request(address, request, timeout_millis).await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(admin_request_error(
                    "delete_subscription_group_list",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(admin_request_error(
                    "delete_subscription_group_list",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(()),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string())
            )),
        }
    }

    #[cfg(feature = "admin-mutation")]
    async fn invoke_broker_to_reset_offset(
        &self,
        address: &str,
        request_header: ResetOffsetRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<HashMap<MessageQueue, i64>> {
        let request = self.create_request_command(RequestCode::InvokeBrokerToResetOffset, request_header);
        let outcome = self.invoke_admin_request(address, request, timeout_millis).await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(admin_request_error(
                    "invoke_broker_to_reset_offset",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(admin_request_error(
                    "invoke_broker_to_reset_offset",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
        reset_offset_table_from_response(&response)
    }

    async fn view_message(
        &self,
        address: &str,
        request_header: ViewMessageRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<MessageExt> {
        let request = self.create_request_command(RequestCode::ViewMessageById, request_header);
        let outcome = self.invoke_admin_request(address, request, timeout_millis).await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(admin_request_error("view_message", RetryInput::Rejected(rejection)));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(admin_request_error("view_message", RetryInput::Contract(contract)));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                if let Some(body) = response.get_body() {
                    let mut bytes = body.clone();
                    MessageDecoder::decode(&mut bytes, true, true, false, false, false)
                        .ok_or_else(|| mq_client_err!("view_message response body decode failed"))
                } else {
                    Err(mq_client_err!("view_message response body is empty"))
                }
            }
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |s| s.to_string())
            )),
        }
    }

    async fn get_broker_cluster_info(&self, address: &str, timeout_millis: u64) -> RocketMQResult<ClusterInfo> {
        let request = self.create_request_command(RequestCode::GetBrokerClusterInfo, EmptyHeader {});
        let outcome = self.invoke_admin_request(address, request, timeout_millis).await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(admin_request_error(
                    "get_broker_cluster_info",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(admin_request_error(
                    "get_broker_cluster_info",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return ClusterInfo::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    async fn get_consumer_connection_list(
        &self,
        address: &str,
        request_header: GetConsumerConnectionListRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<ConsumerConnection> {
        let request = self.create_request_command(RequestCode::GetConsumerConnectionList, request_header);
        let outcome = self.invoke_admin_request(address, request, timeout_millis).await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(admin_request_error(
                    "get_consumer_connection_list",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(admin_request_error(
                    "get_consumer_connection_list",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return ConsumerConnection::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    async fn query_topics_by_consumer(
        &self,
        address: &str,
        request_header: QueryTopicsByConsumerRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<TopicList> {
        let request = self.create_request_command(RequestCode::QueryTopicsByConsumer, request_header);
        let outcome = self.invoke_admin_request(address, request, timeout_millis).await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(admin_request_error(
                    "query_topics_by_consumer",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(admin_request_error(
                    "query_topics_by_consumer",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return TopicList::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    async fn query_subscription_by_consumer(
        &self,
        address: &str,
        request_header: QuerySubscriptionByConsumerRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<SubscriptionData> {
        let request = self.create_request_command(RequestCode::QuerySubscriptionByConsumer, request_header);
        let outcome = self.invoke_admin_request(address, request, timeout_millis).await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(admin_request_error(
                    "query_subscription_by_consumer",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(admin_request_error(
                    "query_subscription_by_consumer",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            let body = response
                .get_body()
                .ok_or_else(|| mq_client_err!("query_subscription_by_consumer response body is empty"))?;
            let response_body: QuerySubscriptionResponseBody = super::decode_admin_json(body.as_ref())?;
            return response_body
                .subscription_data
                .ok_or_else(|| mq_client_err!("query_subscription_by_consumer response subscriptionData is empty"));
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    async fn get_consume_stats(
        &self,
        address: &str,
        request_header: GetConsumeStatsRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<ConsumeStats> {
        let request = self.create_request_command(RequestCode::GetConsumeStats, request_header);
        let outcome = self.invoke_admin_request(address, request, timeout_millis).await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(admin_request_error(
                    "get_consume_stats",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(admin_request_error("get_consume_stats", RetryInput::Contract(contract)));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return ConsumeStats::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    async fn query_topic_consume_by_who(
        &self,
        address: &str,
        request_header: QueryTopicConsumeByWhoRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<GroupList> {
        let request = self.create_request_command(RequestCode::QueryTopicConsumeByWho, request_header);
        let outcome = self.invoke_admin_request(address, request, timeout_millis).await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(admin_request_error(
                    "query_topic_consume_by_who",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(admin_request_error(
                    "query_topic_consume_by_who",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return GroupList::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |s| s.to_string())
        ))
    }

    async fn get_consumer_running_info(
        &self,
        address: &str,
        request_header: GetConsumerRunningInfoRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<ConsumerRunningInfo> {
        let request = self.create_request_command(RequestCode::GetConsumerRunningInfo, request_header);
        let outcome = self.invoke_admin_request(address, request, timeout_millis).await;
        let mut response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(admin_request_error(
                    "get_consumer_running_info",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(admin_request_error(
                    "get_consumer_running_info",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                let Some(body) = response.take_body() else {
                    return Err(mq_client_err!("get_consumer_running_info response body is empty"));
                };
                ConsumerRunningInfo::decode(body.as_ref())
            }
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |s| s.to_string())
            )),
        }
    }

    #[cfg(feature = "admin-mutation")]
    async fn consume_message_directly(
        &self,
        address: &str,
        request_header: ConsumeMessageDirectlyResultRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<ConsumeMessageDirectlyResult> {
        let request = self.create_request_command(RequestCode::ConsumeMessageDirectly, request_header);
        let outcome = self.invoke_admin_request(address, request, timeout_millis).await;
        let mut response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(admin_request_error(
                    "consume_message_directly",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(admin_request_error(
                    "consume_message_directly",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                let Some(body) = response.take_body() else {
                    return Err(mq_client_err!("consume_message_directly response body is empty"));
                };
                ConsumeMessageDirectlyResult::decode(body.as_ref())
            }
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |s| s.to_string())
            )),
        }
    }
}
