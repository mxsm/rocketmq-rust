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

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::Weak;

use crate::base::client_config::ClientConfig;
use crate::base::query_result::QueryResult;
use crate::base::validators::Validators;
use crate::common::retry_policy::RetryAction;
use crate::common::retry_policy::RetryContext;
use crate::common::retry_policy::RetryIdempotency;
use crate::common::retry_policy::RetryInput;
use crate::common::retry_policy::RetryOperation;
use crate::common::retry_policy::RetryPolicy;
use crate::factory::mq_client_instance;
use crate::factory::mq_client_instance::MQClientInstance;
use crate::implementation::mq_client_api_impl::MQClientAPIImpl;
use cheetah_string::CheetahString;
use rocketmq_error::fields;
use rocketmq_error::Error;
use rocketmq_error::ErrorContext;
use rocketmq_error::RocketMQError;
use rocketmq_model::common::attribute::attribute_parser::AttributeParser;
use rocketmq_model::common::boundary_type::BoundaryType;
use rocketmq_model::common::constant::PermName;
use rocketmq_model::common::message::message_client_id_setter::MessageClientIDSetter;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_queue::MessageQueue;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_model::common::mix_all;
use rocketmq_model::common::TopicFilterType;
use rocketmq_protocol::common::message::message_decoder as MessageDecoder;
use rocketmq_protocol::protocol::header::create_topic_request_header::CreateTopicRequestHeader;
use rocketmq_protocol::protocol::header::query_message_request_header::QueryMessageRequestHeader;
use rocketmq_protocol::protocol::header::view_message_request_header::ViewMessageRequestHeader;
use rocketmq_protocol::protocol::namespace_util::NamespaceUtil;
use rocketmq_protocol::protocol::route_facade::BrokerDataExt;
use rocketmq_transport::api::OutboundRequestContractReason;
use rocketmq_transport::api::OutboundRequestRejectionReason;
use rocketmq_transport::api::OutboundRequestStage;
use rocketmq_transport::api::RequestDeadline;

const CREATE_TOPIC_ATTEMPTS: u32 = 5;

fn create_topic_retry_error(input: RetryInput) -> RocketMQError {
    match input {
        RetryInput::Transport(error) => RocketMQError::Shared(error.into_shared_error()),
        RetryInput::Rejected(rejection) => match rejection.reason() {
            OutboundRequestRejectionReason::DeadlineExpired => {
                let mut context = ErrorContext::new().with_text(fields::OPERATION_DIAGNOSTIC, "create_topic");
                if let Some(timeout_millis) = rejection.timeout_millis() {
                    context = context.with_u64(fields::TIMEOUT_MS, timeout_millis);
                }
                RocketMQError::Shared(Arc::new(
                    Error::new(&rocketmq_error::CORE_OPERATION_TIMED_OUT).with_context(context),
                ))
            }
            OutboundRequestRejectionReason::ClientStopping => RocketMQError::ClientNotStarted,
            OutboundRequestRejectionReason::QueueSaturated => RocketMQError::Shared(Arc::new(Error::new(
                &rocketmq_error::TRANSPORT_ADMISSION_QUEUE_SATURATED,
            ))),
            OutboundRequestRejectionReason::Cancelled
            | OutboundRequestRejectionReason::SessionClosed
            | OutboundRequestRejectionReason::EndpointUnavailable => {
                let phase = match rejection.reason() {
                    OutboundRequestRejectionReason::SessionClosed => "closed",
                    _ => create_topic_stage_phase(rejection.stage()),
                };
                let mut context = ErrorContext::new().with_text(fields::PHASE, phase);
                if rejection.remote_addr_present() {
                    context = context.with_secret_presence(fields::REMOTE_ADDR_PRESENT);
                }
                RocketMQError::Shared(Arc::new(
                    Error::new(&rocketmq_error::TRANSPORT_CONNECTION_FAILED).with_context(context),
                ))
            }
        },
        RetryInput::Contract(contract) => match contract.reason() {
            OutboundRequestContractReason::NameServerEndpointMissing => {
                RocketMQError::Shared(Arc::new(Error::new(&rocketmq_error::TRANSPORT_CONNECTION_FAILED)))
            }
        },
        RetryInput::Response { terminal_error, .. } | RetryInput::BusinessError(terminal_error) => terminal_error,
        // The create-topic request path cannot construct producer send status or route-policy inputs.
        RetryInput::SendStatus(_) | RetryInput::RouteUnavailable => {
            unreachable!("create-topic request returned an unrelated retry input")
        }
    }
}

const fn create_topic_stage_phase(stage: OutboundRequestStage) -> &'static str {
    match stage {
        OutboundRequestStage::BeforeWrite => "before_write",
        OutboundRequestStage::Writing => "writing",
        OutboundRequestStage::AwaitingResponse => "awaiting_response",
        OutboundRequestStage::ResponseReceived => "response_received",
    }
}

pub struct MQAdminImpl {
    timeout_millis: u64,
    client: OnceLock<Weak<MQClientInstance>>,
}

impl MQAdminImpl {
    pub fn new() -> Self {
        MQAdminImpl {
            timeout_millis: 60000,
            client: OnceLock::new(),
        }
    }

    pub fn set_client(&self, client: &Arc<MQClientInstance>) -> bool {
        self.client.set(Arc::downgrade(client)).is_ok()
    }

    fn client(&self) -> rocketmq_error::RocketMQResult<Arc<MQClientInstance>> {
        self.client
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(|| RocketMQError::not_initialized("MQClientInstance"))
    }

    fn encode_topic_attributes(attributes: HashMap<String, String>) -> Option<CheetahString> {
        if attributes.is_empty() {
            return None;
        }
        let encoded = AttributeParser::parse_to_string(&attributes);
        if encoded.is_empty() {
            None
        } else {
            Some(CheetahString::from_string(encoded))
        }
    }

    fn message_matches_query(
        topic: &CheetahString,
        key: &CheetahString,
        msg: &MessageExt,
        unique_key_flag: bool,
    ) -> bool {
        let topic_matches = msg.topic().as_str() == topic.as_str();
        if !topic_matches {
            return false;
        }

        if unique_key_flag {
            if let Some(uniq_id) = MessageClientIDSetter::get_uniq_id(msg) {
                if uniq_id.as_str() == key.as_str() {
                    return true;
                }
            }
            return msg.msg_id.as_str() == key.as_str();
        }

        let Some(keys) = MessageTrait::get_keys(msg) else {
            return false;
        };
        keys.split_str(MessageConst::KEY_SEPARATOR)
            .any(|candidate| candidate == key.as_str())
    }

    fn strip_namespace_from_message(client_config: &ClientConfig, msg: &mut MessageExt) {
        if let Some(namespace) = client_config.namespace.as_ref() {
            let topic = NamespaceUtil::without_namespace_with_namespace(msg.topic().as_str(), namespace.as_str());
            msg.set_topic(topic.into());
        }
    }

    fn query_message_request_header(
        topic: CheetahString,
        key: CheetahString,
        max_num: i32,
        begin: u64,
        end: u64,
        unique_key_flag: bool,
    ) -> rocketmq_error::RocketMQResult<QueryMessageRequestHeader> {
        let begin_timestamp = i64::try_from(begin)
            .map_err(|_| RocketMQError::illegal_argument("queryMessage begin timestamp exceeds Java long range"))?;
        let end_timestamp = i64::try_from(end)
            .map_err(|_| RocketMQError::illegal_argument("queryMessage end timestamp exceeds Java long range"))?;
        let index_type = if unique_key_flag {
            MessageConst::INDEX_UNIQUE_TYPE
        } else {
            MessageConst::INDEX_KEY_TYPE
        };

        Ok(QueryMessageRequestHeader {
            topic,
            key,
            max_num,
            begin_timestamp,
            end_timestamp,
            index_type: Some(CheetahString::from_static_str(index_type)),
            last_key: None,
            topic_request_header: None,
        })
    }

    fn timestamp_to_java_long(operation: &'static str, timestamp: u64) -> rocketmq_error::RocketMQResult<i64> {
        i64::try_from(timestamp)
            .map_err(|_| RocketMQError::illegal_argument(format!("{operation} timestamp exceeds Java long range")))
    }

    fn java_long_to_u64(
        operation: &'static str,
        field: &'static str,
        value: i64,
    ) -> rocketmq_error::RocketMQResult<u64> {
        u64::try_from(value).map_err(|_| {
            RocketMQError::illegal_argument(format!(
                "{operation} {field} is negative and cannot be represented as Rust u64"
            ))
        })
    }
}

impl MQAdminImpl {
    pub async fn create_topic(
        &self,
        key: &str,
        new_topic: &str,
        queue_num: i32,
        topic_sys_flag: i32,
        attributes: HashMap<String, String>,
    ) -> rocketmq_error::RocketMQResult<()> {
        Validators::check_topic(new_topic)?;
        Validators::is_system_topic(new_topic)?;
        if queue_num <= 0 {
            return Err(RocketMQError::illegal_argument("queueNum must be positive"));
        }

        let client = self.client()?;
        let api_impl = client
            .mq_client_api_impl
            .load_full()
            .ok_or_else(|| RocketMQError::not_initialized("MQClientAPIImpl"))?;
        let route_data = api_impl
            .get_topic_route_info_from_name_server(key, self.timeout_millis)
            .await?
            .ok_or_else(|| mq_client_err!("Not found broker, maybe key is wrong"))?;
        if route_data.broker_datas.is_empty() {
            return Err(mq_client_err!("Not found broker, maybe key is wrong"));
        }

        let mut broker_datas = route_data.broker_datas;
        broker_datas.sort();
        let mut create_ok_at_least_once = false;
        let mut last_error: Option<rocketmq_error::RocketMQError> = None;
        for broker_data in broker_datas {
            let Some(addr) = broker_data.broker_addrs().get(&mix_all::MASTER_ID).cloned() else {
                continue;
            };
            let mut create_ok = false;
            let deadline = RequestDeadline::from_timeout_millis(self.timeout_millis);
            for attempt in 1..=CREATE_TOPIC_ATTEMPTS {
                if deadline.is_expired() && last_error.is_some() {
                    break;
                }
                let request_header = CreateTopicRequestHeader {
                    topic: CheetahString::from_slice(new_topic),
                    default_topic: CheetahString::from_slice(key),
                    read_queue_nums: queue_num,
                    write_queue_nums: queue_num,
                    perm: (PermName::PERM_READ | PermName::PERM_WRITE) as i32,
                    topic_filter_type: CheetahString::from_static_str(TopicFilterType::SingleTag.as_str()),
                    topic_sys_flag: Some(topic_sys_flag),
                    order: false,
                    attributes: Self::encode_topic_attributes(attributes.clone()),
                    force: Some(false),
                    topic_request_header: None,
                };
                match api_impl
                    .update_or_create_topic_once(&addr, request_header, deadline)
                    .await
                {
                    Ok(()) => {
                        create_ok = true;
                        create_ok_at_least_once = true;
                        break;
                    }
                    Err(input) => {
                        let action = RetryPolicy::decide(
                            RetryContext {
                                operation: RetryOperation::CreateTopic,
                                idempotency: RetryIdempotency::Idempotent,
                                attempt,
                                max_attempts: CREATE_TOPIC_ATTEMPTS,
                                remaining: deadline.remaining(),
                                producer_retry_response_codes: None,
                                retry_not_store_ok: false,
                            },
                            &input,
                        );
                        match action {
                            RetryAction::Stop
                            | RetryAction::SwitchBroker
                            | RetryAction::RefreshRoute
                            | RetryAction::RefreshLeader => {
                                last_error = Some(create_topic_retry_error(input));
                                break;
                            }
                            RetryAction::RetryNow => {
                                last_error = Some(create_topic_retry_error(input));
                            }
                            RetryAction::RetryAfter(delay) => {
                                last_error = Some(create_topic_retry_error(input));
                                if tokio::time::timeout_at(deadline.instant(), tokio::time::sleep(delay))
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                            }
                        }
                        if deadline.is_expired() {
                            break;
                        }
                    }
                }
            }
            if !create_ok {
                tracing::warn!(
                    "create topic {} on broker {} failed after retries",
                    new_topic,
                    broker_data.broker_name()
                );
            }
        }

        if create_ok_at_least_once {
            Ok(())
        } else {
            Err(last_error.unwrap_or_else(|| mq_client_err!("create topic to broker exception")))
        }
    }

    pub fn parse_publish_message_queues(
        &self,
        message_queue_array: &[MessageQueue],
        client_config: &ClientConfig,
    ) -> Vec<MessageQueue> {
        let mut message_queues = Vec::new();
        for message_queue in message_queue_array {
            let user_topic = NamespaceUtil::without_namespace_with_namespace(
                message_queue.topic_str(),
                client_config.resolved_namespace().unwrap_or_default().as_str(),
            );

            let message_queue =
                MessageQueue::from_parts(user_topic, message_queue.broker_name(), message_queue.queue_id());
            message_queues.push(message_queue);
        }
        message_queues
    }

    pub async fn fetch_publish_message_queues(
        &self,
        topic: &str,
        mq_client_api_impl: Arc<MQClientAPIImpl>,
        client_config: &ClientConfig,
    ) -> rocketmq_error::RocketMQResult<Vec<MessageQueue>> {
        let topic_route_data = mq_client_api_impl
            .get_topic_route_info_from_name_server_detail(topic, self.timeout_millis, true)
            .await?;
        if let Some(mut topic_route_data) = topic_route_data {
            let topic_publish_info =
                mq_client_instance::topic_route_data2topic_publish_info(topic, &mut topic_route_data);
            if topic_publish_info.ok() {
                return Ok(self.parse_publish_message_queues(&topic_publish_info.message_queue_list, client_config));
            }
        }
        Err(mq_client_err!(format!(
            "Unknow why, Can not find Message Queue for this topic, {}",
            topic
        )))
    }

    /// Queries the maximum offset of the given message queue from the broker.
    ///
    /// Retries the broker address lookup via the name server when it is not cached locally.
    ///
    /// # Errors
    ///
    /// Returns an error if the broker address cannot be resolved or the remote call fails.
    pub async fn max_offset(&self, mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64> {
        let client = self.client()?;
        let broker_name = client.get_broker_name_from_message_queue(mq).await;
        let mut broker_addr = client.find_broker_address_in_publish(broker_name.as_ref());
        if broker_addr.is_none() {
            client.update_topic_route_info_from_name_server_topic(mq.topic()).await;
            let broker_name = client.get_broker_name_from_message_queue(mq).await;
            broker_addr = client.find_broker_address_in_publish(broker_name.as_ref());
        }
        if let Some(ref broker_addr) = broker_addr {
            return client
                .mq_client_api_impl
                .load_full()
                .ok_or_else(|| RocketMQError::not_initialized("MQClientAPIImpl"))?
                .get_max_offset(broker_addr, mq, self.timeout_millis)
                .await;
        }
        Err(mq_client_err!(format!("The broker[{}] not exist", mq.broker_name())))
    }

    /// Searches for the queue offset whose store timestamp is closest to `timestamp`.
    ///
    /// Defaults to [`BoundaryType::Lower`], returning the earliest offset whose store
    /// timestamp is greater than or equal to `timestamp`, matching the behaviour of
    /// `MQAdminImpl.searchOffset(MessageQueue, long)` in the Java implementation.
    ///
    /// # Errors
    ///
    /// Returns an error if the broker address cannot be resolved or the remote call fails.
    pub async fn search_offset(&self, mq: &MessageQueue, timestamp: u64) -> rocketmq_error::RocketMQResult<i64> {
        let timestamp = Self::timestamp_to_java_long("searchOffset", timestamp)?;
        let client = self.client()?;
        let broker_name = client.get_broker_name_from_message_queue(mq).await;
        let mut broker_addr = client.find_broker_address_in_publish(broker_name.as_ref());
        if broker_addr.is_none() {
            client.update_topic_route_info_from_name_server_topic(mq.topic()).await;
            let broker_name = client.get_broker_name_from_message_queue(mq).await;
            broker_addr = client.find_broker_address_in_publish(broker_name.as_ref());
        }
        if let Some(ref broker_addr) = broker_addr {
            return client
                .mq_client_api_impl
                .load_full()
                .ok_or_else(|| RocketMQError::not_initialized("MQClientAPIImpl"))?
                .search_offset_by_timestamp(broker_addr, mq, timestamp, BoundaryType::Lower, self.timeout_millis)
                .await;
        }
        Err(mq_client_err!(format!("The broker[{}] not exist", mq.broker_name())))
    }

    pub async fn min_offset(&self, mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64> {
        let client = self.client()?;
        let broker_name = client.get_broker_name_from_message_queue(mq).await;
        let mut broker_addr = client.find_broker_address_in_publish(broker_name.as_ref());
        if broker_addr.is_none() {
            client.update_topic_route_info_from_name_server_topic(mq.topic()).await;
            let broker_name = client.get_broker_name_from_message_queue(mq).await;
            broker_addr = client.find_broker_address_in_publish(broker_name.as_ref());
        }
        if let Some(ref broker_addr) = broker_addr {
            return client
                .mq_client_api_impl
                .load_full()
                .ok_or_else(|| RocketMQError::not_initialized("MQClientAPIImpl"))?
                .get_min_offset(broker_addr, mq, self.timeout_millis)
                .await;
        }
        Err(mq_client_err!(format!("The broker[{}] not exist", mq.broker_name())))
    }

    pub async fn earliest_msg_store_time(&self, mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64> {
        let client = self.client()?;
        let broker_name = client.get_broker_name_from_message_queue(mq).await;
        let mut broker_addr = client.find_broker_address_in_publish(broker_name.as_ref());
        if broker_addr.is_none() {
            client.update_topic_route_info_from_name_server_topic(mq.topic()).await;
            let broker_name = client.get_broker_name_from_message_queue(mq).await;
            broker_addr = client.find_broker_address_in_publish(broker_name.as_ref());
        }
        if let Some(ref broker_addr) = broker_addr {
            return client
                .mq_client_api_impl
                .load_full()
                .ok_or_else(|| RocketMQError::not_initialized("MQClientAPIImpl"))?
                .get_earliest_msg_store_time(broker_addr, mq, self.timeout_millis)
                .await;
        }
        Err(mq_client_err!(format!("The broker[{}] not exist", mq.broker_name())))
    }

    pub async fn view_message(&self, topic: &str, msg_id: &str) -> rocketmq_error::RocketMQResult<MessageExt> {
        let message_id = MessageDecoder::decode_message_id(msg_id).map_err(|_| {
            mq_client_err!(
                rocketmq_protocol::code::response_code::ResponseCode::NoMessage as i32,
                "query message by id finished, but no message."
            )
        })?;
        let broker_addr =
            CheetahString::from_string(format!("{}:{}", message_id.address.ip(), message_id.address.port()));
        let request_header = ViewMessageRequestHeader {
            topic: Some(CheetahString::from_slice(topic)),
            offset: message_id.offset,
        };
        self.client()?
            .mq_client_api_impl
            .load_full()
            .ok_or_else(|| RocketMQError::not_initialized("MQClientAPIImpl"))?
            .view_message(&broker_addr, request_header, self.timeout_millis)
            .await
    }

    pub async fn query_message(
        &self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin: u64,
        end: u64,
    ) -> rocketmq_error::RocketMQResult<QueryResult> {
        self.query_message_with_unique_flag(topic, key, max_num, begin, end, false)
            .await
    }

    pub async fn query_message_with_unique_flag(
        &self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin: u64,
        end: u64,
        unique_key_flag: bool,
    ) -> rocketmq_error::RocketMQResult<QueryResult> {
        let client = self.client()?;
        let api_impl = client
            .mq_client_api_impl
            .load_full()
            .ok_or_else(|| RocketMQError::not_initialized("MQClientAPIImpl"))?;
        let route_data = api_impl
            .get_topic_route_info_from_name_server(topic, self.timeout_millis)
            .await?
            .ok_or_else(|| {
                mq_client_err!(
                    rocketmq_protocol::code::response_code::ResponseCode::TopicNotExist as i32,
                    format!("The topic[{topic}] not matched route info")
                )
            })?;

        let query_topic = CheetahString::from_slice(topic);
        let query_key = CheetahString::from_slice(key);
        let mut index_last_update_timestamp = 0_u64;
        let mut message_list = Vec::new();

        for broker_data in route_data.broker_datas {
            let Some(addr) = broker_data.select_broker_addr() else {
                continue;
            };
            let request_header = Self::query_message_request_header(
                query_topic.clone(),
                query_key.clone(),
                max_num,
                begin,
                end,
                unique_key_flag,
            )?;
            match MQClientAPIImpl::query_message(
                &api_impl,
                &addr,
                request_header,
                unique_key_flag,
                self.timeout_millis * 3,
            )
            .await
            {
                Ok(Some((response_header, body))) => {
                    let timestamp = Self::java_long_to_u64(
                        "queryMessage",
                        "indexLastUpdateTimestamp",
                        response_header.index_last_update_timestamp,
                    )?;
                    index_last_update_timestamp = index_last_update_timestamp.max(timestamp);
                    if let Some(mut body) = body {
                        for mut msg in MessageDecoder::decodes_batch(&mut body, true, true) {
                            if Self::message_matches_query(&query_topic, &query_key, &msg, unique_key_flag) {
                                Self::strip_namespace_from_message(&client.client_config, &mut msg);
                                message_list.push(msg);
                            } else {
                                tracing::warn!(
                                    "queryMessage found message key/topic not matched, maybe hash duplicate: {}",
                                    msg
                                );
                            }
                        }
                    }
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!("queryMessage from broker {} failed: {}", addr, error);
                }
            }
        }

        if message_list.is_empty() {
            return Err(mq_client_err!(
                rocketmq_protocol::code::response_code::ResponseCode::NoMessage as i32,
                "query message by key finished, but no message."
            ));
        }

        Ok(QueryResult::new(index_last_update_timestamp, message_list))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::time::Duration;

    use super::*;
    use rocketmq_protocol::code::request_code::RequestCode;
    use rocketmq_protocol::code::response_code::ResponseCode;
    use rocketmq_protocol::protocol::command_custom_header::CommandCustomHeader;
    use rocketmq_protocol::protocol::route::route_data_view::BrokerData;
    use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
    use rocketmq_protocol::protocol::RemotingSerializable;
    use rocketmq_protocol::RemotingCommand;
    use rocketmq_runtime::RuntimeContext;
    use rocketmq_runtime::ShutdownDeadline;
    use rocketmq_transport::api::AdmissionController;
    use rocketmq_transport::api::AdmissionLimits;
    use rocketmq_transport::test_support::SessionProcessor;
    use rocketmq_transport::test_support::SessionTransportServer;
    use rocketmq_transport::test_support::SessionTransportServerConfig;

    struct CreateTopicProcessor {
        broker_addr: OnceLock<CheetahString>,
        create_responses: Mutex<VecDeque<RemotingCommand>>,
        route_requests: std::sync::atomic::AtomicUsize,
        create_requests: std::sync::atomic::AtomicUsize,
    }

    impl CreateTopicProcessor {
        fn new() -> Self {
            Self {
                broker_addr: OnceLock::new(),
                create_responses: Mutex::new(VecDeque::new()),
                route_requests: std::sync::atomic::AtomicUsize::new(0),
                create_requests: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn push_create_response(&self, response: RemotingCommand) {
            self.create_responses
                .lock()
                .expect("create response queue")
                .push_back(response);
        }

        fn counts(&self) -> (usize, usize) {
            use std::sync::atomic::Ordering;

            (
                self.route_requests.load(Ordering::SeqCst),
                self.create_requests.load(Ordering::SeqCst),
            )
        }

        fn route_response(&self) -> rocketmq_error::RocketMQResult<RemotingCommand> {
            let broker_addr = self
                .broker_addr
                .get()
                .ok_or_else(|| RocketMQError::invariant_violated("test broker address must be initialized"))?;
            let route = TopicRouteData {
                broker_datas: vec![BrokerData::new(
                    CheetahString::from_static_str("cluster-a"),
                    CheetahString::from_static_str("broker-a"),
                    HashMap::from([(mix_all::MASTER_ID, broker_addr.clone())]),
                    None,
                )],
                ..Default::default()
            };
            Ok(RemotingCommand::create_success_response_command().set_body(route.encode()?))
        }
    }

    impl SessionProcessor for CreateTopicProcessor {
        fn process(
            &self,
            request: RemotingCommand,
        ) -> Pin<Box<dyn Future<Output = rocketmq_error::RocketMQResult<RemotingCommand>> + Send + '_>> {
            Box::pin(async move {
                use std::sync::atomic::Ordering;

                let mut response = if request.code() == RequestCode::GetRouteinfoByTopic.to_i32() {
                    self.route_requests.fetch_add(1, Ordering::SeqCst);
                    self.route_response()?
                } else if request.code() == RequestCode::UpdateAndCreateTopic.to_i32() {
                    self.create_requests.fetch_add(1, Ordering::SeqCst);
                    self.create_responses
                        .lock()
                        .expect("create response queue")
                        .pop_front()
                        .ok_or_else(|| RocketMQError::illegal_argument("unexpected create-topic request"))?
                } else {
                    return Err(RocketMQError::illegal_argument(format!(
                        "unexpected request code {}",
                        request.code()
                    )));
                };
                response.set_opaque_mut(request.opaque());
                Ok(response)
            })
        }
    }

    async fn create_topic_harness(
        scope: &'static str,
    ) -> (
        MQAdminImpl,
        Arc<MQClientInstance>,
        Arc<crate::runtime::ClientRuntime>,
        Arc<CreateTopicProcessor>,
        Arc<SessionTransportServer>,
        RuntimeContext,
    ) {
        let server_runtime = RuntimeContext::from_current(scope);
        let admission = Arc::new(AdmissionController::new(AdmissionLimits::default()));
        let processor = Arc::new(CreateTopicProcessor::new());
        let server = SessionTransportServer::bind(
            server_runtime.service_context("create-topic-server"),
            SessionTransportServerConfig::loopback(),
            Arc::clone(&processor) as Arc<dyn SessionProcessor>,
            admission,
        )
        .await
        .expect("bind scripted create-topic server");
        let broker_addr = CheetahString::from_string(server.local_addr().to_string());
        processor
            .broker_addr
            .set(broker_addr.clone())
            .expect("set test broker address once");
        server.start().expect("start create-topic server");

        let client_runtime = crate::runtime::test_client_runtime(scope);
        let mut client_config = ClientConfig::default();
        client_config.set_vip_channel_enabled(false);
        let client_instance = MQClientInstance::new_arc(
            client_config,
            0,
            CheetahString::from_string(format!("{scope}-client")),
            None,
            client_runtime.component("instance"),
            client_runtime.telemetry_handle().clone(),
            client_runtime.pool().request_future_holder(),
        );
        let api = client_instance.get_mq_client_api_impl().expect("client API");
        api.update_name_server_address_list_sync(broker_addr.as_str());
        api.start().await.expect("start client transport");
        let mut admin = MQAdminImpl::new();
        admin.timeout_millis = 2_000;
        assert!(admin.set_client(&client_instance));

        (
            admin,
            client_instance,
            client_runtime,
            processor,
            server,
            server_runtime,
        )
    }

    async fn shutdown_create_topic_harness(
        client_instance: Arc<MQClientInstance>,
        client_runtime: Arc<crate::runtime::ClientRuntime>,
        server: Arc<SessionTransportServer>,
        server_runtime: RuntimeContext,
    ) {
        client_instance.shutdown().await;
        client_runtime
            .shutdown()
            .await
            .assert_no_task_leak()
            .expect("client runtime tasks drained");
        server
            .shutdown_until(ShutdownDeadline::after(Duration::from_secs(5)))
            .await
            .assert_no_task_leak()
            .expect("server tasks drained");
        server_runtime
            .shutdown_tasks(Duration::from_secs(5))
            .await
            .assert_no_task_leak()
            .expect("server runtime tasks drained");
    }

    #[tokio::test]
    async fn create_topic_retries_per_broker_and_returns_the_last_response_failure() {
        let (admin, client_instance, client_runtime, processor, server, server_runtime) =
            create_topic_harness("create-topic-retry-test").await;
        processor.push_create_response(RemotingCommand::create_response_command_with_code_remark(
            ResponseCode::SystemError,
            "retryable-first",
        ));
        processor.push_create_response(RemotingCommand::create_success_response_command());

        admin
            .create_topic("route-key", "created-topic", 4, 0, HashMap::new())
            .await
            .expect("the second counted request should create the topic");
        assert_eq!(processor.counts(), (1, 2));

        for attempt in 1..=CREATE_TOPIC_ATTEMPTS {
            processor.push_create_response(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::SystemError,
                format!("terminal-attempt-{attempt}"),
            ));
        }
        let before = processor.counts();
        let error = admin
            .create_topic("route-key", "exhausted-topic", 4, 0, HashMap::new())
            .await
            .expect_err("five failed counted requests must exhaust the per-broker budget");
        let after = processor.counts();

        assert_eq!(after.0 - before.0, 1, "outer route discovery remains one request");
        assert_eq!(after.1 - before.1, CREATE_TOPIC_ATTEMPTS as usize);
        assert!(
            error.to_string().contains("terminal-attempt-5"),
            "budget exhaustion must return the last broker response: {error}"
        );

        shutdown_create_topic_harness(client_instance, client_runtime, server, server_runtime).await;
    }

    #[test]
    fn client_binding_is_one_time_and_shared() {
        let runtime = crate::runtime::test_client_runtime("mq-admin-impl-test");
        let instance = MQClientInstance::new_arc(
            ClientConfig::default(),
            0,
            "admin-client-binding",
            None,
            runtime.component("instance"),
            runtime.telemetry_handle().clone(),
            runtime.pool().request_future_holder(),
        );
        let admin = MQAdminImpl::new();

        assert!(admin.set_client(&instance));
        assert!(!admin.set_client(&instance));
        assert_eq!(
            admin.client().expect("bound client should remain available").client_id,
            "admin-client-binding"
        );

        let weak = Arc::downgrade(&instance);
        drop(instance);
        assert!(weak.upgrade().is_none());
        assert!(matches!(admin.client(), Err(RocketMQError::NotInitialized(_))));
    }

    #[test]
    fn query_message_request_header_sets_java_index_type_for_key_query() {
        let header = MQAdminImpl::query_message_request_header(
            CheetahString::from_static_str("TopicA"),
            CheetahString::from_static_str("KeyA"),
            32,
            1000,
            2000,
            false,
        )
        .expect("valid key query header should build");

        let fields = header.to_map().expect("header should encode");

        assert_eq!(fields.get("topic").map(|value| value.as_str()), Some("TopicA"));
        assert_eq!(fields.get("key").map(|value| value.as_str()), Some("KeyA"));
        assert_eq!(fields.get("maxNum").map(|value| value.as_str()), Some("32"));
        assert_eq!(fields.get("beginTimestamp").map(|value| value.as_str()), Some("1000"));
        assert_eq!(fields.get("endTimestamp").map(|value| value.as_str()), Some("2000"));
        assert_eq!(
            fields.get("indexType").map(|value| value.as_str()),
            Some(MessageConst::INDEX_KEY_TYPE)
        );
        assert!(!fields.contains_key("lastKey"));
    }

    #[test]
    fn query_message_request_header_sets_java_index_type_for_unique_query() {
        let header = MQAdminImpl::query_message_request_header(
            CheetahString::from_static_str("TopicA"),
            CheetahString::from_static_str("UniqueKeyA"),
            32,
            1000,
            2000,
            true,
        )
        .expect("valid unique query header should build");

        let fields = header.to_map().expect("header should encode");

        assert_eq!(
            fields.get("indexType").map(|value| value.as_str()),
            Some(MessageConst::INDEX_UNIQUE_TYPE)
        );
        assert!(!fields.contains_key("lastKey"));
    }

    #[test]
    fn query_message_request_header_rejects_timestamps_outside_java_long_range() {
        let error = MQAdminImpl::query_message_request_header(
            CheetahString::from_static_str("TopicA"),
            CheetahString::from_static_str("KeyA"),
            32,
            i64::MAX as u64 + 1,
            2000,
            false,
        )
        .expect_err("begin timestamp outside Java long range should fail");

        assert!(error
            .to_string()
            .contains("queryMessage begin timestamp exceeds Java long range"));
    }

    #[test]
    fn search_offset_timestamp_to_java_long_rejects_values_outside_java_range() {
        assert_eq!(
            MQAdminImpl::timestamp_to_java_long("searchOffset", i64::MAX as u64).expect("max Java long is valid"),
            i64::MAX
        );

        let error = MQAdminImpl::timestamp_to_java_long("searchOffset", i64::MAX as u64 + 1)
            .expect_err("timestamp outside Java long range should fail");

        assert!(error
            .to_string()
            .contains("searchOffset timestamp exceeds Java long range"));
    }

    #[test]
    fn query_message_index_timestamp_rejects_negative_java_long() {
        assert_eq!(
            MQAdminImpl::java_long_to_u64("queryMessage", "indexLastUpdateTimestamp", 7)
                .expect("positive Java long should convert"),
            7
        );

        let error = MQAdminImpl::java_long_to_u64("queryMessage", "indexLastUpdateTimestamp", -1)
            .expect_err("negative timestamp should not wrap");

        assert!(error
            .to_string()
            .contains("queryMessage indexLastUpdateTimestamp is negative and cannot be represented as Rust u64"));
    }

    #[test]
    fn message_matches_query_uses_java_unique_message_property() {
        let mut message = MessageExt::default();
        message.set_topic(CheetahString::from_static_str("TopicA"));
        message.set_msg_id(CheetahString::from_static_str("OFFSET-MSG-ID"));
        message.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
            CheetahString::from_static_str("UNIQ-MSG-ID"),
        );

        assert!(MQAdminImpl::message_matches_query(
            &CheetahString::from_static_str("TopicA"),
            &CheetahString::from_static_str("UNIQ-MSG-ID"),
            &message,
            true
        ));
        assert!(!MQAdminImpl::message_matches_query(
            &CheetahString::from_static_str("TopicA"),
            &CheetahString::from_static_str("OTHER-MSG-ID"),
            &message,
            true
        ));
        assert!(!MQAdminImpl::message_matches_query(
            &CheetahString::from_static_str("OtherTopic"),
            &CheetahString::from_static_str("UNIQ-MSG-ID"),
            &message,
            true
        ));
    }
}
