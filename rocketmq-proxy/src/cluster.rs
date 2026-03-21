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

use std::future::Future;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use cheetah_string::CheetahString;
use rocketmq_client_rust::base::client_config::ClientConfig as RocketmqClientConfig;
use rocketmq_client_rust::consumer::ack_callback::AckCallback;
use rocketmq_client_rust::consumer::ack_result::AckResult;
use rocketmq_client_rust::consumer::ack_status::AckStatus;
use rocketmq_client_rust::consumer::pop_callback::PopCallback;
use rocketmq_client_rust::consumer::pop_result::PopResult;
use rocketmq_client_rust::consumer::pop_status::PopStatus;
use rocketmq_client_rust::factory::mq_client_instance::MQClientInstance;
use rocketmq_client_rust::implementation::mq_client_manager::MQClientManager;
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_client_rust::producer::send_result::SendResult;
use rocketmq_common::common::attribute::topic_message_type::TopicMessageType;
use rocketmq_common::common::message::message_id::MessageId;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_queue_assignment::MessageQueueAssignment;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mix_all::MASTER_ID;
use rocketmq_common::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_common::MessageDecoder;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_remoting::protocol::header::ack_message_request_header::AckMessageRequestHeader;
use rocketmq_remoting::protocol::header::change_invisible_time_request_header::ChangeInvisibleTimeRequestHeader;
use rocketmq_remoting::protocol::header::end_transaction_request_header::EndTransactionRequestHeader;
use rocketmq_remoting::protocol::header::extra_info_util::ExtraInfoUtil;
use rocketmq_remoting::protocol::header::namesrv::topic_operation_header::TopicRequestHeader as PopTopicRequestHeader;
use rocketmq_remoting::protocol::header::pop_message_request_header::PopMessageRequestHeader;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_remoting::rpc::rpc_request_header::RpcRequestHeader;
use rocketmq_remoting::rpc::topic_request_header::TopicRequestHeader;
use rocketmq_rust::ArcMut;
use tokio::sync::oneshot;

use crate::config::ClusterConfig;
use crate::context::ProxyContext;
use crate::error::ProxyError;
use crate::error::ProxyResult;
use crate::processor::AckMessageRequest;
use crate::processor::AckMessageResultEntry;
use crate::processor::ChangeInvisibleDurationPlan;
use crate::processor::ChangeInvisibleDurationRequest;
use crate::processor::EndTransactionPlan;
use crate::processor::EndTransactionRequest;
use crate::processor::ReceiveMessagePlan;
use crate::processor::ReceiveMessageRequest;
use crate::processor::ReceivedMessage;
use crate::processor::SendMessageEntry;
use crate::processor::SendMessageRequest;
use crate::processor::SendMessageResultEntry;
use crate::processor::TransactionResolution;
use crate::processor::TransactionSource;
use crate::proto::v2;
use crate::service::ProxyTopicMessageType;
use crate::service::ResourceIdentity;
use crate::service::SubscriptionGroupMetadata;
use crate::status::ProxyPayloadStatus;
use crate::status::ProxyStatusMapper;

#[async_trait]
pub trait ClusterClient: Send + Sync {
    async fn query_route(&self, topic: &ResourceIdentity) -> ProxyResult<TopicRouteData>;

    async fn query_assignment(
        &self,
        topic: &ResourceIdentity,
        group: &ResourceIdentity,
        client_id: &str,
    ) -> ProxyResult<Option<Vec<MessageQueueAssignment>>>;

    async fn query_topic_message_type(&self, topic: &ResourceIdentity) -> ProxyResult<ProxyTopicMessageType>;

    async fn query_subscription_group(
        &self,
        topic: &ResourceIdentity,
        group: &ResourceIdentity,
    ) -> ProxyResult<Option<SubscriptionGroupMetadata>>;

    async fn send_message(
        &self,
        context: &ProxyContext,
        request: &SendMessageRequest,
    ) -> ProxyResult<Vec<SendMessageResultEntry>>;

    async fn receive_message(
        &self,
        context: &ProxyContext,
        request: &ReceiveMessageRequest,
    ) -> ProxyResult<ReceiveMessagePlan>;

    async fn ack_message(
        &self,
        context: &ProxyContext,
        request: &AckMessageRequest,
    ) -> ProxyResult<Vec<AckMessageResultEntry>>;

    async fn change_invisible_duration(
        &self,
        context: &ProxyContext,
        request: &ChangeInvisibleDurationRequest,
    ) -> ProxyResult<ChangeInvisibleDurationPlan>;

    async fn end_transaction(
        &self,
        context: &ProxyContext,
        request: &EndTransactionRequest,
    ) -> ProxyResult<EndTransactionPlan>;
}

pub struct RocketmqClusterClient {
    config: ClusterConfig,
}

impl RocketmqClusterClient {
    pub fn new(config: ClusterConfig) -> Self {
        Self { config }
    }
}

impl Default for RocketmqClusterClient {
    fn default() -> Self {
        Self::new(ClusterConfig::default())
    }
}

#[async_trait]
impl ClusterClient for RocketmqClusterClient {
    async fn query_route(&self, topic: &ResourceIdentity) -> ProxyResult<TopicRouteData> {
        let config = self.config.clone();
        let topic_name = topic.to_string();

        run_cluster_task(move || async move {
            let client = initialize_client_instance(config.clone()).await?;
            let route = client
                .get_mq_client_api_impl()
                .get_topic_route_info_from_name_server(topic_name.as_str(), config.mq_client_api_timeout_ms)
                .await?;

            route.ok_or_else(|| RocketMQError::route_not_found(topic_name).into())
        })
        .await
    }

    async fn query_assignment(
        &self,
        topic: &ResourceIdentity,
        group: &ResourceIdentity,
        client_id: &str,
    ) -> ProxyResult<Option<Vec<MessageQueueAssignment>>> {
        let config = self.config.clone();
        let topic_name = topic.to_string();
        let group_name = group.to_string();
        let client_id = client_id.to_owned();

        run_cluster_task(move || async move {
            let client = initialize_client_instance(config.clone()).await?;
            let route = client
                .get_mq_client_api_impl()
                .get_topic_route_info_from_name_server(topic_name.as_str(), config.mq_client_api_timeout_ms)
                .await?
                .ok_or_else(|| RocketMQError::route_not_found(topic_name.clone()))?;
            let broker_addr = select_master_broker_addr(&route).ok_or_else(|| RocketMQError::BrokerNotFound {
                name: topic_name.clone(),
            })?;
            let mut api = client.get_mq_client_api_impl();
            let assignments = api
                .query_assignment(
                    &broker_addr,
                    CheetahString::from(topic_name),
                    CheetahString::from(group_name),
                    CheetahString::from(client_id),
                    CheetahString::from(config.query_assignment_strategy_name),
                    MessageModel::Clustering,
                    config.mq_client_api_timeout_ms,
                )
                .await?;

            Ok(assignments.map(|items| items.into_iter().collect()))
        })
        .await
    }

    async fn query_topic_message_type(&self, topic: &ResourceIdentity) -> ProxyResult<ProxyTopicMessageType> {
        let config = self.config.clone();
        let topic_name = topic.to_string();

        run_cluster_task(move || async move {
            let client = initialize_client_instance(config.clone()).await?;
            let route = client
                .get_mq_client_api_impl()
                .get_topic_route_info_from_name_server(topic_name.as_str(), config.mq_client_api_timeout_ms)
                .await?
                .ok_or_else(|| RocketMQError::route_not_found(topic_name.clone()))?;
            let broker_addr = select_master_broker_addr(&route).ok_or_else(|| RocketMQError::BrokerNotFound {
                name: topic_name.clone(),
            })?;
            let topic_config = client
                .get_topic_config(
                    &broker_addr,
                    CheetahString::from(topic_name),
                    config.mq_client_api_timeout_ms,
                )
                .await?;

            Ok(convert_topic_message_type(topic_config.get_topic_message_type()))
        })
        .await
    }

    async fn query_subscription_group(
        &self,
        topic: &ResourceIdentity,
        group: &ResourceIdentity,
    ) -> ProxyResult<Option<SubscriptionGroupMetadata>> {
        let config = self.config.clone();
        let topic_name = topic.to_string();
        let group_name = group.to_string();

        run_cluster_task(move || async move {
            let client = initialize_client_instance(config.clone()).await?;
            let route = client
                .get_mq_client_api_impl()
                .get_topic_route_info_from_name_server(topic_name.as_str(), config.mq_client_api_timeout_ms)
                .await?
                .ok_or_else(|| RocketMQError::route_not_found(topic_name.clone()))?;
            let Some(broker_addr) = select_master_broker_addr(&route) else {
                return Ok(None);
            };
            let group_config = client
                .get_subscription_group_config(
                    &broker_addr,
                    CheetahString::from(group_name),
                    config.mq_client_api_timeout_ms,
                )
                .await?;

            Ok(Some(convert_subscription_group(group_config)))
        })
        .await
    }

    async fn send_message(
        &self,
        context: &ProxyContext,
        request: &SendMessageRequest,
    ) -> ProxyResult<Vec<SendMessageResultEntry>> {
        let config = self.config.clone();
        let request = request.clone();
        let client_id = context.client_id().map(ToOwned::to_owned);
        let request_id = context.request_id().to_owned();

        run_cluster_task(move || async move {
            let timeout = effective_send_timeout_ms(&config, request.timeout);
            let mut producer =
                build_send_producer(&config, &request, client_id.as_deref(), request_id.as_str(), timeout);
            if let Err(error) = producer.start().await {
                let error = ProxyError::from(error);
                return Ok(request
                    .messages
                    .iter()
                    .map(|_| failure_send_result_entry(&error))
                    .collect());
            }

            let send_result = async {
                let mut results = Vec::with_capacity(request.messages.len());
                for entry in request.messages {
                    results.push(send_message_entry(&mut producer, entry, timeout).await);
                }
                Ok(results)
            }
            .await;

            producer.shutdown().await;
            send_result
        })
        .await
    }

    async fn receive_message(
        &self,
        context: &ProxyContext,
        request: &ReceiveMessageRequest,
    ) -> ProxyResult<ReceiveMessagePlan> {
        let config = self.config.clone();
        let context_deadline = context.deadline();
        let request = request.clone();

        run_cluster_task(move || async move {
            let mut client = initialize_client_instance(config.clone()).await?;
            let broker_target = resolve_receive_broker(&mut client, &config, &request).await?;
            let timeout_ms = effective_pop_timeout_ms(&config, context_deadline, request.long_polling_timeout);
            let request_header = build_pop_request_header(&broker_target.broker_name, &request);
            let pop_result = pop_message(
                &client,
                &broker_target.broker_name,
                &broker_target.broker_addr,
                request_header,
                timeout_ms,
            )
            .await?;

            Ok(build_receive_plan(pop_result))
        })
        .await
    }

    async fn ack_message(
        &self,
        context: &ProxyContext,
        request: &AckMessageRequest,
    ) -> ProxyResult<Vec<AckMessageResultEntry>> {
        let config = self.config.clone();
        let timeout_ms = effective_request_timeout_ms(config.mq_client_api_timeout_ms, context.deadline());
        let request = request.clone();

        run_cluster_task(move || async move {
            let mut client = initialize_client_instance(config.clone()).await?;
            let mut results = Vec::with_capacity(request.entries.len());

            for entry in &request.entries {
                results.push(ack_message_entry(&mut client, &request.group, &request.topic, entry, timeout_ms).await);
            }

            Ok(results)
        })
        .await
    }

    async fn change_invisible_duration(
        &self,
        context: &ProxyContext,
        request: &ChangeInvisibleDurationRequest,
    ) -> ProxyResult<ChangeInvisibleDurationPlan> {
        let config = self.config.clone();
        let timeout_ms = effective_request_timeout_ms(config.mq_client_api_timeout_ms, context.deadline());
        let request = request.clone();

        run_cluster_task(move || async move {
            let mut client = initialize_client_instance(config.clone()).await?;
            change_invisible_duration_inner(&mut client, &config, &request, timeout_ms).await
        })
        .await
    }

    async fn end_transaction(
        &self,
        context: &ProxyContext,
        request: &EndTransactionRequest,
    ) -> ProxyResult<EndTransactionPlan> {
        let config = self.config.clone();
        let request = request.clone();
        let client_id = context.client_id().map(ToOwned::to_owned);
        let request_id = context.request_id().to_owned();
        let timeout_ms = effective_request_timeout_ms(config.mq_client_api_timeout_ms, context.deadline());

        run_cluster_task(move || async move {
            let mut client = initialize_client_instance(config.clone()).await?;
            end_transaction_inner(
                &mut client,
                &config,
                &request,
                client_id.as_deref(),
                request_id.as_str(),
                timeout_ms,
            )
            .await
        })
        .await
    }
}

async fn run_cluster_task<T, F, Fut>(task: F) -> ProxyResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ProxyResult<T>> + 'static,
{
    tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| ProxyError::Transport {
                message: format!("failed to build proxy cluster runtime: {error}"),
            })?;
        runtime.block_on(task())
    })
    .await
    .map_err(|error| ProxyError::Transport {
        message: format!("proxy cluster task failed: {error}"),
    })?
}

async fn initialize_client_instance(config: ClusterConfig) -> ProxyResult<ArcMut<MQClientInstance>> {
    let mut client_config = RocketmqClientConfig::default();
    client_config.set_instance_name(CheetahString::from(config.instance_name));
    client_config.set_mq_client_api_timeout(config.mq_client_api_timeout_ms);
    if let Some(namesrv_addr) = config.namesrv_addr {
        client_config.set_namesrv_addr(CheetahString::from(namesrv_addr));
    }

    let mut instance = MQClientManager::get_instance().get_or_create_mq_client_instance(client_config, None);
    let this = instance.clone();
    instance.start(this).await?;
    Ok(instance)
}

#[derive(Debug, Clone)]
struct BrokerTarget {
    broker_name: CheetahString,
    broker_addr: CheetahString,
}

#[derive(Debug, Clone)]
struct ParsedReceiptHandle {
    raw: CheetahString,
    broker_name: CheetahString,
    topic: CheetahString,
    queue_id: i32,
    queue_offset: i64,
}

struct PopResultCallback {
    sender: Option<oneshot::Sender<ProxyResult<PopResult>>>,
}

impl PopResultCallback {
    fn new(sender: oneshot::Sender<ProxyResult<PopResult>>) -> Self {
        Self { sender: Some(sender) }
    }
}

impl PopCallback for PopResultCallback {
    async fn on_success(&mut self, pop_result: PopResult) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(Ok(pop_result));
        }
    }

    fn on_error(&mut self, error: Box<dyn std::error::Error + Send>) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(Err(ProxyError::Transport {
                message: error.to_string(),
            }));
        }
    }
}

struct AckResultCallback {
    sender: Mutex<Option<oneshot::Sender<ProxyResult<AckResult>>>>,
}

impl AckResultCallback {
    fn new(sender: oneshot::Sender<ProxyResult<AckResult>>) -> Self {
        Self {
            sender: Mutex::new(Some(sender)),
        }
    }

    fn send(&self, result: ProxyResult<AckResult>) {
        if let Some(sender) = self.sender.lock().expect("ack callback mutex poisoned").take() {
            let _ = sender.send(result);
        }
    }
}

impl AckCallback for AckResultCallback {
    fn on_success(&self, ack_result: AckResult) {
        self.send(Ok(ack_result));
    }

    fn on_exception(&self, error: Box<dyn std::error::Error>) {
        self.send(Err(ProxyError::Transport {
            message: error.to_string(),
        }));
    }
}

async fn resolve_receive_broker(
    client: &mut ArcMut<MQClientInstance>,
    config: &ClusterConfig,
    request: &ReceiveMessageRequest,
) -> ProxyResult<BrokerTarget> {
    if let (Some(broker_name), Some(broker_addr)) =
        (request.target.broker_name.as_ref(), request.target.broker_addr.as_ref())
    {
        return Ok(BrokerTarget {
            broker_name: CheetahString::from(broker_name.as_str()),
            broker_addr: CheetahString::from(broker_addr.as_str()),
        });
    }

    let topic = CheetahString::from(request.target.topic.to_string());
    if let Some(broker_name) = request.target.broker_name.as_ref() {
        let broker_name = CheetahString::from(broker_name.as_str());
        let actual_broker_name =
            resolve_subscription_broker_name(client, &topic, &broker_name, request.target.queue_id).await;
        if let Some(broker_addr) = find_subscribe_broker_addr(client, &actual_broker_name, &topic).await {
            return Ok(BrokerTarget {
                broker_name,
                broker_addr,
            });
        }
    }

    let route = client
        .get_mq_client_api_impl()
        .get_topic_route_info_from_name_server(topic.as_str(), config.mq_client_api_timeout_ms)
        .await?
        .ok_or_else(|| RocketMQError::route_not_found(topic.as_str()))?;
    let broker_addr = select_master_broker_addr(&route).ok_or_else(|| RocketMQError::BrokerNotFound {
        name: topic.to_string(),
    })?;
    let broker_name = route
        .broker_datas
        .iter()
        .find_map(|broker| {
            broker
                .broker_addrs()
                .iter()
                .find(|(_, addr)| *addr == &broker_addr)
                .map(|_| broker.broker_name().clone())
        })
        .unwrap_or_default();

    Ok(BrokerTarget {
        broker_name,
        broker_addr,
    })
}

async fn pop_message(
    client: &ArcMut<MQClientInstance>,
    broker_name: &CheetahString,
    broker_addr: &CheetahString,
    request_header: PopMessageRequestHeader,
    timeout_ms: u64,
) -> ProxyResult<PopResult> {
    let (sender, receiver) = oneshot::channel();
    client
        .get_mq_client_api_impl()
        .pop_message_async(
            broker_name,
            broker_addr,
            request_header,
            timeout_ms,
            PopResultCallback::new(sender),
        )
        .await?;

    receiver.await.map_err(|error| ProxyError::Transport {
        message: format!("proxy pop callback dropped: {error}"),
    })?
}

async fn ack_message_entry(
    client: &mut ArcMut<MQClientInstance>,
    group: &ResourceIdentity,
    topic: &ResourceIdentity,
    entry: &crate::processor::AckMessageEntry,
    timeout_ms: u64,
) -> AckMessageResultEntry {
    let status = match ack_message_entry_inner(client, group, topic, entry, timeout_ms).await {
        Ok(status) => status,
        Err(error) => ProxyStatusMapper::from_error_payload(&error),
    };

    AckMessageResultEntry {
        message_id: entry.message_id.clone(),
        receipt_handle: entry.receipt_handle.clone(),
        status,
    }
}

async fn ack_message_entry_inner(
    client: &mut ArcMut<MQClientInstance>,
    group: &ResourceIdentity,
    topic: &ResourceIdentity,
    entry: &crate::processor::AckMessageEntry,
    timeout_ms: u64,
) -> ProxyResult<ProxyPayloadStatus> {
    let topic_name = entry.lite_topic.clone().unwrap_or_else(|| topic.to_string());
    let group_name = group.to_string();
    let parsed = parse_receipt_handle(entry.receipt_handle.as_str(), topic_name.as_str(), group_name.as_str())?;
    let actual_broker_name =
        resolve_subscription_broker_name(client, &parsed.topic, &parsed.broker_name, parsed.queue_id).await;
    let broker_addr = find_subscribe_broker_addr(client, &actual_broker_name, &parsed.topic)
        .await
        .ok_or_else(|| RocketMQError::BrokerNotFound {
            name: actual_broker_name.to_string(),
        })?;
    let request_header = AckMessageRequestHeader {
        consumer_group: CheetahString::from(group.to_string()),
        topic: parsed.topic.clone(),
        queue_id: parsed.queue_id,
        extra_info: parsed.raw.clone(),
        offset: parsed.queue_offset,
        topic_request_header: Some(TopicRequestHeader {
            rpc_request_header: Some(RpcRequestHeader {
                broker_name: Some(parsed.broker_name.clone()),
                ..Default::default()
            }),
            lo: None,
        }),
    };
    let ack_result = ack_message(client, &broker_addr, request_header, timeout_ms).await?;
    Ok(ack_status_to_payload(&ack_result))
}

async fn ack_message(
    client: &ArcMut<MQClientInstance>,
    broker_addr: &CheetahString,
    request_header: AckMessageRequestHeader,
    timeout_ms: u64,
) -> ProxyResult<AckResult> {
    let (sender, receiver) = oneshot::channel();
    client
        .get_mq_client_api_impl()
        .ack_message_async(broker_addr, request_header, timeout_ms, AckResultCallback::new(sender))
        .await?;

    receiver.await.map_err(|error| ProxyError::Transport {
        message: format!("proxy ack callback dropped: {error}"),
    })?
}

async fn change_invisible_duration_inner(
    client: &mut ArcMut<MQClientInstance>,
    _config: &ClusterConfig,
    request: &ChangeInvisibleDurationRequest,
    timeout_ms: u64,
) -> ProxyResult<ChangeInvisibleDurationPlan> {
    let topic_name = request.lite_topic.clone().unwrap_or_else(|| request.topic.to_string());
    let group_name = request.group.to_string();
    let parsed = parse_receipt_handle(
        request.receipt_handle.as_str(),
        topic_name.as_str(),
        group_name.as_str(),
    )?;
    let actual_broker_name =
        resolve_subscription_broker_name(client, &parsed.topic, &parsed.broker_name, parsed.queue_id).await;
    let broker_addr = find_subscribe_broker_addr(client, &actual_broker_name, &parsed.topic)
        .await
        .ok_or_else(|| RocketMQError::BrokerNotFound {
            name: actual_broker_name.to_string(),
        })?;
    let request_header = ChangeInvisibleTimeRequestHeader {
        consumer_group: CheetahString::from(request.group.to_string()),
        topic: parsed.topic.clone(),
        queue_id: parsed.queue_id,
        extra_info: parsed.raw.clone(),
        offset: parsed.queue_offset,
        invisible_time: request.invisible_duration.as_millis().clamp(1, i64::MAX as u128) as i64,
        topic_request_header: Some(TopicRequestHeader {
            rpc_request_header: Some(RpcRequestHeader {
                broker_name: Some(parsed.broker_name.clone()),
                ..Default::default()
            }),
            lo: None,
        }),
    };
    let ack_result =
        change_invisible_time(client, &parsed.broker_name, &broker_addr, request_header, timeout_ms).await?;

    Ok(ChangeInvisibleDurationPlan {
        status: ack_status_to_payload(&ack_result),
        receipt_handle: ack_result.extra_info().to_string(),
    })
}

async fn change_invisible_time(
    client: &ArcMut<MQClientInstance>,
    broker_name: &CheetahString,
    broker_addr: &CheetahString,
    request_header: ChangeInvisibleTimeRequestHeader,
    timeout_ms: u64,
) -> ProxyResult<AckResult> {
    let (sender, receiver) = oneshot::channel();
    client
        .get_mq_client_api_impl()
        .change_invisible_time_async(
            broker_name,
            broker_addr,
            request_header,
            timeout_ms,
            AckResultCallback::new(sender),
        )
        .await?;

    receiver.await.map_err(|error| ProxyError::Transport {
        message: format!("proxy change invisible callback dropped: {error}"),
    })?
}

async fn end_transaction_inner(
    client: &mut ArcMut<MQClientInstance>,
    config: &ClusterConfig,
    request: &EndTransactionRequest,
    client_id: Option<&str>,
    request_id: &str,
    timeout_ms: u64,
) -> ProxyResult<EndTransactionPlan> {
    let topic_name = request.topic.to_string();
    let route = client
        .get_mq_client_api_impl()
        .get_topic_route_info_from_name_server(topic_name.as_str(), config.mq_client_api_timeout_ms)
        .await?
        .ok_or_else(|| RocketMQError::route_not_found(topic_name.clone()))?;
    let broker_addr = select_master_broker_addr(&route).ok_or_else(|| RocketMQError::BrokerNotFound {
        name: topic_name.clone(),
    })?;
    let producer_group = request
        .producer_group
        .clone()
        .unwrap_or_else(|| build_proxy_producer_group(config, client_id, request_id));
    let transaction_state_table_offset = request
        .transaction_state_table_offset
        .ok_or_else(|| ProxyError::invalid_transaction_id("missing transactional message offset for endTransaction"))?;
    let broker_message_id = decode_end_transaction_message_id(
        request
            .commit_log_message_id
            .as_deref()
            .unwrap_or(request.message_id.as_str()),
    )?;
    let request_header = EndTransactionRequestHeader {
        topic: CheetahString::from(topic_name),
        producer_group: CheetahString::from(producer_group),
        tran_state_table_offset: transaction_state_table_offset,
        commit_log_offset: broker_message_id.offset as u64,
        commit_or_rollback: transaction_resolution_flag(request.resolution),
        from_transaction_check: matches!(request.source, TransactionSource::ServerCheck),
        msg_id: CheetahString::from(request.message_id.as_str()),
        transaction_id: Some(CheetahString::from(request.transaction_id.as_str())),
        rpc_request_header: RpcRequestHeader::default(),
    };

    client
        .get_mq_client_api_impl()
        .end_transaction_oneway(
            &broker_addr,
            request_header,
            CheetahString::from(request.trace_context.as_deref().unwrap_or("")),
            timeout_ms,
        )
        .await?;

    Ok(EndTransactionPlan {
        status: ProxyStatusMapper::ok_payload(),
    })
}

fn build_pop_request_header(broker_name: &CheetahString, request: &ReceiveMessageRequest) -> PopMessageRequestHeader {
    PopMessageRequestHeader {
        consumer_group: CheetahString::from(request.group.to_string()),
        topic: CheetahString::from(request.target.topic.to_string()),
        queue_id: request.target.queue_id,
        max_msg_nums: request.batch_size,
        invisible_time: request.invisible_duration.as_millis().clamp(1, u128::from(u64::MAX)) as u64,
        poll_time: request.long_polling_timeout.as_millis().clamp(0, u128::from(u64::MAX)) as u64,
        born_time: current_millis(),
        init_mode: 0,
        exp_type: Some(CheetahString::from(request.filter_expression.expression_type.as_str())),
        exp: Some(CheetahString::from(request.filter_expression.expression.as_str())),
        order: Some(request.target.fifo),
        attempt_id: request.attempt_id.as_deref().map(CheetahString::from),
        topic_request_header: Some(PopTopicRequestHeader {
            lo: None,
            rpc: Some(RpcRequestHeader {
                broker_name: Some(broker_name.clone()),
                ..Default::default()
            }),
        }),
    }
}

fn build_receive_plan(pop_result: PopResult) -> ReceiveMessagePlan {
    let delivery_timestamp_ms = (pop_result.pop_time > 0).then_some(pop_result.pop_time as i64);
    let invisible_duration = Duration::from_millis(pop_result.invisible_time.max(1));
    match pop_result.pop_status {
        PopStatus::Found => {
            let messages = pop_result
                .msg_found_list
                .unwrap_or_default()
                .into_iter()
                .map(|message| ReceivedMessage {
                    message,
                    invisible_duration,
                })
                .collect::<Vec<_>>();
            let status = if messages.is_empty() {
                ProxyStatusMapper::from_payload_code(v2::Code::MessageNotFound, "no message available")
            } else {
                ProxyStatusMapper::ok_payload()
            };
            ReceiveMessagePlan {
                status,
                delivery_timestamp_ms,
                messages,
            }
        }
        PopStatus::NoNewMsg | PopStatus::PollingNotFound => ReceiveMessagePlan {
            status: ProxyStatusMapper::from_payload_code(v2::Code::MessageNotFound, "no message available"),
            delivery_timestamp_ms,
            messages: Vec::new(),
        },
        PopStatus::PollingFull => ReceiveMessagePlan {
            status: ProxyStatusMapper::from_payload_code(v2::Code::TooManyRequests, "broker polling queue is full"),
            delivery_timestamp_ms,
            messages: Vec::new(),
        },
    }
}

fn ack_status_to_payload(ack_result: &AckResult) -> ProxyPayloadStatus {
    match ack_result.status() {
        AckStatus::Ok => ProxyStatusMapper::ok_payload(),
        AckStatus::NotExist => ProxyStatusMapper::from_payload_code(
            v2::Code::InvalidReceiptHandle,
            "receipt handle has expired or message no longer exists",
        ),
    }
}

fn parse_receipt_handle(receipt_handle: &str, topic: &str, consumer_group: &str) -> ProxyResult<ParsedReceiptHandle> {
    let trimmed = receipt_handle.trim();
    if trimmed.is_empty() {
        return Err(ProxyError::invalid_receipt_handle("receipt handle must not be empty"));
    }

    let parts = ExtraInfoUtil::split(trimmed);
    let broker_name = ExtraInfoUtil::get_broker_name(parts.as_slice())
        .map(CheetahString::from_string)
        .map_err(|error| ProxyError::invalid_receipt_handle(error.to_string()))?;
    let queue_id = ExtraInfoUtil::get_queue_id(parts.as_slice())
        .map_err(|error| ProxyError::invalid_receipt_handle(error.to_string()))?;
    let queue_offset = ExtraInfoUtil::get_queue_offset(parts.as_slice())
        .map_err(|error| ProxyError::invalid_receipt_handle(error.to_string()))?;
    let real_topic = ExtraInfoUtil::get_real_topic(parts.as_slice(), topic, consumer_group)
        .map(CheetahString::from_string)
        .map_err(|error| ProxyError::invalid_receipt_handle(error.to_string()))?;

    Ok(ParsedReceiptHandle {
        raw: CheetahString::from(trimmed),
        broker_name,
        topic: real_topic,
        queue_id,
        queue_offset,
    })
}

async fn find_subscribe_broker_addr(
    client: &mut ArcMut<MQClientInstance>,
    broker_name: &CheetahString,
    topic: &CheetahString,
) -> Option<CheetahString> {
    let mut result = client
        .find_broker_address_in_subscribe(broker_name, MASTER_ID, true)
        .await;
    if result.is_none() {
        client.update_topic_route_info_from_name_server_topic(topic).await;
        result = client
            .find_broker_address_in_subscribe(broker_name, MASTER_ID, true)
            .await;
    }
    result.map(|found| found.broker_addr)
}

async fn resolve_subscription_broker_name(
    client: &ArcMut<MQClientInstance>,
    topic: &CheetahString,
    broker_name: &CheetahString,
    queue_id: i32,
) -> CheetahString {
    if !broker_name.is_empty() && broker_name.starts_with(mix_all::LOGICAL_QUEUE_MOCK_BROKER_PREFIX) {
        let queue = MessageQueue::from_parts(topic.clone(), broker_name.clone(), queue_id);
        client.get_broker_name_from_message_queue(&queue).await
    } else {
        broker_name.clone()
    }
}

fn build_send_producer(
    config: &ClusterConfig,
    request: &SendMessageRequest,
    client_id: Option<&str>,
    request_id: &str,
    timeout_ms: u64,
) -> DefaultMQProducer {
    let mut client_config = RocketmqClientConfig::default();
    client_config.set_instance_name(CheetahString::from(format!(
        "{}-{}",
        config.instance_name,
        sanitize_group_component(request_id)
    )));
    client_config.set_mq_client_api_timeout(config.mq_client_api_timeout_ms);
    if let Some(namesrv_addr) = config.namesrv_addr.as_ref() {
        client_config.set_namesrv_addr(CheetahString::from(namesrv_addr.as_str()));
    }

    let topics = request
        .messages
        .iter()
        .map(|message| CheetahString::from(message.topic.to_string()))
        .collect();

    DefaultMQProducer::builder()
        .client_config(client_config)
        .producer_group(build_proxy_producer_group(config, client_id, request_id))
        .topics(topics)
        .send_msg_timeout(timeout_ms as u32)
        .build()
}

async fn send_message_entry(
    producer: &mut DefaultMQProducer,
    entry: SendMessageEntry,
    timeout: u64,
) -> SendMessageResultEntry {
    match send_message_entry_inner(producer, entry, timeout).await {
        Ok(send_result) => SendMessageResultEntry {
            status: ProxyStatusMapper::from_send_result_payload(&send_result),
            send_result: Some(send_result),
        },
        Err(error) => failure_send_result_entry(&error),
    }
}

async fn send_message_entry_inner(
    producer: &mut DefaultMQProducer,
    entry: SendMessageEntry,
    timeout: u64,
) -> ProxyResult<SendResult> {
    let queue = resolve_target_queue(producer, &entry).await?;
    let result = if let Some(queue) = queue {
        producer
            .send_to_queue_with_timeout(entry.message, queue, timeout)
            .await?
    } else {
        producer.send_with_timeout(entry.message, timeout).await?
    };

    result.ok_or_else(|| ProxyError::Transport {
        message: format!("send result was empty for topic '{}'", entry.topic),
    })
}

async fn resolve_target_queue(
    producer: &mut DefaultMQProducer,
    entry: &SendMessageEntry,
) -> ProxyResult<Option<MessageQueue>> {
    let Some(queue_id) = entry.queue_id else {
        return Ok(None);
    };

    let queues = producer
        .fetch_publish_message_queues(entry.topic.to_string().as_str())
        .await?;
    let max_queue_id = queues.iter().map(MessageQueue::queue_id).max().unwrap_or(-1);

    queues
        .into_iter()
        .find(|queue| queue.queue_id() == queue_id)
        .map(Some)
        .ok_or_else(|| {
            if max_queue_id >= 0 {
                RocketMQError::QueueIdOutOfRange {
                    topic: entry.topic.to_string(),
                    queue_id,
                    max: max_queue_id,
                }
                .into()
            } else {
                RocketMQError::QueueNotExist {
                    topic: entry.topic.to_string(),
                    queue_id,
                }
                .into()
            }
        })
}

fn failure_send_result_entry(error: &ProxyError) -> SendMessageResultEntry {
    SendMessageResultEntry {
        status: ProxyStatusMapper::from_error_payload(error),
        send_result: None,
    }
}

fn sanitize_group_component(value: &str) -> String {
    let sanitized: String = value
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '%' | '|'))
        .collect();

    if sanitized.is_empty() {
        "proxy".to_owned()
    } else {
        sanitized
    }
}

pub(crate) fn build_proxy_producer_group(config: &ClusterConfig, client_id: Option<&str>, request_id: &str) -> String {
    let prefix = sanitize_group_component(config.producer_group_prefix.as_str());
    let identity = sanitize_group_component(client_id.unwrap_or(request_id));
    format!("{prefix}-{identity}")
}

fn transaction_resolution_flag(resolution: TransactionResolution) -> i32 {
    match resolution {
        TransactionResolution::Commit => MessageSysFlag::TRANSACTION_COMMIT_TYPE,
        TransactionResolution::Rollback => MessageSysFlag::TRANSACTION_ROLLBACK_TYPE,
    }
}

fn decode_end_transaction_message_id(message_id: &str) -> ProxyResult<MessageId> {
    MessageDecoder::decode_message_id(&CheetahString::from(message_id)).map_err(|error| {
        ProxyError::invalid_transaction_id(format!("failed to decode transactional message id: {error}"))
    })
}

fn effective_send_timeout_ms(config: &ClusterConfig, deadline: Option<Duration>) -> u64 {
    effective_request_timeout_ms(config.send_message_timeout_ms, deadline)
}

fn effective_request_timeout_ms(configured: u64, deadline: Option<Duration>) -> u64 {
    let configured = configured.max(1);
    let Some(deadline) = deadline else {
        return configured;
    };

    let deadline_ms = deadline.as_millis().clamp(1, u128::from(u64::MAX)) as u64;
    configured.min(deadline_ms).max(1)
}

fn effective_pop_timeout_ms(config: &ClusterConfig, deadline: Option<Duration>, poll_timeout: Duration) -> u64 {
    let minimum = poll_timeout
        .as_millis()
        .saturating_add(500)
        .clamp(1, u128::from(u64::MAX)) as u64;
    effective_request_timeout_ms(config.mq_client_api_timeout_ms.max(minimum), deadline)
}

fn select_master_broker_addr(route: &TopicRouteData) -> Option<CheetahString> {
    route.broker_datas.iter().find_map(|broker| {
        broker
            .broker_addrs()
            .get(&MASTER_ID)
            .cloned()
            .or_else(|| broker.select_broker_addr())
    })
}

fn convert_topic_message_type(message_type: TopicMessageType) -> ProxyTopicMessageType {
    match message_type {
        TopicMessageType::Unspecified => ProxyTopicMessageType::Unspecified,
        TopicMessageType::Normal => ProxyTopicMessageType::Normal,
        TopicMessageType::Fifo => ProxyTopicMessageType::Fifo,
        TopicMessageType::Delay => ProxyTopicMessageType::Delay,
        TopicMessageType::Transaction => ProxyTopicMessageType::Transaction,
        TopicMessageType::Mixed => ProxyTopicMessageType::Mixed,
    }
}

fn convert_subscription_group(group_config: SubscriptionGroupConfig) -> SubscriptionGroupMetadata {
    SubscriptionGroupMetadata {
        consume_message_orderly: group_config.consume_message_orderly(),
        lite_bind_topic: None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;
    use rocketmq_common::common::attribute::topic_message_type::TopicMessageType;
    use rocketmq_remoting::protocol::route::route_data_view::BrokerData;
    use rocketmq_remoting::protocol::route::route_data_view::QueueData;
    use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
    use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;

    use super::convert_subscription_group;
    use super::convert_topic_message_type;
    use super::select_master_broker_addr;
    use crate::service::ProxyTopicMessageType;

    #[test]
    fn cluster_client_prefers_master_broker_address() {
        let mut broker_addrs = HashMap::new();
        broker_addrs.insert(1_u64, CheetahString::from("127.0.0.2:10911"));
        broker_addrs.insert(0_u64, CheetahString::from("127.0.0.1:10911"));
        let route = TopicRouteData {
            queue_datas: vec![QueueData::new(CheetahString::from("broker-a"), 1, 1, 6, 0)],
            broker_datas: vec![BrokerData::new(
                CheetahString::from("cluster-a"),
                CheetahString::from("broker-a"),
                broker_addrs,
                None,
            )],
            ..Default::default()
        };

        assert_eq!(select_master_broker_addr(&route).unwrap().as_str(), "127.0.0.1:10911");
    }

    #[test]
    fn topic_message_type_conversion_matches_proxy_model() {
        assert_eq!(
            convert_topic_message_type(TopicMessageType::Fifo),
            ProxyTopicMessageType::Fifo
        );
    }

    #[test]
    fn subscription_group_conversion_preserves_order_flag() {
        let mut config = SubscriptionGroupConfig::default();
        config.set_consume_message_orderly(true);

        let converted = convert_subscription_group(config);
        assert!(converted.consume_message_orderly);
    }
}
