// Copyright 2026 The RocketMQ Rust Authors
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

//! Producer admin service models and operations.

use std::sync::Arc;
use std::sync::Mutex;

use cheetah_string::CheetahString;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_client_rust::base::client_config::ClientConfig;
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_remoting::protocol::body::producer_table_info::ProducerTableInfo;
use rocketmq_remoting::runtime::RPCHook;
use serde::Deserialize;
use serde::Serialize;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::core::admin::AdminBuilder;
use crate::core::RocketMQError;
use crate::core::RocketMQResult;
use crate::core::ToolsError;

const SEND_MESSAGE_STATUS_PRODUCER_GROUP: &str = "PID_SMSC";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerInfoQueryRequest {
    broker_addr: CheetahString,
    namesrv_addr: Option<String>,
}

impl ProducerInfoQueryRequest {
    pub fn try_new(broker_addr: impl Into<String>) -> RocketMQResult<Self> {
        Ok(Self {
            broker_addr: trim_required_cheetah("brokerAddr", broker_addr)?,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn broker_addr(&self) -> &CheetahString {
        &self.broker_addr
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        builder_with_namesrv(self.namesrv_addr())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerInfoQueryResult {
    pub producer_table_info: ProducerTableInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SendMessageRequest {
    topic: CheetahString,
    body: String,
    keys: Option<String>,
    tags: Option<String>,
    broker_name: Option<CheetahString>,
    queue_id: Option<i32>,
    msg_trace_enable: bool,
}

impl SendMessageRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        topic: impl Into<String>,
        body: impl Into<String>,
        keys: Option<String>,
        tags: Option<String>,
        broker_name: Option<String>,
        queue_id: Option<i32>,
        msg_trace_enable: bool,
    ) -> RocketMQResult<Self> {
        if queue_id.is_some() && trim_optional_string(broker_name.clone()).is_none() {
            return Err(
                ToolsError::validation_error("brokerName", "brokerName must be set if queueId is provided").into(),
            );
        }

        Ok(Self {
            topic: trim_required_cheetah("topic", topic)?,
            body: trim_required_string("body", body)?,
            keys: trim_optional_string(keys),
            tags: trim_optional_string(tags),
            broker_name: trim_optional_string(broker_name)
                .map(|broker_name| trim_required_cheetah("brokerName", broker_name))
                .transpose()?,
            queue_id,
            msg_trace_enable,
        })
    }

    pub fn topic(&self) -> &CheetahString {
        &self.topic
    }

    pub fn body(&self) -> &str {
        &self.body
    }

    pub fn keys(&self) -> Option<&str> {
        self.keys.as_deref()
    }

    pub fn tags(&self) -> Option<&str> {
        self.tags.as_deref()
    }

    pub fn broker_name(&self) -> Option<&CheetahString> {
        self.broker_name.as_ref()
    }

    pub fn queue_id(&self) -> Option<i32> {
        self.queue_id
    }

    pub fn msg_trace_enable(&self) -> bool {
        self.msg_trace_enable
    }

    fn message(&self) -> RocketMQResult<Message> {
        let builder = Message::builder()
            .topic(self.topic.as_str())
            .body(self.body.as_bytes().to_vec());
        let builder = match self.tags() {
            Some(tags) => builder.tags(tags),
            None => builder,
        };
        let builder = match self.keys() {
            Some(keys) => builder.key(keys),
            None => builder,
        };
        builder
            .build()
            .map_err(|error| RocketMQError::Internal(format!("SendMessageRequest: failed to build message: {error}")))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SendMessageResultRow {
    pub broker_name: String,
    pub queue_id: String,
    pub send_status: String,
    pub msg_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SendMessageResult {
    pub row: SendMessageResultRow,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SendMessageStatusRequest {
    broker_name: CheetahString,
    message_size: usize,
    count: u32,
}

impl SendMessageStatusRequest {
    pub fn try_new(broker_name: impl Into<String>, message_size: usize, count: u32) -> RocketMQResult<Self> {
        Ok(Self {
            broker_name: trim_required_cheetah("brokerName", broker_name)?,
            message_size,
            count,
        })
    }

    pub fn broker_name(&self) -> &CheetahString {
        &self.broker_name
    }

    pub fn message_size(&self) -> usize {
        self.message_size
    }

    pub fn count(&self) -> u32 {
        self.count
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SendMessageStatusRow {
    pub rt_millis: u64,
    pub send_result: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SendMessageStatusResult {
    pub rows: Vec<SendMessageStatusRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckMessageSendRtRequest {
    topic: CheetahString,
    amount: u64,
    size: usize,
}

impl CheckMessageSendRtRequest {
    pub fn try_new(topic: impl Into<String>, amount: u64, size: usize) -> RocketMQResult<Self> {
        if amount < 2 {
            return Err(ToolsError::validation_error("amount", "amount must be at least 2").into());
        }
        Ok(Self {
            topic: trim_required_cheetah("topic", topic)?,
            amount,
            size,
        })
    }

    pub fn topic(&self) -> &CheetahString {
        &self.topic
    }

    pub fn amount(&self) -> u64 {
        self.amount
    }

    pub fn size(&self) -> usize {
        self.size
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckMessageSendRtRow {
    pub broker_name: String,
    pub queue_id: i32,
    pub send_success: bool,
    pub rt_millis: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CheckMessageSendRtResult {
    pub rows: Vec<CheckMessageSendRtRow>,
    pub avg_rt: f64,
}

pub struct ProducerService;

impl ProducerService {
    pub async fn query_producer_info_by_request_with_rpc_hook(
        request: ProducerInfoQueryRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<ProducerInfoQueryResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::query_producer_info_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn query_producer_info_with_admin(
        admin: &DefaultMQAdminExt,
        request: &ProducerInfoQueryRequest,
    ) -> RocketMQResult<ProducerInfoQueryResult> {
        let producer_table_info = admin.get_all_producer_info(request.broker_addr.clone()).await?;
        Ok(ProducerInfoQueryResult { producer_table_info })
    }

    pub async fn send_message_by_request_with_rpc_hook(
        request: SendMessageRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<SendMessageResult> {
        let mut builder = DefaultMQProducer::builder().producer_group(current_millis().to_string());
        if let Some(rpc_hook) = rpc_hook {
            builder = builder.rpc_hook(rpc_hook);
        }
        let mut producer = builder.build();

        let result = Self::send_message_with_producer(&mut producer, &request).await;
        producer.shutdown().await;
        result
    }

    pub async fn send_message_with_producer(
        producer: &mut DefaultMQProducer,
        request: &SendMessageRequest,
    ) -> RocketMQResult<SendMessageResult> {
        producer
            .start()
            .await
            .map_err(|error| RocketMQError::Internal(format!("ProducerService: failed to start producer: {error}")))?;

        let message = request.message()?;
        let send_result = if let (Some(broker_name), Some(queue_id)) = (request.broker_name(), request.queue_id()) {
            let message_queue = MessageQueue::from_parts(request.topic().clone(), broker_name.clone(), queue_id);
            producer.send_to_queue(message, message_queue).await
        } else {
            producer.send(message).await
        }
        .map_err(|error| RocketMQError::Internal(format!("ProducerService: failed to send message: {error}")))?;

        let row = if let Some(result) = send_result {
            SendMessageResultRow {
                broker_name: result
                    .message_queue
                    .as_ref()
                    .map(|mq| mq.broker_name().to_string())
                    .unwrap_or_else(|| "Unknown".to_string()),
                queue_id: result
                    .message_queue
                    .as_ref()
                    .map(|mq| mq.queue_id().to_string())
                    .unwrap_or_else(|| "Unknown".to_string()),
                send_status: format!("{:?}", result.send_status),
                msg_id: result
                    .msg_id
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "None".to_string()),
            }
        } else {
            SendMessageResultRow {
                broker_name: "Unknown".to_string(),
                queue_id: "Unknown".to_string(),
                send_status: "Failed".to_string(),
                msg_id: "None".to_string(),
            }
        };

        Ok(SendMessageResult { row })
    }

    pub async fn send_message_status_by_request_with_rpc_hook(
        request: SendMessageStatusRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<SendMessageStatusResult> {
        let instance_name = format!("{SEND_MESSAGE_STATUS_PRODUCER_GROUP}_{}", current_millis());
        let mut client_config = ClientConfig::default();
        client_config.set_instance_name(instance_name.into());

        let mut builder = DefaultMQProducer::builder()
            .producer_group(SEND_MESSAGE_STATUS_PRODUCER_GROUP.to_string())
            .client_config(client_config);
        if let Some(rpc_hook) = rpc_hook {
            builder = builder.rpc_hook(rpc_hook);
        }
        let mut producer = builder.build();

        let result = Self::send_message_status_with_producer(&mut producer, &request).await;
        producer.shutdown().await;
        result
    }

    pub async fn send_message_status_with_producer(
        producer: &mut DefaultMQProducer,
        request: &SendMessageStatusRequest,
    ) -> RocketMQResult<SendMessageStatusResult> {
        producer
            .start()
            .await
            .map_err(|error| RocketMQError::Internal(format!("ProducerService: failed to start producer: {error}")))?;

        producer
            .send(build_diagnostic_message(request.broker_name().as_str(), 16))
            .await
            .map_err(|error| {
                RocketMQError::Internal(format!(
                    "ProducerService: failed to warm up sendMsgStatus producer: {error}"
                ))
            })?;

        let mut rows = Vec::with_capacity(request.count() as usize);
        for _ in 0..request.count() {
            let begin = current_millis();
            let send_result = producer
                .send(build_diagnostic_message(
                    request.broker_name().as_str(),
                    request.message_size(),
                ))
                .await
                .map_err(|error| {
                    RocketMQError::Internal(format!("ProducerService: sendMsgStatus command failed: {error}"))
                })?;
            let rt_millis = current_millis() - begin;
            rows.push(SendMessageStatusRow {
                rt_millis,
                send_result: send_result
                    .map(|result| result.to_string())
                    .unwrap_or_else(|| "None".to_string()),
            });
        }

        Ok(SendMessageStatusResult { rows })
    }

    pub async fn check_message_send_rt_by_request_with_rpc_hook(
        request: CheckMessageSendRtRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<CheckMessageSendRtResult> {
        let mut builder = DefaultMQProducer::builder().producer_group(current_millis().to_string());
        if let Some(rpc_hook) = rpc_hook {
            builder = builder.rpc_hook(rpc_hook);
        }
        let mut producer = builder.build();

        let result = Self::check_message_send_rt_with_producer(&mut producer, &request).await;
        producer.shutdown().await;
        result
    }

    pub async fn check_message_send_rt_with_producer(
        producer: &mut DefaultMQProducer,
        request: &CheckMessageSendRtRequest,
    ) -> RocketMQResult<CheckMessageSendRtResult> {
        producer
            .start()
            .await
            .map_err(|error| RocketMQError::Internal(format!("ProducerService: failed to start producer: {error}")))?;

        let message = Message::builder()
            .topic(request.topic().as_str())
            .body_slice(&vec![b'a'; request.size()])
            .build()
            .map_err(|error| {
                RocketMQError::Internal(format!(
                    "ProducerService: failed to build checkMsgSendRT message: {error}"
                ))
            })?;
        let broker_name_holder = Arc::new(Mutex::new(String::new()));
        let queue_id_holder = Arc::new(Mutex::new(0));
        let mut rows = Vec::with_capacity(request.amount() as usize);
        let mut time_elapsed = 0;

        for index in 0..request.amount() {
            let start = current_millis();
            let broker_name_holder_for_selector = broker_name_holder.clone();
            let queue_id_holder_for_selector = queue_id_holder.clone();
            let selector = move |mqs: &[MessageQueue], _msg: &Message, arg: &u64| -> Option<MessageQueue> {
                if mqs.is_empty() {
                    return None;
                }
                let queue_index = (*arg as usize) % mqs.len();
                let queue = &mqs[queue_index];
                *broker_name_holder_for_selector.lock().unwrap() = queue.broker_name().to_string();
                *queue_id_holder_for_selector.lock().unwrap() = queue.queue_id();
                Some(queue.clone())
            };

            let send_success = producer
                .send_with_selector(message.clone(), selector, index)
                .await
                .is_ok();
            let rt_millis = current_millis() - start;
            if index != 0 {
                time_elapsed += rt_millis;
            }

            rows.push(CheckMessageSendRtRow {
                broker_name: broker_name_holder.lock().unwrap().clone(),
                queue_id: *queue_id_holder.lock().unwrap(),
                send_success,
                rt_millis,
            });
        }

        let avg_rt = time_elapsed as f64 / (request.amount() - 1) as f64;
        Ok(CheckMessageSendRtResult { rows, avg_rt })
    }
}

fn build_diagnostic_message(topic: &str, message_size: usize) -> Message {
    let filler = "hello jodie";
    let mut body = String::new();
    while body.len() < message_size {
        body.push_str(filler);
    }
    Message::builder()
        .topic(topic)
        .body_slice(body.as_bytes())
        .build_unchecked()
}

fn trim_optional_string(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn trim_required_cheetah(field: &'static str, value: impl Into<String>) -> RocketMQResult<CheetahString> {
    let value = value.into();
    let value = value.trim();
    if value.is_empty() {
        return Err(ToolsError::validation_error(field, format!("{field} must not be empty")).into());
    }
    Ok(CheetahString::from(value))
}

fn trim_required_string(field: &'static str, value: impl Into<String>) -> RocketMQResult<String> {
    let value = value.into();
    let value = value.trim();
    if value.is_empty() {
        return Err(ToolsError::validation_error(field, format!("{field} must not be empty")).into());
    }
    Ok(value.to_string())
}

fn builder_with_namesrv(namesrv_addr: Option<&str>) -> AdminBuilder {
    let builder = AdminBuilder::new();
    match namesrv_addr {
        Some(addr) => builder.namesrv_addr(addr),
        None => builder,
    }
}

fn admin_builder_with_rpc_hook(builder: AdminBuilder, rpc_hook: Option<Arc<dyn RPCHook>>) -> AdminBuilder {
    match rpc_hook {
        Some(hook) => builder.rpc_hook(hook),
        None => builder,
    }
}
