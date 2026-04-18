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

//! Message-related admin service models and operations.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_client_rust::consumer::pull_status::PullStatus;
use rocketmq_client_rust::TraceDataEncoder;
use rocketmq_common::common::message::message_decoder;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_common::MessageDecoder::decode_message_id;
use rocketmq_common::MessageDecoder::validate_message_id;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_remoting::runtime::RPCHook;
use rocketmq_rust::ArcMut;
use serde::Deserialize;
use serde::Serialize;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::core::admin::AdminBuilder;
use crate::core::RocketMQError;
use crate::core::RocketMQResult;
use crate::core::ToolsError;

const PULL_BATCH_SIZE: i32 = 32;
const PULL_TIMEOUT_MILLIS: u64 = 3000;
const QUERY_MAX_NUM: i32 = 32;
const DEFAULT_QUERY_WINDOW_MS: i64 = 36 * 60 * 60 * 1000;
const DEFAULT_CLUSTER: &str = "DefaultCluster";

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

fn decode_compaction_log_messages(data: Vec<u8>) -> Vec<MessageExt> {
    let file_size = data.len();
    let mut buf = Bytes::from(data);
    let mut current = 0usize;
    let mut messages = Vec::new();

    while current < file_size {
        if buf.len() < 4 {
            break;
        }

        let size = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        if size <= 0 || size as usize > file_size || buf.len() < size as usize {
            break;
        }

        let mut msg_bytes = buf.split_to(size as usize);
        let Some(message_ext) = message_decoder::decode(&mut msg_bytes, false, false, false, false, false) else {
            break;
        };

        current += size as usize;
        messages.push(message_ext);
    }

    messages
}

fn deal_time_to_hour_stamps(timestamp: i64) -> i64 {
    const HOUR_MS: i64 = 1000 * 60 * 60;
    timestamp / HOUR_MS * HOUR_MS
}

fn build_consume_admin(
    rpc_hook: Option<Arc<dyn RPCHook>>,
    consumer_group: Option<&CheetahString>,
) -> DefaultMQAdminExt {
    let mut admin = match (rpc_hook, consumer_group) {
        (Some(hook), Some(group)) => DefaultMQAdminExt::with_admin_ext_group_and_rpc_hook(group.clone(), hook),
        (Some(hook), None) => DefaultMQAdminExt::with_rpc_hook(hook),
        (None, Some(group)) => DefaultMQAdminExt::with_admin_ext_group(group.clone()),
        (None, None) => DefaultMQAdminExt::new(),
    };
    admin
        .client_config_mut()
        .set_instance_name(current_millis().to_string().into());
    admin
}

fn build_default_admin(rpc_hook: Option<Arc<dyn RPCHook>>) -> DefaultMQAdminExt {
    let mut admin = match rpc_hook {
        Some(hook) => DefaultMQAdminExt::with_rpc_hook(hook),
        None => DefaultMQAdminExt::new(),
    };
    admin
        .client_config_mut()
        .set_instance_name(current_millis().to_string().into());
    admin
}

fn route_topic_queues(
    topic_route: &rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData,
    topic: &str,
) -> Vec<(MessageQueue, CheetahString)> {
    let mut broker_addr_map: HashMap<String, CheetahString> = HashMap::new();
    for broker_data in &topic_route.broker_datas {
        if let Some(addr) = broker_data.select_broker_addr() {
            broker_addr_map.insert(broker_data.broker_name().to_string(), addr);
        }
    }

    let mut message_queues = Vec::new();
    for queue_data in &topic_route.queue_datas {
        let broker_name = queue_data.broker_name().as_str();
        let Some(broker_addr) = broker_addr_map.get(broker_name).cloned() else {
            continue;
        };

        let read_queue_nums = queue_data.read_queue_nums() as i32;
        if read_queue_nums <= 0 {
            continue;
        }

        for queue_id in 0..read_queue_nums {
            message_queues.push((
                MessageQueue::from_parts(topic, broker_name, queue_id),
                broker_addr.clone(),
            ));
        }
    }
    message_queues
}

async fn resolve_message_queues(
    admin: &DefaultMQAdminExt,
    topic: &str,
    route_topic: &str,
) -> RocketMQResult<Vec<(MessageQueue, CheetahString)>> {
    let topic_route = admin
        .examine_topic_route_info(CheetahString::from(route_topic))
        .await
        .map_err(|error| RocketMQError::Internal(format!("Failed to get topic route info: {error}")))?
        .ok_or_else(|| RocketMQError::Internal(format!("Topic route not found for: {route_topic}")))?;

    Ok(route_topic_queues(&topic_route, topic))
}

async fn resolve_broker_addr(
    admin: &DefaultMQAdminExt,
    route_topic: &str,
    broker_name: &str,
) -> RocketMQResult<CheetahString> {
    let topic_route = admin
        .examine_topic_route_info(CheetahString::from(route_topic))
        .await
        .map_err(|error| RocketMQError::Internal(format!("Failed to get topic route info: {error}")))?
        .ok_or_else(|| RocketMQError::Internal(format!("Topic route not found for: {route_topic}")))?;

    let broker_data = topic_route
        .broker_datas
        .iter()
        .find(|broker_data| broker_data.broker_name().as_str() == broker_name)
        .ok_or_else(|| {
            RocketMQError::IllegalArgument(format!(
                "Broker '{broker_name}' not found in topic route for '{route_topic}'"
            ))
        })?;

    broker_data
        .select_broker_addr()
        .ok_or_else(|| RocketMQError::Internal(format!("No available address for broker '{broker_name}'")))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryMessageByKeyRequest {
    topic: CheetahString,
    msg_key: CheetahString,
    begin_timestamp: i64,
    end_timestamp: i64,
    max_num: i32,
    cluster: Option<CheetahString>,
    key_type: CheetahString,
    last_key: Option<CheetahString>,
    namesrv_addr: Option<String>,
}

impl QueryMessageByKeyRequest {
    pub fn try_new(
        topic: impl Into<String>,
        msg_key: impl Into<String>,
        begin_timestamp: Option<i64>,
        end_timestamp: Option<i64>,
        max_num: i32,
        cluster: Option<String>,
        key_type: Option<String>,
        last_key: Option<String>,
    ) -> RocketMQResult<Self> {
        let key_type = trim_optional_string(key_type).unwrap_or_else(|| MessageConst::INDEX_KEY_TYPE.to_string());
        if key_type != MessageConst::INDEX_KEY_TYPE && key_type != MessageConst::INDEX_TAG_TYPE {
            return Err(
                ToolsError::validation_error("keyType", "keyType only supports K for keys or T for tags").into(),
            );
        }

        Ok(Self {
            topic: trim_required_cheetah("topic", topic)?,
            msg_key: trim_required_cheetah("msgKey", msg_key)?,
            begin_timestamp: begin_timestamp.unwrap_or(0),
            end_timestamp: end_timestamp.unwrap_or(i64::MAX),
            max_num,
            cluster: trim_optional_string(cluster)
                .map(|cluster| trim_required_cheetah("cluster", cluster))
                .transpose()?,
            key_type: CheetahString::from(key_type),
            last_key: trim_optional_string(last_key)
                .map(|last_key| trim_required_cheetah("lastKey", last_key))
                .transpose()?,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        builder_with_namesrv(self.namesrv_addr.as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryMessageByKeyRow {
    pub message_id: CheetahString,
    pub queue_id: i32,
    pub queue_offset: i64,
    pub index_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryMessageByKeyResult {
    pub rows: Vec<QueryMessageByKeyRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryMessageByIdRequest {
    message_ids: Vec<CheetahString>,
    topic: CheetahString,
    timeout_millis: u64,
    namesrv_addr: Option<String>,
}

impl QueryMessageByIdRequest {
    pub fn try_new(message_ids: Vec<String>, topic: Option<String>, timeout_millis: u64) -> RocketMQResult<Self> {
        if message_ids.is_empty() {
            return Err(ToolsError::validation_error("messageId", "At least one message ID is required").into());
        }

        let mut normalized_ids = Vec::with_capacity(message_ids.len());
        for msg_id in message_ids {
            validate_message_id(&msg_id).map_err(RocketMQError::IllegalArgument)?;
            normalized_ids.push(trim_required_cheetah("messageId", msg_id)?);
        }

        Ok(Self {
            message_ids: normalized_ids,
            topic: CheetahString::from(trim_optional_string(topic).unwrap_or_default()),
            timeout_millis,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn message_ids(&self) -> &[CheetahString] {
        &self.message_ids
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        let builder = builder_with_namesrv(self.namesrv_addr.as_deref());
        if self.timeout_millis > 0 {
            builder.timeout_millis(self.timeout_millis)
        } else {
            builder
        }
    }
}

#[derive(Debug, Clone)]
pub enum QueryMessageByIdOutcome {
    Found {
        message: Box<MessageExt>,
        broker_addr: String,
        query_time_ms: u64,
    },
    NotFound {
        reason: String,
        query_time_ms: u64,
    },
    Failed {
        error: String,
        query_time_ms: u64,
    },
    TimedOut,
}

#[derive(Debug, Clone)]
pub struct QueryMessageByIdEntry {
    pub message_id: CheetahString,
    pub outcome: QueryMessageByIdOutcome,
}

#[derive(Debug, Clone)]
pub struct QueryMessageByIdResult {
    pub entries: Vec<QueryMessageByIdEntry>,
}

impl QueryMessageByIdResult {
    pub fn success_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| {
                matches!(
                    entry.outcome,
                    QueryMessageByIdOutcome::Found { .. } | QueryMessageByIdOutcome::NotFound { .. }
                )
            })
            .count()
    }

    pub fn failure_count(&self) -> usize {
        self.entries.len().saturating_sub(self.success_count())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecodeMessageIdRequest {
    message_ids: Vec<CheetahString>,
}

impl DecodeMessageIdRequest {
    pub fn try_new(message_ids: Vec<String>) -> RocketMQResult<Self> {
        let message_ids = message_ids
            .into_iter()
            .filter_map(|message_id| trim_optional_string(Some(message_id)))
            .map(CheetahString::from)
            .collect::<Vec<_>>();

        if message_ids.is_empty() {
            return Err(ToolsError::validation_error("messageId", "At least one message ID is required").into());
        }

        Ok(Self { message_ids })
    }

    pub fn message_ids(&self) -> &[CheetahString] {
        &self.message_ids
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DecodeMessageIdOutcome {
    Decoded {
        broker_ip: String,
        broker_port: u16,
        commit_log_offset: i64,
        offset_hex: String,
    },
    Invalid {
        error: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecodeMessageIdEntry {
    pub message_id: CheetahString,
    pub outcome: DecodeMessageIdOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecodeMessageIdResult {
    pub entries: Vec<DecodeMessageIdEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DumpCompactionLogRequest {
    file: Option<PathBuf>,
}

impl DumpCompactionLogRequest {
    pub fn try_new(file: Option<String>) -> Self {
        Self {
            file: trim_optional_string(file).map(PathBuf::from),
        }
    }

    pub fn file(&self) -> Option<&Path> {
        self.file.as_deref()
    }
}

#[derive(Debug, Clone)]
pub struct DumpCompactionLogResult {
    pub messages: Vec<MessageExt>,
    pub missing_file_name: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryMessageByOffsetRequest {
    topic: CheetahString,
    broker_name: CheetahString,
    queue_id: i32,
    offset: i64,
    route_topic: Option<CheetahString>,
    namesrv_addr: Option<String>,
}

impl QueryMessageByOffsetRequest {
    pub fn try_new(
        topic: impl Into<String>,
        broker_name: impl Into<String>,
        queue_id: i32,
        offset: i64,
        route_topic: Option<String>,
    ) -> RocketMQResult<Self> {
        Ok(Self {
            topic: trim_required_cheetah("topic", topic)?,
            broker_name: trim_required_cheetah("brokerName", broker_name)?,
            queue_id,
            offset,
            route_topic: trim_optional_string(route_topic)
                .map(|route_topic| trim_required_cheetah("routeTopic", route_topic))
                .transpose()?,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        builder_with_namesrv(self.namesrv_addr.as_deref())
    }
}

#[derive(Debug, Clone)]
pub struct QueryMessageByOffsetResult {
    pub pull_status: PullStatus,
    pub message: Option<ArcMut<MessageExt>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryMessageByUniqueKeyRequest {
    msg_id: CheetahString,
    consumer_group: Option<CheetahString>,
    client_id: Option<CheetahString>,
    topic: CheetahString,
    show_all: bool,
    cluster: Option<CheetahString>,
    start_time: Option<i64>,
    end_time: Option<i64>,
    namesrv_addr: Option<String>,
}

impl QueryMessageByUniqueKeyRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        msg_id: impl Into<String>,
        consumer_group: Option<String>,
        client_id: Option<String>,
        topic: impl Into<String>,
        show_all: bool,
        cluster: Option<String>,
        start_time: Option<i64>,
        end_time: Option<i64>,
    ) -> RocketMQResult<Self> {
        Ok(Self {
            msg_id: trim_required_cheetah("msgId", msg_id)?,
            consumer_group: trim_optional_string(consumer_group)
                .map(|consumer_group| trim_required_cheetah("consumerGroup", consumer_group))
                .transpose()?,
            client_id: trim_optional_string(client_id)
                .map(|client_id| trim_required_cheetah("clientId", client_id))
                .transpose()?,
            topic: trim_required_cheetah("topic", topic)?,
            show_all,
            cluster: trim_optional_string(cluster)
                .map(|cluster| trim_required_cheetah("cluster", cluster))
                .transpose()?,
            start_time,
            end_time,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        builder_with_namesrv(self.namesrv_addr.as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UniqueKeyDirectStatus {
    PushConsumerUnsupported { client_id: CheetahString },
    NotPushConsumer { client_id: CheetahString },
    RunningInfoFailed { client_id: CheetahString },
}

#[derive(Debug, Clone)]
pub enum QueryMessageByUniqueKeyResult {
    Messages(Vec<MessageExt>),
    DirectStatus(UniqueKeyDirectStatus),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryMessageTraceByIdRequest {
    msg_id: CheetahString,
    trace_topic: CheetahString,
    begin_timestamp: i64,
    end_timestamp: i64,
    max_num: i32,
    namesrv_addr: Option<String>,
}

impl QueryMessageTraceByIdRequest {
    pub fn try_new(
        msg_id: impl Into<String>,
        trace_topic: Option<String>,
        begin_timestamp: Option<i64>,
        end_timestamp: Option<i64>,
        max_num: i32,
    ) -> RocketMQResult<Self> {
        let trace_topic =
            trim_optional_string(trace_topic).unwrap_or_else(|| TopicValidator::RMQ_SYS_TRACE_TOPIC.to_string());
        Ok(Self {
            msg_id: trim_required_cheetah("msgId", msg_id)?,
            trace_topic: trim_required_cheetah("traceTopic", trace_topic)?,
            begin_timestamp: begin_timestamp.unwrap_or(0),
            end_timestamp: end_timestamp.unwrap_or(i64::MAX),
            max_num,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        builder_with_namesrv(self.namesrv_addr.as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageTraceView {
    pub msg_type: String,
    pub group_name: String,
    pub client_host: String,
    pub time_stamp: i64,
    pub cost_time: i32,
    pub status: String,
    pub topic: Option<String>,
    pub tags: Option<String>,
    pub keys: Option<String>,
    pub store_host: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrintMessagesRequest {
    topic: CheetahString,
    sub_expression: CheetahString,
    begin_timestamp: Option<u64>,
    end_timestamp: Option<u64>,
    lmq_parent_topic: Option<CheetahString>,
}

impl PrintMessagesRequest {
    pub fn try_new(
        topic: impl Into<String>,
        sub_expression: impl Into<String>,
        begin_timestamp: Option<u64>,
        end_timestamp: Option<u64>,
        lmq_parent_topic: Option<String>,
    ) -> RocketMQResult<Self> {
        Ok(Self {
            topic: trim_required_cheetah("topic", topic)?,
            sub_expression: trim_required_cheetah("subExpression", sub_expression)?,
            begin_timestamp,
            end_timestamp,
            lmq_parent_topic: trim_optional_string(lmq_parent_topic)
                .map(|lmq_parent_topic| trim_required_cheetah("lmqParentTopic", lmq_parent_topic))
                .transpose()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrintMessagesByQueueRequest {
    topic: CheetahString,
    broker_name: CheetahString,
    queue_id: i32,
    sub_expression: CheetahString,
    begin_timestamp: Option<u64>,
    end_timestamp: Option<u64>,
    print_messages: bool,
    calculate_by_tag: bool,
}

impl PrintMessagesByQueueRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        topic: impl Into<String>,
        broker_name: impl Into<String>,
        queue_id: i32,
        sub_expression: impl Into<String>,
        begin_timestamp: Option<u64>,
        end_timestamp: Option<u64>,
        print_messages: bool,
        calculate_by_tag: bool,
    ) -> RocketMQResult<Self> {
        Ok(Self {
            topic: trim_required_cheetah("topic", topic)?,
            broker_name: trim_required_cheetah("brokerName", broker_name)?,
            queue_id,
            sub_expression: trim_required_cheetah("subExpression", sub_expression)?,
            begin_timestamp,
            end_timestamp,
            print_messages,
            calculate_by_tag,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumeMessagesRequest {
    topic: CheetahString,
    broker_name: Option<CheetahString>,
    queue_id: Option<i32>,
    offset: Option<i64>,
    consumer_group: Option<CheetahString>,
    begin_timestamp: Option<i64>,
    end_timestamp: Option<i64>,
    message_number: i64,
}

impl ConsumeMessagesRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        topic: impl Into<String>,
        broker_name: Option<String>,
        queue_id: Option<i32>,
        offset: Option<i64>,
        consumer_group: Option<String>,
        begin_timestamp: Option<i64>,
        end_timestamp: Option<i64>,
        message_number: i64,
    ) -> RocketMQResult<Self> {
        if message_number <= 0 {
            return Err(ToolsError::validation_error("MessageNumber", "Please input a positive messageNumber!").into());
        }

        if queue_id.is_some() && trim_optional_string(broker_name.clone()).is_none() {
            return Err(ToolsError::validation_error("brokerName", "Please set the brokerName before queueId!").into());
        }

        if offset.is_some() && queue_id.is_none() {
            return Err(ToolsError::validation_error("offset", "Please set queueId before offset!").into());
        }

        let now = current_millis() as i64;
        if let Some(begin_timestamp) = begin_timestamp {
            if begin_timestamp > now {
                return Err(ToolsError::validation_error(
                    "beginTimestamp",
                    "Please set the beginTimestamp before now!",
                )
                .into());
            }
        }

        if let Some(end_timestamp) = end_timestamp {
            if end_timestamp > now {
                return Err(
                    ToolsError::validation_error("endTimestamp", "Please set the endTimestamp before now!").into(),
                );
            }
        }

        if let (Some(begin_timestamp), Some(end_timestamp)) = (begin_timestamp, end_timestamp) {
            if begin_timestamp > end_timestamp {
                return Err(ToolsError::validation_error(
                    "timestamp",
                    "Please make sure that the beginTimestamp is less than or equal to the endTimestamp",
                )
                .into());
            }
        }

        Ok(Self {
            topic: trim_required_cheetah("topic", topic)?,
            broker_name: trim_optional_string(broker_name)
                .map(|broker_name| trim_required_cheetah("brokerName", broker_name))
                .transpose()?,
            queue_id,
            offset,
            consumer_group: trim_optional_string(consumer_group)
                .map(|consumer_group| trim_required_cheetah("consumerGroup", consumer_group))
                .transpose()?,
            begin_timestamp,
            end_timestamp,
            message_number,
        })
    }
}

#[derive(Debug, Clone)]
pub enum MessagePullEvent {
    QueueRange {
        mq: MessageQueue,
        min_offset: i64,
        max_offset: i64,
    },
    Messages {
        messages: Vec<ArcMut<MessageExt>>,
    },
    ConsumeOk,
    CountLimit {
        message_number: i64,
        queue_id: Option<i32>,
    },
    OffsetNotMatched {
        mq: MessageQueue,
        offset: i64,
    },
    NoMatched {
        mq: MessageQueue,
        status: PullStatus,
        offset: i64,
    },
    Finished {
        mq: MessageQueue,
        status: PullStatus,
        offset: i64,
    },
    PullError {
        error: String,
    },
    Separator,
    TagCounts(Vec<(String, i64)>),
}

pub struct MessageService;

impl MessageService {
    pub fn decode_message_ids(request: &DecodeMessageIdRequest) -> DecodeMessageIdResult {
        let entries = request
            .message_ids()
            .iter()
            .map(|message_id| {
                let outcome = validate_message_id(message_id.as_str())
                    .and_then(|_| decode_message_id(message_id.as_str()))
                    .map(|decoded| DecodeMessageIdOutcome::Decoded {
                        broker_ip: decoded.address.ip().to_string(),
                        broker_port: decoded.address.port(),
                        commit_log_offset: decoded.offset,
                        offset_hex: format!("{:#018X}", decoded.offset),
                    })
                    .unwrap_or_else(|error| DecodeMessageIdOutcome::Invalid { error });

                DecodeMessageIdEntry {
                    message_id: message_id.clone(),
                    outcome,
                }
            })
            .collect();

        DecodeMessageIdResult { entries }
    }

    pub fn dump_compaction_log_by_request(
        request: &DumpCompactionLogRequest,
    ) -> RocketMQResult<DumpCompactionLogResult> {
        let Some(file_path) = request.file() else {
            return Ok(DumpCompactionLogResult {
                messages: Vec::new(),
                missing_file_name: true,
            });
        };

        if !file_path.exists() {
            return Err(RocketMQError::Internal(format!(
                "file {} not exist.",
                file_path.display()
            )));
        }

        if file_path.is_dir() {
            return Err(RocketMQError::Internal(format!(
                "file {} is a directory.",
                file_path.display()
            )));
        }

        let data = fs::read(file_path).map_err(|error| {
            RocketMQError::Internal(format!("Failed to read file {}: {}", file_path.display(), error))
        })?;

        Ok(DumpCompactionLogResult {
            messages: decode_compaction_log_messages(data),
            missing_file_name: false,
        })
    }

    pub async fn query_message_by_key_by_request_with_rpc_hook(
        request: QueryMessageByKeyRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<QueryMessageByKeyResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::query_message_by_key_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn query_message_by_key_with_admin(
        admin: &DefaultMQAdminExt,
        request: &QueryMessageByKeyRequest,
    ) -> RocketMQResult<QueryMessageByKeyResult> {
        let query_result = admin
            .query_message_by_key(
                request.cluster.clone(),
                request.topic.clone(),
                request.msg_key.clone(),
                request.max_num,
                request.begin_timestamp,
                request.end_timestamp,
                request.key_type.clone(),
                request.last_key.clone(),
            )
            .await
            .map_err(|error| RocketMQError::Internal(format!("Failed to query message by key: {error}")))?;

        let rows = query_result
            .message_list()
            .iter()
            .map(|message| {
                let index_key = if !request.key_type.is_empty() {
                    let store_timestamp = deal_time_to_hour_stamps(message.store_timestamp());
                    Some(format!(
                        "{}@{}@{}@{}@{}@{}",
                        store_timestamp,
                        request.topic,
                        request.key_type,
                        request.msg_key,
                        message.msg_id(),
                        message.commit_log_offset()
                    ))
                } else {
                    None
                };

                QueryMessageByKeyRow {
                    message_id: message.msg_id().clone(),
                    queue_id: message.queue_id(),
                    queue_offset: message.queue_offset(),
                    index_key,
                }
            })
            .collect();

        Ok(QueryMessageByKeyResult { rows })
    }

    pub async fn query_message_by_id_by_request_with_rpc_hook(
        request: QueryMessageByIdRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<QueryMessageByIdResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::query_message_by_id_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn query_message_by_id_with_admin(
        admin: &DefaultMQAdminExt,
        request: &QueryMessageByIdRequest,
    ) -> RocketMQResult<QueryMessageByIdResult> {
        let mut entries = Vec::with_capacity(request.message_ids.len());
        for message_id in &request.message_ids {
            let query_future = Self::query_single_message_by_id(admin, message_id, &request.topic);
            let outcome = match tokio::time::timeout(Duration::from_millis(request.timeout_millis), query_future).await
            {
                Ok(outcome) => outcome,
                Err(_) => QueryMessageByIdOutcome::TimedOut,
            };
            entries.push(QueryMessageByIdEntry {
                message_id: message_id.clone(),
                outcome,
            });
        }
        Ok(QueryMessageByIdResult { entries })
    }

    async fn query_single_message_by_id(
        admin: &DefaultMQAdminExt,
        message_id: &CheetahString,
        topic: &CheetahString,
    ) -> QueryMessageByIdOutcome {
        let start_time = Instant::now();
        let query_result = admin
            .query_message(
                CheetahString::from_static_str(DEFAULT_CLUSTER),
                topic.clone(),
                message_id.clone(),
            )
            .await;
        let query_time_ms = start_time.elapsed().as_millis() as u64;

        match query_result {
            Ok(message) => QueryMessageByIdOutcome::Found {
                broker_addr: message.store_host().to_string(),
                message: Box::new(message),
                query_time_ms,
            },
            Err(RocketMQError::BrokerOperationFailed { message, .. })
                if message.contains("not found")
                    || message.contains("does not exist")
                    || message.contains("No message") =>
            {
                QueryMessageByIdOutcome::NotFound {
                    reason: message,
                    query_time_ms,
                }
            }
            Err(RocketMQError::Rpc(rpc_error)) => {
                let error = rpc_error.to_string();
                if error.contains("not found") || error.contains("does not exist") || error.contains("No message") {
                    QueryMessageByIdOutcome::NotFound {
                        reason: error,
                        query_time_ms,
                    }
                } else {
                    QueryMessageByIdOutcome::Failed {
                        error: format!("Failed to query message by ID '{message_id}': {error}"),
                        query_time_ms,
                    }
                }
            }
            Err(error) => QueryMessageByIdOutcome::Failed {
                error: format!("Failed to query message by ID '{message_id}': {error}"),
                query_time_ms,
            },
        }
    }

    pub async fn query_message_by_offset_by_request_with_rpc_hook(
        request: QueryMessageByOffsetRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<QueryMessageByOffsetResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::query_message_by_offset_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn query_message_by_offset_with_admin(
        admin: &DefaultMQAdminExt,
        request: &QueryMessageByOffsetRequest,
    ) -> RocketMQResult<QueryMessageByOffsetResult> {
        let route_topic = request.route_topic.as_ref().unwrap_or(&request.topic);
        let broker_addr = resolve_broker_addr(admin, route_topic.as_str(), request.broker_name.as_str()).await?;

        if route_topic != &request.topic {
            let route_mq = MessageQueue::from_parts(route_topic.clone(), request.broker_name.clone(), 0);
            admin
                .pull_message_from_queue(broker_addr.as_str(), &route_mq, "*", 0, 1, PULL_TIMEOUT_MILLIS)
                .await
                .map_err(|error| RocketMQError::Internal(format!("Failed to warm up route topic pull: {error}")))?;
        }

        let mq = MessageQueue::from_parts(request.topic.clone(), request.broker_name.clone(), request.queue_id);
        let pull_result = admin
            .pull_message_from_queue(broker_addr.as_str(), &mq, "*", request.offset, 1, PULL_TIMEOUT_MILLIS)
            .await
            .map_err(|error| RocketMQError::Internal(format!("Failed to pull message by offset: {error}")))?;

        let pull_status = *pull_result.pull_status();
        let message = if pull_status == PullStatus::Found {
            pull_result
                .msg_found_list()
                .and_then(|messages| messages.first().cloned())
        } else {
            None
        };

        Ok(QueryMessageByOffsetResult { pull_status, message })
    }

    pub async fn query_message_by_unique_key_by_request_with_rpc_hook(
        request: QueryMessageByUniqueKeyRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<QueryMessageByUniqueKeyResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::query_message_by_unique_key_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn query_message_by_unique_key_with_admin(
        admin: &DefaultMQAdminExt,
        request: &QueryMessageByUniqueKeyRequest,
    ) -> RocketMQResult<QueryMessageByUniqueKeyResult> {
        if let (Some(consumer_group), Some(client_id)) = (&request.consumer_group, &request.client_id) {
            let consumer_running_info = admin
                .get_consumer_running_info(consumer_group.clone(), client_id.clone(), false, Some(false))
                .await;

            return Ok(QueryMessageByUniqueKeyResult::DirectStatus(
                match consumer_running_info {
                    Ok(info) if info.is_push_type() => UniqueKeyDirectStatus::PushConsumerUnsupported {
                        client_id: client_id.clone(),
                    },
                    Ok(_) => UniqueKeyDirectStatus::NotPushConsumer {
                        client_id: client_id.clone(),
                    },
                    Err(_) => UniqueKeyDirectStatus::RunningInfoFailed {
                        client_id: client_id.clone(),
                    },
                },
            ));
        }

        let (begin_timestamp, end_timestamp) =
            if let (Some(start_time), Some(end_time)) = (request.start_time, request.end_time) {
                (start_time, end_time)
            } else {
                let now = current_millis() as i64;
                (now - DEFAULT_QUERY_WINDOW_MS, now + DEFAULT_QUERY_WINDOW_MS)
            };

        let query_result = admin
            .query_message_by_unique_key(
                request.cluster.clone(),
                request.topic.clone(),
                request.msg_id.clone(),
                QUERY_MAX_NUM,
                begin_timestamp,
                end_timestamp,
            )
            .await?;

        let mut messages = query_result.message_list().clone();
        messages.sort_by_key(|message| message.store_timestamp());
        if !request.show_all && messages.len() > 1 {
            messages.truncate(1);
        }

        Ok(QueryMessageByUniqueKeyResult::Messages(messages))
    }

    pub async fn query_message_trace_by_id_by_request_with_rpc_hook(
        request: QueryMessageTraceByIdRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<Vec<MessageTraceView>> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::query_message_trace_by_id_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn query_message_trace_by_id_with_admin(
        admin: &DefaultMQAdminExt,
        request: &QueryMessageTraceByIdRequest,
    ) -> RocketMQResult<Vec<MessageTraceView>> {
        let query_result = admin
            .query_message_by_key(
                None,
                request.trace_topic.clone(),
                request.msg_id.clone(),
                request.max_num,
                request.begin_timestamp,
                request.end_timestamp,
                CheetahString::from_static_str(""),
                None,
            )
            .await?;

        let mut trace_views = Vec::new();
        for message in query_result.message_list() {
            let Some(body) = message.body() else {
                continue;
            };

            let body_str = String::from_utf8_lossy(body.as_ref());
            if body_str.is_empty() {
                continue;
            }

            let trace_contexts = TraceDataEncoder::decoder_from_trace_data_string(&body_str);
            for context in trace_contexts {
                let Some(trace_type) = context.trace_type else {
                    continue;
                };
                let Some(trace_beans) = &context.trace_beans else {
                    continue;
                };

                for bean in trace_beans {
                    if bean.msg_id.as_str() != request.msg_id.as_str() {
                        continue;
                    }

                    let client_host = if !bean.client_host.is_empty() {
                        bean.client_host.to_string()
                    } else {
                        message.born_host().to_string()
                    };

                    trace_views.push(MessageTraceView {
                        msg_type: trace_type.to_string(),
                        group_name: context.group_name.to_string(),
                        client_host,
                        time_stamp: context.time_stamp as i64,
                        cost_time: context.cost_time,
                        status: if context.is_success {
                            "success".to_string()
                        } else {
                            "failed".to_string()
                        },
                        topic: Some(bean.topic.to_string()),
                        tags: Some(bean.tags.to_string()),
                        keys: Some(bean.keys.to_string()),
                        store_host: Some(bean.store_host.to_string()),
                    });
                }
            }
        }

        Ok(trace_views)
    }

    pub async fn print_messages_by_request_with_rpc_hook<F>(
        request: PrintMessagesRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
        sink: F,
    ) -> RocketMQResult<()>
    where
        F: FnMut(MessagePullEvent) -> RocketMQResult<()>,
    {
        let mut admin = build_default_admin(rpc_hook);
        admin.start().await?;
        let result = Self::print_messages_with_admin(&admin, &request, sink).await;
        admin.shutdown().await;
        result
    }

    pub async fn print_messages_with_admin<F>(
        admin: &DefaultMQAdminExt,
        request: &PrintMessagesRequest,
        mut sink: F,
    ) -> RocketMQResult<()>
    where
        F: FnMut(MessagePullEvent) -> RocketMQResult<()>,
    {
        let route_topic = request.lmq_parent_topic.as_ref().unwrap_or(&request.topic);
        let message_queues = resolve_message_queues(admin, request.topic.as_str(), route_topic.as_str()).await?;
        if message_queues.is_empty() {
            return Err(RocketMQError::Internal("No available message queue found".to_string()));
        }

        for (mq, broker_addr) in message_queues {
            let mut min_offset = admin
                .min_offset(broker_addr.clone(), mq.clone(), PULL_TIMEOUT_MILLIS)
                .await
                .map_err(|error| RocketMQError::Internal(format!("Failed to get min offset: {error}")))?;

            let mut max_offset = admin
                .max_offset(broker_addr.clone(), mq.clone(), PULL_TIMEOUT_MILLIS)
                .await
                .map_err(|error| RocketMQError::Internal(format!("Failed to get max offset: {error}")))?;

            if let Some(begin_timestamp) = request.begin_timestamp {
                min_offset = admin
                    .search_offset(
                        broker_addr.clone(),
                        request.topic.clone(),
                        mq.queue_id(),
                        begin_timestamp,
                        PULL_TIMEOUT_MILLIS,
                    )
                    .await
                    .map_err(|error| RocketMQError::Internal(format!("Failed to search begin offset: {error}")))?
                    as i64;
            }

            if let Some(end_timestamp) = request.end_timestamp {
                max_offset = admin
                    .search_offset(
                        broker_addr.clone(),
                        request.topic.clone(),
                        mq.queue_id(),
                        end_timestamp,
                        PULL_TIMEOUT_MILLIS,
                    )
                    .await
                    .map_err(|error| RocketMQError::Internal(format!("Failed to search end offset: {error}")))?
                    as i64;
            }

            sink(MessagePullEvent::QueueRange {
                mq: mq.clone(),
                min_offset,
                max_offset,
            })?;

            let mut offset = min_offset;
            'read_queue: while offset < max_offset {
                let pull_result = admin
                    .pull_message_from_queue(
                        broker_addr.as_str(),
                        &mq,
                        request.sub_expression.as_str(),
                        offset,
                        PULL_BATCH_SIZE,
                        PULL_TIMEOUT_MILLIS,
                    )
                    .await;

                match pull_result {
                    Ok(result) => {
                        offset = result.next_begin_offset() as i64;
                        match *result.pull_status() {
                            PullStatus::Found => {
                                if let Some(messages) = result.msg_found_list() {
                                    sink(MessagePullEvent::Messages {
                                        messages: messages.clone(),
                                    })?;
                                }
                            }
                            PullStatus::NoMatchedMsg => sink(MessagePullEvent::NoMatched {
                                mq: mq.clone(),
                                status: *result.pull_status(),
                                offset,
                            })?,
                            PullStatus::NoNewMsg | PullStatus::OffsetIllegal => {
                                sink(MessagePullEvent::Finished {
                                    mq: mq.clone(),
                                    status: *result.pull_status(),
                                    offset,
                                })?;
                                break 'read_queue;
                            }
                        }
                    }
                    Err(error) => {
                        sink(MessagePullEvent::PullError {
                            error: error.to_string(),
                        })?;
                        break;
                    }
                }
            }

            sink(MessagePullEvent::Separator)?;
        }

        Ok(())
    }

    pub async fn print_messages_by_queue_by_request_with_rpc_hook<F>(
        request: PrintMessagesByQueueRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
        sink: F,
    ) -> RocketMQResult<()>
    where
        F: FnMut(MessagePullEvent) -> RocketMQResult<()>,
    {
        let mut admin = build_default_admin(rpc_hook);
        admin.start().await?;
        let result = Self::print_messages_by_queue_with_admin(&admin, &request, sink).await;
        admin.shutdown().await;
        result
    }

    pub async fn print_messages_by_queue_with_admin<F>(
        admin: &DefaultMQAdminExt,
        request: &PrintMessagesByQueueRequest,
        mut sink: F,
    ) -> RocketMQResult<()>
    where
        F: FnMut(MessagePullEvent) -> RocketMQResult<()>,
    {
        let broker_addr = resolve_broker_addr(admin, request.topic.as_str(), request.broker_name.as_str()).await?;
        let mq = MessageQueue::from_parts(request.topic.clone(), request.broker_name.clone(), request.queue_id);

        let mut min_offset = admin
            .min_offset(broker_addr.clone(), mq.clone(), PULL_TIMEOUT_MILLIS)
            .await
            .map_err(|error| RocketMQError::Internal(format!("Failed to get min offset: {error}")))?;
        let mut max_offset = admin
            .max_offset(broker_addr.clone(), mq.clone(), PULL_TIMEOUT_MILLIS)
            .await
            .map_err(|error| RocketMQError::Internal(format!("Failed to get max offset: {error}")))?;

        if let Some(begin_timestamp) = request.begin_timestamp {
            min_offset = admin
                .search_offset(
                    broker_addr.clone(),
                    request.topic.clone(),
                    request.queue_id,
                    begin_timestamp,
                    PULL_TIMEOUT_MILLIS,
                )
                .await
                .map_err(|error| RocketMQError::Internal(format!("Failed to search begin offset: {error}")))?
                as i64;
        }

        if let Some(end_timestamp) = request.end_timestamp {
            max_offset = admin
                .search_offset(
                    broker_addr.clone(),
                    request.topic.clone(),
                    request.queue_id,
                    end_timestamp,
                    PULL_TIMEOUT_MILLIS,
                )
                .await
                .map_err(|error| RocketMQError::Internal(format!("Failed to search end offset: {error}")))?
                as i64;
        }

        let mut tag_cal_map: BTreeMap<String, i64> = BTreeMap::new();
        let mut offset = min_offset;

        'read_queue: while offset < max_offset {
            let pull_result = admin
                .pull_message_from_queue(
                    broker_addr.as_str(),
                    &mq,
                    request.sub_expression.as_str(),
                    offset,
                    PULL_BATCH_SIZE,
                    PULL_TIMEOUT_MILLIS,
                )
                .await;

            match pull_result {
                Ok(result) => {
                    offset = result.next_begin_offset() as i64;
                    match *result.pull_status() {
                        PullStatus::Found => {
                            if let Some(messages) = result.msg_found_list() {
                                if request.calculate_by_tag {
                                    for message in messages {
                                        if let Some(tag) = message.tags() {
                                            let tag = tag.to_string();
                                            if !tag.is_empty() {
                                                *tag_cal_map.entry(tag).or_default() += 1;
                                            }
                                        }
                                    }
                                }
                                if request.print_messages {
                                    sink(MessagePullEvent::Messages {
                                        messages: messages.clone(),
                                    })?;
                                }
                            }
                        }
                        PullStatus::NoMatchedMsg | PullStatus::NoNewMsg | PullStatus::OffsetIllegal => {
                            break 'read_queue;
                        }
                    }
                }
                Err(error) => {
                    sink(MessagePullEvent::PullError {
                        error: error.to_string(),
                    })?;
                    break;
                }
            }
        }

        if request.calculate_by_tag {
            let mut tag_counts: Vec<_> = tag_cal_map.into_iter().collect();
            tag_counts.sort_by_key(|(_, count)| std::cmp::Reverse(*count));
            sink(MessagePullEvent::TagCounts(tag_counts))?;
        }

        Ok(())
    }

    pub async fn consume_messages_by_request_with_rpc_hook<F>(
        request: ConsumeMessagesRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
        sink: F,
    ) -> RocketMQResult<()>
    where
        F: FnMut(MessagePullEvent) -> RocketMQResult<()>,
    {
        let mut admin = build_consume_admin(rpc_hook, request.consumer_group.as_ref());
        admin.start().await?;
        let result = Self::consume_messages_with_admin(&admin, &request, sink).await;
        admin.shutdown().await;
        result
    }

    pub async fn consume_messages_with_admin<F>(
        admin: &DefaultMQAdminExt,
        request: &ConsumeMessagesRequest,
        sink: F,
    ) -> RocketMQResult<()>
    where
        F: FnMut(MessagePullEvent) -> RocketMQResult<()>,
    {
        match (request.broker_name.as_ref(), request.queue_id, request.offset) {
            (None, None, None) => Self::consume_messages_default(admin, request, sink).await,
            (Some(broker_name), Some(queue_id), offset) => {
                Self::consume_messages_by_condition(admin, request, broker_name, queue_id, offset.unwrap_or(0), sink)
                    .await
            }
            _ => Err(ToolsError::validation_error("target", "invalid consume message target").into()),
        }
    }

    async fn consume_messages_default<F>(
        admin: &DefaultMQAdminExt,
        request: &ConsumeMessagesRequest,
        mut sink: F,
    ) -> RocketMQResult<()>
    where
        F: FnMut(MessagePullEvent) -> RocketMQResult<()>,
    {
        let topic_route = admin
            .examine_topic_route_info(request.topic.clone())
            .await
            .map_err(|error| RocketMQError::Internal(format!("Failed to examine topic route info: {error}")))?
            .ok_or_else(|| RocketMQError::Internal(format!("Topic route not found for: {}", request.topic)))?;

        let mut count_left = request.message_number;
        for (mq, broker_addr) in route_topic_queues(&topic_route, request.topic.as_str()) {
            if count_left == 0 {
                return Ok(());
            }

            let mut min_offset = admin
                .min_offset(broker_addr.clone(), mq.clone(), PULL_TIMEOUT_MILLIS)
                .await
                .map_err(|error| RocketMQError::Internal(format!("Failed to get min offset: {error}")))?;
            let mut max_offset = admin
                .max_offset(broker_addr.clone(), mq.clone(), PULL_TIMEOUT_MILLIS)
                .await
                .map_err(|error| RocketMQError::Internal(format!("Failed to get max offset: {error}")))?;

            if let Some(begin_timestamp) = request.begin_timestamp {
                if begin_timestamp > 0 {
                    min_offset = admin
                        .search_offset(
                            broker_addr.clone(),
                            request.topic.clone(),
                            mq.queue_id(),
                            begin_timestamp as u64,
                            PULL_TIMEOUT_MILLIS,
                        )
                        .await
                        .map_err(|error| RocketMQError::Internal(format!("Failed to search begin offset: {error}")))?
                        as i64;
                }
            }

            if let Some(end_timestamp) = request.end_timestamp {
                if end_timestamp > 0 {
                    max_offset = admin
                        .search_offset(
                            broker_addr.clone(),
                            request.topic.clone(),
                            mq.queue_id(),
                            end_timestamp as u64,
                            PULL_TIMEOUT_MILLIS,
                        )
                        .await
                        .map_err(|error| RocketMQError::Internal(format!("Failed to search end offset: {error}")))?
                        as i64;
                }
            }

            if max_offset - min_offset > count_left {
                sink(MessagePullEvent::CountLimit {
                    message_number: count_left,
                    queue_id: Some(mq.queue_id()),
                })?;
                max_offset = min_offset + count_left - 1;
                count_left = 0;
            } else {
                count_left = count_left - (max_offset - min_offset) - 1;
            }

            Self::pull_consume_message_by_queue(admin, broker_addr.as_str(), &mq, min_offset, max_offset, &mut sink)
                .await?;
        }

        Ok(())
    }

    async fn consume_messages_by_condition<F>(
        admin: &DefaultMQAdminExt,
        request: &ConsumeMessagesRequest,
        broker_name: &CheetahString,
        queue_id: i32,
        offset: i64,
        mut sink: F,
    ) -> RocketMQResult<()>
    where
        F: FnMut(MessagePullEvent) -> RocketMQResult<()>,
    {
        let broker_addr = resolve_broker_addr(admin, request.topic.as_str(), broker_name.as_str()).await?;
        let mq = MessageQueue::from_parts(request.topic.clone(), broker_name.clone(), queue_id);

        let mut min_offset = admin
            .min_offset(broker_addr.clone(), mq.clone(), PULL_TIMEOUT_MILLIS)
            .await
            .map_err(|error| RocketMQError::Internal(format!("Failed to get min offset: {error}")))?;
        let mut max_offset = admin
            .max_offset(broker_addr.clone(), mq.clone(), PULL_TIMEOUT_MILLIS)
            .await
            .map_err(|error| RocketMQError::Internal(format!("Failed to get max offset: {error}")))?;

        if let Some(begin_timestamp) = request.begin_timestamp {
            if begin_timestamp > 0 {
                min_offset = admin
                    .search_offset(
                        broker_addr.clone(),
                        request.topic.clone(),
                        queue_id,
                        begin_timestamp as u64,
                        PULL_TIMEOUT_MILLIS,
                    )
                    .await
                    .map_err(|error| RocketMQError::Internal(format!("Failed to search begin offset: {error}")))?
                    as i64;
            }
        }

        if let Some(end_timestamp) = request.end_timestamp {
            if end_timestamp > 0 {
                max_offset = admin
                    .search_offset(
                        broker_addr.clone(),
                        request.topic.clone(),
                        queue_id,
                        end_timestamp as u64,
                        PULL_TIMEOUT_MILLIS,
                    )
                    .await
                    .map_err(|error| RocketMQError::Internal(format!("Failed to search end offset: {error}")))?
                    as i64;
            }
        }

        if offset > max_offset {
            sink(MessagePullEvent::OffsetNotMatched { mq, offset })?;
            return Ok(());
        }

        min_offset = min_offset.max(offset);

        if max_offset - min_offset > request.message_number {
            sink(MessagePullEvent::CountLimit {
                message_number: request.message_number,
                queue_id: None,
            })?;
            max_offset = min_offset + request.message_number - 1;
        }

        Self::pull_consume_message_by_queue(admin, broker_addr.as_str(), &mq, min_offset, max_offset, &mut sink).await
    }

    async fn pull_consume_message_by_queue<F>(
        admin: &DefaultMQAdminExt,
        broker_addr: &str,
        mq: &MessageQueue,
        min_offset: i64,
        max_offset: i64,
        sink: &mut F,
    ) -> RocketMQResult<()>
    where
        F: FnMut(MessagePullEvent) -> RocketMQResult<()>,
    {
        let mut offset = min_offset;
        'read_queue: while offset <= max_offset {
            let batch_size = (max_offset - offset + 1) as i32;
            let pull_result = admin
                .pull_message_from_queue(broker_addr, mq, "*", offset, batch_size, PULL_TIMEOUT_MILLIS)
                .await;

            match pull_result {
                Ok(result) => {
                    offset = result.next_begin_offset() as i64;
                    match *result.pull_status() {
                        PullStatus::Found => {
                            sink(MessagePullEvent::ConsumeOk)?;
                            if let Some(messages) = result.msg_found_list() {
                                sink(MessagePullEvent::Messages {
                                    messages: messages.clone(),
                                })?;
                            }
                        }
                        PullStatus::NoMatchedMsg => sink(MessagePullEvent::NoMatched {
                            mq: mq.clone(),
                            status: *result.pull_status(),
                            offset,
                        })?,
                        PullStatus::NoNewMsg | PullStatus::OffsetIllegal => {
                            sink(MessagePullEvent::Finished {
                                mq: mq.clone(),
                                status: *result.pull_status(),
                                offset,
                            })?;
                            break 'read_queue;
                        }
                    }
                }
                Err(error) => {
                    sink(MessagePullEvent::PullError {
                        error: error.to_string(),
                    })?;
                    break 'read_queue;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_key_request_rejects_invalid_key_type() {
        let result =
            QueryMessageByKeyRequest::try_new("topic", "key", None, None, 64, None, Some("X".to_string()), None);
        assert!(result.is_err());
    }

    #[test]
    fn consume_request_requires_broker_before_queue() {
        let result = ConsumeMessagesRequest::try_new("topic", None, Some(0), None, None, None, None, 1);
        assert!(result.is_err());
    }

    #[test]
    fn decode_message_id_request_trims_and_skips_blank_ids() {
        let request =
            DecodeMessageIdRequest::try_new(vec![" 7F0000010007D8260BF075769D36C348 ".into(), " ".into()]).unwrap();

        assert_eq!(request.message_ids(), ["7F0000010007D8260BF075769D36C348"]);
    }

    #[test]
    fn decode_message_ids_returns_decoded_and_invalid_entries() {
        let request =
            DecodeMessageIdRequest::try_new(vec!["7F0000010007D8260BF075769D36C348".into(), "invalid".into()]).unwrap();

        let result = MessageService::decode_message_ids(&request);

        assert_eq!(result.entries.len(), 2);
        assert!(matches!(
            &result.entries[0].outcome,
            DecodeMessageIdOutcome::Decoded {
                broker_ip,
                broker_port: 55334,
                commit_log_offset: 860316681131967304,
                ..
            } if broker_ip == "127.0.0.1"
        ));
        assert!(matches!(
            &result.entries[1].outcome,
            DecodeMessageIdOutcome::Invalid { error } if error.contains("Invalid message ID length")
        ));
    }

    #[test]
    fn dump_compaction_log_request_trims_file_name() {
        let request = DumpCompactionLogRequest::try_new(Some(" ./compact.log ".into()));
        assert_eq!(request.file().unwrap().to_string_lossy(), "./compact.log");
    }

    #[test]
    fn dump_compaction_log_without_file_returns_missing_file_name_result() {
        let request = DumpCompactionLogRequest::try_new(None);
        let result = MessageService::dump_compaction_log_by_request(&request).unwrap();

        assert!(result.missing_file_name);
        assert!(result.messages.is_empty());
    }
}
