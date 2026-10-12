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

//! Exact-Broker, read-only, body-free message index capability.

use cheetah_string::CheetahString;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::MessageConst;
use rocketmq_protocol::common::message::message_decoder as MessageDecoder;
use rocketmq_protocol::protocol::header::query_message_request_header::QueryMessageRequestHeader;
use rocketmq_protocol::trace::TraceType;

use super::default_mq_admin_ext::DefaultMQAdminExt;
use super::mq_admin_read_ext::MessageMetadataRead;
use crate::implementation::mq_client_api_impl::MQClientAPIImpl;
use crate::trace::trace_data_encoder::TraceDataEncoder;

/// What one Broker's index is asked for: the messages of a Topic that carry
/// a key and were stored within a time window.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MessageIndexQuery {
    pub topic: CheetahString,
    pub key: CheetahString,
    /// The most messages the Broker returns.
    pub max_messages: i32,
    /// Store time bounds, in Unix milliseconds, both inclusive.
    pub begin_timestamp: i64,
    pub end_timestamp: i64,
}

/// One record a client wrote to a trace Topic about one step in the life of a
/// message.
///
/// The record names the hosts and carries the keys and tags that the client
/// reported. Callers must redact or pseudonymize them before exposing the
/// value outside their trusted read boundary. A record never holds a message
/// body.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MessageTraceRead {
    pub trace_type: TraceType,
    /// When the step happened, in Unix milliseconds.
    pub timestamp: i64,
    pub group_name: String,
    pub cost_time: i32,
    pub success: bool,
    pub topic: String,
    pub message_id: String,
    pub offset_message_id: String,
    pub tags: String,
    pub keys: String,
    pub store_host: String,
    pub client_host: String,
    pub retry_times: i32,
    pub body_length: i32,
    pub from_transaction_check: bool,
}

/// Additive capability for reading what one already resolved Broker indexes
/// under a message key. Address resolution remains the responsibility of a
/// trusted adapter.
///
/// Neither method returns a message body: a match is reduced to fixed metadata
/// or to decoded trace records before it leaves this capability.
#[allow(async_fn_in_trait)]
pub trait MQAdminMessageIndexReadExt: Send {
    /// Reads the fixed metadata of the messages one exact Broker indexes under
    /// `query.key`.
    ///
    /// A Broker finds a key by its hash, so its answer can hold a message of
    /// another key or Topic. Only the messages of `query.topic` that carry
    /// `query.key` are returned.
    async fn message_metadata_by_key_at(
        &self,
        broker_addr: CheetahString,
        query: MessageIndexQuery,
    ) -> crate::ClientResult<Vec<MessageMetadataRead>>;

    /// Reads the trace records one exact Broker holds for the message
    /// identifier `query.key` in the trace Topic `query.topic`.
    ///
    /// A trace message can carry the records of several messages. Only the
    /// records of the queried identifier are returned.
    async fn message_trace_at(
        &self,
        broker_addr: CheetahString,
        query: MessageIndexQuery,
    ) -> crate::ClientResult<Vec<MessageTraceRead>>;
}

impl MQAdminMessageIndexReadExt for DefaultMQAdminExt {
    async fn message_metadata_by_key_at(
        &self,
        broker_addr: CheetahString,
        query: MessageIndexQuery,
    ) -> crate::ClientResult<Vec<MessageMetadataRead>> {
        let (topic, key) = (query.topic.clone(), query.key.clone());
        let messages = indexed_messages(self, &broker_addr, query).await?;
        Ok(messages
            .iter()
            .filter(|message| {
                let keys = message.properties().get(MessageConst::PROPERTY_KEYS);
                message.topic().as_str() == topic.as_str()
                    && carries_key(keys.map_or("", |keys| keys.as_str()), key.as_str())
            })
            .map(metadata)
            .collect())
    }

    async fn message_trace_at(
        &self,
        broker_addr: CheetahString,
        query: MessageIndexQuery,
    ) -> crate::ClientResult<Vec<MessageTraceRead>> {
        let message_id = query.key.clone();
        let messages = indexed_messages(self, &broker_addr, query).await?;
        Ok(messages
            .iter()
            .flat_map(|message| trace_records(message, message_id.as_str()))
            .collect())
    }
}

async fn indexed_messages(
    admin: &DefaultMQAdminExt,
    broker_addr: &CheetahString,
    query: MessageIndexQuery,
) -> crate::ClientResult<Vec<MessageExt>> {
    let header = QueryMessageRequestHeader {
        topic: query.topic,
        key: query.key,
        max_num: query.max_messages,
        begin_timestamp: query.begin_timestamp,
        end_timestamp: query.end_timestamp,
        index_type: Some(CheetahString::from_static_str(MessageConst::INDEX_KEY_TYPE)),
        last_key: None,
        topic_request_header: None,
    };
    let api = admin.inner().mq_client_api()?;
    let timeout = admin.inner().remoting_timeout_millis()?;
    let found = MQClientAPIImpl::query_message(&api, broker_addr, header, false, timeout).await?;
    Ok(match found {
        Some((_, Some(mut body))) => MessageDecoder::decodes_batch(&mut body, true, true),
        _ => Vec::new(),
    })
}

/// Returns whether `key` is one of the keys a message was sent with.
fn carries_key(keys: &str, key: &str) -> bool {
    !key.is_empty() && keys.split(MessageConst::KEY_SEPARATOR).any(|own| own == key)
}

fn metadata(message: &MessageExt) -> MessageMetadataRead {
    MessageMetadataRead {
        topic: message.topic().to_string(),
        message_id: message.msg_id().to_string(),
        unique_message_id: message
            .properties()
            .get(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
            .map(ToString::to_string)
            .filter(|value| !value.is_empty()),
        born_timestamp: message.born_timestamp(),
        store_timestamp: message.store_timestamp(),
        queue_id: message.queue_id(),
        queue_offset: message.queue_offset(),
        store_size: message.store_size(),
        reconsume_times: message.reconsume_times(),
        sys_flag: message.sys_flag(),
        flag: message.flag(),
        prepared_transaction_offset: message.prepared_transaction_offset(),
    }
}

/// Decodes the records one trace message carries about `message_id`.
fn trace_records(trace_message: &MessageExt, message_id: &str) -> Vec<MessageTraceRead> {
    let Some(body) = trace_message.body() else {
        return Vec::new();
    };
    decode_trace_records(
        &String::from_utf8_lossy(&body),
        message_id,
        &trace_message.born_host().to_string(),
        &trace_message.store_host().to_string(),
    )
}

/// Decodes trace data and keeps the records of one message.
///
/// A record that names no host of its own was written before the message reached a Broker, or
/// by a client that does not report hosts: the hosts of the trace message stand in for them.
fn decode_trace_records(
    trace_data: &str,
    message_id: &str,
    trace_born_host: &str,
    trace_store_host: &str,
) -> Vec<MessageTraceRead> {
    let mut records = Vec::new();
    for context in TraceDataEncoder::decoder_from_trace_data_string(trace_data) {
        let Some(trace_type) = context.trace_type else {
            continue;
        };
        for bean in context.trace_beans.iter().flatten() {
            if bean.msg_id.as_str() != message_id {
                continue;
            }
            let host = |own: &CheetahString, fallback: &str| {
                if own.is_empty() {
                    fallback.to_owned()
                } else {
                    own.to_string()
                }
            };
            records.push(MessageTraceRead {
                trace_type,
                timestamp: if bean.store_time > 0 {
                    bean.store_time
                } else {
                    i64::try_from(context.time_stamp).unwrap_or(i64::MAX)
                },
                group_name: context.group_name.to_string(),
                cost_time: context.cost_time,
                success: context.is_success,
                topic: bean.topic.to_string(),
                message_id: bean.msg_id.to_string(),
                offset_message_id: bean.offset_msg_id.to_string(),
                tags: bean.tags.to_string(),
                keys: bean.keys.to_string(),
                store_host: host(&bean.store_host, trace_store_host),
                client_host: host(&bean.client_host, trace_born_host),
                retry_times: bean.retry_times,
                body_length: bean.body_length,
                from_transaction_check: bean.from_transaction_check,
            });
        }
    }
    records
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trace::trace_bean::TraceBean;
    use crate::trace::trace_context::TraceContext;
    use crate::trace::trace_transfer_bean::TraceTransferBean;

    fn encoded(context: &TraceContext) -> String {
        let transfer: TraceTransferBean =
            TraceDataEncoder::encoder_from_context_bean(context).expect("the trace context encodes");
        transfer.trans_data().to_string()
    }

    fn publish(message_id: &str, client_host: &str) -> TraceContext {
        let mut context = TraceContext::new();
        context.trace_type = Some(TraceType::Pub);
        context.time_stamp = 1_700_000_000_000;
        context.group_name = "producers".into();
        context.cost_time = 12;
        context.is_success = true;
        context.trace_beans = Some(vec![TraceBean {
            topic: "orders".into(),
            msg_id: message_id.into(),
            offset_msg_id: "0A00000100002A9F0000000000000001".into(),
            tags: "TagA".into(),
            keys: "order-42".into(),
            store_host: "10.0.0.1:10911".into(),
            client_host: client_host.into(),
            store_time: 1_700_000_000_050,
            body_length: 128,
            ..Default::default()
        }]);
        context
    }

    #[test]
    fn only_the_records_of_the_queried_message_are_kept_and_none_holds_a_body() {
        let data = format!(
            "{}{}",
            encoded(&publish("UNIQ-A", "10.0.0.7")),
            encoded(&publish("UNIQ-B", "10.0.0.8"))
        );

        let records = decode_trace_records(&data, "UNIQ-A", "10.0.0.9:50000", "10.0.0.2:10911");

        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert_eq!(record.trace_type, TraceType::Pub);
        assert_eq!(record.message_id, "UNIQ-A");
        assert_eq!(record.group_name, "producers");
        assert_eq!(
            (record.topic.as_str(), record.tags.as_str(), record.keys.as_str()),
            ("orders", "TagA", "order-42")
        );
        // A publish record is encoded without a store time of its own: its time is the send time.
        assert_eq!(record.timestamp, 1_700_000_000_000);
        assert_eq!(record.body_length, 128);
        assert!(record.success);
        assert_eq!(record.store_host, "10.0.0.1:10911");
        assert_eq!(record.client_host, "10.0.0.7");
        assert!(decode_trace_records(&data, "UNIQ-C", "", "").is_empty());
        assert!(decode_trace_records("", "UNIQ-A", "", "").is_empty());
    }

    #[test]
    fn a_message_matches_only_by_one_of_its_whole_keys() {
        assert!(carries_key("order-42", "order-42"));
        assert!(carries_key("user-7 order-42", "order-42"));
        // A Broker matches by hash: a key that only resembles the queried one is no match.
        assert!(!carries_key("order-420", "order-42"));
        assert!(!carries_key("order-4 2", "order-42"));
        assert!(!carries_key("", "order-42"));
        assert!(!carries_key("order-42", ""));
    }

    #[test]
    fn a_record_without_hosts_takes_the_hosts_of_its_trace_message() {
        let data = encoded(&publish("UNIQ-A", ""));

        let records = decode_trace_records(&data, "UNIQ-A", "10.0.0.9:50000", "10.0.0.2:10911");

        assert_eq!(records[0].client_host, "10.0.0.9:50000");
        assert_eq!(records[0].store_host, "10.0.0.1:10911");
    }
}
