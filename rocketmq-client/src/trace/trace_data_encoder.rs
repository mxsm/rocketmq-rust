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

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_enum::MessageType;
use rocketmq_common::common::message::MessageConst;

use crate::base::access_channel::AccessChannel;
use crate::producer::local_transaction_state::LocalTransactionState;
use crate::trace::trace_bean::TraceBean;
use crate::trace::trace_constants::TraceConstants;
use crate::trace::trace_context::TraceContext;
use crate::trace::trace_transfer_bean::TraceTransferBean;
use crate::trace::trace_type::TraceType;

/// Encodes and decodes trace data for RocketMQ message tracing.
///
/// Provides utilities for converting structured trace contexts into wire format strings
/// and parsing trace data strings back into trace contexts. The encoder uses pre-allocated
/// buffers and zero-copy string parsing to minimize allocations.
pub struct TraceDataEncoder;

impl TraceDataEncoder {
    /// Decodes trace data from a string into structured trace contexts.
    ///
    /// Parses a trace data string containing one or more encoded trace records separated
    /// by field separators. The result vector is pre-allocated based on separator count
    /// to minimize allocations. Invalid or malformed records are silently skipped.
    ///
    /// Returns an empty vector if the input string is empty.
    pub fn decoder_from_trace_data_string(trace_data: &str) -> Vec<TraceContext> {
        // Early return for empty input
        if trace_data.is_empty() {
            return Vec::new();
        }

        // Pre-allocate result vector based on field separator count
        let estimated_size = trace_data.matches(TraceConstants::FIELD_SPLITOR).count();
        let mut res_list = Vec::with_capacity(estimated_size.max(1));

        // Split by field separator
        for context_str in trace_data.split(TraceConstants::FIELD_SPLITOR) {
            if context_str.is_empty() {
                continue;
            }

            let line: Vec<&str> = context_str.split(TraceConstants::CONTENT_SPLITOR).collect();
            if line.is_empty() {
                continue;
            }

            // Match trace type and parse accordingly
            match line[0] {
                "Pub" => {
                    if let Some(ctx) = Self::decode_pub_context(&line) {
                        res_list.push(ctx);
                    }
                }
                "SubBefore" => {
                    if let Some(ctx) = Self::decode_sub_before_context(&line) {
                        res_list.push(ctx);
                    }
                }
                "SubAfter" => {
                    if let Some(ctx) = Self::decode_sub_after_context(&line) {
                        res_list.push(ctx);
                    }
                }
                "EndTransaction" => {
                    if let Some(ctx) = Self::decode_end_transaction_context(&line) {
                        res_list.push(ctx);
                    }
                }
                "Recall" => {
                    if let Some(ctx) = Self::decode_recall_context(&line) {
                        res_list.push(ctx);
                    }
                }
                _ => {} // Unknown trace type, skip
            }
        }

        res_list
    }

    /// Encodes a trace context into a transfer bean for transmission.
    ///
    /// Serializes the trace context into a wire format string and extracts message keys
    /// for indexing. The string builder is pre-allocated with 256 bytes capacity to reduce
    /// allocations during encoding.
    ///
    /// Returns `None` if the context has no trace beans or an invalid trace type.
    pub fn encoder_from_context_bean(ctx: &TraceContext) -> Option<TraceTransferBean> {
        let trace_beans = ctx.trace_beans.as_ref()?;
        if trace_beans.is_empty() {
            return None;
        }

        let mut transfer_bean = TraceTransferBean::new();
        // Pre-allocate string builder with estimated capacity
        let mut sb = String::with_capacity(256);

        match ctx.trace_type? {
            TraceType::Pub => {
                Self::encode_pub_context(ctx, &trace_beans[0], &mut sb);
            }
            TraceType::SubBefore => {
                Self::encode_sub_before_context(ctx, trace_beans, &mut sb);
            }
            TraceType::SubAfter => {
                Self::encode_sub_after_context(ctx, trace_beans, &mut sb);
            }
            TraceType::EndTransaction => {
                Self::encode_end_transaction_context(ctx, &trace_beans[0], &mut sb);
            }
            TraceType::Recall => {
                Self::encode_recall_context(ctx, &trace_beans[0], &mut sb);
            }
        }

        transfer_bean.set_trans_data(CheetahString::from_string(sb));

        // Extract keys from trace beans
        for bean in trace_beans {
            transfer_bean.add_key(bean.msg_id.clone());

            if !bean.keys.is_empty() {
                let keys: Vec<&str> = bean.keys.split(MessageConst::KEY_SEPARATOR).collect();
                for key in keys {
                    if !key.is_empty() {
                        transfer_bean.add_key(CheetahString::from_slice(key));
                    }
                }
            }
        }

        Some(transfer_bean)
    }

    // ==================== Decoder Helper Methods ====================

    #[inline]
    fn decode_pub_context(line: &[&str]) -> Option<TraceContext> {
        if line.len() < 12 {
            return None;
        }

        let time_stamp = line[1].parse().ok()?;
        let region_id = CheetahString::from_slice(line[2]);
        let group_name = CheetahString::from_slice(line[3]);
        let cost_time = line[10].parse().ok()?;
        let body_length = line[9].parse().ok()?;

        // Parse message type
        let msg_type = if let Ok(msg_type_ordinal) = line[11].parse::<usize>() {
            Self::message_type_from_ordinal(msg_type_ordinal)
        } else {
            None
        };

        // Handle different line lengths for backward compatibility
        let (is_success, offset_msg_id, client_host) = if line.len() == 13 {
            (
                line[12].parse().unwrap_or(true),
                CheetahString::default(),
                CheetahString::default(),
            )
        } else if line.len() == 14 {
            (
                line[13].parse().unwrap_or(true),
                CheetahString::from_slice(line[12]),
                CheetahString::default(),
            )
        } else if line.len() >= 15 {
            (
                line[13].parse().unwrap_or(true),
                CheetahString::from_slice(line[12]),
                CheetahString::from_slice(line[14]),
            )
        } else {
            (true, CheetahString::default(), CheetahString::default())
        };

        let bean = TraceBean {
            topic: CheetahString::from_slice(line[4]),
            msg_id: CheetahString::from_slice(line[5]),
            tags: CheetahString::from_slice(line[6]),
            keys: CheetahString::from_slice(line[7]),
            store_host: CheetahString::from_slice(line[8]),
            body_length,
            msg_type,
            offset_msg_id,
            client_host,
            ..Default::default()
        };

        let ctx = TraceContext {
            trace_type: Some(TraceType::Pub),
            time_stamp,
            region_id,
            group_name,
            cost_time,
            is_success,
            trace_beans: Some(vec![bean]),
            ..Default::default()
        };

        Some(ctx)
    }

    #[inline]
    fn decode_sub_before_context(line: &[&str]) -> Option<TraceContext> {
        if line.len() < 8 {
            return None;
        }

        let bean = TraceBean {
            msg_id: CheetahString::from_slice(line[5]),
            retry_times: line[6].parse().ok()?,
            keys: CheetahString::from_slice(line[7]),
            ..Default::default()
        };

        let ctx = TraceContext {
            trace_type: Some(TraceType::SubBefore),
            time_stamp: line[1].parse().ok()?,
            region_id: CheetahString::from_slice(line[2]),
            group_name: CheetahString::from_slice(line[3]),
            request_id: CheetahString::from_slice(line[4]),
            trace_beans: Some(vec![bean]),
            ..Default::default()
        };

        Some(ctx)
    }

    #[inline]
    fn decode_sub_after_context(line: &[&str]) -> Option<TraceContext> {
        if line.len() < 6 {
            return None;
        }

        let context_code = if line.len() >= 7 {
            line[6].parse().unwrap_or(0)
        } else {
            0
        };

        let (time_stamp, group_name) = if line.len() >= 9 {
            (line[7].parse().unwrap_or(0), CheetahString::from_slice(line[8]))
        } else {
            (0, CheetahString::default())
        };

        let bean = TraceBean {
            msg_id: CheetahString::from_slice(line[2]),
            keys: CheetahString::from_slice(line[5]),
            ..Default::default()
        };

        let ctx = TraceContext {
            trace_type: Some(TraceType::SubAfter),
            request_id: CheetahString::from_slice(line[1]),
            cost_time: line[3].parse().ok()?,
            is_success: line[4].parse().unwrap_or(false),
            context_code,
            time_stamp,
            group_name,
            trace_beans: Some(vec![bean]),
            ..Default::default()
        };

        Some(ctx)
    }

    #[inline]
    fn decode_end_transaction_context(line: &[&str]) -> Option<TraceContext> {
        if line.len() < 13 {
            return None;
        }

        let msg_type = if let Ok(msg_type_ordinal) = line[9].parse::<usize>() {
            Self::message_type_from_ordinal(msg_type_ordinal)
        } else {
            None
        };

        let bean = TraceBean {
            topic: CheetahString::from_slice(line[4]),
            msg_id: CheetahString::from_slice(line[5]),
            tags: CheetahString::from_slice(line[6]),
            keys: CheetahString::from_slice(line[7]),
            store_host: CheetahString::from_slice(line[8]),
            msg_type,
            transaction_id: Some(CheetahString::from_slice(line[10])),
            transaction_state: Self::parse_transaction_state(line[11]),
            from_transaction_check: line[12].parse().unwrap_or(false),
            ..Default::default()
        };

        let ctx = TraceContext {
            trace_type: Some(TraceType::EndTransaction),
            time_stamp: line[1].parse().ok()?,
            region_id: CheetahString::from_slice(line[2]),
            group_name: CheetahString::from_slice(line[3]),
            trace_beans: Some(vec![bean]),
            ..Default::default()
        };

        Some(ctx)
    }

    #[inline]
    fn decode_recall_context(line: &[&str]) -> Option<TraceContext> {
        if line.len() < 7 {
            return None;
        }

        let bean = TraceBean {
            topic: CheetahString::from_slice(line[4]),
            msg_id: CheetahString::from_slice(line[5]),
            ..Default::default()
        };

        let ctx = TraceContext {
            trace_type: Some(TraceType::Recall),
            time_stamp: line[1].parse().ok()?,
            region_id: CheetahString::from_slice(line[2]),
            group_name: CheetahString::from_slice(line[3]),
            is_success: line[6].parse().unwrap_or(false),
            trace_beans: Some(vec![bean]),
            ..Default::default()
        };

        Some(ctx)
    }

    // ==================== Encoder Helper Methods ====================

    #[inline]
    fn encode_pub_context(ctx: &TraceContext, bean: &TraceBean, sb: &mut String) {
        sb.push_str("Pub");
        sb.push(TraceConstants::CONTENT_SPLITOR);
        sb.push_str(&ctx.time_stamp.to_string());
        sb.push(TraceConstants::CONTENT_SPLITOR);
        sb.push_str(&ctx.region_id);
        sb.push(TraceConstants::CONTENT_SPLITOR);
        sb.push_str(&ctx.group_name);
        sb.push(TraceConstants::CONTENT_SPLITOR);
        sb.push_str(&bean.topic);
        sb.push(TraceConstants::CONTENT_SPLITOR);
        sb.push_str(&bean.msg_id);
        sb.push(TraceConstants::CONTENT_SPLITOR);
        sb.push_str(&bean.tags);
        sb.push(TraceConstants::CONTENT_SPLITOR);
        sb.push_str(&bean.keys);
        sb.push(TraceConstants::CONTENT_SPLITOR);
        sb.push_str(&bean.store_host);
        sb.push(TraceConstants::CONTENT_SPLITOR);
        sb.push_str(&bean.body_length.to_string());
        sb.push(TraceConstants::CONTENT_SPLITOR);
        sb.push_str(&ctx.cost_time.to_string());
        sb.push(TraceConstants::CONTENT_SPLITOR);

        if let Some(msg_type) = &bean.msg_type {
            sb.push_str(&Self::message_type_to_ordinal(*msg_type).to_string());
        } else {
            sb.push('0');
        }
        sb.push(TraceConstants::CONTENT_SPLITOR);
        sb.push_str(&bean.offset_msg_id);
        sb.push(TraceConstants::CONTENT_SPLITOR);
        sb.push_str(if ctx.is_success { "true" } else { "false" });
        sb.push(TraceConstants::FIELD_SPLITOR);
    }

    #[inline]
    fn encode_sub_before_context(ctx: &TraceContext, beans: &[TraceBean], sb: &mut String) {
        for bean in beans {
            sb.push_str("SubBefore");
            sb.push(TraceConstants::CONTENT_SPLITOR);
            sb.push_str(&ctx.time_stamp.to_string());
            sb.push(TraceConstants::CONTENT_SPLITOR);
            sb.push_str(&ctx.region_id);
            sb.push(TraceConstants::CONTENT_SPLITOR);
            sb.push_str(&ctx.group_name);
            sb.push(TraceConstants::CONTENT_SPLITOR);
            sb.push_str(&ctx.request_id);
            sb.push(TraceConstants::CONTENT_SPLITOR);
            sb.push_str(&bean.msg_id);
            sb.push(TraceConstants::CONTENT_SPLITOR);
            sb.push_str(&bean.retry_times.to_string());
            sb.push(TraceConstants::CONTENT_SPLITOR);
            sb.push_str(&bean.keys);
            sb.push(TraceConstants::FIELD_SPLITOR);
        }
    }

    #[inline]
    fn encode_sub_after_context(ctx: &TraceContext, beans: &[TraceBean], sb: &mut String) {
        for bean in beans {
            sb.push_str("SubAfter");
            sb.push(TraceConstants::CONTENT_SPLITOR);
            sb.push_str(&ctx.request_id);
            sb.push(TraceConstants::CONTENT_SPLITOR);
            sb.push_str(&bean.msg_id);
            sb.push(TraceConstants::CONTENT_SPLITOR);
            sb.push_str(&ctx.cost_time.to_string());
            sb.push(TraceConstants::CONTENT_SPLITOR);
            sb.push_str(if ctx.is_success { "true" } else { "false" });
            sb.push(TraceConstants::CONTENT_SPLITOR);
            sb.push_str(&bean.keys);
            sb.push(TraceConstants::CONTENT_SPLITOR);
            sb.push_str(&ctx.context_code.to_string());
            sb.push(TraceConstants::CONTENT_SPLITOR);

            // Only add timestamp and group name if not CLOUD access channel
            if !matches!(ctx.access_channel, Some(AccessChannel::Cloud)) {
                sb.push_str(&ctx.time_stamp.to_string());
                sb.push(TraceConstants::CONTENT_SPLITOR);
                sb.push_str(&ctx.group_name);
            }
            sb.push(TraceConstants::FIELD_SPLITOR);
        }
    }

    #[inline]
    fn encode_end_transaction_context(ctx: &TraceContext, bean: &TraceBean, sb: &mut String) {
        sb.push_str("EndTransaction");
        sb.push(TraceConstants::CONTENT_SPLITOR);
        sb.push_str(&ctx.time_stamp.to_string());
        sb.push(TraceConstants::CONTENT_SPLITOR);
        sb.push_str(&ctx.region_id);
        sb.push(TraceConstants::CONTENT_SPLITOR);
        sb.push_str(&ctx.group_name);
        sb.push(TraceConstants::CONTENT_SPLITOR);
        sb.push_str(&bean.topic);
        sb.push(TraceConstants::CONTENT_SPLITOR);
        sb.push_str(&bean.msg_id);
        sb.push(TraceConstants::CONTENT_SPLITOR);
        sb.push_str(&bean.tags);
        sb.push(TraceConstants::CONTENT_SPLITOR);
        sb.push_str(&bean.keys);
        sb.push(TraceConstants::CONTENT_SPLITOR);
        sb.push_str(&bean.store_host);
        sb.push(TraceConstants::CONTENT_SPLITOR);

        if let Some(msg_type) = &bean.msg_type {
            sb.push_str(&Self::message_type_to_ordinal(*msg_type).to_string());
        } else {
            sb.push('0');
        }
        sb.push(TraceConstants::CONTENT_SPLITOR);

        if let Some(ref transaction_id) = bean.transaction_id {
            sb.push_str(transaction_id);
        }
        sb.push(TraceConstants::CONTENT_SPLITOR);

        if let Some(ref state) = bean.transaction_state {
            sb.push_str(&state.to_string());
        }
        sb.push(TraceConstants::CONTENT_SPLITOR);
        sb.push_str(if bean.from_transaction_check { "true" } else { "false" });
        sb.push(TraceConstants::FIELD_SPLITOR);
    }

    #[inline]
    fn encode_recall_context(ctx: &TraceContext, bean: &TraceBean, sb: &mut String) {
        sb.push_str("Recall");
        sb.push(TraceConstants::CONTENT_SPLITOR);
        sb.push_str(&ctx.time_stamp.to_string());
        sb.push(TraceConstants::CONTENT_SPLITOR);
        sb.push_str(&ctx.region_id);
        sb.push(TraceConstants::CONTENT_SPLITOR);
        sb.push_str(&ctx.group_name);
        sb.push(TraceConstants::CONTENT_SPLITOR);
        sb.push_str(&bean.topic);
        sb.push(TraceConstants::CONTENT_SPLITOR);
        sb.push_str(&bean.msg_id);
        sb.push(TraceConstants::CONTENT_SPLITOR);
        sb.push_str(if ctx.is_success { "true" } else { "false" });
        sb.push(TraceConstants::FIELD_SPLITOR);
    }

    // ==================== Utility Methods ====================

    #[inline]
    fn message_type_from_ordinal(ordinal: usize) -> Option<MessageType> {
        match ordinal {
            0 => Some(MessageType::NormalMsg),
            1 => Some(MessageType::TransMsgHalf),
            2 => Some(MessageType::TransMsgCommit),
            3 => Some(MessageType::DelayMsg),
            4 => Some(MessageType::OrderMsg),
            _ => None,
        }
    }

    #[inline]
    fn message_type_to_ordinal(msg_type: MessageType) -> usize {
        match msg_type {
            MessageType::NormalMsg => 0,
            MessageType::TransMsgHalf => 1,
            MessageType::TransMsgCommit => 2,
            MessageType::DelayMsg => 3,
            MessageType::OrderMsg => 4,
        }
    }

    #[inline]
    fn parse_transaction_state(state_str: &str) -> Option<LocalTransactionState> {
        match state_str {
            "COMMIT_MESSAGE" => Some(LocalTransactionState::CommitMessage),
            "ROLLBACK_MESSAGE" => Some(LocalTransactionState::RollbackMessage),
            "UNKNOW" | "UNKNOWN" => Some(LocalTransactionState::Unknown),
            _ => None,
        }
    }
}
