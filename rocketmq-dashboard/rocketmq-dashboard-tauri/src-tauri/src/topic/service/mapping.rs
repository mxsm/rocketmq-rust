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

use super::*;

pub(super) fn map_admin_error(error: AdminError) -> TopicError {
    match error {
        AdminError::InvalidArgument { reason, .. } => TopicError::Validation(reason),
        error => TopicError::Admin(error),
    }
}

pub(super) fn map_route_view(topic: String, route: TopicRoute) -> TopicRouteView {
    TopicRouteView {
        topic,
        brokers: route
            .brokers
            .into_iter()
            .map(|broker| TopicRouteBrokerView {
                cluster_name: broker.cluster,
                broker_name: broker.broker_name,
                addresses: broker
                    .broker_addrs
                    .into_iter()
                    .map(|(broker_id, address)| TopicBrokerAddressView {
                        broker_id: broker_id as i64,
                        address,
                    })
                    .collect(),
            })
            .collect(),
        queues: route
            .queues
            .into_iter()
            .map(|queue| TopicRouteQueueView {
                broker_name: queue.broker_name,
                read_queue_nums: queue.read_queue_nums,
                write_queue_nums: queue.write_queue_nums,
                perm: queue.perm as i32,
            })
            .collect(),
    }
}

pub(super) fn map_status_view(stats: TopicStats) -> TopicStatusView {
    TopicStatusView {
        topic: stats.topic,
        total_message_count: stats.total_message_count,
        queue_count: stats.queue_count,
        offsets: stats
            .offsets
            .into_iter()
            .map(|offset| TopicStatusOffsetView {
                broker_name: offset.broker_name,
                queue_id: offset.queue_id,
                min_offset: offset.min_offset,
                max_offset: offset.max_offset,
                last_update_timestamp: offset.last_update_timestamp,
            })
            .collect(),
    }
}

pub(super) fn map_send_result(result: AdminTopicSendResult) -> TopicSendMessageResult {
    TopicSendMessageResult {
        topic: result.topic,
        send_status: result.send_status,
        message_id: result.message_id,
        broker_name: result.broker_name,
        queue_id: result.queue_id,
        queue_offset: result.queue_offset,
        transaction_id: result.transaction_id,
        region_id: result.region_id,
        local_transaction_state: result.local_transaction_state,
    }
}

pub(super) fn normalize_topic_message_body(body: &str) -> TopicResult<String> {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return Err(TopicError::Validation(
            "Message body is required before sending.".into(),
        ));
    }
    if !(trimmed.starts_with('{') || trimmed.starts_with('[')) {
        return Ok(body.to_string());
    }
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(trimmed) {
        return serde_json::to_string(&value)
            .map_err(|error| TopicError::Validation(format!("Failed to serialize message JSON: {error}")));
    }
    if let Ok(value) = json5::from_str::<serde_json::Value>(trimmed) {
        return serde_json::to_string(&value).map_err(|error| {
            TopicError::Validation(format!("Failed to serialize relaxed JSON message body: {error}"))
        });
    }
    let normalized_numeric_keys = quote_numeric_object_keys(trimmed);
    if normalized_numeric_keys != trimmed
        && let Ok(value) = json5::from_str::<serde_json::Value>(&normalized_numeric_keys)
    {
        return serde_json::to_string(&value).map_err(|error| {
            TopicError::Validation(format!(
                "Failed to serialize relaxed JSON message body after normalizing numeric keys: {error}"
            ))
        });
    }
    Err(TopicError::Validation(
        "Message body looks like JSON but could not be parsed. Standard JSON and relaxed JSON syntax such as numeric \
         keys are supported."
            .into(),
    ))
}

pub(super) fn quote_numeric_object_keys(input: &str) -> String {
    let chars = input.chars().collect::<Vec<_>>();
    let mut output = String::with_capacity(input.len());
    let mut index = 0;
    let mut in_string = false;
    let mut string_delimiter = '\0';
    let mut escaped = false;
    let mut expecting_key = false;
    while index < chars.len() {
        let current = chars[index];
        if in_string {
            output.push(current);
            if escaped {
                escaped = false;
            } else if current == '\\' {
                escaped = true;
            } else if current == string_delimiter {
                in_string = false;
            }
            index += 1;
            continue;
        }
        if current == '"' || current == '\'' {
            in_string = true;
            string_delimiter = current;
            output.push(current);
            index += 1;
            continue;
        }
        match current {
            '{' | ',' => {
                expecting_key = true;
                output.push(current);
                index += 1;
            }
            _ if expecting_key && current.is_whitespace() => {
                output.push(current);
                index += 1;
            }
            _ if expecting_key && (current.is_ascii_digit() || current == '-') => {
                let start = index;
                index += 1;
                while index < chars.len() && chars[index].is_ascii_digit() {
                    index += 1;
                }
                let token = chars[start..index].iter().collect::<String>();
                let mut probe = index;
                while probe < chars.len() && chars[probe].is_whitespace() {
                    probe += 1;
                }
                if probe < chars.len() && chars[probe] == ':' && token.chars().any(|char| char.is_ascii_digit()) {
                    output.push('"');
                    output.push_str(&token);
                    output.push('"');
                } else {
                    output.push_str(&token);
                }
                expecting_key = false;
            }
            _ => {
                expecting_key = false;
                output.push(current);
                index += 1;
            }
        }
    }
    output
}
