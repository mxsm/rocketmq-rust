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

use rocketmq_client_rust::admin_adapter_compat::message::MessageExt;
use rocketmq_client_rust::admin_adapter_compat::message::MessageTrait;

pub(super) fn query<'a>(
    session: &'a mut AdminSession,
    request: &'a dashboard::DashboardMessageQuery,
) -> AdminFuture<'a, dashboard::DashboardMessageList> {
    Box::pin(async move { query_messages(session, request, true).await })
}

pub(super) fn query_dlq<'a>(
    session: &'a mut AdminSession,
    request: &'a dashboard::DashboardDlqMessageQuery,
) -> AdminFuture<'a, dashboard::DashboardMessageList> {
    Box::pin(async move {
        session.ensure_open()?;
        let consumer_group = request.consumer_group.trim();
        if consumer_group.is_empty() {
            return Err(AdminError::invalid_argument("consumerGroup", "must not be empty"));
        }
        let topic = format!("{DLQ_GROUP_TOPIC_PREFIX}{consumer_group}");
        if session
            .inner
            .examine_topic_route_info(topic.as_str().into())
            .await
            .map_err(|error| backend_error("examine_topic_route_info", error))?
            .is_none()
        {
            return Ok(dashboard::DashboardMessageList::default());
        }
        let query = dashboard::DashboardMessageQuery {
            topic: Some(topic),
            key: request.key.clone(),
            message_id: request.message_id.clone(),
            begin: request.begin,
            end: request.end,
            page_num: request.page_num,
            page_size: request.page_size,
        };
        query_messages(session, &query, false).await
    })
}

#[allow(
    deprecated,
    reason = "the client SDK track result remains the compatibility source during M07"
)]
pub(super) fn trace<'a>(
    session: &'a mut AdminSession,
    topic: &'a str,
    message_id: &'a str,
    trace_topic: &'a str,
) -> AdminFuture<'a, dashboard::DashboardMessageTrace> {
    Box::pin(async move {
        session.ensure_open()?;
        let message = crate::client_adapter::message::find_raw_message(session, topic, message_id).await?;
        let tracks = session
            .inner
            .message_track_detail(message)
            .await
            .map_err(|error| backend_error("message_track_detail", error))?;
        let mut nodes = tracks
            .into_iter()
            .map(|track| dashboard::DashboardMessageTraceNode {
                node_type: "CONSUMER".to_string(),
                name: track.consumer_group,
                status: track
                    .track_type
                    .map(|track_type| track_type.to_string())
                    .unwrap_or_else(|| "UNKNOWN".to_string()),
                timestamp: 0,
            })
            .collect::<Vec<_>>();
        nodes.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(dashboard::DashboardMessageTrace {
            message_id: message_id.to_string(),
            trace_topic: trace_topic.to_string(),
            nodes,
        })
    })
}

pub(super) fn consume_directly<'a>(
    session: &'a mut AdminSession,
    request: &'a dashboard::DashboardDirectConsumeRequest,
) -> AdminFuture<'a, dashboard::AdminMutationResult> {
    Box::pin(async move {
        session.ensure_open()?;
        let result = session
            .inner
            .consume_message_directly(
                request.consumer_group.as_str().into(),
                request.client_id.as_deref().unwrap_or_default().into(),
                request.topic.as_str().into(),
                request.message_id.as_str().into(),
            )
            .await
            .map_err(|error| backend_error("consume_message_directly", error))?;
        let consume_result = result
            .consume_result()
            .map(ToString::to_string)
            .unwrap_or_else(|| "UNKNOWN".to_string());
        let mut message = format!(
            "Direct consume returned {consume_result} for `{}` on `{}` in consumer group `{}`",
            request.message_id, request.topic, request.consumer_group
        );
        if let Some(remark) = result.remark().map(ToString::to_string) {
            if !remark.trim().is_empty() {
                message.push_str(&format!(". Remark: {remark}"));
            }
        }
        Ok(dashboard::AdminMutationResult {
            message,
            target_count: 1,
        })
    })
}

fn message_unique_key(message: &MessageExt) -> Option<&str> {
    message.properties().get("UNIQ_KEY").map(CheetahString::as_str)
}

fn map_message(message: &MessageExt) -> dashboard::DashboardMessage {
    let properties = message
        .properties()
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect::<BTreeMap<_, _>>();
    dashboard::DashboardMessage {
        topic: message.topic().to_string(),
        message_id: message.msg_id().to_string(),
        keys: properties.get("KEYS").cloned(),
        tags: message.tags().map(|value| value.to_string()),
        born_timestamp: message.born_timestamp(),
        store_timestamp: message.store_timestamp(),
        born_host: message.born_host().to_string(),
        store_host: message.store_host().to_string(),
        queue_id: message.queue_id(),
        queue_offset: message.queue_offset(),
        store_size: message.store_size(),
        reconsume_times: message.reconsume_times(),
        body_crc: message.body_crc(),
        sys_flag: message.sys_flag(),
        flag: message.flag(),
        prepared_transaction_offset: message.prepared_transaction_offset(),
        body: message.body().map(|body| body.to_vec()).unwrap_or_default(),
        properties,
    }
}

pub(super) async fn query_messages(
    session: &mut AdminSession,
    request: &dashboard::DashboardMessageQuery,
    inclusive_end_offset: bool,
) -> Result<dashboard::DashboardMessageList, AdminError> {
    session.ensure_open()?;
    let topic = request
        .topic
        .as_deref()
        .filter(|topic| !topic.trim().is_empty())
        .ok_or_else(|| AdminError::invalid_argument("topic", "must not be empty"))?;
    if let Some(key) = request.key.as_deref() {
        let result = session
            .inner
            .query_message_by_key(
                None,
                topic.into(),
                key.into(),
                64,
                request.begin.unwrap_or_default(),
                request.end.unwrap_or(i64::MAX),
                CheetahString::from_static_str("K"),
                None,
            )
            .await
            .map_err(|error| backend_error("query_message_by_key", error))?;
        let items = result.message_list().iter().map(map_message).collect::<Vec<_>>();
        return Ok(dashboard::DashboardMessageList {
            total: items.len(),
            items,
        });
    }
    if let Some(message_id) = request.message_id.as_deref() {
        let result = session
            .inner
            .query_message_by_unique_key(
                None,
                topic.into(),
                message_id.into(),
                32,
                request.begin.unwrap_or_default(),
                request.end.unwrap_or(i64::MAX),
            )
            .await
            .map_err(|error| backend_error("query_message_by_unique_key", error))?;
        let items = result
            .message_list()
            .iter()
            .filter(|message| message_unique_key(message) == Some(message_id))
            .map(map_message)
            .collect::<Vec<_>>();
        return Ok(dashboard::DashboardMessageList {
            total: items.len(),
            items,
        });
    }
    scan_messages(session, topic, request, inclusive_end_offset).await
}

pub(super) async fn scan_messages(
    session: &mut AdminSession,
    topic: &str,
    request: &dashboard::DashboardMessageQuery,
    inclusive_end_offset: bool,
) -> Result<dashboard::DashboardMessageList, AdminError> {
    let route = session
        .inner
        .examine_topic_route_info(topic.into())
        .await
        .map_err(|error| backend_error("examine_topic_route_info", error))?;
    let route = route.ok_or_else(|| AdminError::not_found("topic", topic))?;
    let targets = topic_queue_targets(topic, &route);
    let page_size = request
        .page_size
        .unwrap_or(DEFAULT_PAGE_SIZE as u32)
        .clamp(1, MAX_PAGE_SIZE as u32) as usize;
    let skip_count = request.page_num.unwrap_or(1).max(1).saturating_sub(1) as usize * page_size;
    let mut seen = 0usize;
    let mut items = Vec::with_capacity(page_size);
    for (broker_addr, queue) in targets {
        if items.len() >= page_size {
            break;
        }
        let mut min_offset = if let Some(begin) = request.begin.filter(|value| *value > 0) {
            session
                .inner
                .search_offset(
                    broker_addr.as_str().into(),
                    topic.into(),
                    queue.queue_id(),
                    begin as u64,
                    PULL_TIMEOUT_MILLIS,
                )
                .await
                .map_err(|error| backend_error("search_offset", error))? as i64
        } else {
            session
                .inner
                .min_offset(broker_addr.as_str().into(), queue.clone(), PULL_TIMEOUT_MILLIS)
                .await
                .map_err(|error| backend_error("min_offset", error))?
        };
        let max_offset = if let Some(end) = request.end.filter(|value| *value > 0) {
            session
                .inner
                .search_offset(
                    broker_addr.as_str().into(),
                    topic.into(),
                    queue.queue_id(),
                    end as u64,
                    PULL_TIMEOUT_MILLIS,
                )
                .await
                .map_err(|error| backend_error("search_offset", error))? as i64
        } else {
            session
                .inner
                .max_offset(broker_addr.as_str().into(), queue.clone(), PULL_TIMEOUT_MILLIS)
                .await
                .map_err(|error| backend_error("max_offset", error))?
        };
        while has_remaining_offset(min_offset, max_offset, inclusive_end_offset) && items.len() < page_size {
            let batch_size = scan_batch_size(min_offset, max_offset, inclusive_end_offset);
            let result = session
                .inner
                .pull_message_from_queue(&broker_addr, &queue, "*", min_offset, batch_size, PULL_TIMEOUT_MILLIS)
                .await
                .map_err(|error| backend_error("pull_message_from_queue", error))?;
            let next_offset = result.next_begin_offset() as i64;
            min_offset = if next_offset > min_offset {
                next_offset
            } else {
                min_offset + i64::from(batch_size)
            };
            match *result.pull_status() {
                PullStatus::Found => {
                    if let Some(messages) = result.msg_found_list() {
                        for message in messages {
                            let timestamp = message.store_timestamp();
                            if request.begin.is_some_and(|begin| timestamp < begin)
                                || request.end.is_some_and(|end| timestamp > end)
                            {
                                continue;
                            }
                            if seen < skip_count {
                                seen += 1;
                                continue;
                            }
                            items.push(map_message(message));
                            seen += 1;
                            if items.len() >= page_size {
                                break;
                            }
                        }
                    }
                }
                PullStatus::NoMatchedMsg => {}
                PullStatus::NoNewMsg | PullStatus::OffsetIllegal => break,
            }
        }
    }
    items.sort_by_key(|message| Reverse(message.store_timestamp));
    Ok(dashboard::DashboardMessageList { total: seen, items })
}

fn has_remaining_offset(current: i64, end: i64, inclusive_end: bool) -> bool {
    current < end || (inclusive_end && current == end)
}

fn scan_batch_size(current: i64, end: i64, inclusive_end: bool) -> i32 {
    let inclusive_tail = if inclusive_end { 1 } else { 0 };
    end.saturating_sub(current)
        .saturating_add(inclusive_tail)
        .clamp(1, PULL_BATCH_SIZE) as i32
}

pub(super) fn topic_queue_targets(topic: &str, route: &TopicRouteData) -> Vec<(String, MessageQueue)> {
    let addresses = route
        .broker_datas
        .iter()
        .filter_map(|broker| {
            broker
                .broker_addrs()
                .get(&MASTER_ID)
                .or_else(|| broker.broker_addrs().values().next())
                .map(|address| (broker.broker_name().to_string(), address.to_string()))
        })
        .collect::<BTreeMap<_, _>>();
    let mut targets = Vec::new();
    for queue in &route.queue_datas {
        let Some(address) = addresses.get(queue.broker_name().as_str()) else {
            continue;
        };
        for queue_id in 0..queue.read_queue_nums() {
            targets.push((
                address.clone(),
                MessageQueue::from_parts(topic, queue.broker_name().clone(), queue_id as i32),
            ));
        }
    }
    targets.sort_by(|left, right| {
        left.1
            .broker_name()
            .cmp(right.1.broker_name())
            .then(left.1.queue_id().cmp(&right.1.queue_id()))
    });
    targets
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inclusive_message_scan_keeps_the_end_offset() {
        assert!(has_remaining_offset(7, 7, true));
        assert_eq!(scan_batch_size(7, 7, true), 1);
    }

    #[test]
    fn exclusive_dlq_scan_stops_at_the_end_offset() {
        assert!(!has_remaining_offset(7, 7, false));
        assert!(has_remaining_offset(6, 7, false));
        assert_eq!(scan_batch_size(6, 7, false), 1);
    }
}
