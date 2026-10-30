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

//! Message IDs do not authorize contacting the address encoded inside them.

use super::*;
use crate::client_adapter::message_target::lookup_physical_message;

pub(super) async fn lookup_scoped_message(
    session: &AdminSession,
    request: &MessageLookupRequest,
) -> Result<MessageRecord, AdminError> {
    session.ensure_open()?;
    let topic = require_non_empty("topic", &request.topic)?;
    let id = require_non_empty("messageId", &request.message_id)?;
    // Producer unique IDs can have the same hexadecimal shape as physical IDs. Resolve an exact
    // unique-key match through the Topic route before interpreting an embedded physical address.
    if let Ok(result) = session
        .inner
        .query_message_by_unique_key(None, topic.into(), id.into(), 32, 0, i64::MAX)
        .await
    {
        let matches = result
            .message_list()
            .iter()
            .filter(|message| {
                message.topic().as_str() == topic
                    && message
                        .properties()
                        .get(PROPERTY_UNIQUE_KEY)
                        .is_some_and(|value| value.as_str() == id)
            })
            .collect::<Vec<_>>();
        if let Some(message) = newest_unique_match(matches) {
            return Ok(map_message_record!(message));
        }
    }
    let route = rocketmq_client_rust::MQAdminReadExt::examine_topic_route_info(&session.inner, topic.into())
        .await
        .map_err(|source| promote_to_admin_error("message_topic_route", source))?;
    let resolved = lookup_physical_message(&session.inner, topic, id, route.as_ref()).await?;
    Ok(map_message_record!(&resolved.message))
}

fn newest_unique_match(
    mut messages: Vec<&rocketmq_model::common::message::message_ext::MessageExt>,
) -> Option<&rocketmq_model::common::message::message_ext::MessageExt> {
    messages.sort_by(|left, right| {
        right
            .store_timestamp()
            .cmp(&left.store_timestamp())
            .then_with(|| left.msg_id().cmp(right.msg_id()))
    });
    messages.into_iter().next()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_model::common::message::message_ext::MessageExt;
    #[test]
    fn duplicate_producer_ids_resolve_newest_physical_message_with_stable_tie_break() {
        let oldest = MessageExt {
            store_timestamp: 1,
            msg_id: "offset-0".into(),
            ..Default::default()
        };
        let newer_b = MessageExt {
            store_timestamp: 2,
            msg_id: "offset-b".into(),
            ..Default::default()
        };
        let newer_a = MessageExt {
            msg_id: "offset-a".into(),
            ..newer_b.clone()
        };
        assert_eq!(
            newest_unique_match(vec![&oldest, &newer_b, &newer_a])
                .unwrap()
                .msg_id()
                .as_str(),
            "offset-a"
        );
        assert_eq!(
            newest_unique_match(vec![&newer_a, &oldest, &newer_b])
                .unwrap()
                .msg_id()
                .as_str(),
            "offset-a"
        );
    }
}
