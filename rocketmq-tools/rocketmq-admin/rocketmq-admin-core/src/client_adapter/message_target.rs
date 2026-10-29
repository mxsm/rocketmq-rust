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

//! Physical offsets are read only through Brokers in the selected Topic's route.

use crate::core::{AdminError, AdminResult};
use crate::IntoCanonicalError as _;
use rocketmq_client_rust::{DefaultMQAdminExt, MQAdminMessageTargetReadExt, MessageTargetLookup};
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_protocol::{common::message::message_decoder, protocol::route::topic_route_data::TopicRouteData};
use std::{collections::BTreeSet, future::Future};

pub(crate) struct ResolvedPhysicalMessage {
    pub(crate) broker_addr: cheetah_string::CheetahString,
    pub(crate) message: MessageExt,
}

pub(crate) async fn lookup_physical_message(
    admin: &DefaultMQAdminExt,
    topic: &str,
    id: &str,
    route: Option<&TopicRouteData>,
) -> AdminResult<ResolvedPhysicalMessage> {
    lookup_physical_from(topic, id, route, |request| async move {
        admin
            .view_message_at(&request)
            .await
            .map_err(|source| AdminError::from_error("message_lookup", source.into_canonical_error()))
    })
    .await
}

async fn lookup_physical_from<F, Fut>(
    topic: &str,
    id: &str,
    route: Option<&TopicRouteData>,
    mut read: F,
) -> AdminResult<ResolvedPhysicalMessage>
where
    F: FnMut(MessageTargetLookup) -> Fut,
    Fut: Future<Output = AdminResult<MessageExt>>,
{
    let decoded = message_decoder::decode_message_id(id)
        .map_err(|_| AdminError::invalid_argument("messageId", "Message ID must resolve in the selected Topic"))?;
    let mut targets = route
        .into_iter()
        .flat_map(|route| &route.broker_datas)
        .flat_map(|broker| broker.broker_addrs().values().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    // Prefer an advertised literal match, but retain aliases and mapped route targets. Every read
    // goes through an advertised address; the returned physical ID supplies the missing evidence.
    targets.sort_by_key(|address| address.parse::<std::net::SocketAddr>().ok() != Some(decoded.address));
    if targets.is_empty() {
        return Err(AdminError::invalid_argument(
            "messageId",
            "The selected Topic has no Broker route",
        ));
    }
    let mut failure = None;
    for broker_addr in targets {
        match read(MessageTargetLookup {
            broker_addr: broker_addr.clone(),
            topic: topic.into(),
            message_id: id.into(),
        })
        .await
        {
            Ok(message)
                if require_message_match(topic, id, message.topic().as_str(), message.msg_id().as_str()).is_ok() =>
            {
                return Ok(ResolvedPhysicalMessage { broker_addr, message })
            }
            Ok(_) => {}
            Err(error) => failure = Some(error),
        }
    }
    Err(failure.unwrap_or_else(|| {
        AdminError::invalid_argument(
            "message",
            "Message did not match the selected Topic route and physical ID",
        )
    }))
}

pub(crate) fn require_message_match(topic: &str, id: &str, actual_topic: &str, actual_id: &str) -> AdminResult<()> {
    if topic == actual_topic && id.eq_ignore_ascii_case(actual_id) {
        Ok(())
    } else {
        Err(AdminError::invalid_argument(
            "message",
            "Message did not match the confirmed Topic and physical ID",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cheetah_string::CheetahString;
    use rocketmq_model::common::message::MessageTrait as _;
    use rocketmq_protocol::protocol::route::route_data_view::BrokerData;
    use std::{collections::HashMap, sync::Mutex};
    const ID: &str = "7F00000100002A9F0000000000000001";
    fn route(address: &str) -> TopicRouteData {
        TopicRouteData {
            broker_datas: vec![BrokerData::new(
                "cluster".into(),
                "broker".into(),
                HashMap::from([(0, address.into())]),
                None,
            )],
            ..Default::default()
        }
    }
    fn message(topic: &str, id: &str) -> MessageExt {
        let mut message = MessageExt::default();
        message.set_topic(topic.into());
        message.msg_id = id.into();
        message
    }

    #[tokio::test]
    async fn message_hostname_route_reads_only_the_advertised_alias_and_verifies_physical_evidence() {
        let calls = Mutex::new(Vec::new());
        let message = lookup_physical_from("orders", ID, Some(&route("broker.dev.example:10911")), |request| {
            calls
                .lock()
                .unwrap()
                .push((request.broker_addr, request.topic, request.message_id));
            std::future::ready(Ok(message("orders", ID)))
        })
        .await
        .unwrap();
        assert_eq!(message.message.msg_id().as_str(), ID);
        assert_eq!(message.broker_addr.as_str(), "broker.dev.example:10911");
        assert_eq!(
            *calls.lock().unwrap(),
            vec![(
                CheetahString::from("broker.dev.example:10911"),
                CheetahString::from("orders"),
                CheetahString::from(ID)
            )]
        );
    }

    #[tokio::test]
    async fn message_other_environment_id_or_wrong_topic_cannot_escape_the_selected_route() {
        for (requested, returned_topic, returned_id) in [
            ("C000020100002A9F0000000000000001", "orders", ID),
            (ID, "other-topic", ID),
        ] {
            let calls = Mutex::new(Vec::new());
            let result = lookup_physical_from(
                "orders",
                requested,
                Some(&route("trusted-broker.example:10911")),
                |request| {
                    calls.lock().unwrap().push(request.broker_addr);
                    std::future::ready(Ok(message(returned_topic, returned_id)))
                },
            )
            .await;
            assert!(result.is_err());
            assert_eq!(
                *calls.lock().unwrap(),
                vec![CheetahString::from("trusted-broker.example:10911")]
            );
        }
    }

    #[tokio::test]
    async fn message_resolution_retains_the_exact_alias_that_verified_the_message() {
        let mut route = route("a-unrelated.example:10911");
        route.broker_datas.push(BrokerData::new(
            "cluster".into(),
            "broker-b".into(),
            HashMap::from([(0, "b-matching.example:10911".into())]),
            None,
        ));
        let calls = Mutex::new(Vec::new());
        let resolved = lookup_physical_from("orders", ID, Some(&route), |request| {
            let topic = if request.broker_addr.as_str() == "b-matching.example:10911" {
                "orders"
            } else {
                "other-topic"
            };
            calls.lock().unwrap().push(request.broker_addr);
            std::future::ready(Ok(message(topic, ID)))
        })
        .await
        .unwrap();
        assert_eq!(resolved.broker_addr.as_str(), "b-matching.example:10911");
        assert_eq!(resolved.message.topic().as_str(), "orders");
        assert_eq!(
            *calls.lock().unwrap(),
            vec![
                CheetahString::from("a-unrelated.example:10911"),
                CheetahString::from("b-matching.example:10911"),
            ]
        );
    }

    #[tokio::test]
    async fn message_missing_route_or_invalid_physical_id_has_no_broker_read() {
        let read = |_: MessageTargetLookup| async { panic!("unscoped message read") };
        assert!(lookup_physical_from("orders", ID, None, read).await.is_err());
        assert!(
            lookup_physical_from("orders", "not-an-id", Some(&route("broker.example:10911")), read)
                .await
                .is_err()
        );
    }
}
