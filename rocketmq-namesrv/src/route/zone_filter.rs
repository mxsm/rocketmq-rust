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

use std::borrow::Cow;
use std::collections::HashSet;

use cheetah_string::CheetahString;
use rocketmq_model::common::mix_all;
use rocketmq_model::common::mix_all::MASTER_ID;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;

pub(crate) const TYPED_ZONE_ROUTE_MARKER: &str = "__rocketmqRustTypedZoneRoute";
pub(crate) const TYPED_ZONE_ROUTE_ENABLED: &str = "enabled";
pub(crate) const TYPED_ZONE_ROUTE_SHADOW: &str = "shadow";

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ZoneRequest {
    zone_name: Option<CheetahString>,
}

impl ZoneRequest {
    pub(crate) fn from_command(request: &RemotingCommand) -> Self {
        let Some(ext_fields) = request.get_ext_fields() else {
            return Self::default();
        };
        let zone_mode = ext_fields
            .get(mix_all::ZONE_MODE)
            .and_then(|value| value.parse::<bool>().ok())
            .unwrap_or(false);
        if !zone_mode {
            return Self::default();
        }

        let zone_name = ext_fields
            .get(mix_all::ZONE_NAME)
            .filter(|value| !value.trim().is_empty())
            .cloned();
        Self { zone_name }
    }

    pub(crate) fn enabled(zone_name: CheetahString) -> Self {
        Self {
            zone_name: (!zone_name.trim().is_empty()).then_some(zone_name),
        }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.zone_name.is_some()
    }
}

/// Applies Java-compatible zone filtering before response serialization.
///
/// A disabled request or a route for which every broker is retained returns a
/// borrowed value. Only the filtered response owns cloned route components.
pub(crate) fn filter_route_by_zone<'a>(
    route_data: &'a TopicRouteData,
    request: &ZoneRequest,
) -> Cow<'a, TopicRouteData> {
    let Some(zone_name) = request.zone_name.as_ref() else {
        return Cow::Borrowed(route_data);
    };

    let removed_brokers = route_data
        .broker_datas
        .iter()
        .filter(|broker| {
            let master_down = !broker.broker_addrs().contains_key(&MASTER_ID);
            let same_zone = broker
                .zone_name()
                .is_some_and(|candidate| candidate.as_str().eq_ignore_ascii_case(zone_name.as_str()));
            !master_down && !same_zone
        })
        .map(|broker| broker.broker_name().clone())
        .collect::<HashSet<_>>();
    if removed_brokers.is_empty() {
        return Cow::Borrowed(route_data);
    }

    let removed_addresses = route_data
        .broker_datas
        .iter()
        .filter(|broker| removed_brokers.contains(broker.broker_name()))
        .flat_map(|broker| broker.broker_addrs().values().cloned())
        .collect::<HashSet<_>>();
    Cow::Owned(TopicRouteData {
        order_topic_conf: route_data.order_topic_conf.clone(),
        queue_datas: route_data
            .queue_datas
            .iter()
            .filter(|queue| !removed_brokers.contains(queue.broker_name()))
            .cloned()
            .collect(),
        broker_datas: route_data
            .broker_datas
            .iter()
            .filter(|broker| !removed_brokers.contains(broker.broker_name()))
            .cloned()
            .collect(),
        filter_server_table: route_data
            .filter_server_table
            .iter()
            .filter(|(address, _)| !removed_addresses.contains(*address))
            .map(|(address, servers)| (address.clone(), servers.clone()))
            .collect(),
        topic_queue_mapping_by_broker: route_data.topic_queue_mapping_by_broker.clone(),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rocketmq_protocol::protocol::route::route_data_view::BrokerData;
    use rocketmq_protocol::protocol::route::route_data_view::QueueData;

    use super::*;

    fn route() -> TopicRouteData {
        TopicRouteData {
            queue_datas: vec![
                QueueData::new(CheetahString::from_static_str("same"), 4, 4, 6, 0),
                QueueData::new(CheetahString::from_static_str("other"), 4, 4, 6, 0),
                QueueData::new(CheetahString::from_static_str("master-down"), 4, 4, 6, 0),
            ],
            broker_datas: vec![
                BrokerData::new(
                    CheetahString::from_static_str("cluster"),
                    CheetahString::from_static_str("same"),
                    HashMap::from([(MASTER_ID, CheetahString::from_static_str("same:10911"))]),
                    Some(CheetahString::from_static_str("ZONE-A")),
                ),
                BrokerData::new(
                    CheetahString::from_static_str("cluster"),
                    CheetahString::from_static_str("other"),
                    HashMap::from([(MASTER_ID, CheetahString::from_static_str("other:10911"))]),
                    Some(CheetahString::from_static_str("zone-b")),
                ),
                BrokerData::new(
                    CheetahString::from_static_str("cluster"),
                    CheetahString::from_static_str("master-down"),
                    HashMap::from([(1, CheetahString::from_static_str("slave:10911"))]),
                    None,
                ),
            ],
            filter_server_table: HashMap::from([
                (
                    CheetahString::from_static_str("same:10911"),
                    vec![CheetahString::from_static_str("same-filter")],
                ),
                (
                    CheetahString::from_static_str("other:10911"),
                    vec![CheetahString::from_static_str("other-filter")],
                ),
            ]),
            ..TopicRouteData::default()
        }
    }

    #[test]
    fn disabled_zone_request_borrows_the_original_route() {
        let route = route();
        assert!(matches!(
            filter_route_by_zone(&route, &ZoneRequest::default()),
            Cow::Borrowed(_)
        ));
    }

    #[test]
    fn typed_filter_keeps_same_zone_and_master_down_brokers() {
        let route = route();
        let filtered = filter_route_by_zone(&route, &ZoneRequest::enabled(CheetahString::from_static_str("zone-a")));

        assert_eq!(filtered.broker_datas.len(), 2);
        assert!(filtered
            .broker_datas
            .iter()
            .any(|broker| broker.broker_name() == "same"));
        assert!(filtered
            .broker_datas
            .iter()
            .any(|broker| broker.broker_name() == "master-down"));
        assert_eq!(filtered.queue_datas.len(), 2);
        assert!(!filtered.filter_server_table.contains_key("other:10911"));
    }
}
