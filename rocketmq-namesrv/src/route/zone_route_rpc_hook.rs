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

use cheetah_string::CheetahString;
use rocketmq_model::common::mix_all;
#[cfg(test)]
use rocketmq_model::common::mix_all::MASTER_ID;
use rocketmq_observability::metrics::namesrv::NameServerMetrics;
use rocketmq_observability::metrics::namesrv::NameServerRouteStage;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
#[cfg(test)]
use rocketmq_protocol::protocol::route::route_data_view::BrokerData;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_protocol::protocol::RemotingSerializable;
use rocketmq_transport::api::RPCHook;
use std::time::Instant;
use tracing::warn;

#[cfg(test)]
use std::sync::atomic::AtomicU64;
#[cfg(test)]
use std::sync::atomic::Ordering;

use crate::route::zone_filter::filter_route_by_zone;
use crate::route::zone_filter::ZoneRequest;
use crate::route::zone_filter::TYPED_ZONE_ROUTE_ENABLED;
use crate::route::zone_filter::TYPED_ZONE_ROUTE_MARKER;
use crate::route::zone_filter::TYPED_ZONE_ROUTE_SHADOW;

#[cfg(test)]
static ZONE_HOOK_DECODE_COUNT: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Default)]
pub struct ZoneRouteRPCHook {
    metrics: NameServerMetrics,
}

impl ZoneRouteRPCHook {
    pub(crate) fn new(metrics: NameServerMetrics) -> Self {
        Self { metrics }
    }
}

impl RPCHook for ZoneRouteRPCHook {
    #[inline(always)]
    fn do_before_request(
        &self,
        _remote_addr: std::net::SocketAddr,
        _request: &mut rocketmq_protocol::protocol::remoting_command::RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<()> {
        Ok(())
    }

    fn do_after_response(
        &self,
        _remote_addr: std::net::SocketAddr,
        request: &rocketmq_protocol::protocol::remoting_command::RemotingCommand,
        response: &mut rocketmq_protocol::protocol::remoting_command::RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<()> {
        if RequestCode::GetRouteinfoByTopic as i32 != request.code() {
            return Ok(());
        }
        if response.get_body().is_none() || ResponseCode::Success as i32 != response.code() {
            return Ok(());
        }
        let typed_mode = request
            .get_ext_fields()
            .and_then(|fields| fields.get(TYPED_ZONE_ROUTE_MARKER))
            .map(CheetahString::as_str);
        if typed_mode == Some(TYPED_ZONE_ROUTE_ENABLED) {
            return Ok(());
        }
        let Some(ext_fields) = request.get_ext_fields() else {
            return Ok(());
        };
        let zone_mode = ext_fields
            .get(mix_all::ZONE_MODE)
            .and_then(|value| value.parse::<bool>().ok())
            .unwrap_or(false);
        if !zone_mode {
            return Ok(());
        }

        let Some(zone_name) = ext_fields.get(mix_all::ZONE_NAME) else {
            return Ok(());
        };
        if zone_name.trim().is_empty() {
            return Ok(());
        }

        let hook_started = Instant::now();
        #[cfg(test)]
        ZONE_HOOK_DECODE_COUNT.fetch_add(1, Ordering::Relaxed);
        let Some(response_body) = response.get_body() else {
            return Ok(());
        };
        let original_route_data = TopicRouteData::decode(response_body)?;
        let mut topic_route_data = original_route_data.clone();
        filter_by_zone_name(&mut topic_route_data, zone_name);
        let body = topic_route_data.encode()?;
        if typed_mode == Some(TYPED_ZONE_ROUTE_SHADOW) {
            let typed_route = filter_route_by_zone(&original_route_data, &ZoneRequest::enabled(zone_name.clone()));
            let typed_body = typed_route.encode()?;
            if typed_body != body {
                warn!(
                    mode = "shadow",
                    reason = "encoded-route-mismatch",
                    legacy_bytes = body.len(),
                    typed_bytes = typed_body.len(),
                    "typed zone route differs from the legacy hook"
                );
            }
        }
        response.set_body_mut_ref(body);
        self.metrics
            .record_route_stage(NameServerRouteStage::LegacyZoneHook, hook_started.elapsed());
        Ok(())
    }
}

pub fn filter_by_zone_name(topic_route_data: &mut TopicRouteData, zone_name: &CheetahString) {
    if let std::borrow::Cow::Owned(filtered) =
        filter_route_by_zone(topic_route_data, &ZoneRequest::enabled(zone_name.clone()))
    {
        *topic_route_data = filtered;
    }
}

#[cfg(test)]
pub(crate) fn reset_zone_hook_decode_count() {
    ZONE_HOOK_DECODE_COUNT.store(0, Ordering::Relaxed);
}

#[cfg(test)]
pub(crate) fn zone_hook_decode_count() -> u64 {
    ZONE_HOOK_DECODE_COUNT.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::net::SocketAddr;

    use rocketmq_model::version::RocketMqVersion;
    use rocketmq_protocol::protocol::header::client_request_header::GetRouteInfoRequestHeader;
    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    use rocketmq_protocol::protocol::route::route_data_view::QueueData;

    use super::*;

    fn sample_topic_route_data() -> TopicRouteData {
        let mut kept_broker_addrs = HashMap::new();
        kept_broker_addrs.insert(10, CheetahString::from("10.0.0.10:10911"));
        kept_broker_addrs.insert(2, CheetahString::from("10.0.0.2:10911"));

        let mut removed_broker_addrs = HashMap::new();
        removed_broker_addrs.insert(MASTER_ID, CheetahString::from("10.0.1.1:10911"));

        let mut filter_server_table = HashMap::new();
        filter_server_table.insert(
            CheetahString::from("10.0.0.10:10911"),
            vec![CheetahString::from("fs-keep-a")],
        );
        filter_server_table.insert(
            CheetahString::from("10.0.1.1:10911"),
            vec![CheetahString::from("fs-remove-b")],
        );

        TopicRouteData {
            order_topic_conf: Some(CheetahString::from("order-conf")),
            queue_datas: vec![
                QueueData::new(CheetahString::from("broker-a"), 4, 4, 6, 0),
                QueueData::new(CheetahString::from("broker-b"), 8, 8, 6, 0),
            ],
            broker_datas: vec![
                BrokerData::new(
                    CheetahString::from("cluster-a"),
                    CheetahString::from("broker-a"),
                    kept_broker_addrs,
                    Some(CheetahString::from("zone-a")),
                ),
                BrokerData::new(
                    CheetahString::from("cluster-a"),
                    CheetahString::from("broker-b"),
                    removed_broker_addrs,
                    Some(CheetahString::from("zone-b")),
                ),
            ],
            filter_server_table,
            topic_queue_mapping_by_broker: None,
        }
    }

    fn zone_route_request(accept_standard_json_only: Option<bool>, version: i32) -> RemotingCommand {
        let mut request = RemotingCommand::create_request_command(
            RequestCode::GetRouteinfoByTopic,
            GetRouteInfoRequestHeader::new("TestTopic", accept_standard_json_only),
        )
        .set_version(version);
        request.make_custom_header_to_net();
        request
            .add_ext_field(mix_all::ZONE_MODE, "true")
            .add_ext_field(mix_all::ZONE_NAME, "zone-a");
        request
    }

    fn remote_addr() -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], 9876))
    }

    fn assert_hook_keeps_response_body(request: RemotingCommand, mut response: RemotingCommand) {
        let original_body = response.body().cloned();
        ZoneRouteRPCHook::default()
            .do_after_response(remote_addr(), &request, &mut response)
            .unwrap();
        assert_eq!(response.body(), original_body.as_ref());
    }

    #[test]
    fn do_after_response_matches_java_legacy_json_for_zone_requests() {
        let hook = ZoneRouteRPCHook::default();
        let request = zone_route_request(Some(true), RocketMqVersion::V4_9_3 as i32);
        let topic_route_data = sample_topic_route_data();
        let mut expected = topic_route_data.clone();
        let zone_name = CheetahString::from("zone-a");
        filter_by_zone_name(&mut expected, &zone_name);

        let mut response = RemotingCommand::create_response_command_with_code(ResponseCode::Success)
            .set_body(topic_route_data.encode_standard_json().unwrap());

        hook.do_after_response(remote_addr(), &request, &mut response).unwrap();

        let body = response.body().expect("zone hook should keep a response body");
        assert!(body.starts_with(br#"{"orderTopicConf""#));
        let decoded = TopicRouteData::decode(body).unwrap();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn do_after_response_keeps_legacy_json_for_legacy_requests_without_standard_flag() {
        let hook = ZoneRouteRPCHook::default();
        let request = zone_route_request(Some(false), RocketMqVersion::V4_9_3 as i32);
        let topic_route_data = sample_topic_route_data();
        let mut expected = topic_route_data.clone();
        let zone_name = CheetahString::from("zone-a");
        filter_by_zone_name(&mut expected, &zone_name);

        let mut response = RemotingCommand::create_response_command_with_code(ResponseCode::Success)
            .set_body(topic_route_data.encode().unwrap());

        hook.do_after_response(remote_addr(), &request, &mut response).unwrap();

        let decoded = TopicRouteData::decode(response.body().expect("legacy response should keep a body")).unwrap();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn do_after_response_leaves_body_for_non_route_requests() {
        let request = RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo);
        let response = RemotingCommand::create_response_command_with_code(ResponseCode::Success)
            .set_body(sample_topic_route_data().encode().unwrap());

        assert_hook_keeps_response_body(request, response);
    }

    #[test]
    fn do_after_response_leaves_body_for_non_success_responses() {
        let request = zone_route_request(Some(false), RocketMqVersion::V4_9_3 as i32);
        let response = RemotingCommand::create_response_command_with_code(ResponseCode::SystemError)
            .set_body(sample_topic_route_data().encode().unwrap());

        assert_hook_keeps_response_body(request, response);
    }

    #[test]
    fn do_after_response_leaves_body_when_zone_mode_is_missing_or_false() {
        let mut missing_zone_mode = RemotingCommand::create_request_command(
            RequestCode::GetRouteinfoByTopic,
            GetRouteInfoRequestHeader::new("TestTopic", None),
        )
        .set_version(RocketMqVersion::V4_9_3 as i32);
        missing_zone_mode.make_custom_header_to_net();
        missing_zone_mode.add_ext_field(mix_all::ZONE_NAME, "zone-a");
        let response = RemotingCommand::create_response_command_with_code(ResponseCode::Success)
            .set_body(sample_topic_route_data().encode().unwrap());
        assert_hook_keeps_response_body(missing_zone_mode, response);

        let mut disabled_zone_mode = zone_route_request(Some(false), RocketMqVersion::V4_9_3 as i32);
        disabled_zone_mode.add_ext_field(mix_all::ZONE_MODE, "false");
        let response = RemotingCommand::create_response_command_with_code(ResponseCode::Success)
            .set_body(sample_topic_route_data().encode().unwrap());
        assert_hook_keeps_response_body(disabled_zone_mode, response);
    }

    #[test]
    fn do_after_response_leaves_body_when_zone_name_is_missing_or_empty() {
        let mut missing_zone_name = RemotingCommand::create_request_command(
            RequestCode::GetRouteinfoByTopic,
            GetRouteInfoRequestHeader::new("TestTopic", None),
        )
        .set_version(RocketMqVersion::V4_9_3 as i32);
        missing_zone_name.make_custom_header_to_net();
        missing_zone_name.add_ext_field(mix_all::ZONE_MODE, "true");
        let response = RemotingCommand::create_response_command_with_code(ResponseCode::Success)
            .set_body(sample_topic_route_data().encode().unwrap());
        assert_hook_keeps_response_body(missing_zone_name, response);

        let mut empty_zone_name = zone_route_request(Some(false), RocketMqVersion::V4_9_3 as i32);
        empty_zone_name.add_ext_field(mix_all::ZONE_NAME, "");
        let response = RemotingCommand::create_response_command_with_code(ResponseCode::Success)
            .set_body(sample_topic_route_data().encode().unwrap());
        assert_hook_keeps_response_body(empty_zone_name, response);
    }

    #[test]
    fn filter_by_zone_name_matches_zone_case_insensitively() {
        let mut topic_route_data = sample_topic_route_data();
        topic_route_data.broker_datas[0]
            .broker_addrs_mut()
            .insert(MASTER_ID, CheetahString::from("10.0.0.1:10911"));

        filter_by_zone_name(&mut topic_route_data, &CheetahString::from("ZONE-A"));

        assert_eq!(topic_route_data.broker_datas.len(), 1);
        assert_eq!(topic_route_data.broker_datas[0].broker_name(), "broker-a");
        assert_eq!(topic_route_data.queue_datas.len(), 1);
        assert_eq!(topic_route_data.queue_datas[0].broker_name, "broker-a");
    }
}
