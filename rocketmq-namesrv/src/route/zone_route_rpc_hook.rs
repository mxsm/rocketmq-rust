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
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mix_all::MASTER_ID;
use rocketmq_common::common::mq_version::RocketMqVersion;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::header::client_request_header::GetRouteInfoRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::route::route_data_view::BrokerData;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::runtime::RPCHook;

pub struct ZoneRouteRPCHook;

impl RPCHook for ZoneRouteRPCHook {
    #[inline(always)]
    fn do_before_request(
        &self,
        _remote_addr: std::net::SocketAddr,
        _request: &mut rocketmq_remoting::protocol::remoting_command::RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<()> {
        Ok(())
    }

    fn do_after_response(
        &self,
        _remote_addr: std::net::SocketAddr,
        request: &rocketmq_remoting::protocol::remoting_command::RemotingCommand,
        response: &mut rocketmq_remoting::protocol::remoting_command::RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<()> {
        if RequestCode::GetRouteinfoByTopic as i32 != request.code() {
            return Ok(());
        }
        if response.get_body().is_none() || ResponseCode::Success as i32 != response.code() {
            return Ok(());
        }
        let zone_mode = if let Some(ext) = request.get_ext_fields() {
            ext.get(mix_all::ZONE_MODE)
                .unwrap_or(&"false".into())
                .parse::<bool>()
                .unwrap_or(false)
        } else {
            return Ok(());
        };
        if !zone_mode {
            return Ok(());
        }

        let zone_name = request.get_ext_fields().unwrap().get(mix_all::ZONE_NAME);
        if zone_name.is_none() {
            return Ok(());
        }
        let zone_name = zone_name.unwrap();
        if zone_name.is_empty() {
            return Ok(());
        }

        let mut topic_route_data = TopicRouteData::decode(response.get_body().unwrap())?;
        filter_by_zone_name(&mut topic_route_data, zone_name);
        let body = if should_use_standard_json_route_response(request) {
            topic_route_data.encode_standard_json()?
        } else {
            topic_route_data.encode()?
        };
        response.set_body_mut_ref(body);
        Ok(())
    }
}

fn should_use_standard_json_route_response(request: &RemotingCommand) -> bool {
    request.version() >= RocketMqVersion::V4_9_4 as i32 || accept_standard_json_only(request)
}

fn accept_standard_json_only(request: &RemotingCommand) -> bool {
    if let Some(header) = request.read_custom_header_ref::<GetRouteInfoRequestHeader>() {
        if header.accept_standard_json_only.unwrap_or(false) {
            return true;
        }
    }

    request
        .get_ext_fields()
        .and_then(|ext_fields| ext_fields.get("acceptStandardJsonOnly"))
        .and_then(|value| value.parse::<bool>().ok())
        .unwrap_or(false)
}

pub fn filter_by_zone_name(topic_route_data: &mut TopicRouteData, zone_name: &CheetahString) {
    use std::collections::HashMap;

    let mut broker_data_reserved = Vec::new();
    let mut broker_data_removed: HashMap<CheetahString, BrokerData> = HashMap::new();

    for bd in topic_route_data.broker_datas.iter() {
        let addrs = bd.broker_addrs();

        // master down => keep (consume from slave)
        let master_down = !addrs.contains_key(&MASTER_ID);

        let same_zone = bd
            .zone_name()
            .map(|z| z.eq_ignore_ascii_case(zone_name))
            .unwrap_or(false);

        if master_down || same_zone {
            broker_data_reserved.push(bd.clone());
        } else {
            broker_data_removed.insert(bd.broker_name().clone(), bd.clone());
        }
    }

    topic_route_data.broker_datas = broker_data_reserved;

    // Filter queue data
    let mut queue_data_reserved = Vec::new();
    for qd in topic_route_data.queue_datas.iter() {
        if !broker_data_removed.contains_key(&qd.broker_name) {
            queue_data_reserved.push(qd.clone());
        }
    }
    topic_route_data.queue_datas = queue_data_reserved;

    // Remove filter server entries whose broker addresses belong to removed brokers

    if !topic_route_data.filter_server_table.is_empty() {
        for (_, bd) in broker_data_removed.iter() {
            for addr in bd.broker_addrs().values() {
                topic_route_data.filter_server_table.remove(addr);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::net::SocketAddr;

    use rocketmq_common::common::mq_version::RocketMqVersion;
    use rocketmq_remoting::protocol::header::client_request_header::GetRouteInfoRequestHeader;
    use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
    use rocketmq_remoting::protocol::route::route_data_view::QueueData;

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

    #[test]
    fn do_after_response_preserves_standard_json_for_accept_standard_json_only_requests() {
        let hook = ZoneRouteRPCHook;
        let request = zone_route_request(Some(true), RocketMqVersion::V4_9_3 as i32);
        let topic_route_data = sample_topic_route_data();
        let mut expected = topic_route_data.clone();
        let zone_name = CheetahString::from("zone-a");
        filter_by_zone_name(&mut expected, &zone_name);

        let mut response = RemotingCommand::create_response_command_with_code(ResponseCode::Success)
            .set_body(topic_route_data.encode_standard_json().unwrap());

        hook.do_after_response(remote_addr(), &request, &mut response).unwrap();

        let body = response.body().expect("zone hook should keep a response body");
        let body = std::str::from_utf8(body).expect("route body should stay utf-8 json");
        let broker_addrs_index = body
            .find("\"brokerAddrs\":{\"10\"")
            .expect("standard json should sort brokerAddrs");
        let broker_addrs_second_index = body
            .find("\"2\":\"10.0.0.2:10911\"")
            .expect("standard json should include broker id 2");
        assert!(broker_addrs_index < broker_addrs_second_index);

        let decoded = TopicRouteData::decode(body.as_bytes()).unwrap();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn do_after_response_keeps_legacy_json_for_legacy_requests_without_standard_flag() {
        let hook = ZoneRouteRPCHook;
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
}
