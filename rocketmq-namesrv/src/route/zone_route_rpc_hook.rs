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
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::route::route_data_view::BrokerData;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_remoting::protocol::RemotingDeserializable;
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
        response.set_body_mut_ref(topic_route_data.encode()?);
        Ok(())
    }
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
