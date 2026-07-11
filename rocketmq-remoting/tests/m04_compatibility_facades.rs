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

use std::collections::HashMap;
use std::sync::Arc;

use bytes::BytesMut;
use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_remoting::protocol::body::subscription_group_wrapper::SubscriptionGroupWrapper;
use rocketmq_remoting::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper;
use rocketmq_remoting::protocol::header::ack_message_request_header::AckMessageRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand as DeepRemotingCommand;
use rocketmq_remoting::protocol::static_topic::logic_queue_mapping_item::LogicQueueMappingItem;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;
use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_remoting::protocol::SerializeType;
use rocketmq_remoting::rpc::rpc_response::RpcResponse;
use rocketmq_remoting::RemotingCommand;
use rocketmq_rust::ArcMut;

type LegacySubscriptionGroupTable = DashMap<CheetahString, Arc<SubscriptionGroupConfig>>;
type LegacyForbiddenTable = DashMap<CheetahString, HashMap<CheetahString, i32>>;

fn accepts_deep(command: DeepRemotingCommand) -> DeepRemotingCommand {
    command
}

fn accepts_root(command: RemotingCommand) -> RemotingCommand {
    command
}

#[test]
fn remoting_command_root_and_deep_paths_are_one_complete_type() {
    let mut command = accepts_root(accepts_deep(RemotingCommand::create_remoting_command(10)));
    let _: &RemotingCommand = command.as_ref();
    let _: &mut RemotingCommand = command.as_mut();
    let _ = RemotingCommand::create_new_request_id();
    let _ = RemotingCommand::mark_serialize_type(1, SerializeType::JSON);
    let encoded = serde_json::to_vec(&command).unwrap();
    let mut header = BytesMut::from(encoded.as_slice());
    let _ = RemotingCommand::header_decode(&mut header, encoded.len(), SerializeType::JSON).unwrap();

    let mut incomplete = BytesMut::new();
    assert!(RemotingCommand::decode(&mut incomplete).unwrap().is_none());
}

#[test]
fn owner_factories_return_the_canonical_command_type() {
    let command: DeepRemotingCommand = rocketmq_remoting::protocol::remoting_command_facade::create_remoting_command(7);
    assert_eq!(command.code(), 7);
    assert!(command.version() > 0);
}

#[test]
fn legacy_subscription_wrapper_keeps_dashmap_arc_mutation_semantics() {
    let _: fn() -> SubscriptionGroupWrapper = SubscriptionGroupWrapper::new;
    let _: fn(&mut SubscriptionGroupWrapper, LegacySubscriptionGroupTable) =
        SubscriptionGroupWrapper::set_subscription_group_table;
    let _: for<'a> fn(&'a SubscriptionGroupWrapper) -> &'a LegacyForbiddenTable =
        SubscriptionGroupWrapper::forbidden_table;
    let _: fn(&mut SubscriptionGroupWrapper, LegacyForbiddenTable) = SubscriptionGroupWrapper::set_forbidden_table;
    let _: fn(&mut SubscriptionGroupWrapper, rocketmq_remoting::protocol::DataVersion) =
        SubscriptionGroupWrapper::set_data_version;

    let table: LegacySubscriptionGroupTable = DashMap::new();
    table.insert("group".into(), Arc::new(SubscriptionGroupConfig::default()));
    let forbidden_table: LegacyForbiddenTable = DashMap::new();
    forbidden_table.insert("group".into(), HashMap::from([("topic".into(), 1)]));
    let data_version = rocketmq_remoting::protocol::DataVersion::with_values(11, 22, 33);

    let mut wrapper = SubscriptionGroupWrapper::new();
    wrapper.set_subscription_group_table(table);
    wrapper.set_forbidden_table(forbidden_table);
    wrapper.set_data_version(data_version.clone());

    assert!(wrapper.subscription_group_table.get("group").is_some());
    assert_eq!(wrapper.forbidden_table().get("group").unwrap().get("topic"), Some(&1));
    assert_eq!(wrapper.data_version(), &data_version);
    let canonical: rocketmq_protocol::protocol::body::subscription_group_wrapper::SubscriptionGroupWrapper =
        (&wrapper).into();
    let round_trip = SubscriptionGroupWrapper::from(canonical);
    assert!(round_trip.subscription_group_table.get("group").is_some());
    assert_eq!(
        round_trip.forbidden_table().get("group").unwrap().get("topic"),
        Some(&1)
    );
    assert_eq!(round_trip.data_version(), &data_version);
}

#[test]
fn legacy_mapping_wrapper_keeps_dashmap_arcmut_fields() {
    let wrapper = TopicConfigAndMappingSerializeWrapper::default();
    wrapper
        .topic_queue_mapping_detail_map
        .insert("topic".into(), ArcMut::new(TopicQueueMappingDetail::default()));
    assert!(wrapper.topic_queue_mapping_detail_map.get("topic").is_some());

    let canonical: rocketmq_protocol::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper =
        (&wrapper).into();
    let round_trip = TopicConfigAndMappingSerializeWrapper::from(canonical);
    assert!(round_trip.topic_queue_mapping_detail_map.get("topic").is_some());
}

#[test]
#[allow(deprecated)]
fn legacy_static_topic_adapter_mutates_arcmut_detail() {
    let detail = ArcMut::new(TopicQueueMappingDetail::default());
    rocketmq_remoting::protocol::static_topic::put_mapping_info(
        detail.clone(),
        3,
        vec![LogicQueueMappingItem::default()],
    );

    assert!(TopicQueueMappingDetail::get_mapping_info(detail.as_ref(), 3).is_some());
}

#[test]
fn legacy_rpc_response_keeps_arcmut_header_storage_and_converts() {
    let response = RpcResponse::default();
    let _: &Option<ArcMut<Box<dyn rocketmq_remoting::CommandCustomHeader + Send + Sync>>> = &response.header;
    let canonical: rocketmq_protocol::rpc::rpc_response::RpcResponse = match response.try_into_canonical() {
        Ok(response) => response,
        Err(_) => panic!("unshared legacy response should convert"),
    };
    let _legacy = RpcResponse::from(canonical);
}

#[test]
#[allow(deprecated)]
fn legacy_rpc_shared_reference_mutation_facade_has_exact_safe_signature() {
    let _: for<'a> fn(&'a RpcResponse) -> Option<&'a mut AckMessageRequestHeader> =
        RpcResponse::get_header_mut_from_ref::<AckMessageRequestHeader>;

    assert!(RpcResponse::default()
        .get_header_mut_from_ref::<AckMessageRequestHeader>()
        .is_none());
}

#[test]
fn data_version_owner_default_uses_wall_clock_and_advances() {
    use rocketmq_remoting::protocol::data_version_facade::DataVersionExt;

    let mut version = rocketmq_remoting::protocol::data_version_facade::new_data_version();
    let initial_timestamp = version.get_timestamp();
    assert!(initial_timestamp > 0);
    let initial_counter = version.get_counter();
    std::thread::sleep(std::time::Duration::from_millis(2));
    version.next_version();
    assert!(version.get_timestamp() > initial_timestamp);
    assert_eq!(version.get_counter(), initial_counter + 1);
}

#[test]
fn owner_defaults_supply_current_timestamps() {
    let elect = rocketmq_remoting::protocol::header_facade::default_elect_master_request_header();
    assert!(elect.invoke_time > 0);
    let subscription = rocketmq_remoting::protocol::heartbeat_facade::default_subscription_data();
    assert!(subscription.sub_version > 0);
}

#[test]
fn route_slave_selection_uses_caller_supplied_choice() {
    use rocketmq_remoting::protocol::route::route_data_view::BrokerData;

    let broker = BrokerData::new(
        "cluster".into(),
        "broker".into(),
        HashMap::from([(2, "slave-2".into()), (7, "slave-7".into())]),
        None,
    );
    assert_eq!(broker.select_broker_addr_with_index(0).as_deref(), Some("slave-2"));
    assert_eq!(broker.select_broker_addr_with_index(1).as_deref(), Some("slave-7"));
}

#[test]
fn owner_route_adapter_does_not_collapse_to_lowest_slave_id() {
    use rocketmq_remoting::protocol::route::route_data_view::BrokerData;

    let broker = BrokerData::new(
        "cluster".into(),
        "broker".into(),
        HashMap::from([(2, "slave-2".into()), (7, "slave-7".into())]),
        None,
    );
    let selected = (0..64)
        .filter_map(|_| rocketmq_remoting::protocol::route_facade::select_broker_addr(&broker))
        .collect::<std::collections::HashSet<_>>();

    assert!(selected.contains("slave-2"));
    assert!(selected.contains("slave-7"));
}
