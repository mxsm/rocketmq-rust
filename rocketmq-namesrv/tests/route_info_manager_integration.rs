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

#![recursion_limit = "512"]

use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mq_version::RocketMqVersion;
use rocketmq_common::common::namesrv::namesrv_config::NamesrvConfig;
use rocketmq_common::common::server::config::ServerConfig;
use rocketmq_common::utils::crc32_utils;
use rocketmq_error::RocketMQResult;
use rocketmq_namesrv::bootstrap::Builder;
use rocketmq_remoting::clients::rocketmq_tokio_client::RocketmqDefaultClient;
use rocketmq_remoting::clients::RemotingClient;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::body::broker_body::register_broker_body::RegisterBrokerBody;
use rocketmq_remoting::protocol::body::kv_table::KVTable;
use rocketmq_remoting::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper;
use rocketmq_remoting::protocol::header::client_request_header::GetRouteInfoRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::broker_request::UnRegisterBrokerRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::config_header::GetNamesrvConfigRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::kv_config_header::DeleteKVConfigRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::kv_config_header::GetKVConfigRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::kv_config_header::GetKVConfigResponseHeader;
use rocketmq_remoting::protocol::header::namesrv::kv_config_header::GetKVListByNamespaceRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::kv_config_header::PutKVConfigRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::register_broker_header::RegisterBrokerRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::register_broker_header::RegisterBrokerResponseHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_remoting::protocol::RemotingDeserializable;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::remoting::RemotingService;
use rocketmq_remoting::request_processor::default_request_processor::DefaultRemotingRequestProcessor;
use rocketmq_remoting::runtime::config::client_config::TokioClientConfig;
use rocketmq_rust::ArcMut;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::Instant;

const REQUEST_TIMEOUT_MILLIS: u64 = 3_000;
const MASTER_ID: u64 = 0;
const ORDER_TOPIC_NAMESPACE: &str = "ORDER_TOPIC_CONFIG";
const STARTUP_RETRY_LIMIT: usize = 3;

struct NamesrvHarness {
    addr: CheetahString,
    client: ArcMut<RocketmqDefaultClient<DefaultRemotingRequestProcessor>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    server_task: JoinHandle<RocketMQResult<()>>,
}

impl NamesrvHarness {
    async fn start(mut namesrv_config: NamesrvConfig) -> Self {
        let mut last_error = None;

        for _attempt in 0..STARTUP_RETRY_LIMIT {
            let port = reserve_local_port();
            let addr = CheetahString::from_string(format!("127.0.0.1:{port}"));
            let data_dir = isolated_namesrv_data_dir(port);
            namesrv_config.kv_config_path = data_dir.join("kvConfig.json").display().to_string();
            namesrv_config.config_store_path = data_dir.join("rocketmq-namesrv.properties").display().to_string();
            let server_config = ServerConfig {
                listen_port: port as u32,
                bind_address: "127.0.0.1".to_string(),
            };
            let bootstrap = Builder::new()
                .set_name_server_config(namesrv_config.clone())
                .set_server_config(server_config)
                .build();

            let (shutdown_tx, shutdown_rx) = oneshot::channel();
            let mut server_task = tokio::spawn(async move {
                bootstrap
                    .boot_with_shutdown(async move {
                        let _ = shutdown_rx.await;
                    })
                    .await
            });

            let client = ArcMut::new(RocketmqDefaultClient::new(
                Arc::new(TokioClientConfig::default()),
                DefaultRemotingRequestProcessor,
            ));
            client.update_name_server_address_list(vec![addr.clone()]).await;
            let weak_client = ArcMut::downgrade(&client);
            client.start(weak_client).await;

            match wait_until_ready(&addr, &client, &mut server_task).await {
                Ok(()) => {
                    return Self {
                        addr,
                        client,
                        shutdown_tx: Some(shutdown_tx),
                        server_task,
                    };
                }
                Err(error) => {
                    last_error = Some(error);
                    let _ = shutdown_tx.send(());
                    client.mut_from_ref().shutdown();
                    let _ = tokio::time::timeout(Duration::from_secs(1), &mut server_task).await;
                }
            }
        }

        panic!(
            "namesrv failed to start after {} attempts: {}",
            STARTUP_RETRY_LIMIT,
            last_error.unwrap_or_else(|| "unknown startup error".to_string())
        );
    }

    async fn request(&self, request: RemotingCommand) -> RocketMQResult<RemotingCommand> {
        self.client
            .invoke_request(Some(&self.addr), request, REQUEST_TIMEOUT_MILLIS)
            .await
    }

    async fn shutdown(mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        self.client.mut_from_ref().shutdown();

        let join_result = tokio::time::timeout(Duration::from_secs(10), &mut self.server_task)
            .await
            .expect("namesrv task should stop before timeout")
            .expect("namesrv task should not panic");
        join_result.expect("namesrv task should exit cleanly");
    }
}

impl Drop for NamesrvHarness {
    fn drop(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        self.client.mut_from_ref().shutdown();
    }
}

fn reserve_local_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("should reserve a local port")
        .local_addr()
        .expect("reserved listener should expose a local addr")
        .port()
}

fn isolated_namesrv_data_dir(port: u16) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("rocketmq-namesrv-test-{}-{}", std::process::id(), port));
    std::fs::create_dir_all(&dir).expect("namesrv integration test should create an isolated data dir");
    dir
}

async fn wait_until_ready(
    addr: &CheetahString,
    client: &ArcMut<RocketmqDefaultClient<DefaultRemotingRequestProcessor>>,
    server_task: &mut JoinHandle<RocketMQResult<()>>,
) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(10);

    loop {
        if server_task.is_finished() {
            return Err(describe_finished_server_task(server_task).await);
        }

        let mut request = RemotingCommand::create_request_command(
            RequestCode::GetNamesrvConfig,
            GetNamesrvConfigRequestHeader::default(),
        );
        request.make_custom_header_to_net();

        let failure = match client.invoke_request(Some(addr), request, REQUEST_TIMEOUT_MILLIS).await {
            Ok(response) if ResponseCode::from(response.code()) == ResponseCode::Success => return Ok(()),
            Ok(response) => format!(
                "unexpected response code {:?}, remark {:?}",
                ResponseCode::from(response.code()),
                response.remark()
            ),
            Err(error) => error.to_string(),
        };

        if Instant::now() >= deadline {
            return Err(format!("namesrv did not become ready in time: {}", failure));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn describe_finished_server_task(server_task: &mut JoinHandle<RocketMQResult<()>>) -> String {
    match server_task.await {
        Ok(Ok(())) => "server task exited before readiness probe without an error".to_string(),
        Ok(Err(error)) => format!("server task exited before readiness probe: {}", error),
        Err(error) => format!("server task panicked before readiness probe: {}", error),
    }
}

fn default_v2_namesrv_config() -> NamesrvConfig {
    NamesrvConfig {
        use_route_info_manager_v2: true,
        ..NamesrvConfig::default()
    }
}

fn route_request(topic: &CheetahString) -> RemotingCommand {
    route_request_with_options(topic, Some(false), RocketMqVersion::V4_9_3 as i32, None)
}

fn route_request_with_options(
    topic: &CheetahString,
    accept_standard_json_only: Option<bool>,
    version: i32,
    zone_name: Option<&CheetahString>,
) -> RemotingCommand {
    let mut request = RemotingCommand::create_request_command(
        RequestCode::GetRouteinfoByTopic,
        GetRouteInfoRequestHeader::new(topic.clone(), accept_standard_json_only),
    )
    .set_version(version);
    request.make_custom_header_to_net();
    if let Some(zone_name) = zone_name {
        request
            .add_ext_field(mix_all::ZONE_MODE, "true")
            .add_ext_field(mix_all::ZONE_NAME, zone_name.clone());
    }
    request
}

fn register_broker_request(
    cluster_name: &CheetahString,
    broker_name: &CheetahString,
    broker_addr: &CheetahString,
    topic_name: &CheetahString,
) -> RemotingCommand {
    let topics = [topic_name];
    register_broker_request_with_options(
        cluster_name,
        broker_name,
        broker_addr,
        MASTER_ID,
        &topics,
        None,
        Vec::new(),
    )
}

fn register_broker_request_with_options(
    cluster_name: &CheetahString,
    broker_name: &CheetahString,
    broker_addr: &CheetahString,
    broker_id: u64,
    topic_names: &[&CheetahString],
    zone_name: Option<&CheetahString>,
    filter_server_list: Vec<CheetahString>,
) -> RemotingCommand {
    let mut topic_config_wrapper = TopicConfigAndMappingSerializeWrapper::default();
    for topic_name in topic_names {
        topic_config_wrapper
            .topic_config_serialize_wrapper
            .topic_config_table
            .insert(
                (*topic_name).clone(),
                TopicConfig::with_perm((*topic_name).clone(), 4, 4, 6),
            );
    }

    let body = RegisterBrokerBody::new(topic_config_wrapper, filter_server_list).encode(false);
    let mut request = RemotingCommand::create_request_command(
        RequestCode::RegisterBroker,
        RegisterBrokerRequestHeader::new(
            broker_name.clone(),
            broker_addr.clone(),
            cluster_name.clone(),
            broker_addr.clone(),
            broker_id,
            Some(60_000),
            Some(false),
            false,
            crc32_utils::crc32(body.as_slice()),
        ),
    )
    .set_version(RocketMqVersion::V5_0_0 as i32)
    .set_body(body);
    request.make_custom_header_to_net();
    if let Some(zone_name) = zone_name {
        request.add_ext_field(mix_all::ZONE_NAME, zone_name.clone());
    }
    request
}

fn unregister_broker_request(
    cluster_name: &CheetahString,
    broker_name: &CheetahString,
    broker_addr: &CheetahString,
) -> RemotingCommand {
    let mut request = RemotingCommand::create_request_command(
        RequestCode::UnregisterBroker,
        UnRegisterBrokerRequestHeader {
            cluster_name: cluster_name.clone(),
            broker_addr: broker_addr.clone(),
            broker_name: broker_name.clone(),
            broker_id: MASTER_ID,
        },
    );
    request.make_custom_header_to_net();
    request
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn namesrv_returns_java_style_config_properties_over_remoting() {
    let harness = NamesrvHarness::start(default_v2_namesrv_config()).await;

    let mut request = RemotingCommand::create_request_command(
        RequestCode::GetNamesrvConfig,
        GetNamesrvConfigRequestHeader::default(),
    );
    request.make_custom_header_to_net();
    let response = harness.request(request).await.unwrap();

    assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
    let body = response.body().expect("config response should include a body");
    let properties = std::str::from_utf8(body).expect("config response should be valid utf-8");
    assert!(properties.contains("listenPort="));
    assert!(properties.contains("bindAddress=127.0.0.1"));
    assert!(properties.contains("useRouteInfoManagerV2=true"));

    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn namesrv_kvconfig_roundtrip_works_over_remoting() {
    let harness = NamesrvHarness::start(default_v2_namesrv_config()).await;
    let namespace = CheetahString::from_static_str("phase5-network-namespace");
    let key = CheetahString::from_static_str("phase5-network-key");
    let value = CheetahString::from_static_str("phase5-network-value");

    let mut put_request = RemotingCommand::create_request_command(
        RequestCode::PutKvConfig,
        PutKVConfigRequestHeader::new(namespace.clone(), key.clone(), value.clone()),
    );
    put_request.make_custom_header_to_net();
    let put_response = harness.request(put_request).await.unwrap();
    assert_eq!(ResponseCode::from(put_response.code()), ResponseCode::Success);

    let mut get_request = RemotingCommand::create_request_command(
        RequestCode::GetKvConfig,
        GetKVConfigRequestHeader::new(namespace.clone(), key.clone()),
    );
    get_request.make_custom_header_to_net();
    let get_response = harness.request(get_request).await.unwrap();
    assert_eq!(ResponseCode::from(get_response.code()), ResponseCode::Success);
    let get_header = get_response
        .decode_command_custom_header::<GetKVConfigResponseHeader>()
        .expect("get kv config should decode a response header");
    assert_eq!(get_header.value, Some(value.clone()));

    let mut list_request = RemotingCommand::create_request_command(
        RequestCode::GetKvlistByNamespace,
        GetKVListByNamespaceRequestHeader::new(namespace.clone()),
    );
    list_request.make_custom_header_to_net();
    let list_response = harness.request(list_request).await.unwrap();
    assert_eq!(ResponseCode::from(list_response.code()), ResponseCode::Success);
    let kv_table = KVTable::decode(list_response.body().expect("list response should include a body")).unwrap();
    assert_eq!(kv_table.table.get(&key), Some(&value));

    let mut delete_request = RemotingCommand::create_request_command(
        RequestCode::DeleteKvConfig,
        DeleteKVConfigRequestHeader::new(namespace.clone(), key.clone()),
    );
    delete_request.make_custom_header_to_net();
    let delete_response = harness.request(delete_request).await.unwrap();
    assert_eq!(ResponseCode::from(delete_response.code()), ResponseCode::Success);

    let mut get_after_delete_request = RemotingCommand::create_request_command(
        RequestCode::GetKvConfig,
        GetKVConfigRequestHeader::new(namespace, key),
    );
    get_after_delete_request.make_custom_header_to_net();
    let get_after_delete_response = harness.request(get_after_delete_request).await.unwrap();
    assert_eq!(
        ResponseCode::from(get_after_delete_response.code()),
        ResponseCode::QueryNotFound
    );

    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn namesrv_register_query_unregister_route_roundtrip_works_over_remoting() {
    let harness = NamesrvHarness::start(NamesrvConfig {
        use_route_info_manager_v2: true,
        order_message_enable: true,
        ..NamesrvConfig::default()
    })
    .await;

    let cluster_name = CheetahString::from_static_str("phase5-network-cluster");
    let broker_name = CheetahString::from_static_str("phase5-network-broker");
    let broker_addr = CheetahString::from_static_str("10.10.10.10:10911");
    let topic_name = CheetahString::from_static_str("phase5-network-route-topic");
    let order_conf = CheetahString::from_static_str("phase5-network-broker:4");

    let mut put_order_request = RemotingCommand::create_request_command(
        RequestCode::PutKvConfig,
        PutKVConfigRequestHeader::new(
            CheetahString::from_static_str(ORDER_TOPIC_NAMESPACE),
            topic_name.clone(),
            order_conf.clone(),
        ),
    );
    put_order_request.make_custom_header_to_net();
    let put_order_response = harness.request(put_order_request).await.unwrap();
    assert_eq!(ResponseCode::from(put_order_response.code()), ResponseCode::Success);

    let register_response = harness
        .request(register_broker_request(
            &cluster_name,
            &broker_name,
            &broker_addr,
            &topic_name,
        ))
        .await
        .unwrap();
    assert_eq!(ResponseCode::from(register_response.code()), ResponseCode::Success);
    register_response
        .decode_command_custom_header::<RegisterBrokerResponseHeader>()
        .expect("register response should decode");

    let route_response = harness.request(route_request(&topic_name)).await.unwrap();
    assert_eq!(ResponseCode::from(route_response.code()), ResponseCode::Success);
    let topic_route_data =
        TopicRouteData::decode(route_response.body().expect("route response should include a body")).unwrap();
    assert_eq!(topic_route_data.order_topic_conf.as_ref(), Some(&order_conf));
    assert_eq!(topic_route_data.queue_datas.len(), 1);
    assert_eq!(topic_route_data.broker_datas.len(), 1);
    assert_eq!(topic_route_data.queue_datas[0].broker_name(), &broker_name);
    assert_eq!(
        topic_route_data.broker_datas[0].broker_addrs().get(&MASTER_ID),
        Some(&broker_addr)
    );

    let unregister_response = harness
        .request(unregister_broker_request(&cluster_name, &broker_name, &broker_addr))
        .await
        .unwrap();
    assert_eq!(ResponseCode::from(unregister_response.code()), ResponseCode::Success);

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let route_after_unregister = harness.request(route_request(&topic_name)).await.unwrap();
        match ResponseCode::from(route_after_unregister.code()) {
            ResponseCode::TopicNotExist => break,
            ResponseCode::Success if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            code => panic!("expected TopicNotExist after unregister, got {code:?}"),
        }
    }

    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn namesrv_zone_route_filters_removed_zone_and_keeps_master_down_broker_over_remoting() {
    let harness = NamesrvHarness::start(default_v2_namesrv_config()).await;

    let cluster_name = CheetahString::from_static_str("phase5-zone-cluster");
    let topic_name = CheetahString::from_static_str("phase5-zone-topic");
    let requested_zone = CheetahString::from_static_str("zone-a");
    let zone_a = requested_zone.clone();
    let zone_b = CheetahString::from_static_str("zone-b");
    let zone_c = CheetahString::from_static_str("zone-c");

    let broker_a_name = CheetahString::from_static_str("zone-a-master");
    let broker_a_addr = CheetahString::from_static_str("10.20.0.1:10911");
    let broker_b_name = CheetahString::from_static_str("zone-b-master");
    let broker_b_addr = CheetahString::from_static_str("10.20.0.2:10911");
    let broker_c_name = CheetahString::from_static_str("zone-c-slave-only");
    let broker_c_addr = CheetahString::from_static_str("10.20.0.3:10911");

    let register_specs = [
        (
            &broker_a_name,
            &broker_a_addr,
            MASTER_ID,
            &zone_a,
            vec![CheetahString::from_static_str("fs-a")],
        ),
        (
            &broker_b_name,
            &broker_b_addr,
            MASTER_ID,
            &zone_b,
            vec![CheetahString::from_static_str("fs-b")],
        ),
        (
            &broker_c_name,
            &broker_c_addr,
            2,
            &zone_c,
            vec![CheetahString::from_static_str("fs-c")],
        ),
    ];

    for (broker_name, broker_addr, broker_id, zone_name, filter_servers) in register_specs {
        let response = harness
            .request(register_broker_request_with_options(
                &cluster_name,
                broker_name,
                broker_addr,
                broker_id,
                &[&topic_name],
                Some(zone_name),
                filter_servers,
            ))
            .await
            .unwrap();
        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
    }

    let route_response = harness
        .request(route_request_with_options(
            &topic_name,
            Some(false),
            RocketMqVersion::V4_9_3 as i32,
            Some(&requested_zone),
        ))
        .await
        .unwrap();
    assert_eq!(ResponseCode::from(route_response.code()), ResponseCode::Success);

    let topic_route_data = TopicRouteData::decode(
        route_response
            .body()
            .expect("zone route response should include a body"),
    )
    .unwrap();

    let reserved_broker_names = topic_route_data
        .broker_datas
        .iter()
        .map(|broker| broker.broker_name().clone())
        .collect::<Vec<_>>();
    assert!(reserved_broker_names.contains(&broker_a_name));
    assert!(reserved_broker_names.contains(&broker_c_name));
    assert!(!reserved_broker_names.contains(&broker_b_name));

    let reserved_queue_names = topic_route_data
        .queue_datas
        .iter()
        .map(|queue| queue.broker_name().clone())
        .collect::<Vec<_>>();
    assert!(reserved_queue_names.contains(&broker_a_name));
    assert!(reserved_queue_names.contains(&broker_c_name));
    assert!(!reserved_queue_names.contains(&broker_b_name));

    assert_eq!(
        topic_route_data.filter_server_table.get(&broker_a_addr),
        Some(&vec![CheetahString::from_static_str("fs-a")])
    );
    assert_eq!(
        topic_route_data.filter_server_table.get(&broker_c_addr),
        Some(&vec![CheetahString::from_static_str("fs-c")])
    );
    assert!(!topic_route_data.filter_server_table.contains_key(&broker_b_addr));

    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn namesrv_zone_route_accept_standard_json_only_preserves_standard_json_over_remoting() {
    let harness = NamesrvHarness::start(default_v2_namesrv_config()).await;

    let cluster_name = CheetahString::from_static_str("phase5-standard-json-cluster");
    let topic_name = CheetahString::from_static_str("phase5-standard-json-topic");
    let topic_name_2 = CheetahString::from_static_str("phase5-standard-json-topic-2");
    let requested_zone = CheetahString::from_static_str("zone-a");
    let broker_name = CheetahString::from_static_str("standard-json-broker");
    let broker_id_ten_addr = CheetahString::from_static_str("10.30.0.10:10911");
    let broker_id_two_addr = CheetahString::from_static_str("10.30.0.2:10911");
    let other_zone_name = CheetahString::from_static_str("standard-json-other-zone");
    let other_zone_addr = CheetahString::from_static_str("10.30.1.1:10911");

    let register_specs = [
        (
            &broker_name,
            &broker_id_ten_addr,
            10_u64,
            vec![CheetahString::from_static_str("fs-10")],
        ),
        (
            &broker_name,
            &broker_id_two_addr,
            2_u64,
            vec![CheetahString::from_static_str("fs-2")],
        ),
    ];

    for (current_broker_name, broker_addr, broker_id, filter_servers) in register_specs {
        let response = harness
            .request(register_broker_request_with_options(
                &cluster_name,
                current_broker_name,
                broker_addr,
                broker_id,
                &[&topic_name, &topic_name_2],
                Some(&requested_zone),
                filter_servers,
            ))
            .await
            .unwrap();
        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
    }

    let response = harness
        .request(register_broker_request_with_options(
            &cluster_name,
            &other_zone_name,
            &other_zone_addr,
            MASTER_ID,
            &[&topic_name, &topic_name_2],
            Some(&CheetahString::from_static_str("zone-b")),
            vec![CheetahString::from_static_str("fs-b")],
        ))
        .await
        .unwrap();
    assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);

    let route_response = harness
        .request(route_request_with_options(
            &topic_name,
            Some(true),
            RocketMqVersion::V4_9_3 as i32,
            Some(&requested_zone),
        ))
        .await
        .unwrap();
    assert_eq!(ResponseCode::from(route_response.code()), ResponseCode::Success);

    let body = std::str::from_utf8(
        route_response
            .body()
            .expect("standard json route response should include a body"),
    )
    .expect("standard json route response should stay valid utf-8");
    let broker_addrs_index = body
        .find("\"brokerAddrs\":{\"10\":\"10.30.0.10:10911\"")
        .expect("standard json should preserve sorted brokerAddrs keys");
    let broker_addrs_second_index = body
        .find("\"2\":\"10.30.0.2:10911\"")
        .unwrap_or_else(|| panic!("standard json should include broker id 2, body={body}"));
    assert!(broker_addrs_index < broker_addrs_second_index);

    let topic_route_data = TopicRouteData::decode(body.as_bytes()).unwrap();
    assert_eq!(topic_route_data.broker_datas.len(), 1);
    assert_eq!(topic_route_data.broker_datas[0].broker_name(), &broker_name);
    assert_eq!(
        topic_route_data.broker_datas[0].broker_addrs().get(&10),
        Some(&broker_id_ten_addr)
    );
    assert_eq!(
        topic_route_data.broker_datas[0].broker_addrs().get(&2),
        Some(&broker_id_two_addr)
    );
    assert!(!body.contains(other_zone_addr.as_str()));

    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn namesrv_cluster_test_returns_local_order_conf_and_legacy_route_encoding_over_remoting() {
    let product_env_name = format!("cluster-test-env-{}", reserve_local_port());
    let harness = NamesrvHarness::start(NamesrvConfig {
        use_route_info_manager_v2: true,
        cluster_test: true,
        order_message_enable: false,
        product_env_name,
        ..NamesrvConfig::default()
    })
    .await;

    let cluster_name = CheetahString::from_static_str("cluster-test-cluster");
    let broker_name = CheetahString::from_static_str("cluster-test-broker");
    let broker_addr = CheetahString::from_static_str("127.0.0.1:11911");
    let topic_name = CheetahString::from_static_str("cluster-test-topic");
    let order_conf = CheetahString::from_static_str("broker-a:2");

    let mut put_order_request = RemotingCommand::create_request_command(
        RequestCode::PutKvConfig,
        PutKVConfigRequestHeader::new(
            CheetahString::from_static_str(ORDER_TOPIC_NAMESPACE),
            topic_name.clone(),
            order_conf.clone(),
        ),
    );
    put_order_request.make_custom_header_to_net();
    let put_order_response = harness.request(put_order_request).await.unwrap();
    assert_eq!(ResponseCode::from(put_order_response.code()), ResponseCode::Success);

    let register_response = harness
        .request(register_broker_request(
            &cluster_name,
            &broker_name,
            &broker_addr,
            &topic_name,
        ))
        .await
        .unwrap();
    assert_eq!(ResponseCode::from(register_response.code()), ResponseCode::Success);

    let route_response = harness
        .request(route_request_with_options(
            &topic_name,
            Some(true),
            RocketMqVersion::V5_0_0 as i32,
            None,
        ))
        .await
        .unwrap();
    assert_eq!(
        ResponseCode::from(route_response.code()),
        ResponseCode::Success,
        "unexpected route response remark: {:?}",
        route_response.remark()
    );

    let body = route_response.body().expect("route response should include a body");
    let topic_route_data = TopicRouteData::decode(body).expect("route response body should decode");
    assert_eq!(topic_route_data.order_topic_conf.as_ref(), Some(&order_conf));
    assert_eq!(
        body.as_ref(),
        topic_route_data
            .encode()
            .expect("legacy encoding should succeed")
            .as_slice()
    );

    harness.shutdown().await;
}
