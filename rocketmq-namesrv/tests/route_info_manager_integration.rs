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

use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_common::common::config::TopicConfig;
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

struct NamesrvHarness {
    addr: CheetahString,
    client: ArcMut<RocketmqDefaultClient<DefaultRemotingRequestProcessor>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    server_task: JoinHandle<RocketMQResult<()>>,
}

impl NamesrvHarness {
    async fn start(namesrv_config: NamesrvConfig) -> Self {
        let port = reserve_local_port();
        let addr = CheetahString::from_string(format!("127.0.0.1:{port}"));
        let server_config = ServerConfig {
            listen_port: port as u32,
            bind_address: "127.0.0.1".to_string(),
        };
        let bootstrap = Builder::new()
            .set_name_server_config(namesrv_config)
            .set_server_config(server_config)
            .build();

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server_task = tokio::spawn(async move {
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

        let harness = Self {
            addr,
            client,
            shutdown_tx: Some(shutdown_tx),
            server_task,
        };
        harness.wait_until_ready().await;
        harness
    }

    async fn wait_until_ready(&self) {
        let deadline = Instant::now() + Duration::from_secs(10);

        loop {
            let mut request = RemotingCommand::create_request_command(
                RequestCode::GetNamesrvConfig,
                GetNamesrvConfigRequestHeader::default(),
            );
            request.make_custom_header_to_net();

            let failure = match self.request(request).await {
                Ok(response) if ResponseCode::from(response.code()) == ResponseCode::Success => return,
                Ok(response) => format!(
                    "unexpected response code {:?}, remark {:?}",
                    ResponseCode::from(response.code()),
                    response.remark()
                ),
                Err(error) => error.to_string(),
            };

            assert!(
                Instant::now() < deadline,
                "namesrv did not become ready in time: {}",
                failure
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
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

fn default_v2_namesrv_config() -> NamesrvConfig {
    NamesrvConfig {
        use_route_info_manager_v2: true,
        ..NamesrvConfig::default()
    }
}

fn route_request(topic: &CheetahString) -> RemotingCommand {
    let mut request = RemotingCommand::create_request_command(
        RequestCode::GetRouteinfoByTopic,
        GetRouteInfoRequestHeader::new(topic.clone(), Some(false)),
    )
    .set_version(RocketMqVersion::V4_9_3 as i32);
    request.make_custom_header_to_net();
    request
}

fn register_broker_request(
    cluster_name: &CheetahString,
    broker_name: &CheetahString,
    broker_addr: &CheetahString,
    topic_name: &CheetahString,
) -> RemotingCommand {
    let mut topic_config_wrapper = TopicConfigAndMappingSerializeWrapper::default();
    topic_config_wrapper
        .topic_config_serialize_wrapper
        .topic_config_table
        .insert(topic_name.clone(), TopicConfig::with_perm(topic_name.clone(), 4, 4, 6));

    let body = RegisterBrokerBody::new(topic_config_wrapper, Vec::new()).encode(false);
    let mut request = RemotingCommand::create_request_command(
        RequestCode::RegisterBroker,
        RegisterBrokerRequestHeader::new(
            broker_name.clone(),
            broker_addr.clone(),
            cluster_name.clone(),
            broker_addr.clone(),
            MASTER_ID,
            Some(60_000),
            Some(false),
            false,
            crc32_utils::crc32(body.as_slice()),
        ),
    )
    .set_version(RocketMqVersion::V5_0_0 as i32)
    .set_body(body);
    request.make_custom_header_to_net();
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
