use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_common::common::mix_all::FIRST_BROKER_CONTROLLER_ID;
use rocketmq_controller::config::StorageBackendType;
use rocketmq_controller::controller::broker_heartbeat_manager::BrokerHeartbeatManager;
use rocketmq_controller::processor::controller_request_processor::ControllerRequestProcessor;
use rocketmq_controller::typ::Node;
use rocketmq_controller::ControllerConfig;
use rocketmq_controller::ControllerManager;
use rocketmq_remoting::base::response_future::ResponseFuture;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::connection::Connection;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::net::channel::ChannelInner;
use rocketmq_remoting::protocol::body::broker_replicas_info::BrokerReplicasInfo;
use rocketmq_remoting::protocol::body::elect_master_response_body::ElectMasterResponseBody;
use rocketmq_remoting::protocol::body::sync_state_set_body::SyncStateSet;
use rocketmq_remoting::protocol::header::controller::alter_sync_state_set_request_header::AlterSyncStateSetRequestHeader;
use rocketmq_remoting::protocol::header::controller::alter_sync_state_set_response_header::AlterSyncStateSetResponseHeader;
use rocketmq_remoting::protocol::header::controller::apply_broker_id_request_header::ApplyBrokerIdRequestHeader;
use rocketmq_remoting::protocol::header::controller::apply_broker_id_response_header::ApplyBrokerIdResponseHeader;
use rocketmq_remoting::protocol::header::controller::clean_broker_data_request_header::CleanBrokerDataRequestHeader;
use rocketmq_remoting::protocol::header::controller::elect_master_request_header::ElectMasterRequestHeader;
use rocketmq_remoting::protocol::header::controller::get_next_broker_id_request_header::GetNextBrokerIdRequestHeader;
use rocketmq_remoting::protocol::header::controller::get_next_broker_id_response_header::GetNextBrokerIdResponseHeader;
use rocketmq_remoting::protocol::header::controller::get_replica_info_request_header::GetReplicaInfoRequestHeader;
use rocketmq_remoting::protocol::header::controller::get_replica_info_response_header::GetReplicaInfoResponseHeader;
use rocketmq_remoting::protocol::header::controller::register_broker_to_controller_request_header::RegisterBrokerToControllerRequestHeader;
use rocketmq_remoting::protocol::header::controller::register_broker_to_controller_response_header::RegisterBrokerToControllerResponseHeader;
use rocketmq_remoting::protocol::header::elect_master_response_header::ElectMasterResponseHeader;
use rocketmq_remoting::protocol::header::get_meta_data_response_header::GetMetaDataResponseHeader;
use rocketmq_remoting::protocol::header::namesrv::broker_request::BrokerHeartbeatRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingDeserializable;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContextWrapper;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_rust::ArcMut;
use tokio::time::sleep;

const CLUSTER_NAME: &str = "contract-cluster";
const BROKER_NAME: &str = "contract-broker";
const BROKER_ADDR_1: &str = "127.0.0.1:10911";
const BROKER_ADDR_2: &str = "127.0.0.1:10912";

struct ProcessorHarness {
    manager: ArcMut<ControllerManager>,
    processor: ControllerRequestProcessor,
    channel: Channel,
    ctx: ConnectionHandlerContext,
}

#[derive(Clone, Copy)]
struct ReplicaSeed {
    master_broker_id: i64,
    slave_broker_id: i64,
    master_epoch: i32,
    sync_state_set_epoch: i32,
}

impl ProcessorHarness {
    async fn new() -> Self {
        let listen_addr = reserve_socket_addr();
        let config = ControllerConfig::default()
            .with_node_info(1, listen_addr)
            .with_storage_backend(StorageBackendType::Memory)
            .with_heartbeat_interval_ms(100)
            .with_election_timeout_ms(300);
        let manager = ArcMut::new(ControllerManager::new(config).await.expect("create controller manager"));
        manager
            .clone()
            .initialize()
            .await
            .expect("initialize controller manager");
        manager.clone().start().await.expect("start controller manager");

        let mut nodes = BTreeMap::new();
        nodes.insert(
            1,
            Node {
                node_id: 1,
                rpc_addr: listen_addr.to_string(),
            },
        );
        manager
            .controller()
            .initialize_cluster(nodes)
            .await
            .expect("initialize single-node controller cluster");

        wait_for_leader(&manager).await;

        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let processor = ControllerRequestProcessor::new(manager.clone());
        Self {
            manager,
            processor,
            channel,
            ctx,
        }
    }

    async fn shutdown(self) {
        self.manager.shutdown().await.expect("shutdown controller manager");
        let ProcessorHarness { manager, processor, .. } = self;
        std::mem::forget(processor);
        std::mem::forget(manager);
    }

    async fn send(&mut self, mut request: RemotingCommand) -> RemotingCommand {
        request.make_custom_header_to_net();
        let mut response = self
            .processor
            .process_request(self.channel.clone(), self.ctx.clone(), &mut request)
            .await
            .expect("processor request should succeed")
            .expect("processor should return a response");
        response.make_custom_header_to_net();
        response
    }

    async fn apply_broker_id(
        &mut self,
        broker_id: i64,
        broker_addr: &str,
        register_check_code: &str,
    ) -> RemotingCommand {
        let header = ApplyBrokerIdRequestHeader {
            cluster_name: CheetahString::from_static_str(CLUSTER_NAME),
            broker_name: CheetahString::from_static_str(BROKER_NAME),
            applied_broker_id: broker_id,
            register_check_code: CheetahString::from_string(format!("{broker_addr};{register_check_code}")),
        };
        self.send(RemotingCommand::create_request_command(
            RequestCode::ControllerApplyBrokerId,
            header,
        ))
        .await
    }

    async fn register_broker(&mut self, broker_id: i64, broker_addr: &str) -> RemotingCommand {
        let header = RegisterBrokerToControllerRequestHeader {
            cluster_name: Some(CheetahString::from_static_str(CLUSTER_NAME)),
            broker_name: Some(CheetahString::from_static_str(BROKER_NAME)),
            broker_id: Some(broker_id),
            broker_address: Some(CheetahString::from_string(broker_addr.to_owned())),
            ..Default::default()
        };
        self.send(RemotingCommand::create_request_command(
            RequestCode::ControllerRegisterBroker,
            header,
        ))
        .await
    }

    async fn heartbeat(&mut self, broker_id: i64, broker_addr: &str) -> RemotingCommand {
        let header = BrokerHeartbeatRequestHeader {
            cluster_name: CheetahString::from_static_str(CLUSTER_NAME),
            broker_addr: CheetahString::from_string(broker_addr.to_owned()),
            broker_name: CheetahString::from_static_str(BROKER_NAME),
            broker_id: Some(broker_id),
            epoch: Some(1),
            max_offset: Some(100),
            confirm_offset: Some(80),
            heartbeat_timeout_mills: Some(3_000),
            election_priority: Some(1),
        };
        self.send(RemotingCommand::create_request_command(
            RequestCode::BrokerHeartbeat,
            header,
        ))
        .await
    }

    async fn elect_master(&mut self, broker_id: i64) -> RemotingCommand {
        let header = ElectMasterRequestHeader::new(
            CheetahString::from_static_str(CLUSTER_NAME),
            CheetahString::from_static_str(BROKER_NAME),
            broker_id,
            false,
            0,
        );
        self.send(RemotingCommand::create_request_command(
            RequestCode::ControllerElectMaster,
            header,
        ))
        .await
    }

    async fn seed_two_live_brokers_and_elect_master(&mut self) -> ReplicaSeed {
        let get_next_response = self
            .send(RemotingCommand::create_request_command(
                RequestCode::ControllerGetNextBrokerId,
                GetNextBrokerIdRequestHeader {
                    cluster_name: CheetahString::from_static_str(CLUSTER_NAME),
                    broker_name: CheetahString::from_static_str(BROKER_NAME),
                },
            ))
            .await;
        let first_header = get_next_response
            .decode_command_custom_header::<GetNextBrokerIdResponseHeader>()
            .expect("decode first next broker id response");
        let master_broker_id = first_header.next_broker_id.expect("first broker id should exist") as i64;

        let apply_first = self.apply_broker_id(master_broker_id, BROKER_ADDR_1, "code-1").await;
        assert_eq!(apply_first.code(), ResponseCode::Success as i32);

        let next_after_first = self
            .send(RemotingCommand::create_request_command(
                RequestCode::ControllerGetNextBrokerId,
                GetNextBrokerIdRequestHeader {
                    cluster_name: CheetahString::from_static_str(CLUSTER_NAME),
                    broker_name: CheetahString::from_static_str(BROKER_NAME),
                },
            ))
            .await;
        let second_header = next_after_first
            .decode_command_custom_header::<GetNextBrokerIdResponseHeader>()
            .expect("decode second next broker id response");
        let slave_broker_id = second_header.next_broker_id.expect("second broker id should exist") as i64;

        let apply_second = self.apply_broker_id(slave_broker_id, BROKER_ADDR_2, "code-2").await;
        assert_eq!(apply_second.code(), ResponseCode::Success as i32);

        assert_eq!(
            self.register_broker(master_broker_id, BROKER_ADDR_1).await.code(),
            ResponseCode::Success as i32
        );
        assert_eq!(
            self.register_broker(slave_broker_id, BROKER_ADDR_2).await.code(),
            ResponseCode::Success as i32
        );
        assert_eq!(
            self.heartbeat(master_broker_id, BROKER_ADDR_1).await.code(),
            ResponseCode::Success as i32
        );
        assert_eq!(
            self.heartbeat(slave_broker_id, BROKER_ADDR_2).await.code(),
            ResponseCode::Success as i32
        );

        wait_until(
            Duration::from_secs(5),
            || {
                self.manager
                    .heartbeat_manager()
                    .is_broker_active(CLUSTER_NAME, BROKER_NAME, master_broker_id)
                    && self
                        .manager
                        .heartbeat_manager()
                        .is_broker_active(CLUSTER_NAME, BROKER_NAME, slave_broker_id)
            },
            "two brokers to become active in heartbeat manager",
        )
        .await;

        let elect_response = self.elect_master(master_broker_id).await;
        assert_eq!(elect_response.code(), ResponseCode::Success as i32);
        let elect_header = elect_response
            .decode_command_custom_header::<ElectMasterResponseHeader>()
            .expect("decode elect master response");
        ReplicaSeed {
            master_broker_id,
            slave_broker_id,
            master_epoch: elect_header.master_epoch.expect("master epoch"),
            sync_state_set_epoch: elect_header.sync_state_set_epoch.expect("sync epoch"),
        }
    }
}

fn reserve_socket_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind random port");
    let addr = listener.local_addr().expect("read listener addr");
    drop(listener);
    addr
}

async fn create_test_channel() -> Channel {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind local test listener");
    let local_addr = listener.local_addr().expect("local listener addr");
    let std_stream = std::net::TcpStream::connect(local_addr).expect("connect local test listener");
    std_stream.set_nonblocking(true).expect("set nonblocking");
    drop(listener);
    let tcp_stream = tokio::net::TcpStream::from_std(std_stream).expect("convert tcp stream");
    let connection = Connection::new(tcp_stream);
    let response_table = ArcMut::new(HashMap::<i32, ResponseFuture>::new());
    let inner = ArcMut::new(ChannelInner::new(connection, response_table));
    Channel::new(inner, local_addr, local_addr)
}

async fn wait_for_leader(manager: &ArcMut<ControllerManager>) {
    wait_until(
        Duration::from_secs(5),
        || manager.is_leader(),
        "single-node controller to become leader",
    )
    .await;
}

async fn wait_until<F>(timeout: Duration, mut predicate: F, context: &str)
where
    F: FnMut() -> bool,
{
    let start = std::time::Instant::now();
    loop {
        if predicate() {
            return;
        }
        if start.elapsed() >= timeout {
            panic!("timed out waiting for {context}");
        }
        sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn controller_request_contract_get_next_broker_id() {
    let mut harness = ProcessorHarness::new().await;
    let response = harness
        .send(RemotingCommand::create_request_command(
            RequestCode::ControllerGetNextBrokerId,
            GetNextBrokerIdRequestHeader {
                cluster_name: CheetahString::from_static_str(CLUSTER_NAME),
                broker_name: CheetahString::from_static_str(BROKER_NAME),
            },
        ))
        .await;

    assert_eq!(response.code(), ResponseCode::Success as i32);
    let header = response
        .decode_command_custom_header::<GetNextBrokerIdResponseHeader>()
        .expect("decode next broker id response");
    assert_eq!(
        header.cluster_name.as_deref(),
        Some(CheetahString::from_static_str(CLUSTER_NAME).as_str())
    );
    assert_eq!(
        header.broker_name.as_deref(),
        Some(CheetahString::from_static_str(BROKER_NAME).as_str())
    );
    assert_eq!(header.next_broker_id, Some(FIRST_BROKER_CONTROLLER_ID));

    harness.shutdown().await;
}

#[tokio::test]
async fn controller_request_contract_apply_broker_id() {
    let mut harness = ProcessorHarness::new().await;
    let response = harness
        .apply_broker_id(FIRST_BROKER_CONTROLLER_ID as i64, BROKER_ADDR_1, "apply-contract")
        .await;

    assert_eq!(response.code(), ResponseCode::Success as i32);
    let header = response
        .decode_command_custom_header::<ApplyBrokerIdResponseHeader>()
        .expect("decode apply broker id response");
    assert_eq!(
        header.cluster_name.as_deref(),
        Some(CheetahString::from_static_str(CLUSTER_NAME).as_str())
    );
    assert_eq!(
        header.broker_name.as_deref(),
        Some(CheetahString::from_static_str(BROKER_NAME).as_str())
    );

    let next_response = harness
        .send(RemotingCommand::create_request_command(
            RequestCode::ControllerGetNextBrokerId,
            GetNextBrokerIdRequestHeader {
                cluster_name: CheetahString::from_static_str(CLUSTER_NAME),
                broker_name: CheetahString::from_static_str(BROKER_NAME),
            },
        ))
        .await;
    let next_header = next_response
        .decode_command_custom_header::<GetNextBrokerIdResponseHeader>()
        .expect("decode next broker id response after apply");
    assert_eq!(next_header.next_broker_id, Some(FIRST_BROKER_CONTROLLER_ID + 1));

    harness.shutdown().await;
}

#[tokio::test]
async fn controller_request_contract_register_broker() {
    let mut harness = ProcessorHarness::new().await;
    let broker_id = FIRST_BROKER_CONTROLLER_ID as i64;
    assert_eq!(
        harness
            .apply_broker_id(broker_id, BROKER_ADDR_1, "register-contract")
            .await
            .code(),
        ResponseCode::Success as i32
    );

    let response = harness.register_broker(broker_id, BROKER_ADDR_1).await;
    assert_eq!(response.code(), ResponseCode::Success as i32);

    let header = response
        .decode_command_custom_header::<RegisterBrokerToControllerResponseHeader>()
        .expect("decode register broker response");
    assert_eq!(
        header.cluster_name.as_deref(),
        Some(CheetahString::from_static_str(CLUSTER_NAME).as_str())
    );
    assert_eq!(
        header.broker_name.as_deref(),
        Some(CheetahString::from_static_str(BROKER_NAME).as_str())
    );
    assert_eq!(header.master_broker_id, None);
    assert_eq!(header.master_epoch, None);
    let body = response.body().expect("register broker response body");
    let sync_state_set = SyncStateSet::decode(body.as_ref()).expect("decode register broker sync state");
    assert!(sync_state_set.get_sync_state_set().is_none() || sync_state_set.get_sync_state_set().unwrap().is_empty());
    assert_eq!(sync_state_set.get_sync_state_set_epoch(), 0);

    harness.shutdown().await;
}

#[tokio::test]
async fn controller_request_contract_broker_heartbeat() {
    let mut harness = ProcessorHarness::new().await;
    let broker_id = FIRST_BROKER_CONTROLLER_ID as i64;
    let response = harness.heartbeat(broker_id, BROKER_ADDR_1).await;

    assert_eq!(response.code(), ResponseCode::Success as i32);
    assert_eq!(
        response.remark().map(|remark| remark.as_str()),
        Some("Heart beat success")
    );
    wait_until(
        Duration::from_secs(5),
        || {
            harness
                .manager
                .heartbeat_manager()
                .is_broker_active(CLUSTER_NAME, BROKER_NAME, broker_id)
        },
        "heartbeat request to mark broker active",
    )
    .await;

    harness.shutdown().await;
}

#[tokio::test]
async fn controller_request_contract_get_metadata_info() {
    let mut harness = ProcessorHarness::new().await;
    let response = harness
        .send(RemotingCommand::create_remoting_command(
            RequestCode::ControllerGetMetadataInfo,
        ))
        .await;

    assert_eq!(response.code(), ResponseCode::Success as i32);
    let header = response
        .decode_command_custom_header::<GetMetaDataResponseHeader>()
        .expect("decode controller metadata response");
    assert_eq!(header.controller_leader_id.as_deref(), Some("1"));
    assert_eq!(
        header.controller_leader_address.as_deref(),
        Some(harness.manager.controller_config().listen_addr.to_string().as_str())
    );
    assert_eq!(header.is_leader, Some(true));
    assert!(header.peers.as_ref().is_some_and(|peers| peers
        .as_str()
        .contains(&harness.manager.controller_config().listen_addr.to_string())));

    harness.shutdown().await;
}

#[tokio::test]
async fn controller_request_contract_elect_master() {
    let mut harness = ProcessorHarness::new().await;
    let seed = harness.seed_two_live_brokers_and_elect_master().await;
    let response = harness.elect_master(seed.master_broker_id).await;

    assert_eq!(response.code(), ResponseCode::ControllerMasterStillExist as i32);
    let header = response
        .decode_command_custom_header::<ElectMasterResponseHeader>()
        .expect("decode elect master response");
    assert_eq!(header.master_broker_id, Some(seed.master_broker_id));
    assert_eq!(header.master_epoch, Some(seed.master_epoch));
    assert_eq!(header.sync_state_set_epoch, Some(seed.sync_state_set_epoch));
    let body = ElectMasterResponseBody::decode(response.body().expect("elect master response body").as_ref())
        .expect("decode elect master body");
    assert_eq!(body.sync_state_set, HashSet::from([seed.master_broker_id]));
    let broker_member_group = body
        .broker_member_group
        .expect("elect master body should include broker member group");
    assert_eq!(broker_member_group.broker_addrs.len(), 2);

    harness.shutdown().await;
}

#[tokio::test]
async fn controller_request_contract_get_replica_info() {
    let mut harness = ProcessorHarness::new().await;
    let seed = harness.seed_two_live_brokers_and_elect_master().await;
    let response = harness
        .send(RemotingCommand::create_request_command(
            RequestCode::ControllerGetReplicaInfo,
            GetReplicaInfoRequestHeader {
                broker_name: CheetahString::from_static_str(BROKER_NAME),
            },
        ))
        .await;

    assert_eq!(response.code(), ResponseCode::Success as i32);
    let header = response
        .decode_command_custom_header::<GetReplicaInfoResponseHeader>()
        .expect("decode replica info response");
    assert_eq!(header.master_broker_id, Some(seed.master_broker_id));
    assert_eq!(header.master_address.as_deref(), Some(BROKER_ADDR_1));
    assert_eq!(header.master_epoch, Some(seed.master_epoch));
    let body = SyncStateSet::decode(response.body().expect("replica info response body").as_ref())
        .expect("decode replica info sync state");
    assert_eq!(
        body.get_sync_state_set().cloned().unwrap_or_default(),
        HashSet::from([seed.master_broker_id])
    );
    assert_eq!(body.get_sync_state_set_epoch(), seed.sync_state_set_epoch);

    harness.shutdown().await;
}

#[tokio::test]
async fn controller_request_contract_alter_sync_state_set() {
    let mut harness = ProcessorHarness::new().await;
    let seed = harness.seed_two_live_brokers_and_elect_master().await;
    let request_body = SyncStateSet::with_values(
        HashSet::from([seed.master_broker_id, seed.slave_broker_id]),
        seed.sync_state_set_epoch,
    )
    .encode()
    .expect("encode alter sync state body");
    let response = harness
        .send(
            RemotingCommand::create_request_command(
                RequestCode::ControllerAlterSyncStateSet,
                AlterSyncStateSetRequestHeader {
                    broker_name: CheetahString::from_static_str(BROKER_NAME),
                    master_broker_id: seed.master_broker_id,
                    master_epoch: seed.master_epoch,
                },
            )
            .set_body(request_body),
        )
        .await;

    assert_eq!(response.code(), ResponseCode::Success as i32);
    let header = response
        .decode_command_custom_header::<AlterSyncStateSetResponseHeader>()
        .expect("decode alter sync state response");
    assert_eq!(header.new_sync_state_set_epoch, seed.sync_state_set_epoch + 1);
    let body = SyncStateSet::decode(response.body().expect("alter sync state response body").as_ref())
        .expect("decode alter sync state body");
    assert_eq!(
        body.get_sync_state_set().cloned().unwrap_or_default(),
        HashSet::from([seed.master_broker_id, seed.slave_broker_id])
    );
    assert_eq!(body.get_sync_state_set_epoch(), seed.sync_state_set_epoch + 1);

    harness.shutdown().await;
}

#[tokio::test]
async fn controller_request_contract_get_sync_state_data() {
    let mut harness = ProcessorHarness::new().await;
    let seed = harness.seed_two_live_brokers_and_elect_master().await;
    let broker_names = vec![CheetahString::from_static_str(BROKER_NAME)];
    let response = harness
        .send(
            RemotingCommand::create_remoting_command(RequestCode::ControllerGetSyncStateData)
                .set_body(serde_json::to_vec(&broker_names).expect("encode broker names")),
        )
        .await;

    assert_eq!(response.code(), ResponseCode::Success as i32);
    let replicas_info = BrokerReplicasInfo::decode(response.body().expect("sync state data response body").as_ref())
        .expect("decode sync state data body");
    let broker_info = replicas_info
        .get_replicas_info_table()
        .get(&CheetahString::from_static_str(BROKER_NAME))
        .expect("broker replicas info should exist");
    assert_eq!(broker_info.get_master_broker_id(), seed.master_broker_id as u64);
    assert_eq!(broker_info.get_master_address(), BROKER_ADDR_1);
    assert_eq!(broker_info.get_master_epoch(), seed.master_epoch);
    assert_eq!(broker_info.get_sync_state_set_epoch(), seed.sync_state_set_epoch);
    assert_eq!(broker_info.get_in_sync_replicas().len(), 1);
    assert_eq!(broker_info.get_not_in_sync_replicas().len(), 1);
    assert_eq!(
        broker_info.get_in_sync_replicas()[0].get_broker_id(),
        seed.master_broker_id as u64
    );
    assert_eq!(
        broker_info.get_not_in_sync_replicas()[0].get_broker_id(),
        seed.slave_broker_id as u64
    );
    assert!(broker_info.get_in_sync_replicas()[0].get_alive());
    assert!(broker_info.get_not_in_sync_replicas()[0].get_alive());

    harness.shutdown().await;
}

#[tokio::test]
async fn controller_request_contract_update_controller_config() {
    let mut harness = ProcessorHarness::new().await;
    let response = harness
        .send(
            RemotingCommand::create_remoting_command(RequestCode::UpdateControllerConfig)
                .set_body(b"scanNotActiveBrokerInterval=1234\nnotifyBrokerRoleChanged=false".to_vec()),
        )
        .await;

    assert_eq!(response.code(), ResponseCode::Success as i32);
    assert_eq!(
        harness.manager.controller_config().scan_not_active_broker_interval,
        1234
    );
    assert!(!harness.manager.controller_config().notify_broker_role_changed);

    harness.shutdown().await;
}

#[tokio::test]
async fn controller_request_contract_get_controller_config() {
    let mut harness = ProcessorHarness::new().await;
    let response = harness
        .send(RemotingCommand::create_remoting_command(
            RequestCode::GetControllerConfig,
        ))
        .await;

    assert_eq!(response.code(), ResponseCode::Success as i32);
    let body = String::from_utf8(response.body().expect("get controller config response body").to_vec())
        .expect("decode controller config body as utf8");
    assert!(body.contains("storageBackend=memory"));
    assert!(body.contains("notifyBrokerRoleChanged=true"));
    assert!(body.contains("scanNotActiveBrokerInterval=5000"));

    harness.shutdown().await;
}

#[tokio::test]
async fn controller_request_contract_clean_broker_data() {
    let mut harness = ProcessorHarness::new().await;
    let seed = harness.seed_two_live_brokers_and_elect_master().await;
    let response = harness
        .send(RemotingCommand::create_request_command(
            RequestCode::CleanBrokerData,
            CleanBrokerDataRequestHeader {
                cluster_name: Some(CheetahString::from_static_str(CLUSTER_NAME)),
                broker_name: CheetahString::from_static_str(BROKER_NAME),
                broker_controller_ids_to_clean: Some(CheetahString::from_string(format!(
                    "{},{}",
                    seed.master_broker_id, seed.slave_broker_id
                ))),
                clean_living_broker: true,
                invoke_time: 0,
            },
        ))
        .await;

    assert_eq!(response.code(), ResponseCode::Success as i32);

    let replica_response = harness
        .send(RemotingCommand::create_request_command(
            RequestCode::ControllerGetReplicaInfo,
            GetReplicaInfoRequestHeader {
                broker_name: CheetahString::from_static_str(BROKER_NAME),
            },
        ))
        .await;
    assert_eq!(
        replica_response.code(),
        ResponseCode::ControllerBrokerMetadataNotExist as i32
    );

    harness.shutdown().await;
}
