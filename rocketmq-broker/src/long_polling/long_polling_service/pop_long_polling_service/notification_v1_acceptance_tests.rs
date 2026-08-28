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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::header::notification_request_header::NotificationRequestHeader;
use rocketmq_protocol::protocol::header::notification_response_header::NotificationResponseHeader;
use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::application_remoting_command_factory;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_store::CqExtUnit;
use rocketmq_store::MessageFilter;
use rocketmq_store::MessageStoreConfig;
use rocketmq_transport::api::v1::Channel;
use rocketmq_transport::api::v1::ConnectionHandlerContext;
use rocketmq_transport::api::v1::ConnectionHandlerContextWrapper;
use rocketmq_transport::test_support::Connection;
use rocketmq_transport::test_support::TestChannelBuilder;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use super::PopLongPollingRequestProcessor;
use super::PopLongPollingService;
use super::PopLongPollingServiceContext;
use crate::broker_runtime::BrokerRuntime;
use crate::config::broker_config::BrokerConfig;
use crate::long_polling::long_polling_service::pop_long_polling_service::PopLongPollingPolicy;
use crate::long_polling::polling_header::PollingHeader;
use crate::long_polling::polling_result::PollingResult;

const ARRIVAL_OPAQUE: i32 = 19_833;
const TIMEOUT_OPAQUE: i32 = 19_834;

struct MatchTagFilter(i64);

impl MessageFilter for MatchTagFilter {
    fn is_matched_by_consume_queue(&self, tags_code: Option<i64>, _cq_ext_unit: Option<&CqExtUnit>) -> bool {
        tags_code == Some(self.0)
    }

    fn is_matched_by_commit_log(
        &self,
        _msg_buffer: Option<&[u8]>,
        _properties: Option<&std::collections::HashMap<CheetahString, CheetahString>>,
    ) -> bool {
        true
    }
}

struct LegacyNotificationProcessor {
    calls: AtomicUsize,
    observed: mpsc::UnboundedSender<(i32, NotificationRequestHeader)>,
}

impl PopLongPollingRequestProcessor for LegacyNotificationProcessor {
    async fn process_request_when_wakeup(
        &self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        let opaque = request.opaque();
        let header = request.decode_command_custom_header::<NotificationRequestHeader>()?;
        self.observed
            .send((opaque, header))
            .map_err(|_| rocketmq_error::RocketMQError::illegal_argument("legacy Notification observer closed"))?;
        let response = application_remoting_command_factory()
            .create_success_response_command_with_header(NotificationResponseHeader {
                has_msg: opaque == ARRIVAL_OPAQUE,
                polling_full: false,
            })
            .set_opaque(-1);
        Ok(Some(response))
    }
}

struct LegacySocketHarness {
    owner: RuntimeOwner,
    context: ConnectionHandlerContext,
    peer: Connection,
    shutdown: std::net::TcpStream,
}

impl LegacySocketHarness {
    async fn new(label: &'static str) -> Self {
        let owner = RuntimeOwner::new(RuntimeConfig::server_default(label)).expect("legacy Notification runtime");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind legacy Notification listener");
        let address = listener.local_addr().expect("legacy Notification listener address");
        let (client, accepted) = tokio::join!(TcpStream::connect(address), listener.accept());
        let client = client.expect("connect legacy Notification channel");
        let client = client.into_std().expect("recover legacy Notification socket");
        let shutdown = client.try_clone().expect("clone legacy Notification shutdown handle");
        let client = TcpStream::from_std(client).expect("restore legacy Notification socket");
        let (peer, peer_address) = accepted.expect("accept legacy Notification peer");
        let channel_context = owner.root_context().component("notification-v1.channel");
        let channel = TestChannelBuilder::new(Connection::new(client), channel_context.task_group().clone())
            .addresses(address, peer_address)
            .build()
            .expect("build legacy Notification channel");
        Self {
            owner,
            context: Arc::new(ConnectionHandlerContextWrapper::new(channel)),
            peer: Connection::new(peer),
            shutdown,
        }
    }

    async fn receive_one(&mut self) -> RemotingCommand {
        tokio::time::timeout(Duration::from_secs(2), self.peer.receive_command())
            .await
            .expect("legacy Notification raw frame write remains bounded")
            .expect("legacy Notification connection remains open")
            .expect("decode legacy Notification raw frame")
    }

    async fn finish(mut self) {
        let writer = self.context.channel().close_with_report(Duration::from_secs(1)).await;
        assert!(writer.is_healthy(), "{}", writer.to_json());
        drop(self.context);
        let tasks = self.owner.shutdown_tasks().await;
        assert!(tasks.is_healthy(), "{}", tasks.to_json());
        self.shutdown
            .shutdown(std::net::Shutdown::Both)
            .expect("shutdown legacy Notification socket");
        assert!(
            tokio::time::timeout(Duration::from_secs(2), self.peer.receive_command())
                .await
                .expect("legacy Notification EOF remains bounded")
                .is_none(),
            "EOF proves the legacy wake emitted exactly one frame"
        );
        let background = self.owner.shutdown_background();
        assert!(background.is_healthy(), "{}", background.to_json());
    }
}

fn service<RP>(processor: &Arc<RP>) -> Arc<PopLongPollingService<RP>>
where
    RP: PopLongPollingRequestProcessor + Sync + 'static,
{
    let mut runtime = BrokerRuntime::new(
        Arc::new(BrokerConfig::default()),
        Arc::new(MessageStoreConfig::default()),
    );
    let state = runtime.runtime_state_mut();
    let context = PopLongPollingServiceContext::new(
        PopLongPollingPolicy::from_config(&state.broker_config()),
        state.topic_config_manager_handle(),
        state.subscription_group_manager().config_lookup(),
        state.broker_service_context(),
    );
    Arc::new(PopLongPollingService::new(context, true, Arc::downgrade(processor)))
}

fn notification_header(
    topic: &CheetahString,
    group: &CheetahString,
    born_time: i64,
    poll_time: i64,
) -> NotificationRequestHeader {
    NotificationRequestHeader {
        consumer_group: group.clone(),
        topic: topic.clone(),
        queue_id: 0,
        born_time,
        order: false,
        attempt_id: None,
        exp_type: None,
        exp: None,
        is_lite_consumer: false,
        client_id: None,
        poll_time,
        topic_request_header: None,
    }
}

fn request(header: NotificationRequestHeader, opaque: i32) -> RemotingCommand {
    let mut request = RemotingCommand::create_request_command(RequestCode::Notification, header).set_opaque(opaque);
    request.make_custom_header_to_net();
    request
}

fn assert_response(response: RemotingCommand, opaque: i32, has_msg: bool) {
    assert_eq!(response.opaque(), opaque);
    assert_eq!(response.code(), ResponseCode::Success as i32);
    assert_eq!(response.remark(), None);
    assert!(response.body().is_none());
    let header = response
        .decode_command_custom_header::<NotificationResponseHeader>()
        .expect("legacy Notification response header");
    assert_eq!(header.has_msg, has_msg);
    assert!(!header.polling_full);
}

#[tokio::test]
async fn notification_v1_suspension_filter_arrival_task_group_wake_writes_one_raw_frame() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let processor = Arc::new(LegacyNotificationProcessor {
        calls: AtomicUsize::new(0),
        observed: observed_tx,
    });
    let service = service(&processor);
    PopLongPollingService::start(&service).await;
    let mut harness = LegacySocketHarness::new("notification-v1-arrival-acceptance").await;
    let topic = CheetahString::from_static_str("notification-v1-topic");
    let group = CheetahString::from_static_str("notification-v1-group");
    let born_time = i64::try_from(current_millis()).expect("legacy Notification wall clock fits i64");
    let header = notification_header(&topic, &group, born_time, 60_000);
    let polling_header = PollingHeader::new_from_notification_request_header(&header);
    let mut command = request(header, ARRIVAL_OPAQUE);
    assert_eq!(
        service.polling(
            harness.context.clone(),
            &mut command,
            polling_header,
            Some(SubscriptionData::default()),
            Some(Arc::new(MatchTagFilter(7))),
        ),
        PollingResult::PollingSuc
    );

    assert!(!service.notify_message_arriving(&topic, 0, &group, Some(6), 0, None, None));
    assert_eq!(processor.calls.load(Ordering::Acquire), 0);
    assert!(service.notify_message_arriving(&topic, 0, &group, Some(7), 0, None, None));
    let (opaque, observed_header) = tokio::time::timeout(Duration::from_secs(2), observed_rx.recv())
        .await
        .expect("legacy Notification arrival wake remains bounded")
        .expect("observe legacy Notification arrival wake");
    assert_eq!(opaque, ARRIVAL_OPAQUE);
    assert_eq!(observed_header.consumer_group, group);
    assert_eq!(observed_header.topic, topic);
    assert_eq!(processor.calls.load(Ordering::Acquire), 1);

    let response = harness.receive_one().await;
    assert_response(response, ARRIVAL_OPAQUE, true);
    service.shutdown().await;
    drop(service);
    harness.finish().await;
}

#[tokio::test]
async fn notification_v1_suspension_timeout_task_group_wake_writes_one_raw_frame() {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let processor = Arc::new(LegacyNotificationProcessor {
        calls: AtomicUsize::new(0),
        observed: observed_tx,
    });
    let service = service(&processor);
    PopLongPollingService::start(&service).await;
    let mut harness = LegacySocketHarness::new("notification-v1-timeout-acceptance").await;
    let topic = CheetahString::from_static_str("notification-v1-timeout-topic");
    let group = CheetahString::from_static_str("notification-v1-timeout-group");
    let born_time = i64::try_from(current_millis()).expect("legacy Notification wall clock fits i64");
    let header = notification_header(&topic, &group, born_time, 51);
    let polling_header = PollingHeader::new_from_notification_request_header(&header);
    let mut command = request(header, TIMEOUT_OPAQUE);
    assert_eq!(
        service.polling(harness.context.clone(), &mut command, polling_header, None, None,),
        PollingResult::PollingSuc
    );
    let cutoff = u64::try_from(born_time + 1).expect("positive legacy Notification cutoff");
    while current_millis() <= cutoff {
        tokio::task::yield_now().await;
    }

    let (opaque, observed_header) = tokio::time::timeout(Duration::from_secs(1), observed_rx.recv())
        .await
        .expect("legacy Notification timeout scan remains bounded")
        .expect("observe legacy Notification timeout wake");
    assert_eq!(opaque, TIMEOUT_OPAQUE);
    assert_eq!(observed_header.consumer_group, group);
    assert_eq!(observed_header.topic, topic);
    assert_eq!(processor.calls.load(Ordering::Acquire), 1);

    let response = harness.receive_one().await;
    assert_response(response, TIMEOUT_OPAQUE, false);
    service.shutdown().await;
    drop(service);
    harness.finish().await;
}
