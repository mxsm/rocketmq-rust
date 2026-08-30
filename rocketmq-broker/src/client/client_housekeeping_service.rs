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

use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ScheduledTaskGroup;
use rocketmq_runtime::ScheduledTaskSnapshot;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskGroupLifecycleState;
use rocketmq_store::BrokerStatsManager;
use rocketmq_transport::api::SessionCloseReason;
use rocketmq_transport::api::SessionId;
use rocketmq_transport::api::SessionLifecycleListener;
use rocketmq_transport::api::SessionView;
use tokio::sync::Notify;
use tracing::debug;
use tracing::warn;

use crate::broker_runtime::broker_task_group_or_current;
use crate::client::manager::consumer_manager::ConsumerConnectionHousekeeping;
use crate::client::manager::producer_manager::ProducerConnectionHousekeeping;

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

pub struct ClientHousekeepingService {
    producer_housekeeping: ProducerConnectionHousekeeping,
    consumer_housekeeping: ConsumerConnectionHousekeeping,
    broker_stats_manager: Arc<BrokerStatsManager>,
    service_context: Option<ChildServiceContext>,
    shutdown: Arc<Notify>,
    shutdown_requested: Arc<AtomicBool>,
    task_group: Arc<Mutex<Option<TaskGroup>>>,
    scheduled_tasks: Arc<Mutex<Option<ScheduledTaskGroup>>>,
}

impl Clone for ClientHousekeepingService {
    fn clone(&self) -> Self {
        Self {
            producer_housekeeping: self.producer_housekeeping.clone(),
            consumer_housekeeping: self.consumer_housekeeping.clone(),
            broker_stats_manager: Arc::clone(&self.broker_stats_manager),
            service_context: self.service_context.clone(),
            shutdown: self.shutdown.clone(),
            shutdown_requested: self.shutdown_requested.clone(),
            task_group: self.task_group.clone(),
            scheduled_tasks: self.scheduled_tasks.clone(),
        }
    }
}

impl ClientHousekeepingService {
    pub fn new(
        producer_housekeeping: ProducerConnectionHousekeeping,
        consumer_housekeeping: ConsumerConnectionHousekeeping,
        broker_stats_manager: Arc<BrokerStatsManager>,
        service_context: Option<ChildServiceContext>,
    ) -> Self {
        Self {
            producer_housekeeping,
            consumer_housekeeping,
            broker_stats_manager,
            service_context,
            shutdown: Arc::new(Notify::new()),
            shutdown_requested: Arc::new(AtomicBool::new(false)),
            task_group: Arc::new(Mutex::new(None)),
            scheduled_tasks: Arc::new(Mutex::new(None)),
        }
    }

    pub fn start(&self) {
        if self.shutdown_requested.load(Ordering::Acquire) {
            debug!("Broker client housekeeping service is already shutting down");
            return;
        }

        let Some(task_group) = self.task_group() else {
            return;
        };

        if self.task_count() > 0 {
            debug!("Broker client housekeeping service is already running");
            return;
        }

        let broker_runtime_inner = self.clone();
        let scheduled_tasks = ScheduledTaskGroup::new(task_group.clone());
        if let Err(error) = scheduled_tasks.schedule_fixed_rate_no_overlap(
            ScheduledTaskConfig::fixed_rate_no_overlap(
                "broker.client-housekeeping.scan",
                tokio::time::Duration::from_millis(10_000),
            ),
            move || {
                let broker_runtime_inner = broker_runtime_inner.clone();
                async move {
                    broker_runtime_inner.scan_inactive_sessions().await;
                }
            },
        ) {
            warn!(?error, "failed to spawn broker client housekeeping task");
            return;
        }
        *self.scheduled_tasks.lock() = Some(scheduled_tasks);
    }

    pub async fn shutdown(&self) {
        let _ = self.shutdown_with_report().await;
    }

    pub async fn shutdown_with_report(&self) -> Option<ShutdownReport> {
        self.shutdown_requested.store(true, Ordering::Release);
        self.shutdown.notify_waiters();
        self.scheduled_tasks.lock().take();
        let task_group = { self.task_group.lock().take() };
        if let Some(task_group) = task_group {
            let report = task_group.shutdown(SHUTDOWN_TIMEOUT).await;
            if !report.is_healthy() {
                warn!(
                    report = %report.to_json(),
                    "Broker client housekeeping task shutdown report is unhealthy"
                );
            }
            return Some(report);
        }
        None
    }

    pub(crate) async fn scan_inactive_sessions(&self) {
        let retirements = self
            .producer_housekeeping
            .scan_inactive_sessions()
            .into_iter()
            .chain(self.consumer_housekeeping.scan_inactive_sessions())
            .collect::<Vec<_>>();
        let mut closed_sessions = HashSet::with_capacity(retirements.len());
        for retirement in retirements {
            if !closed_sessions.insert(retirement.session_id()) {
                continue;
            }
            let outcome = retirement.retire(SessionCloseReason::HeartbeatTimeout).await;
            debug!(?outcome, "expired client session retirement completed");
        }
    }

    fn task_group(&self) -> Option<TaskGroup> {
        let mut task_group = self.task_group.lock();
        if let Some(group) = task_group.as_ref() {
            if group.lifecycle_state() == TaskGroupLifecycleState::Open {
                return Some(group.clone());
            }
        }

        let group = broker_task_group_or_current(
            self.service_context.as_ref(),
            "rocketmq-broker.client-housekeeping",
            "failed to start broker client housekeeping outside Tokio runtime",
        )?;
        *task_group = Some(group.clone());
        Some(group)
    }

    pub(crate) fn task_count(&self) -> usize {
        self.task_group
            .lock()
            .as_ref()
            .map(TaskGroup::task_count)
            .unwrap_or_default()
    }

    pub(crate) fn schedule_snapshot(&self) -> Vec<ScheduledTaskSnapshot> {
        self.scheduled_tasks
            .lock()
            .as_ref()
            .map(ScheduledTaskGroup::snapshot)
            .unwrap_or_default()
    }
}

impl SessionLifecycleListener for ClientHousekeepingService {
    fn on_session_connected(&self, _session: &SessionView) {
        self.broker_stats_manager.inc_channel_connect_num()
    }

    fn on_session_disconnected(&self, session_id: SessionId) {
        self.producer_housekeeping.release_session_binding(session_id);
        self.producer_housekeeping.do_session_close_event(session_id);
        self.consumer_housekeeping.do_session_close_event(session_id);
        self.broker_stats_manager.inc_channel_close_num()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::net::TcpListener;
    use std::sync::Arc;

    use crate::config::broker_config::BrokerConfig;
    use bytes::Bytes;
    use cheetah_string::CheetahString;
    use rocketmq_model::common::consumer::consume_from_where::ConsumeFromWhere;
    use rocketmq_protocol::code::request_code::RequestCode;
    use rocketmq_protocol::code::response_code::ResponseCode;
    use rocketmq_protocol::protocol::heartbeat::consume_type::ConsumeType;
    use rocketmq_protocol::protocol::heartbeat::heartbeat_data::HeartbeatData;
    use rocketmq_protocol::protocol::heartbeat::message_model::MessageModel;
    use rocketmq_protocol::protocol::heartbeat::producer_data::ProducerData;
    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    use rocketmq_protocol::protocol::LanguageCode;
    use rocketmq_protocol::protocol::RemotingSerializable;
    use rocketmq_runtime::RuntimeContext;
    use rocketmq_store::FlushDiskType;
    use rocketmq_store::MessageStoreConfig;
    use rocketmq_transport::api::AdmissionController;
    use rocketmq_transport::api::AdmissionLimits;
    use rocketmq_transport::api::SessionRegistry;
    use rocketmq_transport::test_support::Connection;

    use crate::broker_runtime::BrokerRuntime;
    use crate::client::client_session_info::ClientSessionInfo;
    use crate::processor::processor_test_support::start_processor_server_with_session_registry;

    use super::*;

    #[tokio::test]
    async fn start_is_idempotent_and_shutdown_stops_background_task() {
        let broker_config = Arc::new(BrokerConfig::default());
        let message_store_config = Arc::new(MessageStoreConfig::default());
        let mut broker_runtime = BrokerRuntime::new(broker_config, message_store_config);
        let inner = broker_runtime.runtime_state_mut();
        let service = ClientHousekeepingService::new(
            inner.producer_manager().connection_housekeeping(),
            inner.consumer_manager().connection_housekeeping(),
            inner.broker_stats_manager_handle(),
            inner.broker_service_context(),
        );

        service.start();
        service.start();
        assert_eq!(service.task_count(), 1);

        let report = service
            .shutdown_with_report()
            .await
            .expect("shutdown should return a report");

        assert!(service.shutdown_requested.load(Ordering::Acquire));
        assert!(service.task_group.lock().is_none());
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test]
    async fn service_context_parents_task_group() {
        let context = RuntimeContext::from_current("broker-client-housekeeping-context-test");
        let broker_service = context.service_context("broker-service");
        let broker_config = Arc::new(BrokerConfig::default());
        let message_store_config = Arc::new(MessageStoreConfig::default());
        let mut broker_runtime =
            BrokerRuntime::new_with_service_context(broker_config, message_store_config, broker_service.clone());
        let inner = broker_runtime.runtime_state_mut();
        let service = ClientHousekeepingService::new(
            inner.producer_manager().connection_housekeeping(),
            inner.consumer_manager().connection_housekeeping(),
            inner.broker_stats_manager_handle(),
            inner.broker_service_context(),
        );

        service.start();

        let task_group = service
            .task_group
            .lock()
            .as_ref()
            .expect("client housekeeping task group should be installed")
            .clone();
        assert_eq!(task_group.parent_id(), Some(broker_service.task_group().id()));

        let report = service
            .shutdown_with_report()
            .await
            .expect("shutdown should return a report");
        assert!(report.is_healthy(), "{}", report.to_json());
        let broker_report = broker_service.task_group().shutdown(Duration::from_secs(1)).await;
        assert!(broker_report.is_healthy(), "{}", broker_report.to_json());
    }

    fn producer_heartbeat(client_id: &str, group: &str, opaque: i32) -> RemotingCommand {
        let heartbeat = HeartbeatData {
            client_id: client_id.into(),
            producer_data_set: HashSet::from([ProducerData {
                group_name: group.into(),
            }]),
            ..HeartbeatData::default()
        };
        RemotingCommand::create_remoting_command(RequestCode::HeartBeat)
            .set_opaque(opaque)
            .set_body(Bytes::from(heartbeat.encode().expect("encode producer heartbeat")))
    }

    fn empty_membership_heartbeat(client_id: &str, opaque: i32) -> RemotingCommand {
        let heartbeat = HeartbeatData {
            client_id: client_id.into(),
            ..HeartbeatData::default()
        };
        RemotingCommand::create_remoting_command(RequestCode::HeartBeat)
            .set_opaque(opaque)
            .set_body(Bytes::from(
                heartbeat.encode().expect("encode empty membership heartbeat"),
            ))
    }

    async fn send_heartbeat(client: &mut Connection, client_id: &str, group: &str, opaque: i32) -> RemotingCommand {
        client
            .send_command(producer_heartbeat(client_id, group, opaque))
            .await
            .expect("send V2 producer heartbeat");
        tokio::time::timeout(Duration::from_secs(5), client.receive_command())
            .await
            .expect("V2 producer heartbeat should complete")
            .expect("session should remain connected")
            .expect("receive V2 producer heartbeat response")
    }

    async fn initialized_runtime(label: &str) -> (tempfile::TempDir, BrokerRuntime) {
        let root = tempfile::tempdir().expect("create Broker session test root");
        let root_path = root.path().to_string_lossy().into_owned();
        let ha_port = TcpListener::bind("127.0.0.1:0")
            .expect("reserve Broker session test HA port")
            .local_addr()
            .expect("read Broker session test HA port")
            .port();
        let broker_config = Arc::new(BrokerConfig {
            store_path_root_dir: root_path.clone().into(),
            auth_config_path: root.path().join("auth.json").to_string_lossy().into_owned().into(),
            ..BrokerConfig::default()
        });
        let message_store_config = Arc::new(MessageStoreConfig {
            store_path_root_dir: root_path.into(),
            flush_disk_type: FlushDiskType::AsyncFlush,
            ha_listen_port: ha_port as usize,
            ..MessageStoreConfig::default()
        });
        let mut runtime = BrokerRuntime::new_with_service_context(
            broker_config,
            message_store_config,
            crate::test_service_context(format!("broker-session-test.{label}")),
        );
        runtime
            .initialize()
            .await
            .expect("initialize Broker session test runtime");
        (root, runtime)
    }

    async fn wait_until_session_absent(registry: &SessionRegistry, session_id: SessionId) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while registry.contains(session_id) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("retired session should unregister promptly");
    }

    #[tokio::test]
    async fn replacement_closes_old_generation_without_removing_the_new_binding() {
        let (_root, mut runtime) = initialized_runtime("replacement").await;
        let (mut processor, _) = runtime
            .init_processor_checked()
            .expect("initialize canonical Broker processor");
        processor.set_auth_disabled_by_validated_config();
        let registry = runtime.session_registry_for_test();
        let producer = runtime.runtime_state_mut().producer_manager().clone_shared_state();
        let group = CheetahString::from_static_str("replacement-producer-group");
        let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
        let (mut old_client, server) = start_processor_server_with_session_registry(
            "broker-session-replacement",
            processor,
            controller,
            Arc::clone(&registry),
        )
        .await;

        let first = send_heartbeat(&mut old_client, "replacement-client", &group, 41).await;
        assert_eq!(
            ResponseCode::from(first.code()),
            ResponseCode::Success,
            "{:?}",
            first.remark()
        );
        let old_session = producer
            .session_registry()
            .get_available_session(Some(&group))
            .expect("first producer session")
            .session_id();

        let mut replacement_client = server.connect().await;
        let replacement = send_heartbeat(&mut replacement_client, "replacement-client", &group, 42).await;
        assert_eq!(ResponseCode::from(replacement.code()), ResponseCode::Success);
        let replacement_session = producer
            .session_registry()
            .get_available_session(Some(&group))
            .expect("replacement producer session")
            .session_id();
        assert_ne!(old_session, replacement_session);

        let old_terminal = tokio::time::timeout(Duration::from_secs(5), old_client.receive_command())
            .await
            .expect("retired producer session should close promptly");
        assert!(old_terminal.is_none(), "retired producer session must reach EOF");
        wait_until_session_absent(&registry, old_session).await;
        assert!(!registry.contains(old_session));
        assert!(registry.contains(replacement_session));
        assert_eq!(
            producer
                .session_registry()
                .get_available_session(Some(&group))
                .expect("replacement must survive old disconnect cleanup")
                .session_id(),
            replacement_session
        );

        server.finish_and_collect(replacement_client).await;
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn empty_membership_replacement_still_closes_old_generation() {
        let (_root, mut runtime) = initialized_runtime("empty-membership-replacement").await;
        let (mut processor, _) = runtime
            .init_processor_checked()
            .expect("initialize canonical Broker processor");
        processor.set_auth_disabled_by_validated_config();
        let registry = runtime.session_registry_for_test();
        let producer = runtime.runtime_state_mut().producer_manager().clone_shared_state();
        let group = CheetahString::from_static_str("empty-replacement-producer-group");
        let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
        let (mut old_client, server) = start_processor_server_with_session_registry(
            "broker-empty-session-replacement",
            processor,
            controller,
            Arc::clone(&registry),
        )
        .await;

        let first = send_heartbeat(&mut old_client, "empty-replacement-client", &group, 61).await;
        assert_eq!(ResponseCode::from(first.code()), ResponseCode::Success);
        let old_session = producer
            .session_registry()
            .get_available_session(Some(&group))
            .expect("first producer session")
            .session_id();

        let mut replacement_client = server.connect().await;
        replacement_client
            .send_command(empty_membership_heartbeat("empty-replacement-client", 62))
            .await
            .expect("send empty membership replacement heartbeat");
        let replacement = tokio::time::timeout(Duration::from_secs(5), replacement_client.receive_command())
            .await
            .expect("empty membership replacement heartbeat should complete")
            .expect("replacement session should remain connected")
            .expect("receive empty membership replacement response");
        assert_eq!(ResponseCode::from(replacement.code()), ResponseCode::Success);

        let old_terminal = tokio::time::timeout(Duration::from_secs(5), old_client.receive_command())
            .await
            .expect("retired empty-membership session should close promptly");
        assert!(old_terminal.is_none(), "retired producer session must reach EOF");
        wait_until_session_absent(&registry, old_session).await;

        let rebound = send_heartbeat(&mut replacement_client, "empty-replacement-client", &group, 63).await;
        assert_eq!(ResponseCode::from(rebound.code()), ResponseCode::Success);
        let replacement_session = producer
            .session_registry()
            .get_available_session(Some(&group))
            .expect("replacement producer session should register after the empty heartbeat")
            .session_id();
        assert_ne!(old_session, replacement_session);
        assert!(registry.contains(replacement_session));

        server.finish_and_collect(replacement_client).await;
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn expired_session_cannot_rebind_before_retirement_closes_transport() {
        let (_root, mut runtime) = initialized_runtime("expiry-rebind-race").await;
        let (mut processor, _) = runtime
            .init_processor_checked()
            .expect("initialize canonical Broker processor");
        processor.set_auth_disabled_by_validated_config();
        let registry = runtime.session_registry_for_test();
        let producer = runtime.runtime_state_mut().producer_manager().clone_shared_state();
        let group = CheetahString::from_static_str("expiry-rebind-producer-group");
        let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
        let (mut client, server) = start_processor_server_with_session_registry(
            "broker-session-expiry-rebind",
            processor,
            controller,
            Arc::clone(&registry),
        )
        .await;

        let accepted = send_heartbeat(&mut client, "expiry-rebind-client", &group, 71).await;
        assert_eq!(
            ResponseCode::from(accepted.code()),
            ResponseCode::Success,
            "{:?}",
            accepted.remark()
        );
        let session_id = producer
            .session_registry()
            .get_available_session(Some(&group))
            .expect("registered producer session")
            .session_id();

        producer.expire_session_for_test(session_id);
        let retirements = producer.connection_housekeeping().scan_inactive_sessions();
        assert_eq!(retirements.len(), 1);
        assert!(!producer.group_online(group.as_str()));

        let rejected = send_heartbeat(&mut client, "expiry-rebind-client", &group, 72).await;
        assert_ne!(ResponseCode::from(rejected.code()), ResponseCode::Success);
        assert!(!producer.group_online(group.as_str()));

        retirements
            .into_iter()
            .next()
            .expect("expired session retirement")
            .retire(SessionCloseReason::HeartbeatTimeout)
            .await;
        let terminal = tokio::time::timeout(Duration::from_secs(5), client.receive_command())
            .await
            .expect("expired client session should close promptly");
        assert!(terminal.is_none(), "expired producer session must reach EOF");
        wait_until_session_absent(&registry, session_id).await;

        server.finish().await;
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn identity_conflict_is_rejected_and_expiry_deduplicates_typed_close() {
        let (_root, mut runtime) = initialized_runtime("expiry").await;
        let (mut processor, _) = runtime
            .init_processor_checked()
            .expect("initialize canonical Broker processor");
        processor.set_auth_disabled_by_validated_config();
        let registry = runtime.session_registry_for_test();
        let housekeeping = runtime.client_housekeeping_service_for_test();
        let producer = runtime.runtime_state_mut().producer_manager().clone_shared_state();
        let consumer = runtime.runtime_state_mut().consumer_manager().clone_shared_state();
        let producer_group = CheetahString::from_static_str("expiry-producer-group");
        let rejected_group = CheetahString::from_static_str("identity-conflict-group");
        let consumer_group = CheetahString::from_static_str("expiry-consumer-group");
        let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
        let (mut client, server) = start_processor_server_with_session_registry(
            "broker-session-expiry",
            processor,
            controller,
            Arc::clone(&registry),
        )
        .await;

        let accepted = send_heartbeat(&mut client, "stable-client", &producer_group, 51).await;
        assert_eq!(ResponseCode::from(accepted.code()), ResponseCode::Success);
        let session_id = producer
            .session_registry()
            .get_available_session(Some(&producer_group))
            .expect("registered producer session")
            .session_id();

        let rejected = send_heartbeat(&mut client, "conflicting-client", &rejected_group, 52).await;
        assert_ne!(ResponseCode::from(rejected.code()), ResponseCode::Success);
        assert!(producer.group_online(producer_group.as_str()));
        assert!(!producer.group_online(rejected_group.as_str()));

        assert!(consumer.client_registration().register_consumer_session_without_sub(
            &consumer_group,
            ClientSessionInfo::new(
                session_id,
                CheetahString::from_static_str("stable-client"),
                None,
                LanguageCode::RUST,
                1,
            ),
            ConsumeType::ConsumePassively,
            MessageModel::Clustering,
            ConsumeFromWhere::ConsumeFromLastOffset,
            true,
        ));
        consumer
            .client_registration()
            .notify_consumer_ids_changed(&HashSet::from([consumer_group.clone()]))
            .await;
        let notification = tokio::time::timeout(Duration::from_secs(5), client.receive_command())
            .await
            .expect("typed consumer notification should arrive")
            .expect("session should remain connected for typed push")
            .expect("receive typed consumer notification");
        assert_eq!(
            RequestCode::from(notification.code()),
            RequestCode::NotifyConsumerIdsChanged
        );

        producer.expire_session_for_test(session_id);
        consumer.connection_housekeeping().expire_session_for_test(session_id);
        housekeeping.scan_inactive_sessions().await;
        let terminal = tokio::time::timeout(Duration::from_secs(5), client.receive_command())
            .await
            .expect("expired client session should close promptly");
        assert!(terminal.is_none(), "expired client session must reach EOF");
        wait_until_session_absent(&registry, session_id).await;
        assert!(!registry.contains(session_id));
        assert!(!producer.group_online(producer_group.as_str()));
        assert!(consumer.get_consumer_group_info(&consumer_group).is_none());

        server.finish().await;
        runtime.shutdown().await;
    }
}
