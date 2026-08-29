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

#[cfg(feature = "rocksdb_store")]
use std::collections::HashSet;
use std::future::Future;
use std::net::SocketAddr;
#[cfg(feature = "rocksdb_store")]
use std::path::Path;
#[cfg(feature = "rocksdb_store")]
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::Weak;
use std::time::Duration;
use std::time::Instant;

use crate::config::broker_config::BrokerConfig;
use crate::config::config_manager::ConfigManager;
use crate::config::error::BrokerConfigError;
use crate::config::validated::ValidatedBrokerConfig;
use cheetah_string::CheetahString;
use rocketmq_auth::AclClientRpcHook;
use rocketmq_auth::AuthConfig;
use rocketmq_auth::AuthMetricsSnapshot;
use rocketmq_auth::AuthRuntime;
use rocketmq_auth::AuthRuntimeBuilder;
use rocketmq_auth::SignatureAlgorithm;
use rocketmq_model::common::broker::broker_role::BrokerRole;
use rocketmq_model::common::config::TopicConfig;
use rocketmq_model::common::mix_all;
use rocketmq_model::common::mix_all::MASTER_ID;
use rocketmq_observability::TelemetryHandle;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::protocol::body::broker_body::broker_member_group::BrokerMemberGroup;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::common::util_all::compute_next_morning_time_millis;
use rocketmq_runtime::schedule::simple_scheduler::ScheduledTaskManager;
use rocketmq_runtime::BlockingExecutor;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::MetadataDeadline;
use rocketmq_runtime::MetadataIoActor;
use rocketmq_runtime::MetadataIoConfig;
use rocketmq_runtime::MetadataIoError;
use rocketmq_runtime::ResourceBudget;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use rocketmq_security_api::MaintenanceAuthorizer;
use rocketmq_store::BrokerStats;
use rocketmq_store::BrokerStatsManager;
use rocketmq_store::BrokerStorePort;
use rocketmq_store::CommitLogDispatcher;
use rocketmq_store::MessageStoreConfig;
use rocketmq_store::MessageStoreShutdownReport;
use rocketmq_store::StoreError;
use rocketmq_store::StoreErrorKind;
use rocketmq_store::StoreFactory;
use rocketmq_store::StoreFactoryConfig;
use rocketmq_store::StoreOperation;
use rocketmq_store::StorePorts;
#[cfg(all(test, feature = "rocksdb_store"))]
use rocketmq_store::StoreType;
use rocketmq_store::TimerMessageStore;
use rocketmq_transport::api::v1::ChannelEventListener;
use rocketmq_transport::api::v1::ServerConfig;
use rocketmq_transport::api::v1::TransportClientConfig;
use rocketmq_transport::api::v1::TransportServer;
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::auth::auth_admin_service::AuthAdminService;
use crate::broker::broker_admin_runtime::BrokerAdminRuntime;
use crate::broker::broker_control_plane::BrokerControllerRuntime;
use crate::broker::broker_control_plane::BrokerControllerState;
use crate::broker::broker_control_plane::BrokerMembershipState;
use crate::broker::broker_hook::BrokerShutdownHook;
use crate::broker::broker_pre_online_capability::BrokerOnlineRoleState;
use crate::broker::broker_pre_online_capability::BrokerOnlineTransitionCapability;
use crate::broker::broker_pre_online_capability::BrokerPreOnlineContext;
use crate::broker::broker_pre_online_capability::BrokerPreOnlinePolicy;
use crate::broker::broker_pre_online_capability::BrokerPreOnlineStoreCapability;
use crate::broker::broker_pre_online_capability::BrokerRegistrationCapability;
use crate::broker::broker_pre_online_capability::BrokerSpecialServiceCapability;
use crate::broker::broker_pre_online_service::BrokerPreOnlineService;
use crate::broker::broker_registration_runtime::BrokerRegistrationError;
use crate::broker::broker_registration_runtime::BrokerRegistrationRuntime;
use crate::broker::broker_registration_runtime::BrokerRegistrationStatus;
use crate::broker::broker_runtime_config_state::BrokerRuntimeConfigState;
use crate::broker::broker_state_observer::ConsumerStateGetter;
use crate::broker::broker_state_observer::ProducerStateGetter;
use crate::client::client_housekeeping_service::ClientHousekeepingService;
use crate::client::consumer_ids_change_listener::ConsumerIdsChangeListener;
use crate::client::default_consumer_ids_change_listener::DefaultConsumerIdsChangeListener;
use crate::client::manager::consumer_manager::ConsumerManager;
use crate::client::manager::producer_manager::ProducerManager;
use crate::client::net::broker_to_client::Broker2Client;
use crate::client::rebalance::rebalance_lock_manager::RebalanceLockManager;
use crate::coldctr::cold_data_cg_ctr_service::ColdDataCgCtrService;
#[cfg(feature = "rocksdb_store")]
use crate::config::rocksdb_manager::RocksDbBrokerConfigManager;
#[cfg(feature = "rocksdb_store")]
use crate::config::rocksdb_manager::RocksDbBrokerConfigManagerConfig;
#[cfg(feature = "rocksdb_store")]
use crate::config::rocksdb_manager::RocksDbBrokerConfigStorageLayout;
use crate::controller::replicas_manager::ReplicasManager;
use crate::failover::escape_bridge::EscapeBridge;
use crate::failover::escape_bridge_capability::EscapeBridgePolicyState;
use crate::filter::commit_log_dispatcher_calc_bit_map::CommitLogDispatcherCalcBitMap;
use crate::filter::manager::consumer_filter_manager::ConsumerFilterManager;
use crate::hook::batch_check_before_put_message::BatchCheckBeforePutMessageHook;
use crate::hook::check_before_put_message::CheckBeforePutMessageHook;
use crate::hook::schedule_message_hook::ScheduleMessageHook;
use crate::latency::broker_fast_failure::BrokerFastFailure;
use crate::lifecycle::BrokerComponent;
use crate::lifecycle::BrokerReadiness;
use crate::lifecycle::BrokerStartupError;
use crate::lifecycle::StartupJournal;
use crate::lite::lite_event_dispatcher::LiteEventDispatcher;
use crate::lite::lite_lifecycle_manager::LiteLifecycleManager;
use crate::lite::lite_sharding::LiteShardingView;
use crate::long_polling::long_polling_service::pop_lite_long_polling_service::PopLiteLongPollingPolicy;
use crate::long_polling::long_polling_service::pop_lite_long_polling_service::PopLiteLongPollingServiceContext;
use crate::long_polling::long_polling_service::pop_long_polling_service::PopLongPollingPolicy;
use crate::long_polling::long_polling_service::pop_long_polling_service::PopLongPollingServiceContext;
use crate::long_polling::long_polling_service::pull_request_hold_service::PullRequestHoldService;
use crate::long_polling::notify_message_arriving_listener::NotifyMessageArrivingListener;
use crate::offset::manager::broadcast_offset_manager::BroadcastOffsetManager;
use crate::offset::manager::broadcast_offset_manager::SCAN_INTERVAL;
use crate::offset::manager::consumer_offset_manager::ConsumerOffsetManager;
use crate::offset::manager::consumer_order_info_manager::ConsumerOrderInfoManager;
use crate::out_api::broker_outer_api::BrokerOuterAPI;
use crate::plugin::broker_attached_plugin::BrokerAttachedPlugin;
use crate::processor::ack_message_processor::AckMessageOffsetCapability;
use crate::processor::ack_message_processor::AckMessageOrderCapability;
use crate::processor::ack_message_processor::AckMessagePolicy;
use crate::processor::ack_message_processor::AckMessagePopCapability;
use crate::processor::ack_message_processor::AckMessageProcessor;
use crate::processor::ack_message_processor::AckMessageProcessorContext;
use crate::processor::ack_message_processor::AckMessageStoreCapability;
use crate::processor::admin_broker_processor::AdminBrokerProcessor;
use crate::processor::change_invisible_time_processor::ChangeInvisibleTimeLiteCapability;
use crate::processor::change_invisible_time_processor::ChangeInvisibleTimeOrderCapability;
use crate::processor::change_invisible_time_processor::ChangeInvisibleTimePolicy;
use crate::processor::change_invisible_time_processor::ChangeInvisibleTimePopCapability;
use crate::processor::change_invisible_time_processor::ChangeInvisibleTimeProcessor;
use crate::processor::change_invisible_time_processor::ChangeInvisibleTimeProcessorContext;
use crate::processor::change_invisible_time_processor::ChangeInvisibleTimeStoreCapability;
use crate::processor::client_manage_processor::ClientManageProcessor;
use crate::processor::client_manage_processor::ClientManageProcessorContext;
use crate::processor::consumer_manage_processor::ConsumerManageProcessor;
use crate::processor::consumer_manage_processor::ConsumerManageProcessorContext;
use crate::processor::default_pull_message_result_handler::DefaultPullMessageResultHandler;
use crate::processor::end_transaction_processor::EndTransactionPolicy;
use crate::processor::end_transaction_processor::EndTransactionProcessor;
use crate::processor::end_transaction_processor::EndTransactionProcessorContext;
use crate::processor::end_transaction_processor::EndTransactionStoreCapability;
use crate::processor::lite_manager_processor::LiteManagerContext;
use crate::processor::lite_manager_processor::LiteManagerOffsetCapability;
use crate::processor::lite_manager_processor::LiteManagerPolicy;
use crate::processor::lite_manager_processor::LiteManagerProcessor;
use crate::processor::lite_manager_processor::LiteManagerStoreCapability;
use crate::processor::lite_subscription_ctl_processor::LiteSubscriptionCtlContext;
use crate::processor::lite_subscription_ctl_processor::LiteSubscriptionCtlPolicy;
use crate::processor::lite_subscription_ctl_processor::LiteSubscriptionCtlProcessor;
use crate::processor::maintenance_request_processor::MaintenanceRequestProcessor;
use crate::processor::notification_processor::NotificationPolicy;
use crate::processor::notification_processor::NotificationPopOffsetCapability;
use crate::processor::notification_processor::NotificationProcessor;
use crate::processor::notification_processor::NotificationProcessorContext;
use crate::processor::notification_processor::NotificationStoreCapability;
use crate::processor::peek_message_processor::PeekMessagePolicy;
use crate::processor::peek_message_processor::PeekMessageProcessor;
use crate::processor::peek_message_processor::PeekMessageProcessorContext;
use crate::processor::peek_message_processor::PeekMessageStoreCapability;
use crate::processor::peek_message_processor::PeekPopOffsetCapability;
use crate::processor::polling_info_processor::PollingInfoProcessor;
use crate::processor::pop_inflight_message_counter::PopInflightMessageCounter;
use crate::processor::pop_lite_message_processor::PopLiteMessagePolicy;
use crate::processor::pop_lite_message_processor::PopLiteMessageProcessor;
use crate::processor::pop_lite_message_processor::PopLiteMessageProcessorContext;
use crate::processor::pop_lite_message_processor::PopLiteMessageStoreCapability;
use crate::processor::pop_lite_message_processor::PopLiteOffsetCapability;
use crate::processor::pop_message_processor::capability::PopBufferMergeContext;
use crate::processor::pop_message_processor::capability::PopConsumerCapability;
use crate::processor::pop_message_processor::capability::PopMessageProcessorContext;
use crate::processor::pop_message_processor::capability::PopOrderCapability;
use crate::processor::pop_message_processor::capability::PopPolicyState;
use crate::processor::pop_message_processor::capability::PopReviveContext;
use crate::processor::pop_message_processor::capability::PopStoreCapability;
use crate::processor::pop_message_processor::PopMessageProcessor;
use crate::processor::pop_message_processor::QueueLockManager;
use crate::processor::processor_service::PopReviveService;
use crate::processor::pull_message_processor::capability::PullMessagePolicyState;
use crate::processor::pull_message_processor::capability::PullMessageProcessorContext;
use crate::processor::pull_message_processor::capability::PullMessageStoreCapability;
use crate::processor::pull_message_processor::PullMessageProcessor;
use crate::processor::query_assignment_processor::QueryAssignmentProcessor;
use crate::processor::query_message_processor::QueryMessageProcessor;
use crate::processor::query_message_processor::QueryMessageStoreCapability;
use crate::processor::recall_message_processor::RecallMessagePolicy;
use crate::processor::recall_message_processor::RecallMessageProcessor;
use crate::processor::recall_message_processor::RecallMessageProcessorContext;
use crate::processor::recall_message_processor::RecallMessageStoreCapability;
use crate::processor::reply_message_processor::ReplyMessageProcessor;
use crate::processor::send_message_processor::capability::SendMessagePolicyState;
use crate::processor::send_message_processor::capability::SendMessageProcessorContext;
use crate::processor::send_message_processor::capability::SendMessageStoreCapability;
use crate::processor::send_message_processor::capability::SendMessageTopicCapability;
use crate::processor::send_message_processor::SendMessageProcessor;
use crate::processor::v2::BrokerProcessorTypeV2;
use crate::processor::v2::BrokerRequestProcessorV2;
use crate::processor::BrokerProcessorType;
use crate::processor::BrokerRequestProcessor;
use crate::schedule::schedule_message_service::ScheduleMessageService;
use crate::slave::slave_synchronize::SlaveMasterAddress;
use crate::slave::slave_synchronize::SlaveSynchronize;
use crate::slave::slave_synchronize::SlaveSynchronizeContext;
use crate::slave::slave_synchronize::SlaveSynchronizePolicy;
use crate::slave::slave_synchronize::SlaveTimerStoreCapability;
use crate::subscription::lite_subscription_registry::LiteSubscriptionRegistry;
use crate::subscription::manager::subscription_group_manager::SubscriptionGroupManager;
use crate::subscription::manager::subscription_group_manager::SubscriptionGroupManagerConfig;
use crate::topic::manager::topic_config_coordinator::TopicConfigCoordinator;
use crate::topic::manager::topic_config_coordinator::TopicConfigCoordinatorShutdownReport;
use crate::topic::manager::topic_config_coordinator::TopicRegistrationAction;
use crate::topic::manager::topic_config_coordinator::TopicRegistrationFuture;
use crate::topic::manager::topic_config_manager::TopicConfigCreation;
use crate::topic::manager::topic_config_manager::TopicConfigManager;
use crate::topic::manager::topic_config_manager::TopicConfigUpdate;
use crate::topic::manager::topic_queue_mapping_manager::TopicQueueMappingManager;
use crate::topic::manager::topic_route_info_manager::TopicRouteInfoManager;
use crate::topic::topic_queue_mapping_clean_service::TopicQueueMappingCleanConfig;
use crate::topic::topic_queue_mapping_clean_service::TopicQueueMappingCleanService;
use crate::transaction::queue::default_transactional_message_check_listener::DefaultTransactionalMessageCheckListener;
use crate::transaction::queue::default_transactional_message_service::DefaultTransactionalMessageService;
use crate::transaction::queue::transaction_message_store::TransactionMessageStore;
use crate::transaction::queue::transaction_topic_registration::TransactionTopicRegistration;
use crate::transaction::queue::transaction_topic_registration::TransactionTopicRegistrationContext;
use crate::transaction::queue::transactional_message_bridge::TransactionalMessageBridge;
use crate::transaction::queue::transactional_message_bridge::TransactionalMessageBridgeContext;
use crate::transaction::transactional_message_check_service::TransactionalMessageCheckService;

mod composition;
mod control_plane;
mod data_plane;
mod deferred;
pub(crate) mod deferred_producer;
mod lifecycle;
mod metadata;
mod request_pipeline;
mod shutdown_report;

use composition::BrokerComposition;
use deferred::BrokerDeferredResourceSnapshot;
use lifecycle::BrokerLifecycle;
#[cfg(test)]
use shutdown_report::record_message_store_shutdown_outcome;
pub(crate) use shutdown_report::BrokerBasicServiceShutdownReport;
pub(crate) use shutdown_report::BrokerRemotingServerReport;
use shutdown_report::BrokerRemotingServerReportReceiver;
pub(crate) use shutdown_report::BrokerRemotingServerShutdownReport;
pub(crate) use shutdown_report::BrokerShutdownComponentReport;
use shutdown_report::BrokerShutdownProgress;

pub(crate) type BrokerMessageStore = StorePorts;

type DefaultServerProcessor =
    BrokerRequestProcessor<BrokerMessageStore, DefaultTransactionalMessageService<BrokerMessageStore>>;

type FasterServerProcessor =
    BrokerRequestProcessor<BrokerMessageStore, DefaultTransactionalMessageService<BrokerMessageStore>>;

type DefaultServerProcessorV2 = BrokerRequestProcessorV2<
    BrokerProcessorTypeV2<BrokerMessageStore, DefaultTransactionalMessageService<BrokerMessageStore>>,
>;

type BrokerScheduledTasks = ScheduledTaskManager;

pub(crate) async fn complete_topic_config_creation<F, Fut>(
    coordinator: Arc<TopicConfigCoordinator>,
    creation: TopicConfigCreation,
    start_time: Instant,
    async_persist: bool,
    register_update: F,
) -> Arc<TopicConfig>
where
    F: FnOnce(TopicConfigUpdate) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let TopicConfigCreation {
        topic_config,
        update,
        register,
        created,
    } = creation;

    if let Some(update) = update {
        let registration = register.then(|| {
            Box::new(move || {
                Box::pin(async move {
                    register_update(update).await;
                    Ok(())
                }) as TopicRegistrationFuture
            }) as TopicRegistrationAction
        });
        let result = match (async_persist, registration) {
            (true, Some(registration)) => coordinator.persist_and_register_accepted(registration).await,
            (true, None) => coordinator.persist_accepted().await,
            (false, Some(registration)) => coordinator.persist_and_register_wait(registration).await,
            (false, None) => coordinator.persist_and_wait().await,
        };
        if let Err(error) = result {
            warn!(?error, "failed to coordinate topic create persistence and registration");
        }
    }

    if created {
        coordinator.manager().record_topic_create_latency(start_time);
    }
    topic_config
}

const SCHEDULED_TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const BROKER_OUTER_API_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const BROKER_BASIC_SERVICE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(35);
const REMOTING_SERVER_STARTUP_TIMEOUT: Duration = Duration::from_secs(10);

async fn await_shutdown_deadline<T, F>(deadline: ShutdownDeadline, future: F) -> Result<T, Duration>
where
    F: Future<Output = T>,
{
    let started = Instant::now();
    tokio::time::timeout(deadline.remaining(), future)
        .await
        .map_err(|_| started.elapsed())
}

#[derive(Debug)]
enum BrokerBlockingShutdownError {
    MissingServiceContext,
    Spawn(rocketmq_runtime::RuntimeError),
    Execution(rocketmq_runtime::RuntimeError),
    ResultChannelClosed,
    TimedOut,
}

impl BrokerBlockingShutdownError {
    fn is_timed_out(&self) -> bool {
        matches!(self, Self::TimedOut)
    }

    fn detail(&self) -> String {
        match self {
            Self::MissingServiceContext => "missing shutdown service context".to_string(),
            Self::Spawn(error) => format!("failed to spawn owned shutdown task: {error}"),
            Self::Execution(error) => format!("blocking shutdown operation failed: {error}"),
            Self::ResultChannelClosed => "owned shutdown task closed without a result".to_string(),
            Self::TimedOut => "timed out".to_string(),
        }
    }
}

async fn await_remoting_server_startup(
    listener: &'static str,
    receiver: oneshot::Receiver<Result<SocketAddr, rocketmq_transport::api::v1::ServerStartError>>,
    timeout: Duration,
) -> Result<SocketAddr, BrokerStartupError> {
    let startup = tokio::time::timeout(timeout, receiver)
        .await
        .map_err(|_| BrokerStartupError::ListenerStartup {
            listener,
            detail: format!("startup acknowledgement exceeded {} ms", timeout.as_millis()),
        })?
        .map_err(|_| BrokerStartupError::ListenerStartupDropped { listener })?;
    startup.map_err(|error| BrokerStartupError::ListenerStartup {
        listener,
        detail: error.to_string(),
    })
}

async fn run_shutdown_blocking_operation<T, F>(
    service_context: &ChildServiceContext,
    deadline: ShutdownDeadline,
    name: &'static str,
    operation: F,
) -> Result<T, BrokerBlockingShutdownError>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let blocking = service_context.metadata_io().clone();
    let (result_tx, result_rx) = oneshot::channel();
    service_context
        .spawn_service(format!("{name}.owner"), async move {
            let result = blocking.spawn_io(name, operation).await;
            let _ = result_tx.send(result);
        })
        .map_err(BrokerBlockingShutdownError::Spawn)?;

    match tokio::time::timeout(deadline.remaining(), result_rx).await {
        Ok(Ok(Ok(value))) => Ok(value),
        Ok(Ok(Err(error))) => Err(BrokerBlockingShutdownError::Execution(error)),
        Ok(Err(_closed)) => Err(BrokerBlockingShutdownError::ResultChannelClosed),
        Err(_elapsed) => Err(BrokerBlockingShutdownError::TimedOut),
    }
}

async fn persist_config_manager<T>(
    manager: Arc<T>,
    resource: &'static str,
    metadata_io: Option<MetadataIoActor>,
    blocking: BlockingExecutor,
    deadline: MetadataDeadline,
) -> rocketmq_error::RocketMQResult<()>
where
    T: ConfigManager + Send + Sync + 'static,
{
    if manager.supports_metadata_io_actor() {
        if let Some(metadata_io) = metadata_io {
            let content = manager.encode_pretty(true);
            if content.is_empty() {
                return Ok(());
            }
            metadata_io
                .submit_next_durable(resource, manager.config_file_path(), content.into_bytes(), deadline)
                .await
                .map_err(crate::runtime_to_rocketmq_error)?;
            return Ok(());
        }
    }

    blocking
        .spawn_io(resource, move || manager.persist())
        .await
        .map_err(|error| rocketmq_error::RocketMQError::IO(std::io::Error::other(error)))?
}

enum MessageStoreShutdownOutcome {
    Absent,
    Completed(MessageStoreShutdownReport),
    Failed(StoreError),
    TimedOut,
}

fn build_auth_config(broker_config: &BrokerConfig) -> AuthConfig {
    AuthConfig {
        config_name: broker_config.broker_identity.broker_name.clone(),
        cluster_name: broker_config.broker_identity.broker_cluster_name.clone(),
        auth_config_path: broker_config.auth_config_path.clone(),
        acl_file: broker_config.acl_file.clone(),
        acl_file_watch_enabled: broker_config.acl_file_watch_enabled,
        acl_file_watch_interval_millis: broker_config.acl_file_watch_interval_millis,
        authentication_enabled: broker_config.authentication_enabled,
        authentication_provider: broker_config.authentication_provider.clone(),
        authentication_metadata_provider: broker_config.authentication_metadata_provider.clone(),
        authentication_strategy: broker_config.authentication_strategy.clone(),
        authentication_whitelist: broker_config.authentication_whitelist.clone(),
        init_authentication_user: broker_config.init_authentication_user.clone(),
        inner_client_authentication_credentials: broker_config.inner_client_authentication_credentials.clone(),
        signature_algorithm: SignatureAlgorithm::from_java_name(broker_config.signature_algorithm.as_str())
            .unwrap_or_default(),
        request_timestamp_expired_millis: broker_config.request_timestamp_expired_millis,
        authorization_enabled: broker_config.authorization_enabled,
        authorization_provider: broker_config.authorization_provider.clone(),
        authorization_metadata_provider: broker_config.authorization_metadata_provider.clone(),
        authorization_strategy: broker_config.authorization_strategy.clone(),
        authorization_whitelist: broker_config.authorization_whitelist.clone(),
        maintenance_enabled: broker_config.maintenance_enabled,
        maintenance_policy_path: broker_config.maintenance_policy_path.clone(),
        maintenance_policy_version: broker_config.maintenance_policy_version,
        maintenance_policy_sha256: broker_config.maintenance_policy_sha256.clone(),
        migrate_auth_from_v1_enabled: broker_config.migrate_auth_from_v1_enabled,
        user_cache_max_num: broker_config.user_cache_max_num,
        user_cache_expired_second: broker_config.user_cache_expired_second,
        user_cache_refresh_second: broker_config.user_cache_refresh_second,
        acl_cache_max_num: broker_config.acl_cache_max_num,
        acl_cache_expired_second: broker_config.acl_cache_expired_second,
        acl_cache_refresh_second: broker_config.acl_cache_refresh_second,
        stateful_authentication_cache_max_num: broker_config.stateful_authentication_cache_max_num,
        stateful_authentication_cache_expired_second: broker_config.stateful_authentication_cache_expired_second,
        stateful_authorization_cache_max_num: broker_config.stateful_authorization_cache_max_num,
        stateful_authorization_cache_expired_second: broker_config.stateful_authorization_cache_expired_second,
        stateful_authorization_cache_negative_enable: broker_config.stateful_authorization_cache_negative_enable,
    }
}

pub fn build_broker_telemetry_bootstrap_config(
    broker_config: &BrokerConfig,
) -> rocketmq_observability::TelemetryBootstrapConfig {
    let mut observability = build_broker_observability_config(broker_config);
    observability.subscriber_install_policy = rocketmq_observability::SubscriberInstallPolicy::Required;

    rocketmq_observability::TelemetryBootstrapConfig {
        observability,
        logging: build_broker_logging_config(broker_config),
    }
}

/// Builds the Broker telemetry defaults and applies one canonical file override section.
#[must_use]
pub fn build_broker_telemetry_bootstrap_config_with_overrides(
    broker_config: &BrokerConfig,
    file: &rocketmq_observability::ObservabilityOverrides,
) -> rocketmq_observability::TelemetryBootstrapConfig {
    let mut config = build_broker_telemetry_bootstrap_config(broker_config);
    file.apply_to(&mut config.observability);
    config
}

fn build_broker_observability_config(broker_config: &BrokerConfig) -> rocketmq_observability::ObservabilityConfig {
    rocketmq_observability::ObservabilityConfig {
        service_name: "rocketmq-broker".to_string(),
        service_namespace: "rocketmq".to_string(),
        cluster: broker_config.broker_identity.broker_cluster_name.to_string(),
        node_type: "broker".to_string(),
        node_id: broker_config.broker_identity.get_canonical_name(),
        ..rocketmq_observability::ObservabilityConfig::default()
    }
}

fn build_broker_logging_config(broker_config: &BrokerConfig) -> rocketmq_observability::LoggingConfig {
    let mut config = rocketmq_observability::LoggingConfig::default();
    config.file.directory = std::path::Path::new(broker_config.store_path_root_dir.as_str())
        .join("logs")
        .to_string_lossy()
        .into_owned();
    config.file.file_name_prefix = "rocketmq-broker".to_string();
    config
}

#[cfg(feature = "rocksdb_store")]
struct BrokerRocksDbConfigManagers {
    topic: Arc<RocksDbBrokerConfigManager>,
    consumer_offset: Arc<RocksDbBrokerConfigManager>,
    subscription_group: Arc<RocksDbBrokerConfigManager>,
}

#[cfg(feature = "rocksdb_store")]
impl BrokerRocksDbConfigManagers {
    fn close_all(self) {
        let mut closed = HashSet::new();
        for manager in [self.topic, self.consumer_offset, self.subscription_group] {
            if closed.insert(manager.backend_identity()) {
                manager.close();
            }
        }
    }
}

#[cfg(feature = "rocksdb_store")]
fn open_broker_rocksdb_config_managers(
    broker_config: &BrokerConfig,
    message_store_config: &MessageStoreConfig,
) -> Option<BrokerRocksDbConfigManagers> {
    if !message_store_config.is_enable_rocksdb_store() {
        return None;
    }

    let root = Path::new(broker_config.store_path_root_dir.as_str());
    let use_single = broker_config.use_single_rocksdb_for_all_configs;
    let topic_path = RocksDbBrokerConfigStorageLayout::topic_path(root, use_single);
    let consumer_offset_path = RocksDbBrokerConfigStorageLayout::consumer_offset_path(root, use_single);
    let subscription_group_path = RocksDbBrokerConfigStorageLayout::subscription_group_path(root, use_single);

    if use_single {
        return open_shared_broker_rocksdb_config_managers(topic_path, consumer_offset_path, subscription_group_path);
    }

    if !prepare_rocksdb_config_path_for_json_migration(&topic_path)
        || !prepare_rocksdb_config_path_for_json_migration(&consumer_offset_path)
        || !prepare_rocksdb_config_path_for_json_migration(&subscription_group_path)
    {
        return None;
    }

    let topic = match RocksDbBrokerConfigManager::open(RocksDbBrokerConfigManagerConfig::topic(topic_path)) {
        Ok(manager) => Arc::new(manager),
        Err(error) => {
            error!("Open RocksDB topic config manager failed: {error}");
            return None;
        }
    };
    let consumer_offset =
        match RocksDbBrokerConfigManager::open(RocksDbBrokerConfigManagerConfig::consumer_offset(consumer_offset_path))
        {
            Ok(manager) => Arc::new(manager),
            Err(error) => {
                error!("Open RocksDB consumer offset config manager failed: {error}");
                return None;
            }
        };
    let subscription_group = match RocksDbBrokerConfigManager::open(
        RocksDbBrokerConfigManagerConfig::subscription_group(subscription_group_path),
    ) {
        Ok(manager) => Arc::new(manager),
        Err(error) => {
            error!("Open RocksDB subscription group config manager failed: {error}");
            return None;
        }
    };

    Some(BrokerRocksDbConfigManagers {
        topic,
        consumer_offset,
        subscription_group,
    })
}

#[cfg(feature = "rocksdb_store")]
fn open_shared_broker_rocksdb_config_managers(
    topic_path: PathBuf,
    consumer_offset_path: PathBuf,
    subscription_group_path: PathBuf,
) -> Option<BrokerRocksDbConfigManagers> {
    if !prepare_rocksdb_config_path_for_json_migration(&topic_path) {
        return None;
    }
    let configs = vec![
        RocksDbBrokerConfigManagerConfig::topic(topic_path),
        RocksDbBrokerConfigManagerConfig::consumer_offset(consumer_offset_path),
        RocksDbBrokerConfigManagerConfig::subscription_group(subscription_group_path),
    ];
    let mut managers = match RocksDbBrokerConfigManager::open_shared(configs) {
        Ok(managers) => managers,
        Err(error) => {
            error!("Open shared RocksDB broker config manager failed: {error}");
            return None;
        }
    };
    if managers.len() != 3 {
        error!(
            "Open shared RocksDB broker config manager returned invalid manager count: {}",
            managers.len()
        );
        return None;
    }

    let topic = managers.remove(0);
    let consumer_offset = managers.remove(0);
    let subscription_group = managers.remove(0);
    Some(BrokerRocksDbConfigManagers {
        topic,
        consumer_offset,
        subscription_group,
    })
}

#[cfg(feature = "rocksdb_store")]
fn prepare_rocksdb_config_path_for_json_migration(path: &Path) -> bool {
    if !path.is_file() {
        return true;
    }
    let backup_path = PathBuf::from(format!("{}.bak", path.to_string_lossy()));
    if backup_path.exists() {
        if let Err(error) = std::fs::remove_file(&backup_path) {
            error!(
                "Remove stale broker config backup before RocksDB migration failed: {}",
                error
            );
            return false;
        }
    }
    match std::fs::rename(path, &backup_path) {
        Ok(()) => true,
        Err(error) => {
            error!("Move broker config file before RocksDB migration failed: {}", error);
            false
        }
    }
}

pub(crate) struct BrokerRuntime {
    composition: BrokerComposition,
    lifecycle: BrokerLifecycle,
}

#[cfg(test)]
mod test_support {
    use super::*;

    impl BrokerRuntime {
        pub(crate) fn new(broker_config: Arc<BrokerConfig>, message_store_config: Arc<MessageStoreConfig>) -> Self {
            let validated = ValidatedBrokerConfig::try_from_parts(
                broker_config.as_ref().clone(),
                message_store_config.as_ref().clone(),
            )
            .expect("broker runtime test configuration should be valid");
            Self::new_with_validated_config(Arc::new(validated), crate::test_service_context("broker-runtime"))
        }

        pub(crate) fn new_with_service_context(
            broker_config: Arc<BrokerConfig>,
            message_store_config: Arc<MessageStoreConfig>,
            service_context: ChildServiceContext,
        ) -> Self {
            let validated = ValidatedBrokerConfig::try_from_parts(
                broker_config.as_ref().clone(),
                message_store_config.as_ref().clone(),
            )
            .expect("broker runtime test configuration should be valid");
            Self::new_with_validated_config(Arc::new(validated), service_context)
        }
    }
}

impl BrokerRuntime {}

impl BrokerRuntime {}

pub(crate) struct BrokerRuntimeState<MS: BrokerStorePort> {
    shutdown: Arc<AtomicBool>,
    store_host: SocketAddr,
    broker_addr: CheetahString,
    config_state: BrokerRuntimeConfigState,
    command_factory: RemotingCommandFactory,
    resource_budget: ResourceBudget,
    send_message_policy_state: SendMessagePolicyState,
    pull_message_policy_state: PullMessagePolicyState,
    pop_policy_state: PopPolicyState,
    escape_bridge_policy_state: EscapeBridgePolicyState,
    topic_config_manager: Option<Arc<TopicConfigManager>>,
    topic_config_coordinator: Option<Arc<TopicConfigCoordinator>>,
    topic_queue_mapping_manager: Arc<TopicQueueMappingManager>,
    consumer_offset_manager: Arc<ConsumerOffsetManager<MS>>,
    subscription_group_manager: Option<SubscriptionGroupManager>,
    consumer_filter_manager: Option<ConsumerFilterManager>,
    consumer_order_info_manager: Option<Arc<ConsumerOrderInfoManager>>,
    message_store: Option<Arc<MS>>,
    broker_stats: Option<Arc<BrokerStats<MS>>>,
    schedule_message_service: Option<Arc<ScheduleMessageService<MS>>>,
    timer_message_store: Option<Arc<TimerMessageStore>>,
    lite_event_dispatcher: Arc<LiteEventDispatcher>,
    lite_lifecycle_manager: Arc<LiteLifecycleManager>,
    lite_subscription_registry: Arc<LiteSubscriptionRegistry>,
    broker_outer_api: BrokerOuterAPI,
    producer_manager: ProducerManager,
    consumer_manager: ConsumerManager,
    broadcast_offset_manager: BroadcastOffsetManager,
    broker_stats_manager: Option<Arc<BrokerStatsManager>>,
    topic_queue_mapping_clean_service: Option<TopicQueueMappingCleanService>,
    update_master_haserver_addr_periodically: bool,
    should_start_time: Arc<AtomicU64>,
    online_role_state: Arc<BrokerOnlineRoleState>,
    pull_request_hold_service: Option<Arc<PullRequestHoldService<MS>>>,
    rebalance_lock_manager: RebalanceLockManager,
    broker_member_group: BrokerMembershipState,
    transactional_message_check_listener: Option<DefaultTransactionalMessageCheckListener>,
    transactional_message_check_service: Option<Arc<TransactionalMessageCheckService<MS>>>,
    topic_route_info_manager: Option<TopicRouteInfoManager>,
    escape_bridge: Option<Weak<EscapeBridge<MS>>>,
    pop_inflight_message_counter: PopInflightMessageCounter,
    controller_state: BrokerControllerState,
    broker_fast_failure: BrokerFastFailure,
    log_filter_control: Option<Arc<crate::broker::log_filter_control::BrokerLogFilterControl>>,
    telemetry_handle: TelemetryHandle,
    transport_telemetry: rocketmq_transport::api::v1::TransportTelemetry,
    store_telemetry: rocketmq_store::StoreTelemetry,
    broker_metrics_manager: Option<Arc<crate::metrics::broker_metrics_manager::BrokerMetricsManager>>,
    pop_metrics_manager: Option<Arc<crate::metrics::pop_metrics_manager::PopMetricsManager>>,
    observability_guard: Option<rocketmq_observability::TelemetryRuntimeGuard>,
    #[cfg(feature = "otel-metrics")]
    observability_metrics_initialized: bool,
    cold_data_cg_ctr_service: Option<Arc<ColdDataCgCtrService>>,
    is_schedule_service_start: Arc<AtomicBool>,
    is_transaction_check_service_start: Arc<AtomicBool>,
    client_housekeeping_service: Option<Arc<ClientHousekeepingService>>,
    //Processor
    pop_message_processor: Option<Arc<PopMessageProcessor<MS>>>,
    pop_lite_message_processor: Option<Arc<PopLiteMessageProcessor<MS>>>,
    ack_message_processor: Option<Arc<AckMessageProcessor<MS>>>,
    notification_processor: Option<Arc<NotificationProcessor<MS>>>,
    query_assignment_processor: Option<Arc<QueryAssignmentProcessor>>,
    metadata_io: Option<Result<MetadataIoActor, MetadataIoError>>,
    broker_attached_plugins: Vec<Arc<dyn BrokerAttachedPlugin>>,
    transactional_message_service: Option<Arc<DefaultTransactionalMessageService<MS>>>,
    slave_synchronize: Option<Arc<SlaveSynchronize<MS>>>,
    slave_master_addr: Arc<SlaveMasterAddress>,
    broker_pre_online_service: Option<BrokerPreOnlineService<MS>>,
    service_context: Option<ChildServiceContext>,
    lock: Mutex<()>,
}

pub(crate) fn broker_task_group_or_current(
    service_context: Option<&ChildServiceContext>,
    name: impl Into<Arc<str>>,
    no_runtime_warning: &'static str,
) -> Option<TaskGroup> {
    let name = name.into();
    service_context
        .map(|service_context| service_context.component(name).task_group().clone())
        .or_else(|| {
            warn!("{no_runtime_warning}");
            None
        })
}

#[cfg(test)]
#[path = "../tests/broker_runtime/unit.rs"]
mod tests;
