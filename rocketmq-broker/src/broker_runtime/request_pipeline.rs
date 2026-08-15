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

use super::*;

mod startup;

pub(super) struct BrokerRequestPipeline {
    pub(super) proxy_request_processor: Option<DefaultServerProcessor>,
    pub(super) authorized_dispatcher:
        Option<Arc<rocketmq_transport::api::v1::AuthorizedCommandDispatcher<DefaultServerProcessor>>>,
    pub(super) auth_runtime: Option<Arc<AuthRuntime>>,
    pub(super) maintenance_authorizer: Option<Arc<MaintenanceAuthorizer>>,
    pub(super) auth_admin_service: Option<Arc<AuthAdminService>>,
    pub(super) consumer_ids_change_listener: Arc<dyn ConsumerIdsChangeListener + Send + Sync + 'static>,
    pub(super) processor_wiring_complete: bool,
}

impl BrokerRequestPipeline {
    pub(super) fn new(
        consumer_ids_change_listener: Arc<dyn ConsumerIdsChangeListener + Send + Sync + 'static>,
    ) -> Self {
        Self {
            proxy_request_processor: None,
            authorized_dispatcher: None,
            auth_runtime: None,
            maintenance_authorizer: None,
            auth_admin_service: None,
            consumer_ids_change_listener,
            processor_wiring_complete: false,
        }
    }
}

impl BrokerRuntime {
    #[cfg(test)]
    pub(crate) fn init_processor_for_test(&mut self) {
        let _ = self.init_processor();
    }

    #[cfg(test)]
    pub(super) fn init_processor(&mut self) -> (DefaultServerProcessor, FasterServerProcessor) {
        if self.composition.request_pipeline.auth_admin_service.is_none() {
            let provider_registry =
                rocketmq_auth::ProviderRegistry::local(&AuthConfig::default()).expect("create in-memory auth registry");
            self.composition.request_pipeline.auth_admin_service =
                Some(Arc::new(AuthAdminService::with_provider_registry(provider_registry)));
        }
        self.init_processor_checked()
            .expect("test runtime should initialize request processor dependencies")
    }

    pub(super) fn init_processor_checked(
        &mut self,
    ) -> Result<(DefaultServerProcessor, FasterServerProcessor), BrokerStartupError> {
        self.composition.request_pipeline.processor_wiring_complete = false;
        let transactional_message_service = self
            .composition
            .state
            .transactional_message_service
            .as_ref()
            .cloned()
            .ok_or_else(|| BrokerStartupError::Initialization {
                component: "transactional_message_service",
                detail: "request processors require an initialized transactional message service".to_owned(),
            })?;
        self.detach_message_store_provider();
        let processors = self.init_processor_with_exclusive_store(transactional_message_service);
        self.bind_message_store_provider();
        let processors = processors?;
        if self.composition.request_pipeline.processor_wiring_complete {
            Ok(processors)
        } else {
            Err(BrokerStartupError::Initialization {
                component: "request_processors",
                detail: "message-arrival listener could not be installed on the exclusively owned Store".to_owned(),
            })
        }
    }

    pub(super) fn init_processor_with_exclusive_store(
        &mut self,
        transactional_message_service: Arc<DefaultTransactionalMessageService<BrokerMessageStore>>,
    ) -> Result<(DefaultServerProcessor, FasterServerProcessor), BrokerStartupError> {
        let send_message_topic_capability = Arc::new(SendMessageTopicCapability::new(
            self.composition.state.send_message_policy_state.clone(),
            self.composition.state.topic_config_manager_handle(),
            self.composition.state.topic_config_coordinator_handle(),
            self.composition.state.topic_queue_mapping_manager_handle(),
            self.composition.state.broker_outer_api().clone(),
            self.composition.state.get_ha_server_addr(),
            TransactionMessageStore::new(&self.composition.data_plane.escape_bridge_owner),
            self.composition
                .state
                .slave_synchronize()
                .map(SlaveSynchronize::master_addr_handle),
            self.composition.state.update_master_haserver_addr_periodically,
            Arc::clone(&self.composition.state.shutdown),
        ));
        let send_message_context = Arc::new(
            SendMessageProcessorContext::new(
                self.composition.state.send_message_policy_state.clone(),
                self.composition.state.telemetry_handle.clone(),
                SendMessageStoreCapability::new(&self.composition.data_plane.escape_bridge_owner),
                send_message_topic_capability,
                self.composition.state.subscription_group_manager().config_lookup(),
                self.composition.state.rebalance_lock_manager().clone(),
                self.composition.state.broker_stats_manager_handle(),
                self.composition.state.broker_metrics_manager.clone(),
                self.composition.state.producer_manager().reply_channel_registry(),
            )
            .with_command_factory(self.composition.state.command_factory()),
        );
        let send_message_processor = SendMessageProcessor::new(
            Arc::clone(&transactional_message_service),
            Arc::clone(&send_message_context),
        );
        let reply_message_processor =
            ReplyMessageProcessor::new(Arc::clone(&transactional_message_service), send_message_context);
        let pull_message_context = self.composition.state.build_pull_message_context();
        let pull_message_result_handler = Arc::new(DefaultPullMessageResultHandler::new(
            Arc::new(Default::default()), //optimize
            Arc::clone(&pull_message_context),
            self.composition.state.broker_metrics_manager.clone(),
        ));

        let pull_message_processor = Arc::new(PullMessageProcessor::new(
            pull_message_result_handler,
            Arc::clone(&pull_message_context),
        ));

        let consumer_manage_processor = self.composition.state.build_consumer_manage_processor();
        let pull_request_hold_service = Arc::new(PullRequestHoldService::new(Arc::downgrade(&pull_message_processor)));
        if !pull_message_context.install_pull_request_hold_service(Arc::clone(&pull_request_hold_service)) {
            warn!("Pull request hold service is already installed in the pull processor context");
        }
        self.composition.state.pull_request_hold_service = Some(Arc::clone(&pull_request_hold_service));

        let pop_message_processor = self.composition.state.build_pop_message_processor().map_err(|error| {
            BrokerStartupError::Initialization {
                component: "pop_consumer_profile",
                detail: error.to_string(),
            }
        })?;
        let polling_count_provider = pop_message_processor.polling_count_provider();
        self.composition.state.pop_message_processor = Some(pop_message_processor.clone());
        let pop_lite_topic_config_manager = self.composition.state.topic_config_manager_handle();
        let pop_lite_subscription_group_lookup = self.composition.state.subscription_group_manager().config_lookup();
        let pop_lite_event_dispatcher = self.composition.state.lite_event_dispatcher().clone();
        let pop_lite_service_context = self.composition.state.service_context.clone();
        let pop_lite_queue_lock_manager = pop_lite_service_context
            .clone()
            .map(QueueLockManager::new_with_service_context)
            .expect("BrokerRuntime always has an injected ChildServiceContext");
        let pop_lite_long_polling_context = PopLiteLongPollingServiceContext::try_with_resource_budget(
            PopLiteLongPollingPolicy::from_config(&self.composition.state.broker_config()),
            pop_lite_event_dispatcher.clone(),
            pop_lite_service_context,
            self.composition.state.resource_budget(),
        )
        .map_err(|error| BrokerStartupError::Initialization {
            component: "pop_lite_long_polling",
            detail: error.to_string(),
        })?;
        let pop_lite_offset_manager = self.composition.state.consumer_offset_manager_handle();
        let pop_lite_escape_bridge = self.composition.state.escape_bridge();
        let pop_lite_message_processor = PopLiteMessageProcessor::new(
            PopLiteMessageProcessorContext::new(
                PopLiteMessagePolicy::from_config(&self.composition.state.broker_config()),
                pop_lite_topic_config_manager,
                pop_lite_subscription_group_lookup,
                PopLiteOffsetCapability::new(&pop_lite_offset_manager),
                PopLiteMessageStoreCapability::new(&pop_lite_escape_bridge),
                pop_lite_event_dispatcher,
                pop_lite_queue_lock_manager,
                pop_lite_long_polling_context,
            )
            .with_command_factory(self.composition.state.command_factory()),
        );
        let pop_lite_message_processor_provider = Arc::downgrade(&pop_lite_message_processor);
        self.composition.state.pop_lite_message_processor = Some(pop_lite_message_processor.clone());
        let ack_policy = AckMessagePolicy::from_config(
            &self.composition.state.broker_config(),
            self.composition.state.store_host(),
        );
        let is_run_pop_revive = self.composition.state.broker_config().broker_identity.broker_id == MASTER_ID;
        let pop_revive_context = self.composition.state.build_pop_revive_context();
        let pop_revive_services = (0..self.composition.state.broker_config().revive_queue_num)
            .map(|queue_id| {
                let service = Arc::new(PopReviveService::new(
                    ack_policy.revive_topic().clone(),
                    queue_id as i32,
                    Arc::clone(&pop_revive_context),
                ));
                service.set_should_run_pop_revive(is_run_pop_revive);
                service
            })
            .collect();
        let ack_escape_bridge = self.composition.state.escape_bridge();
        let ack_offset_manager = self.composition.state.consumer_offset_manager_handle();
        let ack_order_info = self.composition.state.consumer_order_info_manager_handle();
        let ack_message_processor = Arc::new(AckMessageProcessor::new(
            AckMessageProcessorContext::new(
                ack_policy,
                self.composition.state.topic_config_manager_handle(),
                AckMessageOffsetCapability::new(&ack_offset_manager),
                AckMessageOrderCapability::new(&ack_order_info),
                AckMessageStoreCapability::new(&ack_escape_bridge),
                self.composition.state.pop_inflight_message_counter().clone(),
                AckMessagePopCapability::new(&pop_message_processor),
                pop_revive_services,
            )
            .with_command_factory(self.composition.state.command_factory()),
        ));
        self.composition.state.ack_message_processor = Some(ack_message_processor.clone());
        let query_assignment_processor = Arc::new(QueryAssignmentProcessor::new_with_metadata_io_and_factory(
            self.composition.state.broker_config_arc(),
            self.composition.state.message_store_config_arc(),
            self.composition.state.topic_route_info_manager().clone(),
            self.composition.state.consumer_manager().assignment_view(),
            self.composition
                .state
                .metadata_io
                .as_ref()
                .and_then(|result| result.as_ref().ok())
                .cloned(),
            self.composition.state.command_factory(),
        ));
        if let Some(slave_synchronize) = self.composition.state.slave_synchronize() {
            slave_synchronize
                .bind_message_request_mode_manager(query_assignment_processor.message_request_mode_manager());
        }
        self.composition.state.query_assignment_processor = Some(query_assignment_processor.clone());

        let notification_escape_bridge = self.composition.state.escape_bridge();
        let notification_topic_config_manager = self.composition.state.topic_config_manager_handle();
        let notification_subscription_group_lookup =
            self.composition.state.subscription_group_manager().config_lookup();
        let notification_consumer_filter_manager = Arc::new(self.composition.state.consumer_filter_manager().clone());
        let notification_long_polling_context = PopLongPollingServiceContext::new(
            PopLongPollingPolicy::from_config(&self.composition.state.broker_config()),
            Arc::clone(&notification_topic_config_manager),
            notification_subscription_group_lookup.clone(),
            self.composition.state.broker_service_context(),
        );
        let notification_processor = NotificationProcessor::new(
            NotificationProcessorContext::new(
                NotificationPolicy::from_config(&self.composition.state.broker_config()),
                notification_topic_config_manager,
                notification_subscription_group_lookup,
                notification_consumer_filter_manager,
                self.composition.state.consumer_order_info_manager_handle(),
                self.composition
                    .state
                    .consumer_offset_manager_handle()
                    .query_capability(),
                NotificationStoreCapability::new(&notification_escape_bridge),
                NotificationPopOffsetCapability::new(pop_message_processor.pop_buffer_merge_service()),
                notification_long_polling_context,
            )
            .with_command_factory(self.composition.state.command_factory()),
        );
        self.composition.state.notification_processor = Some(notification_processor.clone());
        let message_arriving_listener = NotifyMessageArrivingListener::new(
            &pull_request_hold_service,
            &pop_message_processor,
            &notification_processor,
            self.composition.state.lite_subscription_registry().clone(),
            self.composition.state.lite_event_dispatcher().clone(),
            self.composition.state.subscription_group_manager().config_lookup(),
            self.composition.state.broker_config().max_client_event_count.max(1) as usize,
            self.composition
                .state
                .broker_config()
                .lite_event_full_dispatch_delay_time,
            self.composition
                .state
                .broker_config()
                .lite_event_full_dispatch_delay_time_for_wildcard_group,
        );
        if let Some(message_store) = self.composition.state.message_store_exclusive_mut() {
            message_store.set_message_arriving_listener(Some(Arc::new(Box::new(message_arriving_listener))));
            self.composition.request_pipeline.processor_wiring_complete = true;
        } else {
            error!("Message store is not exclusively owned while installing request processors");
        }
        let mut broker_request_processor =
            BrokerRequestProcessor::new_with_factory(self.composition.state.command_factory());
        let request_processor_task_group = self.lifecycle.request_processor_task_group.clone().or_else(|| {
            self.broker_task_group_or_current(
                "rocketmq-broker.request-processor",
                "failed to initialize broker request processor task group outside Tokio runtime",
            )
        });
        if let Some(task_group) = request_processor_task_group.clone() {
            pull_message_processor.set_wakeup_task_group(task_group.clone());
            broker_request_processor.set_request_task_group(task_group);
        }
        self.lifecycle.request_processor_task_group = request_processor_task_group;
        if let Some(auth_runtime) = &self.composition.request_pipeline.auth_runtime {
            broker_request_processor.set_auth_runtime(auth_runtime.clone());
        }
        broker_request_processor.set_broker_fast_failure(self.composition.state.broker_fast_failure.clone());
        let broker_config = self.composition.state.broker_config();
        if broker_config.maintenance_enabled {
            let auth_runtime = self
                .composition
                .request_pipeline
                .auth_runtime
                .as_ref()
                .cloned()
                .ok_or_else(|| BrokerStartupError::Initialization {
                    component: "maintenance_request_processor",
                    detail: "maintenance API requires an initialized auth runtime".to_string(),
                })?;
            let authorizer = self
                .composition
                .request_pipeline
                .maintenance_authorizer
                .as_ref()
                .cloned()
                .ok_or_else(|| BrokerStartupError::Initialization {
                    component: "maintenance_request_processor",
                    detail: "maintenance API requires a validated maintenance policy".to_string(),
                })?;
            let store =
                self.composition
                    .state
                    .message_store_weak()
                    .ok_or_else(|| BrokerStartupError::Initialization {
                        component: "maintenance_request_processor",
                        detail: "maintenance API requires the Broker-owned Store".to_string(),
                    })?;
            let service_context = self
                .composition
                .state
                .service_context
                .as_ref()
                .cloned()
                .ok_or_else(|| BrokerStartupError::Initialization {
                    component: "maintenance_request_processor",
                    detail: "maintenance API requires a lifecycle-owned service context".to_string(),
                })?;
            let checkpoint_service = Arc::new(rocketmq_store::StoreReleaseCheckpointService::new(
                store,
                std::path::PathBuf::from(broker_config.maintenance_checkpoint_root.as_str()),
                service_context.component("broker.release-checkpoint"),
            ));
            let maintenance_processor = Arc::new(MaintenanceRequestProcessor::new_with_factory(
                Arc::clone(&broker_config),
                auth_runtime,
                authorizer,
                checkpoint_service,
                self.composition.state.command_factory(),
            ));
            for request_code in [
                RequestCode::MaintenanceGetCapabilities,
                RequestCode::MaintenanceCreateStoreCheckpoint,
                RequestCode::MaintenanceVerifyCheckpoint,
                RequestCode::MaintenanceRestoreVerify,
            ] {
                broker_request_processor.register_processor(
                    request_code as i32,
                    BrokerProcessorType::Maintenance(Arc::clone(&maintenance_processor)),
                );
            }
        }
        let send_message_processor = Arc::new(send_message_processor);

        broker_request_processor.register_processor(
            RequestCode::SendMessage as i32,
            BrokerProcessorType::Send(send_message_processor.clone()),
        );
        broker_request_processor.register_processor(
            RequestCode::SendMessageV2 as i32,
            BrokerProcessorType::Send(send_message_processor.clone()),
        );
        broker_request_processor.register_processor(
            RequestCode::SendBatchMessage as i32,
            BrokerProcessorType::Send(send_message_processor.clone()),
        );
        broker_request_processor.register_processor(
            RequestCode::ConsumerSendMsgBack as i32,
            BrokerProcessorType::Send(send_message_processor),
        );

        //PullMessageProcessor
        broker_request_processor.register_processor(
            RequestCode::PullMessage as i32,
            BrokerProcessorType::Pull(pull_message_processor.clone()),
        );
        broker_request_processor.register_processor(
            RequestCode::LitePullMessage as i32,
            BrokerProcessorType::Pull(pull_message_processor),
        );

        //PeekMessageProcessor
        let escape_bridge = self.composition.state.escape_bridge();
        let consumer_offset_query = self
            .composition
            .state
            .consumer_offset_manager_handle()
            .query_capability();
        let peek_message_processor = Arc::new(PeekMessageProcessor::new(
            PeekMessageProcessorContext::new(
                PeekMessagePolicy::from_config(&self.composition.state.broker_config()),
                self.composition.state.topic_config_manager_handle(),
                self.composition.state.subscription_group_manager().config_lookup(),
                consumer_offset_query,
                self.composition.state.broker_stats_manager_handle(),
                PeekMessageStoreCapability::new(&escape_bridge),
                PeekPopOffsetCapability::new(pop_message_processor.pop_buffer_merge_service()),
            )
            .with_command_factory(self.composition.state.command_factory()),
        ));
        broker_request_processor.register_processor(
            RequestCode::PeekMessage as i32,
            BrokerProcessorType::Peek(peek_message_processor),
        );

        //PopMessageProcessor
        broker_request_processor.register_processor(
            RequestCode::PopMessage as i32,
            BrokerProcessorType::Pop(pop_message_processor.clone()),
        );
        broker_request_processor.register_processor(
            RequestCode::PopLiteMessage as i32,
            BrokerProcessorType::PopLite(pop_lite_message_processor.clone()),
        );

        //AckMessageProcessor
        broker_request_processor.register_processor(
            RequestCode::AckMessage as i32,
            BrokerProcessorType::Ack(ack_message_processor.clone()),
        );
        broker_request_processor.register_processor(
            RequestCode::BatchAckMessage as i32,
            BrokerProcessorType::Ack(ack_message_processor),
        );
        //ChangeInvisibleTimeProcessor
        let change_invisible_escape_bridge = self.composition.state.escape_bridge();
        let change_invisible_order_info = self.composition.state.consumer_order_info_manager_handle();
        broker_request_processor.register_processor(
            RequestCode::ChangeMessageInvisibleTime as i32,
            BrokerProcessorType::ChangeInvisible(Arc::new(ChangeInvisibleTimeProcessor::new(
                ChangeInvisibleTimeProcessorContext::new(
                    ChangeInvisibleTimePolicy::from_config(
                        &self.composition.state.broker_config(),
                        self.composition.state.store_host(),
                    ),
                    self.composition.state.topic_config_manager_handle(),
                    self.composition
                        .state
                        .consumer_offset_manager_handle()
                        .query_capability(),
                    ChangeInvisibleTimeOrderCapability::new(&change_invisible_order_info),
                    ChangeInvisibleTimeLiteCapability::new(&pop_lite_message_processor),
                    self.composition.state.broker_stats_manager_handle(),
                    ChangeInvisibleTimeStoreCapability::new(&change_invisible_escape_bridge),
                    ChangeInvisibleTimePopCapability::new(pop_message_processor.pop_buffer_merge_service()),
                    pop_message_processor.queue_lock_manager().clone(),
                )
                .with_command_factory(self.composition.state.command_factory()),
            ))),
        );
        //notificationProcessor
        broker_request_processor.register_processor(
            RequestCode::Notification as i32,
            BrokerProcessorType::Notification(notification_processor),
        );

        //pollingInfoProcessor
        broker_request_processor.register_processor(
            RequestCode::PollingInfo as i32,
            BrokerProcessorType::PollingInfo(Arc::new(PollingInfoProcessor::new_with_factory(
                self.composition.state.broker_config_arc(),
                self.composition.state.topic_config_manager_handle(),
                self.composition.state.subscription_group_manager().config_lookup(),
                polling_count_provider,
                self.composition.state.command_factory(),
            ))),
        );

        //ReplyMessageProcessor
        let reply_message_processor = Arc::new(reply_message_processor);
        broker_request_processor.register_processor(
            RequestCode::SendReplyMessage as i32,
            BrokerProcessorType::Reply(reply_message_processor.clone()),
        );
        broker_request_processor.register_processor(
            RequestCode::SendReplyMessageV2 as i32,
            BrokerProcessorType::Reply(reply_message_processor),
        );

        //RecallMessageProcessor
        let recall_message_processor = Arc::new(RecallMessageProcessor::new(
            RecallMessageProcessorContext::new(
                RecallMessagePolicy::from_configs(
                    &self.composition.state.broker_config(),
                    &self.composition.state.message_store_config(),
                    self.composition.state.store_host(),
                ),
                self.composition.state.topic_config_manager_handle(),
                RecallMessageStoreCapability::new(&self.composition.data_plane.escape_bridge_owner),
                self.composition.state.broker_stats_manager_handle(),
            )
            .with_command_factory(self.composition.state.command_factory()),
        ));
        broker_request_processor.register_processor(
            RequestCode::RecallMessage as i32,
            BrokerProcessorType::Recall(recall_message_processor),
        );

        //QueryMessageProcessor
        let query_message_processor = Arc::new(QueryMessageProcessor::new_with_factory(
            self.composition.state.message_store_config().default_query_max_num,
            QueryMessageStoreCapability::new(&self.composition.data_plane.escape_bridge_owner),
            self.composition.state.command_factory(),
        ));
        broker_request_processor.register_processor(
            RequestCode::QueryMessage as i32,
            BrokerProcessorType::QueryMessage(query_message_processor.clone()),
        );
        broker_request_processor.register_processor(
            RequestCode::ViewMessageById as i32,
            BrokerProcessorType::QueryMessage(query_message_processor),
        );
        //ClientManageProcessor
        let client_manage_processor = Arc::new(self.composition.state.build_client_manage_processor());
        broker_request_processor.register_processor(
            RequestCode::HeartBeat as i32,
            BrokerProcessorType::ClientManage(client_manage_processor.clone()),
        );
        broker_request_processor.register_processor(
            RequestCode::UnregisterClient as i32,
            BrokerProcessorType::ClientManage(client_manage_processor.clone()),
        );
        broker_request_processor.register_processor(
            RequestCode::CheckClientConfig as i32,
            BrokerProcessorType::ClientManage(client_manage_processor),
        );

        //ConsumerManageProcessor
        let consumer_manage_processor = Arc::new(consumer_manage_processor);

        broker_request_processor.register_processor(
            RequestCode::GetConsumerListByGroup as i32,
            BrokerProcessorType::ConsumerManage(consumer_manage_processor.clone()),
        );
        broker_request_processor.register_processor(
            RequestCode::UpdateConsumerOffset as i32,
            BrokerProcessorType::ConsumerManage(consumer_manage_processor.clone()),
        );

        broker_request_processor.register_processor(
            RequestCode::QueryConsumerOffset as i32,
            BrokerProcessorType::ConsumerManage(consumer_manage_processor),
        );

        //QueryAssignmentProcessor
        broker_request_processor.register_processor(
            RequestCode::QueryAssignment as i32,
            BrokerProcessorType::QueryAssignment(query_assignment_processor.clone()),
        );
        broker_request_processor.register_processor(
            RequestCode::SetMessageRequestMode as i32,
            BrokerProcessorType::QueryAssignment(query_assignment_processor),
        );

        let lite_manager_processor = Arc::new(LiteManagerProcessor::new({
            let consumer_offset_manager = self.composition.state.consumer_offset_manager_handle();
            let escape_bridge = self.composition.state.escape_bridge();
            LiteManagerContext::new(
                LiteManagerPolicy::from_configs(
                    &self.composition.state.broker_config(),
                    &self.composition.state.message_store_config(),
                ),
                self.composition.state.topic_config_manager_handle(),
                self.composition.state.subscription_group_manager().clone(),
                self.composition.state.lite_subscription_registry().clone(),
                self.composition.state.lite_event_dispatcher().clone(),
                self.composition.state.lite_lifecycle_manager().clone(),
                LiteShardingView::new(
                    self.composition.state.broker_config().broker_name().clone(),
                    self.composition.state.topic_route_info_manager(),
                ),
                LiteManagerOffsetCapability::new(&consumer_offset_manager),
                LiteManagerStoreCapability::new(&escape_bridge),
                pop_lite_message_processor_provider.clone(),
            )
            .with_command_factory(self.composition.state.command_factory())
        }));
        broker_request_processor.register_processor(
            RequestCode::GetBrokerLiteInfo as i32,
            BrokerProcessorType::LiteManager(Arc::clone(&lite_manager_processor)),
        );
        broker_request_processor.register_processor(
            RequestCode::GetParentTopicInfo as i32,
            BrokerProcessorType::LiteManager(Arc::clone(&lite_manager_processor)),
        );
        broker_request_processor.register_processor(
            RequestCode::GetLiteTopicInfo as i32,
            BrokerProcessorType::LiteManager(Arc::clone(&lite_manager_processor)),
        );
        broker_request_processor.register_processor(
            RequestCode::GetLiteClientInfo as i32,
            BrokerProcessorType::LiteManager(Arc::clone(&lite_manager_processor)),
        );
        broker_request_processor.register_processor(
            RequestCode::GetLiteGroupInfo as i32,
            BrokerProcessorType::LiteManager(Arc::clone(&lite_manager_processor)),
        );
        broker_request_processor.register_processor(
            RequestCode::TriggerLiteDispatch as i32,
            BrokerProcessorType::LiteManager(lite_manager_processor),
        );
        broker_request_processor.register_processor(
            RequestCode::LiteSubscriptionCtl as i32,
            BrokerProcessorType::LiteSubscriptionCtl(Arc::new(LiteSubscriptionCtlProcessor::new({
                let consumer_offset_manager = self.composition.state.consumer_offset_manager_handle();
                let escape_bridge = self.composition.state.escape_bridge();
                LiteSubscriptionCtlContext::new(
                    LiteSubscriptionCtlPolicy::from_config(&self.composition.state.broker_config()),
                    self.composition.state.lite_subscription_registry().clone(),
                    self.composition.state.lite_event_dispatcher().clone(),
                    self.composition.state.subscription_group_manager().clone(),
                    PopLiteOffsetCapability::new(&consumer_offset_manager),
                    PopLiteMessageStoreCapability::new(&escape_bridge),
                    pop_lite_message_processor_provider,
                )
                .with_command_factory(self.composition.state.command_factory())
            }))),
        );

        //EndTransactionProcessor
        broker_request_processor.register_processor(
            RequestCode::EndTransaction as i32,
            BrokerProcessorType::EndTransaction(Arc::new(EndTransactionProcessor::new(
                transactional_message_service,
                EndTransactionProcessorContext::new(
                    EndTransactionPolicy::from_configs(
                        &self.composition.state.broker_config(),
                        &self.composition.state.message_store_config(),
                    ),
                    EndTransactionStoreCapability::new(&self.composition.data_plane.escape_bridge_owner),
                    self.composition.state.broker_stats_manager_handle(),
                    self.composition.state.broker_metrics_manager.clone(),
                )
                .with_command_factory(self.composition.state.command_factory()),
            ))),
        );
        let auth_admin_service = self
            .composition
            .request_pipeline
            .auth_admin_service
            .clone()
            .ok_or_else(|| BrokerStartupError::Initialization {
                component: "auth_admin_service",
                detail: "auth admin service must be initialized before request processors".to_owned(),
            })?;
        let admin_broker_processor = Arc::new(AdminBrokerProcessor::new_with_factory(
            self.admin_runtime(),
            auth_admin_service,
            self.composition.state.command_factory(),
        ));
        broker_request_processor.register_default_processor(BrokerProcessorType::AdminBroker(admin_broker_processor));

        Ok((broker_request_processor.clone(), broker_request_processor))
    }

    pub(crate) fn proxy_request_processor(&self) -> Option<DefaultServerProcessor> {
        self.composition.request_pipeline.proxy_request_processor.clone()
    }

    pub(crate) fn authorized_dispatcher(
        &self,
    ) -> Option<Arc<rocketmq_transport::api::v1::AuthorizedCommandDispatcher<DefaultServerProcessor>>> {
        self.composition.request_pipeline.authorized_dispatcher.clone()
    }
}
