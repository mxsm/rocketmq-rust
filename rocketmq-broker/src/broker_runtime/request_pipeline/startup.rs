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

//! Listener and background-service startup owned by the Broker request pipeline.

use super::super::*;
use crate::deferred_generation_handoff::DeferredGenerationV2PublishError;
use crate::deferred_generation_handoff::DeferredGenerationV2Publisher;

/// Coarse transport gate for the prepared Broker V2 graph.
///
/// This decision only continues to the Broker-owned AuthRuntime check. It is
/// deliberately not a substitute for Broker authentication or ACL policy.
struct BrokerV2IngressPolicy;

impl rocketmq_security_api::IngressPolicy for BrokerV2IngressPolicy {
    fn evaluate_ingress(
        &self,
        _request: rocketmq_security_api::SecurityRequestView<'_>,
    ) -> rocketmq_security_api::LayerEvaluation<rocketmq_security_api::IngressDecision> {
        Ok(rocketmq_security_api::IngressDecision::AllowToContinue)
    }
}

fn prepared_v2_transport_security() -> Arc<rocketmq_transport::api::v1::TransportSecurity> {
    Arc::new(
        rocketmq_transport::api::v1::TransportSecurity::development_insecure_loopback(None, None)
            .with_ingress_policy(Arc::new(BrokerV2IngressPolicy)),
    )
}

impl BrokerRuntime {
    pub(in crate::broker_runtime) async fn start_basic_service(
        &mut self,
    ) -> Result<(SocketAddr, SocketAddr), BrokerStartupError> {
        self.start_message_store()
            .await
            .map_err(|error| BrokerStartupError::component_start("message_store", error))?;

        self.composition
            .state
            .controller_state
            .with_replicas_mut(ReplicasManager::start);

        if self.composition.state.transactional_message_service.is_none() {
            return Err(BrokerStartupError::Initialization {
                component: "transactional_message_service",
                detail: "request processors require an initialized transactional message service".to_owned(),
            });
        }
        self.initialize_deferred_lifecycle()?;
        let (request_processor, _fast_request_processor) = self.init_processor_checked()?;
        self.initialize_consumer_lag_observability();
        let service_context =
            self.composition
                .state
                .service_context
                .as_ref()
                .ok_or_else(|| BrokerStartupError::Initialization {
                    component: "service_context",
                    detail: "broker remoting servers require an injected service context".to_owned(),
                })?;
        let admission = self
            .composition
            .request_pipeline
            .admission_controller()
            .ok_or_else(|| BrokerStartupError::Initialization {
                component: "authorized_dispatcher",
                detail: "shared Broker admission boundary was not initialized".to_owned(),
            })?;
        let transport_security =
            Arc::new(rocketmq_transport::api::v1::TransportSecurity::development_insecure_loopback(None, None));
        let prepared_v2_transport_security = prepared_v2_transport_security();
        let mut prepared_v2_processor = DefaultServerProcessorV2::from_legacy_graph(&request_processor);
        let broker_config = self.composition.state.broker_config();
        if !broker_config.authentication_enabled && !broker_config.authorization_enabled {
            prepared_v2_processor.set_auth_disabled_by_validated_config();
        }
        if !prepared_v2_processor.is_auth_configured() {
            return Err(BrokerStartupError::Initialization {
                component: "broker_v2_auth",
                detail: "prepared Broker V2 dispatcher requires an explicit AuthRuntime or validated disabled state"
                    .to_owned(),
            });
        }
        let prepared_v2_dispatcher = Arc::new(
            rocketmq_transport::api::v2::AuthorizedCommandDispatcherV2::new_with_telemetry(
                prepared_v2_processor,
                Vec::new(),
                prepared_v2_transport_security,
                Arc::clone(&admission),
                self.composition.state.transport_telemetry.clone(),
            ),
        );
        let authorized_dispatcher = Arc::new(
            rocketmq_transport::api::v1::AuthorizedCommandDispatcher::try_new(
                request_processor.clone(),
                Vec::new(),
                &service_context.process_budget(),
                self.composition.state.transport_telemetry.clone(),
                transport_security,
                admission,
            )
            .map_err(|error| BrokerStartupError::Initialization {
                component: "authorized_dispatcher",
                detail: error.to_string(),
            })?,
        );
        self.composition.request_pipeline.proxy_request_processor = Some(request_processor.clone());
        self.composition.request_pipeline.authorized_dispatcher = Some(authorized_dispatcher.clone());
        let deferred_handoff =
            self.composition
                .deferred_generation_handoff()
                .ok_or_else(|| BrokerStartupError::Initialization {
                    component: "deferred_generation_handoff",
                    detail: "Broker deferred handoff must exist before canonical V2 publication".to_owned(),
                })?;
        #[cfg(test)]
        {
            let pop_lite_processor = self
                .composition
                .state
                .pop_lite_message_processor
                .as_ref()
                .cloned()
                .ok_or_else(|| BrokerStartupError::Initialization {
                    component: "broker_v2_pre_publish_checkpoint",
                    detail: "PopLite processor must exist before the test checkpoint".to_owned(),
                })?;
            self.composition
                .request_pipeline
                .reach_v2_pre_publish_checkpoint(
                    Arc::clone(&prepared_v2_dispatcher),
                    pop_lite_processor,
                    Arc::clone(&deferred_handoff),
                )
                .await;
        }
        {
            let pipeline = &self.composition.request_pipeline;
            let mut cutover =
                deferred_handoff
                    .cutover_transaction()
                    .map_err(|error| BrokerStartupError::Initialization {
                        component: "broker_v2_cutover",
                        detail: format!("failed to begin canonical V2 publication transaction: {error:?}"),
                    })?;
            cutover
                .seal_legacy_acceptance()
                .map_err(|error| BrokerStartupError::Initialization {
                    component: "broker_v2_cutover",
                    detail: format!("failed to seal legacy deferred acceptance: {error:?}"),
                })?;
            cutover
                .publish_v2_aggregate(DeferredGenerationV2Publisher::nonblocking_atomic(|| {
                    pipeline.publish_canonical_v2_dispatcher(prepared_v2_dispatcher)
                }))
                .map_err(|error| BrokerStartupError::Initialization {
                    component: "broker_v2_dispatcher",
                    detail: match error {
                        DeferredGenerationV2PublishError::Cutover(error) => {
                            format!("canonical V2 dispatcher publication violated cutover ordering: {error:?}")
                        }
                        DeferredGenerationV2PublishError::Publish(error) => {
                            format!("canonical V2 dispatcher publication failed: {error:?}")
                        }
                    },
                })?;
            cutover
                .publish_default_new()
                .map_err(|error| BrokerStartupError::Initialization {
                    component: "broker_v2_cutover",
                    detail: format!("failed to publish New as the default deferred generation: {error:?}"),
                })?;
        }
        let canonical_v2_dispatcher = self
            .composition
            .request_pipeline
            .canonical_v2_dispatcher()
            .ok_or_else(|| BrokerStartupError::Initialization {
                component: "broker_v2_dispatcher",
                detail: "canonical V2 dispatcher was not visible after successful publication".to_owned(),
            })?;
        self.lifecycle
            .startup_journal
            .complete(BrokerComponent::RequestProcessors);

        let Some(remoting_server_task_group) = self.broker_task_group_or_current(
            "rocketmq-broker.remoting-server",
            "failed to start broker remoting servers outside Tokio runtime",
        ) else {
            return Err(BrokerStartupError::ComponentStart {
                component: "remoting_servers",
                detail: "a Tokio runtime and owned task group are required".to_owned(),
            });
        };
        self.lifecycle.remoting_server_task_group = Some(remoting_server_task_group.clone());

        let broker_config = self.composition.state.broker_config();
        let normal_dispatcher = Arc::clone(&canonical_v2_dispatcher);
        let fast_dispatcher = Arc::clone(&canonical_v2_dispatcher);
        #[cfg(test)]
        {
            let embedded_proxy_dispatcher = self
                .composition
                .request_pipeline
                .canonical_v2_dispatcher()
                .expect("canonical V2 dispatcher was published above");
            self.composition.request_pipeline.record_v2_dispatcher_identity(
                &canonical_v2_dispatcher,
                &normal_dispatcher,
                &fast_dispatcher,
                &embedded_proxy_dispatcher,
            );
        }
        let v2_session_registry = self.composition.request_pipeline.v2_session_registry();
        let server = TransportServerV2::new_with_authorized_dispatcher(
            Arc::new(broker_config.broker_server_config.clone()),
            service_context.component("broker.remoting-server.normal"),
            normal_dispatcher,
        )
        .with_telemetry(self.composition.state.transport_telemetry.clone())
        .with_session_registry(Arc::clone(&v2_session_registry));
        // Start the normal Broker remoting server.
        let shutdown_token = remoting_server_task_group.cancellation_token();
        let (normal_report_tx, normal_report_rx) = oneshot::channel();
        let (normal_startup_tx, normal_startup_rx) = oneshot::channel();
        if let Err(error) = remoting_server_task_group.spawn_service("broker.remoting-server.normal", async move {
            let report = server
                .try_run_with_shutdown_report_and_startup(
                    async move {
                        shutdown_token.cancelled().await;
                    },
                    normal_startup_tx,
                )
                .await;
            if let Ok(report) = report.as_ref() {
                report.log_if_unhealthy();
            }
            let _ = normal_report_tx.send(report.ok());
        }) {
            return Err(BrokerStartupError::component_start("normal_remoting_server", error));
        }
        // Start the fast Broker remoting server.
        let mut fast_server_config = broker_config.broker_server_config.clone();
        fast_server_config.listen_port = broker_config.broker_server_config.listen_port - 2;
        let fast_server = TransportServerV2::new_with_authorized_dispatcher(
            Arc::new(fast_server_config),
            service_context.component("broker.remoting-server.fast"),
            fast_dispatcher,
        )
        .with_telemetry(self.composition.state.transport_telemetry.clone())
        .with_session_registry(v2_session_registry);
        let shutdown_token = remoting_server_task_group.cancellation_token();
        let (fast_report_tx, fast_report_rx) = oneshot::channel();
        let (fast_startup_tx, fast_startup_rx) = oneshot::channel();
        if let Err(error) = remoting_server_task_group.spawn_service("broker.remoting-server.fast", async move {
            let report = fast_server
                .try_run_with_shutdown_report_and_startup(
                    async move {
                        shutdown_token.cancelled().await;
                    },
                    fast_startup_tx,
                )
                .await;
            if let Ok(report) = report.as_ref() {
                report.log_if_unhealthy();
            }
            let _ = fast_report_tx.send(report.ok());
        }) {
            return Err(BrokerStartupError::component_start("fast_remoting_server", error));
        }
        let (normal_listener, fast_listener) = tokio::join!(
            await_remoting_server_startup("normal", normal_startup_rx, REMOTING_SERVER_STARTUP_TIMEOUT),
            await_remoting_server_startup("fast", fast_startup_rx, REMOTING_SERVER_STARTUP_TIMEOUT),
        );
        if normal_listener.is_ok() {
            self.lifecycle
                .remoting_server_report_receivers
                .push(BrokerRemotingServerReportReceiver {
                    name: "broker.remoting-server.normal",
                    receiver: normal_report_rx,
                });
            self.lifecycle.startup_journal.complete(BrokerComponent::NormalListener);
        }
        if fast_listener.is_ok() {
            self.lifecycle
                .remoting_server_report_receivers
                .push(BrokerRemotingServerReportReceiver {
                    name: "broker.remoting-server.fast",
                    receiver: fast_report_rx,
                });
            self.lifecycle.startup_journal.complete(BrokerComponent::FastListener);
        }
        let normal_listener = normal_listener?;
        let fast_listener = fast_listener?;

        if let Some(pop_message_processor) = self.composition.state.pop_message_processor.as_ref() {
            pop_message_processor.start().await;
        }
        if let Some(pop_lite_message_processor) = self.composition.state.pop_lite_message_processor.as_ref() {
            pop_lite_message_processor.start().await;
        }
        if let Some(ack_message_processor) = self.composition.state.ack_message_processor.as_mut() {
            ack_message_processor.start();
        }

        if let Some(notification_processor) = self.composition.state.notification_processor.as_ref() {
            notification_processor.start().await;
        }

        if let Some(topic_queue_mapping_clean_service) =
            self.composition.state.topic_queue_mapping_clean_service.as_mut()
        {
            topic_queue_mapping_clean_service.start();
        }

        let pull_request_hold_task_group = self.broker_task_group_or_current(
            "rocketmq-broker.long-polling.pull-request-hold",
            "failed to start PullRequestHoldService outside Tokio runtime",
        );
        if let (Some(pull_request_hold_service), Some(task_group)) = (
            self.composition.state.pull_request_hold_service.as_ref(),
            pull_request_hold_task_group,
        ) {
            PullRequestHoldService::start(pull_request_hold_service, task_group).await;
        }

        if let Some(broker_stats_manager) = self.composition.state.broker_stats_manager.as_mut() {
            broker_stats_manager.start();
        }

        self.composition.state.broker_fast_failure.start();

        if !self.composition.control_plane.broadcast_offset_scan_started {
            let broadcast_offset_manager = self.composition.state.broadcast_offset_manager.clone();
            self.lifecycle
                .scheduled_task_manager
                .add_fixed_rate_no_overlap_task(SCAN_INTERVAL, SCAN_INTERVAL, move |cancellation| {
                    let broadcast_offset_manager = broadcast_offset_manager.clone();
                    async move {
                        if !cancellation.is_cancelled() {
                            broadcast_offset_manager.scan_offset_data();
                        }
                        Ok(())
                    }
                })
                .map_err(|error| BrokerStartupError::component_start("broadcast_offset_manager", error))?;
            self.composition.control_plane.broadcast_offset_scan_started = true;
        }

        if let Some(topic_route_info_manager) = self.composition.state.topic_route_info_manager.as_mut() {
            topic_route_info_manager.start();
        }

        let broker_config = self.composition.state.broker_config();
        if broker_config.enable_slave_acting_master
            && !broker_config.skip_pre_online
            && self.composition.state.broker_pre_online_service.is_none()
        {
            self.composition.state.broker_pre_online_service = Some(
                self.composition
                    .state
                    .build_broker_pre_online_service(&self.composition.data_plane.escape_bridge_owner),
            );
        }
        if let Some(broker_pre_online_service) = self.composition.state.broker_pre_online_service.as_ref() {
            broker_pre_online_service
                .start()
                .await
                .map_err(|error| BrokerStartupError::component_start("broker_pre_online_service", error))?;
        }

        if let Some(client_housekeeping_service) = self.composition.state.client_housekeeping_service.as_mut() {
            client_housekeeping_service.start();
        }
        self.lifecycle
            .startup_journal
            .complete(BrokerComponent::BackgroundServices);
        Ok((normal_listener, fast_listener))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use rocketmq_protocol::code::response_code::ResponseCode;
    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    use rocketmq_runtime::RuntimeConfig;
    use rocketmq_runtime::RuntimeOwner;
    use rocketmq_security_api::Principal;
    use rocketmq_transport::api::v1::AdmissionController;
    use rocketmq_transport::api::v1::AdmissionLimits;
    use rocketmq_transport::api::v2::AuthorizedCommandDispatcherV2;
    use rocketmq_transport::api::v2::EmbeddedDispatchOutcome;
    use rocketmq_transport::api::v2::HandlerOutcome;
    use rocketmq_transport::api::v2::RemotingRequest;
    use rocketmq_transport::api::v2::RequestProcessorV2;
    use rocketmq_transport::api::v2::ResponsePlan;

    use super::prepared_v2_transport_security;
    use crate::processor::v2::BrokerRequestProcessorV2;

    const STARTUP_PROBE_CODE: i32 = 98_520;

    #[derive(Clone)]
    struct StartupProbe {
        calls: Arc<AtomicUsize>,
    }

    impl RequestProcessorV2 for StartupProbe {
        async fn process(&mut self, _request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(HandlerOutcome::Reply(ResponsePlan::empty_response(
                ResponseCode::Success as i32,
            )))
        }
    }

    #[tokio::test]
    async fn prepared_v2_security_continues_to_broker_acl_and_acl_remains_fail_closed() {
        let owner = RuntimeOwner::new(RuntimeConfig::server_default("broker-prepared-v2-security-test"))
            .expect("prepared V2 security test runtime");
        let service = owner.root_context().component("broker-prepared-v2-security");

        let allowed_calls = Arc::new(AtomicUsize::new(0));
        let mut explicitly_disabled = BrokerRequestProcessorV2::new();
        explicitly_disabled.set_auth_disabled_by_validated_config();
        explicitly_disabled.register_processor(
            STARTUP_PROBE_CODE,
            StartupProbe {
                calls: Arc::clone(&allowed_calls),
            },
        );
        let allowed = AuthorizedCommandDispatcherV2::new(
            explicitly_disabled,
            Vec::new(),
            prepared_v2_transport_security(),
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
        )
        .dispatch_embedded_v2(
            service.task_group(),
            Principal::new("broker-proxy"),
            None,
            RemotingCommand::create_remoting_command(STARTUP_PROBE_CODE),
        )
        .await
        .expect("coarse ingress should continue to Broker ACL");
        let EmbeddedDispatchOutcome::Reply(plan) = allowed else {
            panic!("explicitly disabled Broker ACL should reach the leaf")
        };
        assert_eq!(plan.response_code(), ResponseCode::Success as i32);
        assert_eq!(allowed_calls.load(Ordering::SeqCst), 1);

        let denied_calls = Arc::new(AtomicUsize::new(0));
        let mut unconfigured = BrokerRequestProcessorV2::new();
        unconfigured.register_processor(
            STARTUP_PROBE_CODE,
            StartupProbe {
                calls: Arc::clone(&denied_calls),
            },
        );
        let denied = AuthorizedCommandDispatcherV2::new(
            unconfigured,
            Vec::new(),
            prepared_v2_transport_security(),
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
        )
        .dispatch_embedded_v2(
            service.task_group(),
            Principal::new("broker-proxy"),
            None,
            RemotingCommand::create_remoting_command(STARTUP_PROBE_CODE),
        )
        .await
        .expect("Broker ACL denial should be a response plan");
        let EmbeddedDispatchOutcome::Reply(plan) = denied else {
            panic!("unconfigured Broker ACL should fail closed with one reply")
        };
        assert_eq!(plan.response_code(), ResponseCode::NoPermission as i32);
        assert_eq!(denied_calls.load(Ordering::SeqCst), 0);

        assert!(owner.shutdown_tasks().await.is_healthy());
        assert!(owner.shutdown_background().is_healthy());
    }
}

#[cfg(test)]
#[path = "startup/cutover_network_tests.rs"]
mod cutover_network_tests;
