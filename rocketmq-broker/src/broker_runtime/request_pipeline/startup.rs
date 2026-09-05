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

/// Coarse transport gate for the prepared Broker graph.
///
/// This decision only continues to the Broker-owned AuthRuntime check. It is
/// deliberately not a substitute for Broker authentication or ACL policy.
struct BrokerIngressPolicy;

impl rocketmq_security_api::IngressPolicy for BrokerIngressPolicy {
    fn evaluate_ingress(
        &self,
        _request: rocketmq_security_api::SecurityRequestView<'_>,
    ) -> rocketmq_security_api::LayerEvaluation<rocketmq_security_api::IngressDecision> {
        Ok(rocketmq_security_api::IngressDecision::AllowToContinue)
    }
}

fn prepared_transport_security() -> Arc<rocketmq_transport::api::TransportSecurity> {
    Arc::new(
        rocketmq_transport::api::TransportSecurity::development_insecure_loopback(None, None)
            .with_ingress_policy(Arc::new(BrokerIngressPolicy)),
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
        let (mut prepared_processor, _fast_request_processor) = self.init_processor_checked()?;
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
        let prepared_transport_security = prepared_transport_security();
        let broker_config = self.composition.state.broker_config();
        if !broker_config.authentication_enabled && !broker_config.authorization_enabled {
            prepared_processor.set_auth_disabled_by_validated_config();
        }
        if !prepared_processor.is_auth_configured() {
            return Err(BrokerStartupError::Initialization {
                component: "broker_auth",
                detail: "prepared Broker dispatcher requires an explicit AuthRuntime or validated disabled state"
                    .to_owned(),
            });
        }
        let prepared_dispatcher = Arc::new(
            rocketmq_transport::api::AuthorizedCommandDispatcher::try_new_with_telemetry_and_budget(
                prepared_processor,
                Vec::new(),
                prepared_transport_security,
                Arc::clone(&admission),
                self.composition.state.transport_telemetry.clone(),
                self.composition.state.resource_budget(),
            )
            .map_err(|error| BrokerStartupError::Initialization {
                component: "server_request_pending",
                detail: error.to_string(),
            })?,
        );
        self.composition
            .request_pipeline
            .publish_canonical_dispatcher(prepared_dispatcher)
            .map_err(|error| BrokerStartupError::Initialization {
                component: "broker_dispatcher",
                detail: format!("canonical dispatcher publication failed: {error:?}"),
            })?;
        let canonical_dispatcher = self
            .composition
            .request_pipeline
            .canonical_dispatcher()
            .ok_or_else(|| BrokerStartupError::Initialization {
                component: "broker_dispatcher",
                detail: "canonical dispatcher was not visible after successful publication".to_owned(),
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
        let normal_dispatcher = Arc::clone(&canonical_dispatcher);
        let fast_dispatcher = Arc::clone(&canonical_dispatcher);
        #[cfg(test)]
        {
            let embedded_proxy_dispatcher = self
                .composition
                .request_pipeline
                .canonical_dispatcher()
                .expect("canonical dispatcher was published above");
            self.composition.request_pipeline.record_dispatcher_identity(
                &canonical_dispatcher,
                &normal_dispatcher,
                &fast_dispatcher,
                &embedded_proxy_dispatcher,
            );
        }
        let session_registry = self.composition.request_pipeline.session_registry();
        let server = TransportServer::new_with_authorized_dispatcher(
            Arc::new(broker_config.broker_server_config.clone()),
            service_context.component("broker.remoting-server.normal"),
            normal_dispatcher,
        )
        .with_telemetry(self.composition.state.transport_telemetry.clone())
        .with_session_registry(Arc::clone(&session_registry));
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
        let fast_server = TransportServer::new_with_authorized_dispatcher(
            Arc::new(fast_server_config),
            service_context.component("broker.remoting-server.fast"),
            fast_dispatcher,
        )
        .with_telemetry(self.composition.state.transport_telemetry.clone())
        .with_session_registry(session_registry);
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

        if let Some(topic_queue_mapping_clean_service) =
            self.composition.state.topic_queue_mapping_clean_service.as_mut()
        {
            topic_queue_mapping_clean_service.start();
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
    use rocketmq_transport::api::AdmissionController;
    use rocketmq_transport::api::AdmissionLimits;
    use rocketmq_transport::api::AuthorizedCommandDispatcher;
    use rocketmq_transport::api::EmbeddedDispatchOutcome;
    use rocketmq_transport::api::HandlerOutcome;
    use rocketmq_transport::api::RemotingRequest;
    use rocketmq_transport::api::RemotingResponse;
    use rocketmq_transport::api::RequestProcessor;

    use super::prepared_transport_security;
    use crate::processor::dispatcher::BrokerRequestProcessor;

    const STARTUP_PROBE_CODE: i32 = 98_520;

    #[derive(Clone)]
    struct StartupProbe {
        calls: Arc<AtomicUsize>,
    }

    impl RequestProcessor for StartupProbe {
        async fn process(&mut self, _request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(HandlerOutcome::Reply(RemotingResponse::empty_response(
                ResponseCode::Success as i32,
            )))
        }
    }

    #[tokio::test]
    async fn prepared_security_continues_to_broker_acl_and_acl_remains_fail_closed() {
        let owner = RuntimeOwner::plan(RuntimeConfig::server_default("broker-prepared-security-test"))
            .expect("runtime configuration is valid")
            .build()
            .expect("prepared security test runtime");
        let service = owner.root_context().component("broker-prepared-security");

        let allowed_calls = Arc::new(AtomicUsize::new(0));
        let mut explicitly_disabled = BrokerRequestProcessor::new();
        explicitly_disabled.set_auth_disabled_by_validated_config();
        explicitly_disabled.register_processor(
            STARTUP_PROBE_CODE,
            StartupProbe {
                calls: Arc::clone(&allowed_calls),
            },
        );
        let allowed = AuthorizedCommandDispatcher::new(
            explicitly_disabled,
            Vec::new(),
            prepared_transport_security(),
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
        )
        .dispatch_embedded(
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
        let mut unconfigured = BrokerRequestProcessor::new();
        unconfigured.register_processor(
            STARTUP_PROBE_CODE,
            StartupProbe {
                calls: Arc::clone(&denied_calls),
            },
        );
        let denied = AuthorizedCommandDispatcher::new(
            unconfigured,
            Vec::new(),
            prepared_transport_security(),
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
        )
        .dispatch_embedded(
            service.task_group(),
            Principal::new("broker-proxy"),
            None,
            RemotingCommand::create_remoting_command(STARTUP_PROBE_CODE),
        )
        .await
        .expect("Broker ACL denial should be a remoting response");
        let EmbeddedDispatchOutcome::Reply(plan) = denied else {
            panic!("unconfigured Broker ACL should fail closed with one reply")
        };
        assert_eq!(plan.response_code(), ResponseCode::NoPermission as i32);
        assert_eq!(denied_calls.load(Ordering::SeqCst), 0);

        assert!(owner.shutdown_tasks().await.is_healthy());
        assert!(owner.shutdown_background().is_healthy());
    }
}
