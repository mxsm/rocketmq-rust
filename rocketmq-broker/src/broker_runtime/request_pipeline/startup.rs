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
        let (request_processor, fast_request_processor) = self.init_processor_checked()?;
        let service_context =
            self.composition
                .state
                .service_context
                .as_ref()
                .ok_or_else(|| BrokerStartupError::Initialization {
                    component: "service_context",
                    detail: "broker remoting servers require an injected service context".to_owned(),
                })?;
        let admission = Arc::new(
            rocketmq_transport::api::v1::AdmissionController::try_new_with_budget(
                rocketmq_transport::api::v1::AdmissionLimits::default(),
                &service_context.process_budget(),
            )
            .map_err(|error| BrokerStartupError::Initialization {
                component: "authorized_dispatcher",
                detail: format!("failed to create shared Broker admission boundary: {error}"),
            })?,
        );
        let authorized_dispatcher = Arc::new(
            rocketmq_transport::api::v1::AuthorizedCommandDispatcher::try_new(
                request_processor.clone(),
                Vec::new(),
                &service_context.process_budget(),
                self.composition.state.transport_telemetry.clone(),
                Arc::new(rocketmq_transport::api::v1::TransportSecurity::development_insecure_loopback(None, None)),
                admission,
            )
            .map_err(|error| BrokerStartupError::Initialization {
                component: "authorized_dispatcher",
                detail: error.to_string(),
            })?,
        );
        self.composition.request_pipeline.proxy_request_processor = Some(request_processor.clone());
        self.composition.request_pipeline.authorized_dispatcher = Some(authorized_dispatcher.clone());
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
        let mut server = TransportServer::new_with_telemetry(
            Arc::new(broker_config.broker_server_config.clone()),
            service_context.component("broker.remoting-server.normal"),
            self.composition.state.transport_telemetry.clone(),
        )
        .with_authorized_dispatcher(authorized_dispatcher.clone());
        // Start the normal Broker remoting server.
        let client_housekeeping_service_main = self
            .composition
            .state
            .client_housekeeping_service
            .clone()
            .map(|item| item as Arc<dyn ChannelEventListener>);
        let client_housekeeping_service_fast = client_housekeeping_service_main.clone();
        let shutdown_token = remoting_server_task_group.cancellation_token();
        let (normal_report_tx, normal_report_rx) = oneshot::channel();
        let (normal_startup_tx, normal_startup_rx) = oneshot::channel();
        if let Err(error) = remoting_server_task_group.spawn_service("broker.remoting-server.normal", async move {
            let report = server
                .run_with_shutdown_report_and_startup(
                    request_processor,
                    client_housekeeping_service_main,
                    async move {
                        shutdown_token.cancelled().await;
                    },
                    normal_startup_tx,
                )
                .await;
            if let Some(report) = report.as_ref() {
                report.log_if_unhealthy();
            }
            let _ = normal_report_tx.send(report);
        }) {
            return Err(BrokerStartupError::component_start("normal_remoting_server", error));
        }
        // Start the fast Broker remoting server.
        let mut fast_server_config = broker_config.broker_server_config.clone();
        fast_server_config.listen_port = broker_config.broker_server_config.listen_port - 2;
        let mut fast_server = TransportServer::new_with_telemetry(
            Arc::new(fast_server_config),
            service_context.component("broker.remoting-server.fast"),
            self.composition.state.transport_telemetry.clone(),
        )
        .with_authorized_dispatcher(authorized_dispatcher);
        let shutdown_token = remoting_server_task_group.cancellation_token();
        let (fast_report_tx, fast_report_rx) = oneshot::channel();
        let (fast_startup_tx, fast_startup_rx) = oneshot::channel();
        if let Err(error) = remoting_server_task_group.spawn_service("broker.remoting-server.fast", async move {
            let report = fast_server
                .run_with_shutdown_report_and_startup(
                    fast_request_processor,
                    client_housekeeping_service_fast,
                    async move {
                        shutdown_token.cancelled().await;
                    },
                    fast_startup_tx,
                )
                .await;
            if let Some(report) = report.as_ref() {
                report.log_if_unhealthy();
            }
            let _ = fast_report_tx.send(report);
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

        if self.composition.state.broker_pre_online_service.is_none() {
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
