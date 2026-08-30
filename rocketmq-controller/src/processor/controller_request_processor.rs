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

//! Routes remoting requests through the Controller while applying shared timeout and metrics
//! handling.

mod broker_lifecycle;
mod configuration;
mod maintenance;

use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::sync::Weak;
use std::time::Duration;
use std::time::Instant;

use crate::controller::broker_heartbeat_manager::BrokerSession;
use crate::heartbeat::default_broker_heartbeat_manager::DefaultBrokerHeartbeatManager;
use crate::manager::ControllerManager;
use crate::metrics::RequestHandleStatus;
use crate::metrics::RequestType as MetricsRequestType;
use crate::Controller;
use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::body::sync_state_set_body::SyncStateSet;
use rocketmq_protocol::protocol::header::controller::alter_sync_state_set_request_header::AlterSyncStateSetRequestHeader;
use rocketmq_protocol::protocol::header::controller::elect_master_request_header::ElectMasterRequestHeader;
use rocketmq_protocol::protocol::header::controller::get_replica_info_request_header::GetReplicaInfoRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_protocol::protocol::RemotingDeserializable;
use rocketmq_transport::api::HandlerOutcome;
use rocketmq_transport::api::RemotingRequest;
use rocketmq_transport::api::RemotingResponse;
use rocketmq_transport::api::RequestProcessor;
use rocketmq_transport::api::SessionView;
use tracing::warn;

const WAIT_TIMEOUT_SECONDS: u64 = 5;

/// Routes Controller remoting requests without retaining the manager after shutdown.
#[derive(Clone)]
pub struct ControllerRequestProcessor {
    controller_manager: Weak<ControllerManager>,
    command_factory: RemotingCommandFactory,
    heartbeat_manager: Arc<DefaultBrokerHeartbeatManager>,
    config_blacklist: Arc<HashSet<String>>,
}

impl ControllerRequestProcessor {
    /// Creates a processor whose manager reference does not extend the service lifecycle.
    pub fn new(controller_manager: Arc<ControllerManager>) -> Self {
        let heartbeat_manager = controller_manager.heartbeat_manager().clone();
        let config_blacklist = Arc::new(Self::init_config_blacklist(&controller_manager));
        let command_factory = controller_manager.remoting_command_factory();

        Self {
            controller_manager: Arc::downgrade(&controller_manager),
            command_factory,
            heartbeat_manager,
            config_blacklist,
        }
    }

    fn controller_manager(&self) -> RocketMQResult<Arc<ControllerManager>> {
        self.controller_manager
            .upgrade()
            .ok_or_else(|| RocketMQError::not_initialized("controller manager is no longer available"))
    }

    pub(crate) async fn handle_request(
        &self,
        session: BrokerSession,
        channel_identity: &str,
        request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        match RequestCode::from(request.code()) {
            RequestCode::ControllerAlterSyncStateSet => self.handle_alter_sync_state_set(request).await,
            RequestCode::ControllerElectMaster => self.handle_elect_master(request).await,
            RequestCode::ControllerGetReplicaInfo => self.handle_get_replica_info(request).await,
            RequestCode::ControllerGetMetadataInfo => self.handle_get_metadata_info().await,
            RequestCode::BrokerHeartbeat => self.handle_broker_heartbeat(session, request).await,
            RequestCode::ControllerGetSyncStateData => self.handle_get_sync_state_data(request).await,
            RequestCode::UpdateControllerConfig => self.handle_update_controller_config(request).await,
            RequestCode::GetControllerConfig => self.handle_get_controller_config(),
            RequestCode::CleanBrokerData => self.handle_clean_broker_data(request).await,
            RequestCode::ControllerGetNextBrokerId => self.handle_get_next_broker_id(request).await,
            RequestCode::ControllerApplyBrokerId => self.handle_apply_broker_id(request).await,
            RequestCode::ControllerRegisterBroker => self.handle_register_broker(request).await,
            RequestCode::MaintenanceGetCapabilities => {
                self.handle_maintenance_capabilities(channel_identity, request).await
            }
            RequestCode::MaintenanceCreateControllerSnapshot => {
                self.handle_create_release_snapshot(channel_identity, request).await
            }
            RequestCode::MaintenanceVerifyCheckpoint => {
                self.handle_verify_release_snapshot(channel_identity, request).await
            }
            RequestCode::MaintenanceRestoreVerify => self.handle_restore_verify(channel_identity, request).await,
            _ => Ok(Some(self.command_factory.create_response_command_with_code_remark(
                ResponseCode::RequestCodeNotSupported,
                format!("request type {} not supported", request.code()),
            ))),
        }
    }

    async fn handle_alter_sync_state_set(
        &self,
        request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        let request_header = request
            .decode_command_custom_header::<AlterSyncStateSetRequestHeader>()
            .map_err(|error| {
                RocketMQError::request_header_error(format!(
                    "Failed to decode AlterSyncStateSetRequestHeader: {:?}",
                    error
                ))
            })?;

        let sync_state_set = if let Some(body) = request.body() {
            SyncStateSet::decode(body)?
        } else {
            return Err(RocketMQError::request_body_invalid(
                "ALTER_SYNC_STATE_SET",
                "Request body is empty",
            ));
        };

        self.controller_manager()?
            .controller()
            .alter_sync_state_set(&request_header, sync_state_set)
            .await
    }

    async fn handle_elect_master(&self, request: &mut RemotingCommand) -> RocketMQResult<Option<RemotingCommand>> {
        let request_header = request
            .decode_command_custom_header::<ElectMasterRequestHeader>()
            .map_err(|error| {
                RocketMQError::request_header_error(format!("Failed to decode ElectMasterRequestHeader: {:?}", error))
            })?;

        let controller_manager = self.controller_manager()?;
        let config = controller_manager.controller_config();
        let response = controller_manager.controller().elect_master(&request_header).await?;

        if let Some(response_command) = response.as_ref() {
            if response_command.code() == ResponseCode::Success as i32 && config.notify_broker_role_changed {
                if let Err(error) = controller_manager
                    .notify_broker_role_changed(response_command.clone())
                    .await
                {
                    warn!("Failed to notify brokers after elect-master request: {}", error);
                }
            }
        }

        Ok(response)
    }

    async fn handle_get_replica_info(&self, request: &mut RemotingCommand) -> RocketMQResult<Option<RemotingCommand>> {
        let request_header = request
            .decode_command_custom_header::<GetReplicaInfoRequestHeader>()
            .map_err(|error| {
                RocketMQError::request_header_error(format!(
                    "Failed to decode GetReplicaInfoRequestHeader: {:?}",
                    error
                ))
            })?;

        self.controller_manager()?
            .controller()
            .get_replica_info(&request_header)
            .await
    }

    async fn handle_get_metadata_info(&self) -> RocketMQResult<Option<RemotingCommand>> {
        self.controller_manager()?.controller().get_controller_metadata().await
    }

    async fn handle_get_sync_state_data(
        &self,
        request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        if let Some(body) = request.body() {
            let broker_names: Vec<CheetahString> = serde_json::from_slice(body).unwrap_or_default();
            if !broker_names.is_empty() {
                return self
                    .controller_manager()?
                    .controller()
                    .get_sync_state_data(&broker_names)
                    .await;
            }
        }
        Ok(Some(self.command_factory.create_success_response_command()))
    }

    fn record_request_metrics(&self, request_name: &str, status: RequestHandleStatus, latency_us: u64) {
        #[cfg(feature = "metrics")]
        if let Some(controller_manager) = self.controller_manager.upgrade() {
            controller_manager
                .metrics_manager()
                .inc_request_total(request_name, status);
            controller_manager
                .metrics_manager()
                .record_request_latency(request_name, latency_us);
        }

        #[cfg(not(feature = "metrics"))]
        let _ = (request_name, status, latency_us);
    }

    pub(crate) async fn complete_request<F>(
        &self,
        request_name: Option<&'static str>,
        dispatch: F,
    ) -> RocketMQResult<RemotingCommand>
    where
        F: Future<Output = RocketMQResult<Option<RemotingCommand>>>,
    {
        let start = Instant::now();
        let result = tokio::time::timeout(Duration::from_secs(WAIT_TIMEOUT_SECONDS), dispatch).await;
        let latency_us = start.elapsed().as_micros().try_into().unwrap_or(u64::MAX);

        match result {
            Ok(Ok(response)) => {
                let response = response.unwrap_or_else(|| {
                    self.command_factory.create_response_command_with_code_remark(
                        ResponseCode::SystemError,
                        "Controller request completed without a response",
                    )
                });
                if let Some(name) = request_name {
                    let status = if response.code() == ResponseCode::Success as i32 {
                        RequestHandleStatus::Success
                    } else {
                        RequestHandleStatus::Failed
                    };
                    self.record_request_metrics(name, status, latency_us);
                }
                Ok(response)
            }
            Ok(Err(error)) => {
                if let Some(name) = request_name {
                    self.record_request_metrics(name, RequestHandleStatus::Failed, latency_us);
                }
                Err(error)
            }
            Err(_) => {
                if let Some(name) = request_name {
                    self.record_request_metrics(name, RequestHandleStatus::Timeout, latency_us);
                }
                Ok(self.command_factory.create_response_command_with_code_remark(
                    ResponseCode::SystemError,
                    "Controller request timed out",
                ))
            }
        }
    }

    fn response_outcome(mut response: RemotingCommand) -> RocketMQResult<HandlerOutcome> {
        let body = response.take_body();
        let response = match body {
            Some(body) => RemotingResponse::bytes(response, body),
            None => RemotingResponse::command(response),
        }
        .map_err(|error| RocketMQError::response_process_failed("controller.remoting_response", error.to_string()))?;
        Ok(HandlerOutcome::Reply(response))
    }
}

impl RequestProcessor for ControllerRequestProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> RocketMQResult<HandlerOutcome> {
        let owner_id = request.original_identity().request_id().owner_id();
        let channel_identity = match request.session() {
            SessionView::Network { .. } => format!("transport-session-{owner_id}"),
            SessionView::Embedded { .. } => format!("embedded-proxy-{owner_id}"),
            _ => format!("transport-session-{owner_id}"),
        };
        let session = BrokerSession::new(request.session().id(), owner_id, request.session().state().clone());
        let request_name = RequestCode::from(request.command().code()).get_controller_request_name();
        let dispatch = self.handle_request(session, &channel_identity, request.command_mut());
        let response = self.complete_request(request_name, dispatch).await?;
        Self::response_outcome(response)
    }
}
