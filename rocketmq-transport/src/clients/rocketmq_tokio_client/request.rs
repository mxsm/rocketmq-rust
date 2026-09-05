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

//! Canonical request and one-way execution for the Tokio transport client.

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_error::RpcClientError;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::ResourcePermit;
use tokio::time;
use tracing::debug;
use tracing::error;
use tracing::warn;

use super::RequestTarget;
use super::SendReceipt;
use super::TransportClient;
use crate::clients::TransportSession;
use crate::deadline::RequestDeadline;
use crate::error_helpers::connection_failed_without_source_for_remote;
use crate::error_helpers::network;
use crate::error_helpers::response_timeout_for_remote;
use crate::error_helpers::TransportStage;
use crate::telemetry::TransportGoAwayOutcome;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;

impl<PR: Send + Sync + Clone + 'static> TransportClient<PR> {
    pub(super) fn session_cache_identity(
        &self,
        requested_addr: Option<&CheetahString>,
        session: &TransportSession<PR>,
    ) -> CheetahString {
        requested_addr
            .cloned()
            .or_else(|| self.connection_registry.session_identity(session))
            .or_else(|| self.endpoint_state.load().chosen().cloned())
            .unwrap_or_else(|| CheetahString::from_string(session.remote_address().to_string()))
    }

    pub(super) fn remove_cached_session_if_matches(
        &self,
        identity: &CheetahString,
        expected: &TransportSession<PR>,
    ) -> bool {
        self.connection_registry
            .remove_session_if_matches(identity, expected)
            .is_some()
    }

    fn start_go_away_drain(&self, _identity: CheetahString, session: TransportSession<PR>) {
        session.begin_drain();
        let drain_timeout = session.max_pending_request_age();
        let spawned = self.spawn_worker_task("rocketmq.transport.go-away-drain", async move {
            let report = session.drain_and_close(drain_timeout).await;
            if !report.is_healthy() {
                warn!(report = %report.to_json(), "GO_AWAY session drain was unhealthy");
            }
        });
        if spawned.is_none() {
            warn!("GO_AWAY session drain could not be scheduled because the client is shutting down");
        }
    }

    pub(super) async fn invoke_oneway_until(
        &self,
        addr: &CheetahString,
        request: RemotingCommand,
        deadline: RequestDeadline,
        permit: Option<ResourcePermit>,
    ) -> RocketMQResult<()> {
        deadline.ensure_before_send()?;
        if self.is_stopping() {
            return Err(RocketMQError::ClientNotStarted);
        }
        let Some(mut client) = self.get_and_create_client_until(Some(addr), deadline).await? else {
            return Err(network(connection_failed_without_source_for_remote(
                addr,
                TransportStage::Closed,
            )));
        };
        if self.is_stopping() {
            return Err(RocketMQError::ClientNotStarted);
        }

        let mut request = request;
        let remote_address = client.remote_address();
        if let Some(hooks) = self.cmd_handler.hook_snapshot() {
            request.make_custom_header_to_net();
            self.cmd_handler.do_before_rpc_hooks_with_snapshot(
                Some(hooks.as_ref()),
                remote_address,
                Some(&mut request),
            )?;
        }
        deadline.ensure_before_send()?;
        request.mark_oneway_rpc_ref();
        match permit {
            Some(permit) => client.send_until_with_permit(request, deadline, permit).await,
            None => client.send_until(request, deadline).await,
        }
    }

    /// Sends one canonical request under an absolute deadline.
    pub(super) async fn request_inner(
        &self,
        target: RequestTarget,
        request: RemotingCommand,
        deadline: RequestDeadline,
    ) -> RocketMQResult<RemotingCommand> {
        match target {
            RequestTarget::Endpoint(endpoint) => {
                self.invoke_request_with_deadline(Some(&endpoint), request, deadline)
                    .await
            }
            RequestTarget::NameServer => self.invoke_request_with_deadline(None, request, deadline).await,
        }
    }

    /// Sends one command and resolves only after the sole writer has completed it.
    pub(super) async fn send_oneway_inner(
        &self,
        target: RequestTarget,
        request: RemotingCommand,
        deadline: RequestDeadline,
    ) -> RocketMQResult<SendReceipt> {
        match target {
            RequestTarget::Endpoint(endpoint) => {
                self.invoke_oneway_until(&endpoint, request, deadline, None).await?;
                Ok(SendReceipt {
                    endpoint,
                    written_at_millis: current_millis(),
                })
            }
            RequestTarget::NameServer => {
                let started_at = time::Instant::now();
                deadline.ensure_before_send()?;
                let Some(selection) = self.get_and_create_nameserver_client_until(deadline).await? else {
                    return Err(network(connection_failed_without_source_for_remote(
                        "<nameserver>",
                        TransportStage::Closed,
                    )));
                };
                let metric_identity = selection.identity.clone();
                let metric_lease = selection.lease.clone();
                let selection_generation = selection.state.generation();
                let mut client = selection.session;
                debug!(
                    generation = selection_generation,
                    "Sending one-way request to selected nameserver"
                );
                let result = async {
                    let endpoint = CheetahString::from_string(client.remote_address().to_string());
                    let mut request = request;
                    if let Some(hooks) = self.cmd_handler.hook_snapshot() {
                        request.make_custom_header_to_net();
                        self.cmd_handler.do_before_rpc_hooks_with_snapshot(
                            Some(hooks.as_ref()),
                            client.remote_address(),
                            Some(&mut request),
                        )?;
                    }
                    request.mark_oneway_rpc_ref();
                    client.send_until(request, deadline).await?;
                    Ok(SendReceipt {
                        endpoint,
                        written_at_millis: current_millis(),
                    })
                }
                .await;
                self.record_nameserver_outcome(
                    Some(&metric_identity),
                    Some(&metric_lease),
                    started_at.elapsed(),
                    result.is_ok(),
                );
                result
            }
        }
    }

    pub async fn invoke_request_with_deadline(
        &self,
        addr: Option<&CheetahString>,
        request: RemotingCommand,
        deadline: RequestDeadline,
    ) -> RocketMQResult<RemotingCommand> {
        let nameserver_request = addr.is_none_or(CheetahString::is_empty);
        let start = time::Instant::now();
        let timeout_millis = deadline.budget_millis();
        let target = if nameserver_request {
            "<nameserver>".to_string()
        } else {
            addr.map_or_else(|| "<nameserver>".to_string(), ToString::to_string)
        };
        deadline.ensure_before_send()?;
        let nameserver_diagnostics = nameserver_request.then(|| self.endpoint_state.load());
        let nameserver_selection = if nameserver_request {
            self.get_and_create_nameserver_client_until(deadline).await?
        } else {
            None
        };
        let nameserver_metric_addr = nameserver_selection
            .as_ref()
            .map(|selection| selection.identity.clone());
        let nameserver_lease = nameserver_selection.as_ref().map(|selection| selection.lease.clone());
        let nameserver_generation = nameserver_selection
            .as_ref()
            .map(|selection| selection.state.generation());
        let mut client = match nameserver_selection {
            Some(selection) => Some(selection.session),
            None if nameserver_request => None,
            None => self.get_and_create_client_until(addr, deadline).await?,
        }
        .ok_or_else(|| {
            if target == "<nameserver>" {
                if let Some(state) = nameserver_diagnostics.as_ref() {
                    error!(
                        configured = state.endpoints().len(),
                        available = state.available().len(),
                        cached_choice = state.chosen().is_some(),
                        connections = self.connection_registry.len(),
                        "Failed to get client for <nameserver>"
                    );
                }
            } else {
                error!("Failed to get client for direct endpoint");
            }

            network(connection_failed_without_source_for_remote(
                &target,
                TransportStage::Connect,
            ))
        })?;

        if self.is_stopping() {
            return Err(RocketMQError::ClientNotStarted);
        }

        let mut request = request;
        let initial_remote_address = client.remote_address();
        deadline.ensure_before_send()?;
        let hooks = self.cmd_handler.hook_snapshot();
        let request_for_after = if let Some(hooks) = hooks {
            request.make_custom_header_to_net();
            self.cmd_handler.do_before_rpc_hooks_with_snapshot(
                Some(hooks.as_ref()),
                initial_remote_address,
                Some(&mut request),
            )?;
            deadline.ensure_before_send()?;
            Some((request.clone(), hooks))
        } else {
            None
        };
        let apply_final_hooks =
            |mut response: RemotingCommand, remote_address: std::net::SocketAddr| -> RocketMQResult<RemotingCommand> {
                if let Some((request, hooks)) = request_for_after.as_ref() {
                    self.cmd_handler.do_after_rpc_hooks_with_snapshot(
                        Some(hooks.as_ref()),
                        remote_address,
                        request,
                        Some(&mut response),
                    )?;
                }
                if deadline.is_expired() {
                    return Err(network(response_timeout_for_remote(
                        remote_address.to_string(),
                        timeout_millis,
                    )));
                }
                Ok(response)
            };
        let retry_allowed = self.go_away_policy.allows_request(request.code()) && !request.is_oneway_rpc();
        let retry_request = request.clone();
        let mut attempted_retry = false;

        loop {
            let remote_address = client.remote_address();
            let identity = if nameserver_request {
                self.connection_registry
                    .session_identity(&client)
                    .or_else(|| nameserver_metric_addr.clone())
                    .unwrap_or_else(|| CheetahString::from_string(remote_address.to_string()))
            } else {
                self.session_cache_identity(addr, &client)
            };
            let mut attempt_request = retry_request.clone();
            if attempted_retry {
                attempt_request.set_opaque_mut(RemotingCommand::get_and_add());
            }

            match client.send_read(attempt_request, deadline).await {
                Ok(response) if response.code() == ResponseCode::GoAway.to_i32() => {
                    self.telemetry.record_go_away(TransportGoAwayOutcome::Received);
                    if !retry_allowed {
                        let response = apply_final_hooks(response, remote_address)?;
                        let latency = start.elapsed();
                        self.record_nameserver_outcome(
                            nameserver_metric_addr.as_ref(),
                            nameserver_lease.as_ref(),
                            latency,
                            true,
                        );
                        debug!(
                            endpoint_kind = if nameserver_request { "nameserver" } else { "direct" },
                            elapsed_ms = latency.as_millis() as u64,
                            "request completed with GO_AWAY retry disabled"
                        );
                        return Ok(response);
                    }
                    self.remove_cached_session_if_matches(&identity, &client);
                    self.start_go_away_drain(identity.clone(), client);

                    if attempted_retry {
                        self.telemetry.record_go_away(TransportGoAwayOutcome::RetryFailed);
                        self.record_nameserver_outcome(
                            nameserver_metric_addr.as_ref(),
                            nameserver_lease.as_ref(),
                            start.elapsed(),
                            false,
                        );
                        return Err(RpcClientError::unexpected_response_code(
                            response.code(),
                            "GO_AWAY after replacement-connection retry",
                        )
                        .into());
                    }

                    attempted_retry = true;
                    if let Err(error) = deadline.ensure_before_send() {
                        self.telemetry.record_go_away(TransportGoAwayOutcome::RetryFailed);
                        self.record_nameserver_outcome(
                            nameserver_metric_addr.as_ref(),
                            nameserver_lease.as_ref(),
                            start.elapsed(),
                            false,
                        );
                        return Err(error);
                    }
                    let replacement = match self.get_and_create_client_until(addr, deadline).await {
                        Ok(Some(replacement)) => Ok(replacement),
                        Ok(None) => Err(network(connection_failed_without_source_for_remote(
                            &identity,
                            TransportStage::Closed,
                        ))),
                        Err(error) => Err(error),
                    };
                    client = match replacement {
                        Ok(replacement) => replacement,
                        Err(error) => {
                            self.telemetry.record_go_away(TransportGoAwayOutcome::RetryFailed);
                            self.record_nameserver_outcome(
                                nameserver_metric_addr.as_ref(),
                                nameserver_lease.as_ref(),
                                start.elapsed(),
                                false,
                            );
                            return Err(error);
                        }
                    };
                }
                Ok(response) => {
                    let response = match apply_final_hooks(response, remote_address) {
                        Ok(response) => response,
                        Err(error) => {
                            if attempted_retry {
                                self.telemetry.record_go_away(TransportGoAwayOutcome::RetryFailed);
                            }
                            self.record_nameserver_outcome(
                                nameserver_metric_addr.as_ref(),
                                nameserver_lease.as_ref(),
                                start.elapsed(),
                                false,
                            );
                            return Err(error);
                        }
                    };
                    if attempted_retry {
                        self.telemetry.record_go_away(TransportGoAwayOutcome::RetrySuccess);
                    }
                    let latency = start.elapsed();
                    self.record_nameserver_outcome(
                        nameserver_metric_addr.as_ref(),
                        nameserver_lease.as_ref(),
                        latency,
                        true,
                    );
                    debug!(
                        endpoint_kind = if nameserver_request { "nameserver" } else { "direct" },
                        nameserver_generation = ?nameserver_generation,
                        elapsed_ms = latency.as_millis() as u64,
                        "request completed"
                    );
                    return Ok(response);
                }
                Err(error) => {
                    let descriptor_code = match &error {
                        RocketMQError::Network(source) => Some(source.code()),
                        RocketMQError::Shared(shared) => match shared.as_error() {
                            RocketMQError::Network(source) => Some(source.code()),
                            _ => None,
                        },
                        _ => None,
                    };
                    if descriptor_code.is_some_and(|code| {
                        code == rocketmq_error::TRANSPORT_WRITE_TIMEOUT.code()
                            || code == rocketmq_error::TRANSPORT_RESPONSE_TIMEOUT.code()
                    }) {
                        client.retire_after_timeout().await;
                        self.remove_cached_session_if_matches(&identity, &client);
                    }
                    if attempted_retry {
                        self.telemetry.record_go_away(TransportGoAwayOutcome::RetryFailed);
                    }
                    let latency = start.elapsed();
                    self.record_nameserver_outcome(
                        nameserver_metric_addr.as_ref(),
                        nameserver_lease.as_ref(),
                        latency,
                        false,
                    );
                    warn!(
                        endpoint_kind = if nameserver_request { "nameserver" } else { "direct" },
                        elapsed_ms = latency.as_millis() as u64,
                        "request failed"
                    );
                    return Err(error);
                }
            }
        }
    }
}
