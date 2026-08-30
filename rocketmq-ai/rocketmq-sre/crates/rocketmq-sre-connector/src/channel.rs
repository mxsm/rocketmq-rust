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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use chrono::Utc;
use reqwest::Client;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::TaskKind;
use rocketmq_sre_contracts::ConnectorHeartbeat;
use rocketmq_sre_contracts::ConnectorQueryEnvelope;
use rocketmq_sre_contracts::ConnectorRegister;
use rocketmq_sre_contracts::ConnectorResponseEnvelope;
use rocketmq_sre_contracts::ConnectorSessionId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::SchemaVersion;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::Mutex;

use crate::ConnectorConfig;
use crate::ConnectorEngine;
use crate::ConnectorError;
use crate::ConnectorErrorCode;
use crate::config::ControlPlaneConfig;
use crate::mcp::McpGateway;
use crate::sources::CancelSignal;
use crate::sources::bounded_response;

const CHANNEL_SCHEMA_FAMILY: &str = "rocketmq-sre.connector-channel";
const CHANNEL_SCHEMA_MAJOR: u16 = 1;
const MAX_CHANNEL_RESPONSE_BYTES: usize = 256 * 1024;
const MAX_COMMANDS_PER_POLL: usize = 32;
const MAX_PENDING_RESPONSES: usize = 64;
const RETRY_MIN: Duration = Duration::from_secs(1);
const RETRY_MAX: Duration = Duration::from_secs(30);
const INVENTORY_REFRESH_MIN: Duration = Duration::from_secs(60);

#[derive(Debug, Deserialize)]
struct RegisterAcknowledgement {
    schema: SchemaVersion,
    accepted: bool,
    resume_after_sequence: u64,
}

#[derive(Debug, Serialize)]
struct PollRequest {
    schema: SchemaVersion,
    session_id: ConnectorSessionId,
    after_sequence: u64,
    wait_millis: u64,
    max_commands: usize,
}

#[derive(Debug, Deserialize)]
struct PollResponse {
    schema: SchemaVersion,
    #[serde(default)]
    commands: Vec<ChannelCommand>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ChannelCommand {
    Query {
        envelope: ConnectorQueryEnvelope,
    },
    Cancel {
        schema: SchemaVersion,
        session_id: ConnectorSessionId,
        correlation_id: CorrelationId,
        sequence: u64,
    },
}

/// Active HTTP/2 reverse channel. No listener is created: all registration,
/// polling, heartbeat and response traffic originates from the connector.
pub(crate) struct ControlPlaneChannel<G> {
    connector: Arc<ConnectorEngine<G>>,
    connector_config: Arc<ConnectorConfig>,
    config: ControlPlaneConfig,
    client: Client,
    session_id: ConnectorSessionId,
    active: Arc<Mutex<BTreeMap<CorrelationId, CancelSignal>>>,
    pending_responses: Arc<Mutex<BTreeMap<u64, ConnectorResponseEnvelope>>>,
    last_inventory_upload: Arc<Mutex<Option<Instant>>>,
}

impl<G> ControlPlaneChannel<G>
where
    G: McpGateway,
{
    pub(crate) fn new(
        connector: Arc<ConnectorEngine<G>>,
        connector_config: Arc<ConnectorConfig>,
    ) -> Result<Option<Self>, ConnectorError> {
        let Some(config) = connector_config.control_plane.clone() else {
            return Ok(None);
        };
        let mut builder = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(config.poll_wait + connector_config.request_timeout)
            .pool_max_idle_per_host(2)
            .http2_adaptive_window(true)
            .user_agent(concat!("rocketmq-sre-connector/", env!("CARGO_PKG_VERSION")));
        if config.base_url.scheme() == "http" {
            builder = builder.http2_prior_knowledge();
        }
        if !config.ca_pem.is_empty() {
            let certificates = reqwest::Certificate::from_pem_bundle(&config.ca_pem)
                .map_err(|_| ConnectorError::configuration("control-plane CA bundle is invalid"))?;
            for certificate in certificates {
                builder = builder.add_root_certificate(certificate);
            }
        }
        if !config.client_identity_pem.is_empty() {
            let identity = reqwest::Identity::from_pem(&config.client_identity_pem)
                .map_err(|_| ConnectorError::configuration("control-plane client identity PEM is invalid"))?;
            builder = builder.identity(identity);
        }
        let client = builder
            .build()
            .map_err(|_| ConnectorError::configuration("control-plane HTTP/2 client cannot be built"))?;
        Ok(Some(Self {
            connector,
            connector_config,
            config,
            client,
            session_id: ConnectorSessionId::new(),
            active: Arc::new(Mutex::new(BTreeMap::new())),
            pending_responses: Arc::new(Mutex::new(BTreeMap::new())),
            last_inventory_upload: Arc::new(Mutex::new(None)),
        }))
    }

    pub(crate) async fn run(self: Arc<Self>, context: ChildServiceContext) {
        let shutdown = context.task_group().cancellation_token();
        let mut retry = RETRY_MIN;
        loop {
            if shutdown.is_cancelled() {
                self.cancel_all().await;
                return;
            }
            self.connector.set_channel_ready(false);
            match self.register_and_poll(&context).await {
                Ok(()) => return,
                Err(error) => {
                    tracing::warn!(
                        code = error.code.as_str(),
                        retryable = error.retryable,
                        "control-plane reverse channel disconnected"
                    );
                }
            }
            tokio::select! {
                _ = shutdown.cancelled() => {
                    self.cancel_all().await;
                    return;
                }
                _ = tokio::time::sleep(retry) => {}
            }
            retry = retry.saturating_mul(2).min(RETRY_MAX);
        }
    }

    async fn register_and_poll(&self, context: &ChildServiceContext) -> Result<(), ConnectorError> {
        let capability = self.connector.sources_capability().await;
        capability.validate_read_only().map_err(|_| {
            ConnectorError::capability(
                ConnectorErrorCode::CapabilityMismatch,
                "connector advertised a mutation capability",
            )
        })?;
        let register = ConnectorRegister {
            schema: channel_schema(),
            session_id: self.session_id,
            tenant_id: self.connector_config.tenant_id,
            cluster_id: self.config.cluster_id,
            subject: self.config.connector_subject.clone(),
            capability,
            observed_at: Utc::now(),
        };
        let acknowledgement: RegisterAcknowledgement =
            self.post_json("/internal/v1/connectors/v1/register", &register).await?;
        validate_schema(&acknowledgement.schema)?;
        if !acknowledgement.accepted {
            return Err(ConnectorError::new(
                ConnectorErrorCode::UnauthorizedScope,
                false,
                "control plane rejected connector registration",
            ));
        }
        self.connector.set_channel_ready(true);
        self.try_inventory_upload(true).await;
        self.flush_pending().await?;
        let mut after_sequence = acknowledgement.resume_after_sequence;
        let mut heartbeat = tokio::time::interval(self.config.heartbeat_interval);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        heartbeat.tick().await;
        let shutdown = context.task_group().cancellation_token();

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    self.cancel_all().await;
                    return Ok(());
                }
                _ = heartbeat.tick() => {
                    self.heartbeat().await?;
                }
                poll = self.poll(after_sequence) => {
                    let commands = poll?;
                    for command in commands {
                        let sequence = command.sequence();
                        if sequence <= after_sequence {
                            continue;
                        }
                        if sequence != after_sequence.saturating_add(1) {
                            return Err(ConnectorError::capability(
                                ConnectorErrorCode::CapabilityMismatch,
                                "control-plane command sequence is not contiguous",
                            ));
                        }
                        self.handle_command(command, context).await?;
                        after_sequence = sequence;
                    }
                }
            }
        }
    }

    async fn heartbeat(&self) -> Result<(), ConnectorError> {
        let heartbeat = ConnectorHeartbeat {
            schema: channel_schema(),
            session_id: self.session_id,
            tenant_id: self.connector_config.tenant_id,
            cluster_id: self.config.cluster_id,
            capability: self.connector.sources_capability().await,
            observed_at: Utc::now(),
        };
        self.post_empty("/internal/v1/connectors/v1/heartbeat", &heartbeat)
            .await?;
        self.try_inventory_upload(false).await;
        Ok(())
    }

    async fn poll(&self, after_sequence: u64) -> Result<Vec<ChannelCommand>, ConnectorError> {
        let request = PollRequest {
            schema: channel_schema(),
            session_id: self.session_id,
            after_sequence,
            wait_millis: self.config.poll_wait.as_millis().min(u64::MAX as u128) as u64,
            max_commands: MAX_COMMANDS_PER_POLL,
        };
        let response: PollResponse = self
            .post_json(
                &format!("/internal/v1/connectors/v1/{}/commands:poll", self.session_id),
                &request,
            )
            .await?;
        validate_schema(&response.schema)?;
        if response.commands.len() > MAX_COMMANDS_PER_POLL {
            return Err(ConnectorError::new(
                ConnectorErrorCode::OutputTooLarge,
                false,
                "control-plane command batch exceeds the negotiated bound",
            ));
        }
        for command in &response.commands {
            command.validate(self.session_id)?;
        }
        Ok(response.commands)
    }

    async fn handle_command(
        &self,
        command: ChannelCommand,
        context: &ChildServiceContext,
    ) -> Result<(), ConnectorError> {
        match command {
            ChannelCommand::Query { envelope } => {
                let cancel = CancelSignal::default();
                {
                    let mut active = self.active.lock().await;
                    if active.contains_key(&envelope.correlation_id) {
                        return Err(ConnectorError::capability(
                            ConnectorErrorCode::CapabilityMismatch,
                            "duplicate in-flight correlation identifier",
                        ));
                    }
                    active.insert(envelope.correlation_id, cancel.clone());
                }
                let channel = Arc::new(self.clone_for_task());
                let active = self.active.clone();
                let correlation_id = envelope.correlation_id;
                let sequence = envelope.sequence;
                let session_id = self.session_id;
                let spawn_result = context.spawn(format!("connector-query-{sequence}"), TaskKind::Worker, async move {
                    let result = channel
                        .connector
                        .collect_contract_query(
                            envelope.query,
                            &channel.config.connector_subject,
                            envelope.deadline,
                            &cancel,
                        )
                        .await;
                    let response = match result {
                        Ok(evidence) => ConnectorResponseEnvelope {
                            schema: channel_schema(),
                            session_id,
                            correlation_id,
                            sequence,
                            evidence: Some(evidence),
                            error_code: None,
                            retryable: false,
                        },
                        Err(error) => ConnectorResponseEnvelope {
                            schema: channel_schema(),
                            session_id,
                            correlation_id,
                            sequence,
                            evidence: None,
                            error_code: Some(error.code.as_str().to_owned()),
                            retryable: error.retryable,
                        },
                    };
                    if let Err(error) = channel.deliver_response(response).await {
                        tracing::warn!(
                            code = error.code.as_str(),
                            sequence,
                            "connector could not deliver a bounded evidence response"
                        );
                    }
                    active.lock().await.remove(&correlation_id);
                });
                if spawn_result.is_err() {
                    self.active.lock().await.remove(&correlation_id);
                    return Err(ConnectorError::new(
                        ConnectorErrorCode::ChannelUnavailable,
                        true,
                        "query worker could not be owned by the connector TaskGroup",
                    ));
                }
                Ok(())
            }
            ChannelCommand::Cancel {
                correlation_id,
                sequence,
                ..
            } => {
                if let Some(cancel) = self.active.lock().await.get(&correlation_id).cloned() {
                    cancel.cancel();
                }
                self.deliver_response(ConnectorResponseEnvelope {
                    schema: channel_schema(),
                    session_id: self.session_id,
                    correlation_id,
                    sequence,
                    evidence: None,
                    error_code: Some(ConnectorErrorCode::QueryCancelled.as_str().to_owned()),
                    retryable: false,
                })
                .await
            }
        }
    }

    fn clone_for_task(&self) -> Self {
        Self {
            connector: self.connector.clone(),
            connector_config: self.connector_config.clone(),
            config: self.config.clone(),
            client: self.client.clone(),
            session_id: self.session_id,
            active: self.active.clone(),
            pending_responses: self.pending_responses.clone(),
            last_inventory_upload: self.last_inventory_upload.clone(),
        }
    }

    async fn try_inventory_upload(&self, force: bool) {
        if let Err(error) = self.upload_inventory(force).await {
            tracing::warn!(
                code = error.code.as_str(),
                retryable = error.retryable,
                "bounded read-only inventory upload is temporarily unavailable"
            );
        }
    }

    async fn upload_inventory(&self, force: bool) -> Result<(), ConnectorError> {
        if !force
            && self
                .last_inventory_upload
                .lock()
                .await
                .is_some_and(|last| last.elapsed() < INVENTORY_REFRESH_MIN)
        {
            return Ok(());
        }
        let inventory = self
            .connector
            .inventory(self.config.cluster_id, &self.config.connector_subject)
            .await?;
        let path = format!("/internal/v1/connectors/v1/{}/inventory", self.session_id);
        let response = self.send(&path, &inventory).await?;
        let cancel = CancelSignal::default();
        let deadline = Utc::now()
            + chrono::Duration::from_std(self.connector_config.request_timeout)
                .unwrap_or_else(|_| chrono::Duration::seconds(15));
        bounded_response(response, MAX_CHANNEL_RESPONSE_BYTES, deadline, &cancel).await?;
        *self.last_inventory_upload.lock().await = Some(Instant::now());
        Ok(())
    }

    async fn deliver_response(&self, response: ConnectorResponseEnvelope) -> Result<(), ConnectorError> {
        {
            let mut pending = self.pending_responses.lock().await;
            if pending.len() >= MAX_PENDING_RESPONSES && !pending.contains_key(&response.sequence) {
                return Err(ConnectorError::new(
                    ConnectorErrorCode::ChannelUnavailable,
                    true,
                    "connector pending response budget is exhausted",
                ));
            }
            pending.insert(response.sequence, response.clone());
        }
        self.post_response(&response).await?;
        self.pending_responses.lock().await.remove(&response.sequence);
        Ok(())
    }

    async fn flush_pending(&self) -> Result<(), ConnectorError> {
        let responses = self
            .pending_responses
            .lock()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for response in responses {
            self.post_response(&response).await?;
            self.pending_responses.lock().await.remove(&response.sequence);
        }
        Ok(())
    }

    async fn post_response(&self, response: &ConnectorResponseEnvelope) -> Result<(), ConnectorError> {
        self.post_empty(
            &format!("/internal/v1/connectors/v1/{}/responses", self.session_id),
            response,
        )
        .await
    }

    async fn cancel_all(&self) {
        let mut active = self.active.lock().await;
        for cancel in active.values() {
            cancel.cancel();
        }
        active.clear();
        self.connector.set_channel_ready(false);
    }

    async fn post_json<T, R>(&self, path: &str, body: &T) -> Result<R, ConnectorError>
    where
        T: Serialize + ?Sized,
        R: for<'de> Deserialize<'de>,
    {
        let response = self.send(path, body).await?;
        let cancel = CancelSignal::default();
        let deadline = Utc::now()
            + chrono::Duration::from_std(self.connector_config.request_timeout)
                .unwrap_or_else(|_| chrono::Duration::seconds(15));
        let bytes = bounded_response(response, MAX_CHANNEL_RESPONSE_BYTES, deadline, &cancel).await?;
        serde_json::from_slice(&bytes)
            .map_err(|_| ConnectorError::source("control-plane channel response is invalid JSON"))
    }

    async fn post_empty<T>(&self, path: &str, body: &T) -> Result<(), ConnectorError>
    where
        T: Serialize + ?Sized,
    {
        let response = self.send(path, body).await?;
        let cancel = CancelSignal::default();
        let deadline = Utc::now()
            + chrono::Duration::from_std(self.connector_config.request_timeout)
                .unwrap_or_else(|_| chrono::Duration::seconds(15));
        bounded_response(response, 4096, deadline, &cancel).await?;
        Ok(())
    }

    async fn send<T>(&self, path: &str, body: &T) -> Result<reqwest::Response, ConnectorError>
    where
        T: Serialize + ?Sized,
    {
        let endpoint = self
            .config
            .base_url
            .join(path)
            .map_err(|_| ConnectorError::configuration("control-plane channel URL cannot be constructed"))?;
        let response = self
            .client
            .post(endpoint)
            .bearer_auth(self.connector_config.internal_token())
            .header("x-rocketmq-connector-subject", &self.config.connector_subject)
            .header("x-rocketmq-connector-issuer", &self.config.connector_issuer)
            .json(body)
            .send()
            .await
            .map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorCode::ChannelUnavailable,
                    true,
                    "control-plane channel request failed",
                )
            })?;
        if response.version() != reqwest::Version::HTTP_2 {
            return Err(ConnectorError::capability(
                ConnectorErrorCode::CapabilityMismatch,
                "control-plane connector channel did not negotiate HTTP/2",
            ));
        }
        match response.status() {
            status if status.is_success() => Ok(response),
            reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN => Err(ConnectorError::new(
                ConnectorErrorCode::UnauthorizedScope,
                false,
                "control plane rejected connector channel authorization",
            )),
            reqwest::StatusCode::CONFLICT => Err(ConnectorError::capability(
                ConnectorErrorCode::CapabilityMismatch,
                "control plane rejected the connector channel contract",
            )),
            _ => Err(ConnectorError::new(
                ConnectorErrorCode::ChannelUnavailable,
                true,
                "control-plane channel returned an unsuccessful status",
            )),
        }
    }
}

impl ChannelCommand {
    fn sequence(&self) -> u64 {
        match self {
            Self::Query { envelope } => envelope.sequence,
            Self::Cancel { sequence, .. } => *sequence,
        }
    }

    fn validate(&self, expected_session: ConnectorSessionId) -> Result<(), ConnectorError> {
        match self {
            Self::Query { envelope } => {
                validate_schema(&envelope.schema)?;
                if envelope.session_id != expected_session {
                    return Err(session_mismatch());
                }
            }
            Self::Cancel { schema, session_id, .. } => {
                validate_schema(schema)?;
                if *session_id != expected_session {
                    return Err(session_mismatch());
                }
            }
        }
        Ok(())
    }
}

fn session_mismatch() -> ConnectorError {
    ConnectorError::capability(
        ConnectorErrorCode::CapabilityMismatch,
        "control-plane command session does not match this connector",
    )
}

fn channel_schema() -> SchemaVersion {
    SchemaVersion::new(CHANNEL_SCHEMA_FAMILY, CHANNEL_SCHEMA_MAJOR, 0)
}

fn validate_schema(schema: &SchemaVersion) -> Result<(), ConnectorError> {
    schema
        .ensure_compatible(
            CHANNEL_SCHEMA_FAMILY,
            CHANNEL_SCHEMA_MAJOR,
            &BTreeSet::from(["cancel".to_owned(), "reverse_poll".to_owned()]),
        )
        .map_err(|error| match error {
            rocketmq_sre_contracts::ContractError::UnsupportedSchemaMajor { .. }
            | rocketmq_sre_contracts::ContractError::UnsupportedSchemaFamily { .. } => ConnectorError::capability(
                ConnectorErrorCode::UnsupportedSchemaMajor,
                "control-plane channel schema is unsupported",
            ),
            rocketmq_sre_contracts::ContractError::MissingRequiredFeature { .. } => ConnectorError::capability(
                ConnectorErrorCode::MissingRequiredFeature,
                "control-plane channel requires an unsupported feature",
            ),
            _ => ConnectorError::capability(
                ConnectorErrorCode::CapabilityMismatch,
                "control-plane channel schema is invalid",
            ),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expired_query_remains_a_valid_durable_channel_command() {
        let session_id = ConnectorSessionId::new();
        let now = Utc::now();
        let correlation_id = CorrelationId::new();
        let command = ChannelCommand::Query {
            envelope: ConnectorQueryEnvelope {
                schema: channel_schema(),
                session_id,
                correlation_id,
                sequence: 1,
                deadline: now - chrono::Duration::seconds(1),
                query: rocketmq_sre_contracts::EvidenceQuery {
                    query_id: rocketmq_sre_contracts::QueryId::new(),
                    correlation_id,
                    tenant_id: rocketmq_sre_contracts::TenantId::new(),
                    cluster_id: rocketmq_sre_contracts::ClusterId::new(),
                    source: "rocketmq-mcp".to_owned(),
                    resource: "consumer-lag/example".to_owned(),
                    time_range: rocketmq_sre_contracts::TimeRange::new(now, now).expect("test range is valid"),
                },
            },
        };

        command
            .validate(session_id)
            .expect("expired work must reach the query worker and produce a terminal response");
    }

    #[test]
    fn cancel_command_fails_closed_on_cross_session_use() {
        let session_id = ConnectorSessionId::new();
        let command = ChannelCommand::Cancel {
            schema: channel_schema(),
            session_id: ConnectorSessionId::new(),
            correlation_id: CorrelationId::new(),
            sequence: 1,
        };
        assert_eq!(
            command.validate(session_id).expect_err("session mismatch").code,
            ConnectorErrorCode::CapabilityMismatch
        );
    }

    #[test]
    fn unknown_required_channel_feature_is_rejected() {
        let schema = channel_schema().requiring(["future_feature"]);
        assert_eq!(
            validate_schema(&schema).expect_err("unknown feature").code,
            ConnectorErrorCode::MissingRequiredFeature
        );
    }
}
