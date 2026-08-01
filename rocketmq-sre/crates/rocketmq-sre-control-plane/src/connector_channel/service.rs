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

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use axum::http::HeaderMap;
use chrono::Utc;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::ConnectorCapabilityState;
use rocketmq_sre_contracts::ConnectorHeartbeat;
use rocketmq_sre_contracts::ConnectorRegister;
use rocketmq_sre_contracts::ConnectorResponseEnvelope;
use rocketmq_sre_contracts::ConnectorSessionId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::EvidenceQuery;
use rocketmq_sre_contracts::TenantId;
use subtle::ConstantTimeEq;
use tokio::sync::watch;

use super::ConnectorChannelStatus;
use super::ConnectorChannelStore;
use super::ConnectorCommand;
use super::ConnectorLiveness;
use super::ConnectorPrincipal;
use super::MAX_SOURCES;
use super::PollRequest;
use super::PollResponse;
use super::PostgresConnectorChannelStore;
use super::RegisterAcknowledgement;
use super::ResponseDisposition;
use super::channel_schema;
use super::validate_channel_schema;
use super::validate_poll_request;
use super::validate_response;
use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::observability::ConnectorHealthSample;
use crate::observability::DependencyStatus;
use crate::observability::HealthReasonCode;

const DEFAULT_STALE_AFTER: Duration = Duration::from_secs(45);
const MAX_HEALTH_SAMPLES: usize = 256;
const MAX_SOURCE_NAME_BYTES: usize = 128;
const MAX_RESOURCE_BYTES: usize = 2048;

#[derive(Clone)]
pub(crate) struct ConnectorChannelService<S = PostgresConnectorChannelStore> {
    store: S,
    internal_token: Arc<str>,
    stale_after: Duration,
    command_signal: watch::Sender<u64>,
    response_signal: watch::Sender<u64>,
}

impl ConnectorChannelService<PostgresConnectorChannelStore> {
    pub(crate) fn postgres(
        repository: PostgresRepository,
        internal_token: impl Into<Arc<str>>,
    ) -> Result<Self, ControlPlaneError> {
        Self::new(PostgresConnectorChannelStore::new(repository), internal_token)
    }
}

impl<S> ConnectorChannelService<S>
where
    S: ConnectorChannelStore,
{
    pub(crate) fn new(store: S, internal_token: impl Into<Arc<str>>) -> Result<Self, ControlPlaneError> {
        let internal_token = internal_token.into();
        if internal_token.is_empty() {
            return Err(ControlPlaneError::configuration(
                "connector channel internal token must not be empty",
            ));
        }
        let (command_signal, _) = watch::channel(0);
        let (response_signal, _) = watch::channel(0);
        Ok(Self {
            store,
            internal_token,
            stale_after: DEFAULT_STALE_AFTER,
            command_signal,
            response_signal,
        })
    }

    #[cfg(test)]
    fn with_stale_after(mut self, stale_after: Duration) -> Self {
        self.stale_after = stale_after;
        self
    }

    pub(crate) fn authenticate(&self, headers: &HeaderMap) -> Result<ConnectorPrincipal, ControlPlaneError> {
        let supplied = required_header(headers, axum::http::header::AUTHORIZATION.as_str())?
            .strip_prefix("Bearer ")
            .filter(|value| !value.is_empty())
            .ok_or(ControlPlaneError::Unauthorized)?;
        let token_matches = supplied.len() == self.internal_token.len()
            && bool::from(supplied.as_bytes().ct_eq(self.internal_token.as_bytes()));
        if !token_matches {
            return Err(ControlPlaneError::Unauthorized);
        }
        let subject = bounded_identity_header(headers, "x-rocketmq-connector-subject", 512)?;
        let issuer = bounded_identity_header(headers, "x-rocketmq-connector-issuer", 1024)?;
        Ok(ConnectorPrincipal { subject, issuer })
    }

    pub(crate) async fn register(
        &self,
        principal: &ConnectorPrincipal,
        request: &ConnectorRegister,
    ) -> Result<RegisterAcknowledgement, ControlPlaneError> {
        validate_channel_schema(&request.schema)?;
        if request.subject != principal.subject {
            return Err(ControlPlaneError::forbidden(
                "unauthorized_scope",
                "connector body subject does not match the authenticated identity",
            ));
        }
        validate_capability(&request.capability)?;
        let result = self.store.register(principal, request).await?;
        Ok(RegisterAcknowledgement {
            schema: channel_schema(),
            accepted: true,
            resume_after_sequence: result.resume_after_sequence,
        })
    }

    pub(crate) async fn heartbeat(
        &self,
        principal: &ConnectorPrincipal,
        request: &ConnectorHeartbeat,
    ) -> Result<(), ControlPlaneError> {
        validate_channel_schema(&request.schema)?;
        validate_capability(&request.capability)?;
        self.store.heartbeat(principal, request).await?;
        Ok(())
    }

    pub(crate) async fn poll(
        &self,
        principal: &ConnectorPrincipal,
        path_session_id: ConnectorSessionId,
        request: &PollRequest,
    ) -> Result<PollResponse, ControlPlaneError> {
        validate_poll_request(path_session_id, request)?;
        let scope = self.store.session_scope(principal, path_session_id).await?;
        let mut signal = self.command_signal.subscribe();
        let mut commands = self
            .store
            .commands(&scope, request.after_sequence, request.max_commands)
            .await?;
        if commands.is_empty() && request.wait_millis > 0 {
            let wait = Duration::from_millis(request.wait_millis);
            let _ = tokio::time::timeout(wait, signal.changed()).await;
            commands = self
                .store
                .commands(&scope, request.after_sequence, request.max_commands)
                .await?;
        }
        Ok(PollResponse {
            schema: channel_schema(),
            commands,
        })
    }

    pub(crate) async fn submit_response(
        &self,
        principal: &ConnectorPrincipal,
        path_session_id: ConnectorSessionId,
        response: &ConnectorResponseEnvelope,
    ) -> Result<ResponseDisposition, ControlPlaneError> {
        let scope = self.store.session_scope(principal, path_session_id).await?;
        validate_response(path_session_id, &scope, response)?;
        let disposition = self.store.append_response(&scope, response).await?;
        self.signal_response();
        Ok(disposition)
    }

    /// Resolves the tenant and cluster owned by an authenticated, live
    /// Connector session.
    ///
    /// This is the authorization boundary for Connector-initiated auxiliary
    /// uploads such as normalized inventory. Callers must derive scope from
    /// the persisted session instead of trusting tenant or cluster headers.
    pub(crate) async fn authorize_session(
        &self,
        principal: &ConnectorPrincipal,
        session_id: ConnectorSessionId,
    ) -> Result<super::SessionScope, ControlPlaneError> {
        let scope = self.store.session_scope(principal, session_id).await?;
        if scope.last_heartbeat_at < self.stale_before() {
            return Err(ControlPlaneError::forbidden(
                "unauthorized_scope",
                "stale connector sessions cannot upload cluster inventory",
            ));
        }
        Ok(scope)
    }

    pub(crate) async fn enqueue_query(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        query: EvidenceQuery,
        deadline: chrono::DateTime<Utc>,
    ) -> Result<ConnectorCommand, ControlPlaneError> {
        validate_query(tenant_id, cluster_id, &query, deadline)?;
        let command = self
            .store
            .enqueue_query(tenant_id, cluster_id, query, deadline, self.stale_before())
            .await?;
        self.signal_command();
        Ok(command)
    }

    pub(crate) async fn enqueue_cancel(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        correlation_id: CorrelationId,
    ) -> Result<ConnectorCommand, ControlPlaneError> {
        let command = self.store.enqueue_cancel(tenant_id, cluster_id, correlation_id).await?;
        self.signal_command();
        Ok(command)
    }

    pub(crate) async fn query_and_wait(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        query: EvidenceQuery,
        deadline: chrono::DateTime<Utc>,
    ) -> Result<ConnectorResponseEnvelope, ControlPlaneError> {
        let mut response_signal = self.response_signal.subscribe();
        let command = self.enqueue_query(tenant_id, cluster_id, query, deadline).await?;
        let session_id = match &command {
            ConnectorCommand::Query { envelope } => envelope.session_id,
            ConnectorCommand::Cancel { .. } => {
                return Err(ControlPlaneError::configuration(
                    "connector query was persisted as an invalid command kind",
                ));
            }
        };
        let sequence = command.sequence();
        let correlation_id = command.correlation_id();
        loop {
            if let Some(response) = self.store.response(session_id, sequence).await? {
                return Ok(response);
            }
            let Ok(remaining) = (deadline - Utc::now()).to_std() else {
                let _ = self.enqueue_cancel(tenant_id, cluster_id, correlation_id).await;
                return Ok(deadline_response(session_id, sequence, correlation_id));
            };
            if remaining.is_zero()
                || tokio::time::timeout(remaining, response_signal.changed())
                    .await
                    .is_err()
            {
                let _ = self.enqueue_cancel(tenant_id, cluster_id, correlation_id).await;
                return Ok(deadline_response(session_id, sequence, correlation_id));
            }
        }
    }

    pub(crate) async fn status(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
    ) -> Result<Option<ConnectorChannelStatus>, ControlPlaneError> {
        let Some(scope) = self.store.latest_session(tenant_id, cluster_id).await? else {
            return Ok(None);
        };
        let liveness = if scope.last_heartbeat_at >= self.stale_before() {
            ConnectorLiveness::Online
        } else {
            ConnectorLiveness::Stale
        };
        Ok(Some(ConnectorChannelStatus {
            session_id: scope.session_id,
            tenant_id: scope.tenant_id,
            cluster_id: scope.cluster_id,
            liveness,
            last_heartbeat_at: scope.last_heartbeat_at,
        }))
    }

    pub(crate) async fn health_samples(&self, limit: usize) -> Result<Vec<ConnectorHealthSample>, ControlPlaneError> {
        if limit == 0 || limit > MAX_HEALTH_SAMPLES {
            return Err(ControlPlaneError::validation(
                "output_too_large",
                "connector health sample limit must be between 1 and 256",
            ));
        }
        let stale_before = self.stale_before();
        let now = Utc::now();
        let samples = self
            .store
            .latest_sessions(limit)
            .await?
            .into_iter()
            .map(|scope| {
                let online = scope.last_heartbeat_at >= stale_before;
                let heartbeat_age_seconds =
                    u64::try_from((now - scope.last_heartbeat_at).num_seconds()).unwrap_or_default();
                ConnectorHealthSample::new(
                    if online {
                        DependencyStatus::Healthy
                    } else {
                        DependencyStatus::Degraded
                    },
                    Some(heartbeat_age_seconds),
                    scope.queryable_sources,
                    (!online).then_some(HealthReasonCode::HeartbeatStale),
                )
            })
            .collect();
        Ok(samples)
    }

    fn stale_before(&self) -> chrono::DateTime<Utc> {
        Utc::now() - chrono::Duration::from_std(self.stale_after).unwrap_or_else(|_| chrono::Duration::seconds(45))
    }

    fn signal_command(&self) {
        let next = self.command_signal.borrow().wrapping_add(1);
        self.command_signal.send_replace(next);
    }

    fn signal_response(&self) {
        let next = self.response_signal.borrow().wrapping_add(1);
        self.response_signal.send_replace(next);
    }
}

fn deadline_response(
    session_id: ConnectorSessionId,
    sequence: u64,
    correlation_id: CorrelationId,
) -> ConnectorResponseEnvelope {
    ConnectorResponseEnvelope {
        schema: channel_schema(),
        session_id,
        correlation_id,
        sequence,
        evidence: None,
        error_code: Some("deadline_exceeded".to_owned()),
        retryable: true,
    }
}

fn validate_capability(capability: &ConnectorCapabilityState) -> Result<(), ControlPlaneError> {
    capability.validate_read_only().map_err(|_| {
        ControlPlaneError::forbidden(
            "capability_mismatch",
            "connector channel rejects mutation-capable identities",
        )
    })?;
    if capability.sources.len() > MAX_SOURCES {
        return Err(ControlPlaneError::validation(
            "output_too_large",
            "connector advertises more than 64 evidence sources",
        ));
    }
    let mut names = BTreeSet::new();
    for source in &capability.sources {
        if source.source.is_empty()
            || source.source.len() > MAX_SOURCE_NAME_BYTES
            || !names.insert(source.source.as_str())
        {
            return Err(ControlPlaneError::validation(
                "capability_mismatch",
                "connector source names must be unique, non-empty, and bounded",
            ));
        }
        if source.schema_major == 0
            || source.max_rows == 0
            || source.max_bytes == 0
            || source.max_time_range_seconds == 0
        {
            return Err(ControlPlaneError::validation(
                "capability_mismatch",
                "connector source bounds and schema major must be positive",
            ));
        }
    }
    Ok(())
}

fn validate_query(
    tenant_id: TenantId,
    cluster_id: ClusterId,
    query: &EvidenceQuery,
    deadline: chrono::DateTime<Utc>,
) -> Result<(), ControlPlaneError> {
    if query.tenant_id != tenant_id {
        return Err(ControlPlaneError::forbidden(
            "tenant_mismatch",
            "evidence query crosses the requested tenant boundary",
        ));
    }
    if query.cluster_id != cluster_id {
        return Err(ControlPlaneError::forbidden(
            "cluster_not_allowed",
            "evidence query crosses the requested cluster boundary",
        ));
    }
    if query.source.is_empty()
        || query.source.len() > MAX_SOURCE_NAME_BYTES
        || query.resource.is_empty()
        || query.resource.len() > MAX_RESOURCE_BYTES
        || query.time_range.start > query.time_range.end
    {
        return Err(ControlPlaneError::validation(
            "capability_mismatch",
            "evidence query source, resource, or time range is invalid",
        ));
    }
    if deadline <= Utc::now() {
        return Err(ControlPlaneError::validation(
            "capability_mismatch",
            "connector query deadline has elapsed",
        ));
    }
    Ok(())
}

fn required_header<'a>(headers: &'a HeaderMap, name: &str) -> Result<&'a str, ControlPlaneError> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .ok_or(ControlPlaneError::Unauthorized)
}

fn bounded_identity_header(headers: &HeaderMap, name: &str, max_bytes: usize) -> Result<String, ControlPlaneError> {
    let value = required_header(headers, name)?;
    if value.len() > max_bytes || value.chars().any(char::is_control) {
        return Err(ControlPlaneError::forbidden(
            "unauthorized_scope",
            "connector identity header is invalid",
        ));
    }
    Ok(value.to_owned())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::DateTime;
    use rocketmq_sre_contracts::ConnectorCapabilityState;
    use rocketmq_sre_contracts::ConnectorSourceCapability;
    use rocketmq_sre_contracts::ConnectorSourceStatus;
    use rocketmq_sre_contracts::QueryId;
    use rocketmq_sre_contracts::SchemaVersion;
    use rocketmq_sre_contracts::TimeRange;
    use tokio::sync::Mutex;

    use super::*;
    use crate::connector_channel::SessionScope;
    use crate::connector_channel::model::CHANNEL_SCHEMA_FAMILY;
    use crate::connector_channel::repository::RegistrationResult;

    #[derive(Clone, Default)]
    struct MemoryStore {
        state: Arc<Mutex<MemoryState>>,
    }

    #[derive(Default)]
    struct MemoryState {
        sessions: BTreeMap<ConnectorSessionId, SessionScope>,
        capabilities: BTreeMap<ConnectorSessionId, ConnectorCapabilityState>,
        commands: BTreeMap<ConnectorSessionId, Vec<ConnectorCommand>>,
        responses: BTreeMap<(ConnectorSessionId, u64), ConnectorResponseEnvelope>,
    }

    impl ConnectorChannelStore for MemoryStore {
        async fn register(
            &self,
            principal: &ConnectorPrincipal,
            request: &ConnectorRegister,
        ) -> Result<RegistrationResult, ControlPlaneError> {
            let mut state = self.state.lock().await;
            if let Some(existing) = state.sessions.get(&request.session_id) {
                if existing.tenant_id != request.tenant_id {
                    return Err(ControlPlaneError::forbidden(
                        "tenant_mismatch",
                        "test session tenant mismatch",
                    ));
                }
                if existing.cluster_id != request.cluster_id
                    || existing.subject != principal.subject
                    || existing.issuer != principal.issuer
                {
                    return Err(ControlPlaneError::Forbidden {
                        code: "unauthorized_scope",
                        detail: "test session scope mismatch".to_owned(),
                    });
                }
            }
            let scope = SessionScope {
                session_id: request.session_id,
                tenant_id: request.tenant_id,
                cluster_id: request.cluster_id,
                subject: principal.subject.clone(),
                issuer: principal.issuer.clone(),
                last_heartbeat_at: Utc::now(),
                queryable_sources: u16::try_from(
                    request
                        .capability
                        .sources
                        .iter()
                        .filter(|source| {
                            matches!(source.status, rocketmq_sre_contracts::ConnectorSourceStatus::Queryable)
                        })
                        .count(),
                )
                .unwrap_or(u16::MAX),
            };
            state.sessions.insert(request.session_id, scope.clone());
            state
                .capabilities
                .insert(request.session_id, request.capability.clone());
            let resume_after_sequence = response_frontier(
                state
                    .commands
                    .get(&request.session_id)
                    .map(Vec::as_slice)
                    .unwrap_or(&[]),
                &state.responses,
                request.session_id,
            );
            Ok(RegistrationResult { resume_after_sequence })
        }

        async fn heartbeat(
            &self,
            principal: &ConnectorPrincipal,
            request: &ConnectorHeartbeat,
        ) -> Result<SessionScope, ControlPlaneError> {
            let mut state = self.state.lock().await;
            let scope = state
                .sessions
                .get_mut(&request.session_id)
                .ok_or(ControlPlaneError::NotFound)?;
            enforce_memory_scope(scope, request.tenant_id, request.cluster_id, principal)?;
            scope.last_heartbeat_at = Utc::now();
            let updated = scope.clone();
            state
                .capabilities
                .insert(request.session_id, request.capability.clone());
            Ok(updated)
        }

        async fn session_scope(
            &self,
            principal: &ConnectorPrincipal,
            session_id: ConnectorSessionId,
        ) -> Result<SessionScope, ControlPlaneError> {
            let state = self.state.lock().await;
            let scope = state.sessions.get(&session_id).ok_or(ControlPlaneError::NotFound)?;
            enforce_memory_scope(scope, scope.tenant_id, scope.cluster_id, principal)?;
            Ok(scope.clone())
        }

        async fn commands(
            &self,
            scope: &SessionScope,
            after_sequence: u64,
            max_commands: usize,
        ) -> Result<Vec<ConnectorCommand>, ControlPlaneError> {
            let state = self.state.lock().await;
            Ok(state
                .commands
                .get(&scope.session_id)
                .into_iter()
                .flatten()
                .filter(|command| command.sequence() > after_sequence)
                .take(max_commands)
                .cloned()
                .collect())
        }

        async fn enqueue_query(
            &self,
            tenant_id: TenantId,
            cluster_id: ClusterId,
            query: EvidenceQuery,
            deadline: DateTime<Utc>,
            stale_before: DateTime<Utc>,
        ) -> Result<ConnectorCommand, ControlPlaneError> {
            let mut state = self.state.lock().await;
            let scope = state
                .sessions
                .values()
                .find(|scope| {
                    scope.tenant_id == tenant_id
                        && scope.cluster_id == cluster_id
                        && scope.last_heartbeat_at >= stale_before
                        && state.capabilities.get(&scope.session_id).is_some_and(|capability| {
                            capability.sources.iter().any(|source| {
                                source.source == query.source
                                    && matches!(
                                        source.status,
                                        ConnectorSourceStatus::Queryable | ConnectorSourceStatus::Degraded
                                    )
                            })
                        })
                })
                .cloned()
                .ok_or_else(|| ControlPlaneError::conflict("no test connector"))?;
            let commands = state.commands.entry(scope.session_id).or_default();
            let sequence = commands.len() as u64 + 1;
            let command = ConnectorCommand::Query {
                envelope: rocketmq_sre_contracts::ConnectorQueryEnvelope {
                    schema: channel_schema(),
                    session_id: scope.session_id,
                    correlation_id: query.correlation_id,
                    sequence,
                    deadline,
                    query,
                },
            };
            commands.push(command.clone());
            Ok(command)
        }

        async fn enqueue_cancel(
            &self,
            tenant_id: TenantId,
            cluster_id: ClusterId,
            correlation_id: CorrelationId,
        ) -> Result<ConnectorCommand, ControlPlaneError> {
            let mut state = self.state.lock().await;
            let scope = state
                .sessions
                .values()
                .find(|scope| scope.tenant_id == tenant_id && scope.cluster_id == cluster_id)
                .cloned()
                .ok_or_else(|| ControlPlaneError::conflict("no test connector"))?;
            let commands = state.commands.entry(scope.session_id).or_default();
            let sequence = commands.len() as u64 + 1;
            let command = ConnectorCommand::Cancel {
                schema: channel_schema(),
                session_id: scope.session_id,
                correlation_id,
                sequence,
            };
            commands.push(command.clone());
            Ok(command)
        }

        async fn append_response(
            &self,
            scope: &SessionScope,
            response: &ConnectorResponseEnvelope,
        ) -> Result<ResponseDisposition, ControlPlaneError> {
            let mut state = self.state.lock().await;
            let expected = state
                .commands
                .get(&scope.session_id)
                .and_then(|commands| commands.iter().find(|command| command.sequence() == response.sequence))
                .ok_or_else(|| ControlPlaneError::conflict("test command missing"))?;
            if expected.correlation_id() != response.correlation_id {
                return Err(ControlPlaneError::conflict("test correlation mismatch"));
            }
            let key = (scope.session_id, response.sequence);
            if let Some(existing) = state.responses.get(&key) {
                if existing.correlation_id != response.correlation_id {
                    return Err(ControlPlaneError::conflict("test duplicate correlation mismatch"));
                }
                return Ok(ResponseDisposition::Duplicate);
            }
            state.responses.insert(key, response.clone());
            Ok(ResponseDisposition::Inserted)
        }

        async fn response(
            &self,
            session_id: ConnectorSessionId,
            sequence: u64,
        ) -> Result<Option<ConnectorResponseEnvelope>, ControlPlaneError> {
            Ok(self.state.lock().await.responses.get(&(session_id, sequence)).cloned())
        }

        async fn latest_session(
            &self,
            tenant_id: TenantId,
            cluster_id: ClusterId,
        ) -> Result<Option<SessionScope>, ControlPlaneError> {
            Ok(self
                .state
                .lock()
                .await
                .sessions
                .values()
                .find(|scope| scope.tenant_id == tenant_id && scope.cluster_id == cluster_id)
                .cloned())
        }

        async fn latest_sessions(&self, limit: usize) -> Result<Vec<SessionScope>, ControlPlaneError> {
            Ok(self.state.lock().await.sessions.values().take(limit).cloned().collect())
        }
    }

    fn enforce_memory_scope(
        scope: &SessionScope,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        principal: &ConnectorPrincipal,
    ) -> Result<(), ControlPlaneError> {
        if scope.tenant_id != tenant_id {
            return Err(ControlPlaneError::forbidden("tenant_mismatch", "test tenant mismatch"));
        }
        if scope.cluster_id != cluster_id || scope.subject != principal.subject || scope.issuer != principal.issuer {
            return Err(ControlPlaneError::forbidden(
                "unauthorized_scope",
                "test scope mismatch",
            ));
        }
        Ok(())
    }

    fn response_frontier(
        commands: &[ConnectorCommand],
        responses: &BTreeMap<(ConnectorSessionId, u64), ConnectorResponseEnvelope>,
        session_id: ConnectorSessionId,
    ) -> u64 {
        commands
            .iter()
            .take_while(|command| responses.contains_key(&(session_id, command.sequence())))
            .map(ConnectorCommand::sequence)
            .last()
            .unwrap_or(0)
    }

    fn principal() -> ConnectorPrincipal {
        ConnectorPrincipal {
            subject: "connector-a".to_owned(),
            issuer: "issuer-a".to_owned(),
        }
    }

    fn capability(mutation_supported: bool) -> ConnectorCapabilityState {
        capability_with_status(mutation_supported, ConnectorSourceStatus::Queryable)
    }

    fn capability_with_status(mutation_supported: bool, status: ConnectorSourceStatus) -> ConnectorCapabilityState {
        ConnectorCapabilityState {
            mutation_supported,
            sources: vec![ConnectorSourceCapability {
                source: "consumer_lag".to_owned(),
                schema_major: 1,
                status,
                max_rows: 100,
                max_bytes: 65_536,
                max_time_range_seconds: 300,
                last_success_at: None,
                freshness_seconds: Some(15),
            }],
        }
    }

    fn registration(tenant_id: TenantId, cluster_id: ClusterId, session_id: ConnectorSessionId) -> ConnectorRegister {
        ConnectorRegister {
            schema: channel_schema(),
            session_id,
            tenant_id,
            cluster_id,
            subject: principal().subject,
            capability: capability(false),
            observed_at: Utc::now(),
        }
    }

    fn query(tenant_id: TenantId, cluster_id: ClusterId) -> EvidenceQuery {
        let now = Utc::now();
        EvidenceQuery {
            query_id: QueryId::new(),
            correlation_id: CorrelationId::new(),
            tenant_id,
            cluster_id,
            source: "consumer_lag".to_owned(),
            resource: "group-a".to_owned(),
            time_range: TimeRange::new(now - chrono::Duration::minutes(1), now).expect("valid test range"),
        }
    }

    #[tokio::test]
    async fn reconnect_resumes_after_contiguous_responses() {
        let store = MemoryStore::default();
        let service = ConnectorChannelService::new(store, Arc::<str>::from("test-token")).expect("test service");
        let tenant_id = TenantId::new();
        let cluster_id = ClusterId::new();
        let session_id = ConnectorSessionId::new();
        let register = registration(tenant_id, cluster_id, session_id);
        service.register(&principal(), &register).await.expect("register");
        let first = service
            .enqueue_query(
                tenant_id,
                cluster_id,
                query(tenant_id, cluster_id),
                Utc::now() + chrono::Duration::seconds(30),
            )
            .await
            .expect("first query");
        let second = service
            .enqueue_query(
                tenant_id,
                cluster_id,
                query(tenant_id, cluster_id),
                Utc::now() + chrono::Duration::seconds(30),
            )
            .await
            .expect("second query");
        let response = ConnectorResponseEnvelope {
            schema: channel_schema(),
            session_id,
            correlation_id: first.correlation_id(),
            sequence: first.sequence(),
            evidence: None,
            error_code: Some("source_unavailable".to_owned()),
            retryable: true,
        };
        assert_eq!(
            service
                .submit_response(&principal(), session_id, &response)
                .await
                .expect("first response"),
            ResponseDisposition::Inserted
        );
        let acknowledgement = service.register(&principal(), &register).await.expect("reconnect");
        assert_eq!(acknowledgement.resume_after_sequence, 1);
        let poll = service
            .poll(
                &principal(),
                session_id,
                &PollRequest {
                    schema: channel_schema(),
                    session_id,
                    after_sequence: acknowledgement.resume_after_sequence,
                    wait_millis: 0,
                    max_commands: 64,
                },
            )
            .await
            .expect("resume poll");
        assert_eq!(poll.commands, vec![second]);
    }

    #[tokio::test]
    async fn degraded_source_remains_dispatchable_but_missing_and_unsupported_do_not() {
        let store = MemoryStore::default();
        let service = ConnectorChannelService::new(store, "test-token").expect("test service");
        let tenant_id = TenantId::new();
        let cluster_id = ClusterId::new();
        let session_id = ConnectorSessionId::new();
        let mut register = registration(tenant_id, cluster_id, session_id);
        register.capability = capability_with_status(false, ConnectorSourceStatus::Degraded);
        service
            .register(&principal(), &register)
            .await
            .expect("degraded source registration");
        service
            .enqueue_query(
                tenant_id,
                cluster_id,
                query(tenant_id, cluster_id),
                Utc::now() + chrono::Duration::seconds(30),
            )
            .await
            .expect("degraded source remains dispatchable");

        for status in [ConnectorSourceStatus::Missing, ConnectorSourceStatus::Unsupported] {
            register.capability = capability_with_status(false, status);
            service
                .register(&principal(), &register)
                .await
                .expect("non-dispatchable source registration");
            assert!(
                service
                    .enqueue_query(
                        tenant_id,
                        cluster_id,
                        query(tenant_id, cluster_id),
                        Utc::now() + chrono::Duration::seconds(30),
                    )
                    .await
                    .is_err(),
                "{status:?} source must remain fail closed"
            );
        }
    }

    #[tokio::test]
    async fn duplicate_response_is_idempotent() {
        let service = ConnectorChannelService::new(MemoryStore::default(), "test-token").expect("test service");
        let tenant_id = TenantId::new();
        let cluster_id = ClusterId::new();
        let session_id = ConnectorSessionId::new();
        service
            .register(&principal(), &registration(tenant_id, cluster_id, session_id))
            .await
            .expect("register");
        let command = service
            .enqueue_query(
                tenant_id,
                cluster_id,
                query(tenant_id, cluster_id),
                Utc::now() + chrono::Duration::seconds(30),
            )
            .await
            .expect("query");
        let response = ConnectorResponseEnvelope {
            schema: channel_schema(),
            session_id,
            correlation_id: command.correlation_id(),
            sequence: command.sequence(),
            evidence: None,
            error_code: Some("source_unavailable".to_owned()),
            retryable: true,
        };
        assert_eq!(
            service
                .submit_response(&principal(), session_id, &response)
                .await
                .expect("first response"),
            ResponseDisposition::Inserted
        );
        assert_eq!(
            service
                .submit_response(&principal(), session_id, &response)
                .await
                .expect("duplicate response"),
            ResponseDisposition::Duplicate
        );
    }

    #[tokio::test]
    async fn query_and_wait_returns_the_durable_response() {
        let store = MemoryStore::default();
        let service = ConnectorChannelService::new(store.clone(), "test-token").expect("test service");
        let tenant_id = TenantId::new();
        let cluster_id = ClusterId::new();
        let session_id = ConnectorSessionId::new();
        service
            .register(&principal(), &registration(tenant_id, cluster_id, session_id))
            .await
            .expect("register");
        let query = query(tenant_id, cluster_id);
        let expected_correlation = query.correlation_id;
        let query_service = service.clone();
        let response_service = service.clone();
        let response_store = store.clone();
        let operation = async move {
            tokio::join!(
                query_service.query_and_wait(tenant_id, cluster_id, query, Utc::now() + chrono::Duration::seconds(2),),
                async move {
                    loop {
                        let command = response_store
                            .state
                            .lock()
                            .await
                            .commands
                            .get(&session_id)
                            .and_then(|commands| commands.first())
                            .cloned();
                        if let Some(command) = command {
                            let response = ConnectorResponseEnvelope {
                                schema: channel_schema(),
                                session_id,
                                correlation_id: command.correlation_id(),
                                sequence: command.sequence(),
                                evidence: None,
                                error_code: Some("source_unavailable".to_owned()),
                                retryable: true,
                            };
                            response_service
                                .submit_response(&principal(), session_id, &response)
                                .await
                                .expect("response");
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                }
            )
        };
        let (response, ()) = tokio::time::timeout(Duration::from_secs(3), operation)
            .await
            .expect("query should complete before its deadline");
        let response = response.expect("query response");
        assert_eq!(response.correlation_id, expected_correlation);
        assert_eq!(response.error_code.as_deref(), Some("source_unavailable"));
    }

    #[tokio::test]
    async fn query_deadline_appends_a_cancel_command() {
        let store = MemoryStore::default();
        let service = ConnectorChannelService::new(store.clone(), "test-token").expect("test service");
        let tenant_id = TenantId::new();
        let cluster_id = ClusterId::new();
        let session_id = ConnectorSessionId::new();
        service
            .register(&principal(), &registration(tenant_id, cluster_id, session_id))
            .await
            .expect("register");

        let response = service
            .query_and_wait(
                tenant_id,
                cluster_id,
                query(tenant_id, cluster_id),
                Utc::now() + chrono::Duration::milliseconds(20),
            )
            .await
            .expect("deadline response");
        assert_eq!(response.error_code.as_deref(), Some("deadline_exceeded"));
        let commands = store
            .state
            .lock()
            .await
            .commands
            .get(&session_id)
            .cloned()
            .expect("commands");
        assert_eq!(commands.len(), 2);
        assert!(matches!(commands[1], ConnectorCommand::Cancel { .. }));
    }

    #[tokio::test]
    async fn cross_tenant_heartbeat_fails_closed() {
        let service = ConnectorChannelService::new(MemoryStore::default(), "test-token").expect("test service");
        let tenant_id = TenantId::new();
        let cluster_id = ClusterId::new();
        let session_id = ConnectorSessionId::new();
        service
            .register(&principal(), &registration(tenant_id, cluster_id, session_id))
            .await
            .expect("register");
        let heartbeat = ConnectorHeartbeat {
            schema: channel_schema(),
            session_id,
            tenant_id: TenantId::new(),
            cluster_id,
            capability: capability(false),
            observed_at: Utc::now(),
        };
        assert!(service.heartbeat(&principal(), &heartbeat).await.is_err());
    }

    #[tokio::test]
    async fn mismatched_query_scope_is_rejected_before_enqueue() {
        let store = MemoryStore::default();
        let service = ConnectorChannelService::new(store.clone(), "test-token").expect("test service");
        let tenant_id = TenantId::new();
        let cluster_id = ClusterId::new();
        let session_id = ConnectorSessionId::new();
        service
            .register(&principal(), &registration(tenant_id, cluster_id, session_id))
            .await
            .expect("register");

        for mismatched in [query(TenantId::new(), cluster_id), query(tenant_id, ClusterId::new())] {
            assert!(
                service
                    .enqueue_query(
                        tenant_id,
                        cluster_id,
                        mismatched,
                        Utc::now() + chrono::Duration::seconds(30),
                    )
                    .await
                    .is_err()
            );
        }

        assert!(
            store
                .state
                .lock()
                .await
                .commands
                .get(&session_id)
                .is_none_or(Vec::is_empty)
        );
    }

    #[tokio::test]
    async fn auxiliary_upload_scope_comes_from_the_live_registered_session() {
        let service = ConnectorChannelService::new(MemoryStore::default(), "test-token").expect("test service");
        let tenant_id = TenantId::new();
        let cluster_id = ClusterId::new();
        let session_id = ConnectorSessionId::new();
        service
            .register(&principal(), &registration(tenant_id, cluster_id, session_id))
            .await
            .expect("register");

        let scope = service
            .authorize_session(&principal(), session_id)
            .await
            .expect("live scope");
        assert_eq!(scope.tenant_id, tenant_id);
        assert_eq!(scope.cluster_id, cluster_id);

        let other = ConnectorPrincipal {
            subject: "connector-b".to_owned(),
            issuer: principal().issuer,
        };
        assert!(service.authorize_session(&other, session_id).await.is_err());
    }

    #[tokio::test]
    async fn mutation_capability_and_unknown_major_are_rejected() {
        let service = ConnectorChannelService::new(MemoryStore::default(), "test-token").expect("test service");
        let mut request = registration(TenantId::new(), ClusterId::new(), ConnectorSessionId::new());
        request.capability = capability(true);
        assert!(service.register(&principal(), &request).await.is_err());

        request.capability = capability(false);
        request.schema = SchemaVersion::new(CHANNEL_SCHEMA_FAMILY, 2, 0);
        assert!(service.register(&principal(), &request).await.is_err());
    }

    #[test]
    fn registration_fixtures_preserve_the_read_only_boundary() {
        let read_only: ConnectorRegister = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/connector-channel/register-read-only.json"
        ))
        .expect("read-only registration fixture");
        let mutation: ConnectorRegister = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/connector-channel/register-mutation-rejected.json"
        ))
        .expect("mutation registration fixture");

        assert!(validate_capability(&read_only.capability).is_ok());
        assert!(validate_capability(&mutation.capability).is_err());
    }

    #[tokio::test]
    async fn stale_status_is_reported() {
        let store = MemoryStore::default();
        let service = ConnectorChannelService::new(store.clone(), "test-token")
            .expect("test service")
            .with_stale_after(Duration::ZERO);
        let tenant_id = TenantId::new();
        let cluster_id = ClusterId::new();
        let session_id = ConnectorSessionId::new();
        service
            .register(&principal(), &registration(tenant_id, cluster_id, session_id))
            .await
            .expect("register");
        let status = service
            .status(tenant_id, cluster_id)
            .await
            .expect("status")
            .expect("session");
        assert_eq!(status.liveness, ConnectorLiveness::Stale);
    }

    #[tokio::test]
    async fn health_samples_are_bounded_and_redacted() {
        let service = ConnectorChannelService::new(MemoryStore::default(), "test-token").expect("test service");
        let tenant_id = TenantId::new();
        let cluster_id = ClusterId::new();
        service
            .register(
                &principal(),
                &registration(tenant_id, cluster_id, ConnectorSessionId::new()),
            )
            .await
            .expect("register");

        let samples = service.health_samples(8).await.expect("health samples");
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].status, DependencyStatus::Healthy);
        assert_eq!(samples[0].queryable_sources, 1);
        assert!(service.health_samples(0).await.is_err());
        assert!(service.health_samples(MAX_HEALTH_SAMPLES + 1).await.is_err());

        let debug = format!("{samples:?}");
        assert!(!debug.contains(&tenant_id.to_string()));
        assert!(!debug.contains(&cluster_id.to_string()));
        assert!(!debug.contains("connector-a"));
        assert!(!debug.contains("issuer-a"));
    }
}
