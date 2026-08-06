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

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use rocketmq_sre_contracts::ConversationAnswerMode;
use rocketmq_sre_contracts::ConversationCitation;
use rocketmq_sre_contracts::ConversationId;
use rocketmq_sre_contracts::ConversationQueryIntent;
use rocketmq_sre_contracts::ConversationQueryKind;
use rocketmq_sre_contracts::ConversationTurn;
use rocketmq_sre_contracts::ConversationTurnStatus;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::EvidenceContent;
use rocketmq_sre_contracts::EvidenceQuery;
use rocketmq_sre_contracts::EvidenceSnapshot;
use rocketmq_sre_contracts::QueryId;
use rocketmq_sre_contracts::TimeRange;
use tokio::sync::Mutex;
use tokio::sync::watch;

use super::ConversationCancelResult;
use super::ConversationTurnPage;
use super::ConversationTurnRequest;
use super::ConversationTurnView;
use super::conversation_query::conversation_tools;
use super::conversation_query::deterministic_intent;
use super::conversation_query::model_intent;
use super::conversation_repository::ConversationCompletion;
use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::auth::AuthContext;
use crate::connector_channel::PostgresConnectorChannelService;
use crate::evidence::EvidenceService;
use crate::models::ModelGatewayService;

const QUERY_TIMEOUT: Duration = Duration::from_secs(15);
const CANCEL_POLL_INTERVAL: Duration = Duration::from_millis(250);
const MAX_WARNINGS: usize = 16;
const MAX_EVIDENCE_SUMMARY_CHARS: usize = 3_000;

#[derive(Clone)]
pub(crate) struct ConversationQueryService {
    repository: PostgresRepository,
    connector: PostgresConnectorChannelService,
    evidence: EvidenceService,
    models: ModelGatewayService,
    active: Arc<Mutex<HashMap<ConversationId, watch::Sender<bool>>>>,
}

impl ConversationQueryService {
    pub(crate) fn new(
        repository: PostgresRepository,
        connector: PostgresConnectorChannelService,
        evidence: EvidenceService,
        models: ModelGatewayService,
    ) -> Self {
        Self {
            repository,
            connector,
            evidence,
            models,
            active: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) async fn submit_turn(
        &self,
        auth: &AuthContext,
        conversation_id: ConversationId,
        request: &ConversationTurnRequest,
        correlation_id: CorrelationId,
    ) -> Result<ConversationTurnView, ControlPlaneError> {
        request.validate()?;
        let conversation_view = self.repository.conversation(auth, conversation_id).await?;
        let scoped_resource = request
            .resource
            .as_deref()
            .or(conversation_view.conversation.resource.as_deref());
        let deterministic = deterministic_intent(&request.question, scoped_resource, request.window_seconds)?;
        let (cancel_sender, cancel_receiver) = watch::channel(false);
        {
            let mut active = self.active.lock().await;
            match active.entry(conversation_id) {
                Entry::Vacant(entry) => {
                    entry.insert(cancel_sender);
                }
                Entry::Occupied(_) => {
                    return Err(ControlPlaneError::conflict_code(
                        "conversation_query_in_progress",
                        "only one read-only query may run per conversation",
                    ));
                }
            }
        }
        let turn = match self
            .repository
            .begin_conversation_turn(
                auth,
                &conversation_view.conversation,
                request,
                deterministic.as_ref(),
                correlation_id,
            )
            .await
        {
            Ok(turn) => turn,
            Err(error) => {
                self.active.lock().await.remove(&conversation_id);
                return Err(error);
            }
        };
        let result = self
            .run_turn(
                auth,
                &conversation_view.conversation,
                &turn,
                request,
                scoped_resource,
                deterministic,
                cancel_receiver,
            )
            .await;
        self.active.lock().await.remove(&conversation_id);
        match result {
            Ok(view) => Ok(view),
            Err(_) => {
                self.repository
                    .complete_conversation_turn(
                        auth,
                        &turn,
                        ConversationCompletion {
                            status: ConversationTurnStatus::Failed,
                            intent: turn.query_intent.clone(),
                            answer: "The bounded read-only query could not be completed. No cluster change was attempted. Retry after checking the registered source and model status.".to_owned(),
                            mode: ConversationAnswerMode::RulesOnly,
                            citations: Vec::new(),
                            evidence_ids: Vec::new(),
                            model_invocation_id: None,
                            partial: true,
                            warnings: vec!["conversation_query_failed".to_owned()],
                        },
                    )
                    .await
            }
        }
    }

    pub(crate) async fn turns(
        &self,
        auth: &AuthContext,
        conversation_id: ConversationId,
    ) -> Result<ConversationTurnPage, ControlPlaneError> {
        let conversation = self.repository.conversation(auth, conversation_id).await?;
        self.repository
            .conversation_turns(auth, &conversation.conversation)
            .await
    }

    pub(crate) async fn cancel(
        &self,
        auth: &AuthContext,
        conversation_id: ConversationId,
    ) -> Result<ConversationCancelResult, ControlPlaneError> {
        let conversation = self.repository.conversation(auth, conversation_id).await?;
        let persisted = self
            .repository
            .request_conversation_cancel(auth, &conversation.conversation)
            .await?;
        let local = self
            .active
            .lock()
            .await
            .get(&conversation_id)
            .is_some_and(|sender| sender.send(true).is_ok());
        Ok(ConversationCancelResult {
            schema_version: "rocketmq-sre.conversation-cancel.v1",
            cancellation_requested: persisted || local,
            observed_at: Utc::now(),
        })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "one immutable conversation turn keeps its authenticated scope and cancellation channel explicit"
    )]
    async fn run_turn(
        &self,
        auth: &AuthContext,
        conversation: &rocketmq_sre_contracts::Conversation,
        turn: &ConversationTurn,
        request: &ConversationTurnRequest,
        scoped_resource: Option<&str>,
        deterministic: Option<ConversationQueryIntent>,
        mut cancel: watch::Receiver<bool>,
    ) -> Result<ConversationTurnView, ControlPlaneError> {
        let mut warnings = Vec::new();
        let tool_prompt = scoped_resource.map_or_else(
            || request.question.trim().to_owned(),
            |resource| format!("{}\nOperator-scoped resource: {resource}", request.question.trim()),
        );
        let tools = tools_for_intent(deterministic.as_ref());
        let model_selection = tokio::select! {
            selection = self.models.select_conversation_tool(
                auth,
                conversation.id,
                conversation.investigation_id,
                conversation.cluster_id,
                &tool_prompt,
                &tools,
                turn.correlation_id,
            ) => selection?,
            cancellation = self.wait_for_cancel(auth, turn, &mut cancel) => {
                cancellation?;
                return self.complete_cancelled(auth, turn, deterministic, warnings).await;
            }
        };
        if cancellation_requested(&cancel) || self.repository.conversation_cancel_requested(auth, turn).await? {
            return self.complete_cancelled(auth, turn, deterministic, warnings).await;
        }
        let model_selected = model_selection
            .as_ref()
            .map(|selection| model_intent(&selection.tool_call, scoped_resource, request.window_seconds));
        let selected = match (deterministic, model_selected) {
            (Some(local), Some(Ok(model))) if local == model => Some(local),
            (Some(local), Some(Ok(_))) => {
                warnings.push("model_tool_selection_rejected".to_owned());
                Some(local)
            }
            (Some(local), Some(Err(_))) => {
                warnings.push("model_tool_arguments_rejected".to_owned());
                Some(local)
            }
            (Some(local), None) => Some(local),
            (None, Some(Ok(model))) => Some(model),
            (None, Some(Err(_))) => {
                warnings.push("model_tool_arguments_rejected".to_owned());
                None
            }
            (None, None) => None,
        };
        let Some(intent) = selected else {
            return self
                .repository
                .complete_conversation_turn(
                    auth,
                    turn,
                    ConversationCompletion {
                        status: ConversationTurnStatus::NeedsScope,
                        intent: None,
                        answer: "Specify a registered read-only resource, such as a consumer group and topic, a broker name, cluster overview, topic list, or an approved metric.".to_owned(),
                        mode: ConversationAnswerMode::RulesOnly,
                        citations: Vec::new(),
                        evidence_ids: Vec::new(),
                        model_invocation_id: None,
                        partial: true,
                        warnings: bounded_warnings(warnings),
                    },
                )
                .await;
        };
        let now = Utc::now();
        let time_range = TimeRange::new(now - chrono::Duration::seconds(i64::from(intent.window_seconds)), now)
            .map_err(|_| ControlPlaneError::validation("invalid_request", "conversation time range is invalid"))?;
        let query = EvidenceQuery {
            query_id: QueryId::new(),
            correlation_id: turn.correlation_id,
            tenant_id: auth.tenant_id,
            cluster_id: conversation.cluster_id,
            source: intent.source.clone(),
            resource: intent.resource.clone(),
            time_range,
        };
        let deadline = now
            + chrono::Duration::from_std(QUERY_TIMEOUT)
                .map_err(|_| ControlPlaneError::configuration("conversation query timeout is invalid"))?;
        let response = tokio::select! {
            response = self.connector.query_and_wait(
                auth.tenant_id,
                conversation.cluster_id,
                query,
                deadline,
            ) => Some(response),
            cancellation = self.wait_for_cancel(auth, turn, &mut cancel) => {
                cancellation?;
                let _ = self.connector.enqueue_cancel(
                    auth.tenant_id,
                    conversation.cluster_id,
                    turn.correlation_id,
                ).await;
                None
            }
        };
        let Some(response) = response else {
            return self.complete_cancelled(auth, turn, Some(intent), warnings).await;
        };
        let response = match response {
            Ok(response) => response,
            Err(_) => {
                warnings.push("source_unavailable".to_owned());
                return self.complete_missing(auth, turn, intent, warnings).await;
            }
        };
        if let Some(code) = response.error_code.as_deref() {
            warnings.push(stable_warning(code));
        }
        let Some(evidence) = response.evidence else {
            return self.complete_missing(auth, turn, intent, warnings).await;
        };
        let evidence = match self.evidence.persist_cluster(auth, evidence).await {
            Ok(evidence) => evidence,
            Err(_) => {
                warnings.push("evidence_persistence_failed".to_owned());
                return self.complete_missing(auth, turn, intent, warnings).await;
            }
        };
        self.repository
            .link_conversation_evidence(auth, turn, evidence.evidence_id)
            .await?;
        warnings.extend(evidence.warnings.iter().map(|warning| stable_warning(warning)));
        let deterministic_answer = evidence_answer(&intent, &evidence);
        if cancellation_requested(&cancel) || self.repository.conversation_cancel_requested(auth, turn).await? {
            return self.complete_cancelled(auth, turn, Some(intent), warnings).await;
        }
        let model_answer = tokio::select! {
            answer = self.models.answer_conversation(
                auth,
                conversation.id,
                conversation.investigation_id,
                conversation.cluster_id,
                &request.question,
                &deterministic_answer,
                std::slice::from_ref(&evidence),
                turn.correlation_id,
            ) => answer?,
            cancellation = self.wait_for_cancel(auth, turn, &mut cancel) => {
                cancellation?;
                return self.complete_cancelled(auth, turn, Some(intent), warnings).await;
            }
        };
        if cancellation_requested(&cancel) || self.repository.conversation_cancel_requested(auth, turn).await? {
            return self.complete_cancelled(auth, turn, Some(intent), warnings).await;
        }
        let citation = citation(&evidence);
        let (answer, mode, evidence_ids, model_invocation_id) = match model_answer {
            Some(answer) => (
                answer.answer,
                ConversationAnswerMode::ModelAssisted,
                answer.cited_evidence_ids,
                Some(answer.invocation_id),
            ),
            None => (
                deterministic_answer,
                ConversationAnswerMode::RulesOnly,
                vec![evidence.evidence_id],
                None,
            ),
        };
        let citations = evidence_ids
            .iter()
            .filter(|id| **id == evidence.evidence_id)
            .map(|_| citation.clone())
            .collect::<Vec<_>>();
        self.repository
            .complete_conversation_turn(
                auth,
                turn,
                ConversationCompletion {
                    status: ConversationTurnStatus::Answered,
                    intent: Some(intent),
                    answer,
                    mode,
                    citations,
                    evidence_ids,
                    model_invocation_id,
                    partial: evidence.partial,
                    warnings: bounded_warnings(warnings),
                },
            )
            .await
    }

    async fn wait_for_cancel(
        &self,
        auth: &AuthContext,
        turn: &ConversationTurn,
        local: &mut watch::Receiver<bool>,
    ) -> Result<(), ControlPlaneError> {
        let mut poll = tokio::time::interval(CANCEL_POLL_INTERVAL);
        poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                changed = local.changed() => {
                    if changed.is_ok() && *local.borrow() {
                        return Ok(());
                    }
                }
                _ = poll.tick() => {
                    if self.repository.conversation_cancel_requested(auth, turn).await? {
                        return Ok(());
                    }
                }
            }
        }
    }

    async fn complete_cancelled(
        &self,
        auth: &AuthContext,
        turn: &ConversationTurn,
        intent: Option<ConversationQueryIntent>,
        mut warnings: Vec<String>,
    ) -> Result<ConversationTurnView, ControlPlaneError> {
        warnings.push("query_cancelled".to_owned());
        self.repository
            .complete_conversation_turn(
                auth,
                turn,
                ConversationCompletion {
                    status: ConversationTurnStatus::Cancelled,
                    intent,
                    answer: "The read-only metric query was cancelled. No cluster change was attempted.".to_owned(),
                    mode: ConversationAnswerMode::RulesOnly,
                    citations: Vec::new(),
                    evidence_ids: Vec::new(),
                    model_invocation_id: None,
                    partial: true,
                    warnings: bounded_warnings(warnings),
                },
            )
            .await
    }

    async fn complete_missing(
        &self,
        auth: &AuthContext,
        turn: &ConversationTurn,
        intent: ConversationQueryIntent,
        mut warnings: Vec<String>,
    ) -> Result<ConversationTurnView, ControlPlaneError> {
        warnings.push("missing_evidence".to_owned());
        self.repository
            .complete_conversation_turn(
                auth,
                turn,
                ConversationCompletion {
                    status: ConversationTurnStatus::NeedsEvidence,
                    intent: Some(intent),
                    answer: "The registered read-only source did not return usable Evidence. No value was fabricated; verify Connector readiness and source coverage.".to_owned(),
                    mode: ConversationAnswerMode::RulesOnly,
                    citations: Vec::new(),
                    evidence_ids: Vec::new(),
                    model_invocation_id: None,
                    partial: true,
                    warnings: bounded_warnings(warnings),
                },
            )
            .await
    }
}

fn tools_for_intent(intent: Option<&ConversationQueryIntent>) -> Vec<rocketmq_sre_model_gateway::ModelTool> {
    let tools = conversation_tools();
    let Some(intent) = intent else {
        return tools;
    };
    let expected = match intent.kind {
        ConversationQueryKind::ClusterOverview => "query_cluster_overview",
        ConversationQueryKind::TopicList => "list_topics",
        ConversationQueryKind::TopicDescribe => "describe_topic",
        ConversationQueryKind::ConsumerLag => "query_consumer_lag",
        ConversationQueryKind::BrokerRuntime => "query_broker_runtime",
        ConversationQueryKind::MetricInstant | ConversationQueryKind::MetricRange => "query_metric",
    };
    tools.into_iter().filter(|tool| tool.name == expected).collect()
}

fn cancellation_requested(receiver: &watch::Receiver<bool>) -> bool {
    *receiver.borrow()
}

fn citation(evidence: &EvidenceSnapshot) -> ConversationCitation {
    ConversationCitation {
        evidence_id: evidence.evidence_id,
        source: evidence.source.clone(),
        resource: evidence.resource.clone(),
        content_hash: evidence.content_hash.clone(),
        observed_at: evidence.observed_at,
        freshness_seconds: evidence.freshness_seconds,
        partial: evidence.partial,
    }
}

fn evidence_answer(intent: &ConversationQueryIntent, evidence: &EvidenceSnapshot) -> String {
    let content = match &evidence.content {
        EvidenceContent::Inline(value) => bounded_json(value),
        EvidenceContent::Reference(reference) => format!(
            "Evidence content is stored by digest {} with {} bytes.",
            reference.digest, reference.size_bytes
        ),
    };
    format!(
        "The registered {:?} read returned Evidence {} from {} for {} at {} (freshness {}s, partial: {}). {}",
        intent.kind,
        evidence.evidence_id,
        evidence.source,
        evidence.resource,
        evidence.observed_at,
        evidence.freshness_seconds,
        evidence.partial,
        content
    )
}

fn bounded_json(value: &serde_json::Value) -> String {
    let encoded =
        serde_json::to_string(value).unwrap_or_else(|_| "Evidence content could not be summarized.".to_owned());
    let mut bounded = encoded.chars().take(MAX_EVIDENCE_SUMMARY_CHARS).collect::<String>();
    if encoded.chars().count() > MAX_EVIDENCE_SUMMARY_CHARS {
        bounded.push('…');
    }
    bounded
}

fn stable_warning(value: &str) -> String {
    let value = value
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '.'))
        .take(128)
        .collect::<String>();
    if value.is_empty() {
        "source_warning".to_owned()
    } else {
        value
    }
}

fn bounded_warnings(mut warnings: Vec<String>) -> Vec<String> {
    warnings.sort();
    warnings.dedup();
    warnings.truncate(MAX_WARNINGS);
    warnings
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_sre_contracts::CoverageStatus;
    use rocketmq_sre_contracts::EvidenceExposure;
    use rocketmq_sre_contracts::SchemaVersion;
    use rocketmq_sre_contracts::Sensitivity;
    use rocketmq_sre_contracts::TenantId;
    use serde_json::json;

    #[test]
    fn deterministic_answer_preserves_evidence_provenance() {
        let now = Utc::now();
        let query = EvidenceQuery {
            query_id: QueryId::new(),
            correlation_id: CorrelationId::new(),
            tenant_id: TenantId::new(),
            cluster_id: rocketmq_sre_contracts::ClusterId::new(),
            source: "prometheus".to_owned(),
            resource: "instant/rocketmq_broker_up".to_owned(),
            time_range: TimeRange::new(now, now).expect("valid range"),
        };
        let mut evidence = EvidenceSnapshot::capture(
            query,
            SchemaVersion::new("rocketmq-sre.evidence", 1, 0),
            now,
            EvidenceContent::Inline(json!({"value": 1})),
        )
        .expect("valid evidence");
        evidence.coverage = CoverageStatus::Available;
        evidence.sensitivity = Sensitivity::Internal;
        evidence.exposure = EvidenceExposure::PrometheusApi;
        let intent = ConversationQueryIntent {
            schema_version: "rocketmq-sre.conversation-query-intent.v1".to_owned(),
            kind: ConversationQueryKind::MetricInstant,
            source: "prometheus".to_owned(),
            resource: "instant/rocketmq_broker_up".to_owned(),
            window_seconds: 300,
        };

        let answer = evidence_answer(&intent, &evidence);
        assert!(answer.contains(&evidence.evidence_id.to_string()));
        assert!(answer.contains("rocketmq_broker_up"));
    }
}
