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

use chrono::Utc;
use rocketmq_sre_contracts::Conversation;
use rocketmq_sre_contracts::ConversationAnswerMode;
use rocketmq_sre_contracts::ConversationAnswerRevision;
use rocketmq_sre_contracts::ConversationAnswerRevisionId;
use rocketmq_sre_contracts::ConversationCitation;
use rocketmq_sre_contracts::ConversationQueryIntent;
use rocketmq_sre_contracts::ConversationTurn;
use rocketmq_sre_contracts::ConversationTurnId;
use rocketmq_sre_contracts::ConversationTurnStatus;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::ModelInvocationId;
use rocketmq_sre_contracts::TenantId;
use sqlx::Postgres;
use sqlx::Row;
use sqlx::Transaction;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use super::ConversationTurnPage;
use super::ConversationTurnRequest;
use super::ConversationTurnView;
use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::auth::AuthContext;

pub(super) struct ConversationCompletion {
    pub(super) status: ConversationTurnStatus,
    pub(super) intent: Option<ConversationQueryIntent>,
    pub(super) answer: String,
    pub(super) mode: ConversationAnswerMode,
    pub(super) citations: Vec<ConversationCitation>,
    pub(super) evidence_ids: Vec<EvidenceId>,
    pub(super) model_invocation_id: Option<ModelInvocationId>,
    pub(super) partial: bool,
    pub(super) warnings: Vec<String>,
}

impl PostgresRepository {
    pub(super) async fn begin_conversation_turn(
        &self,
        auth: &AuthContext,
        conversation: &Conversation,
        request: &ConversationTurnRequest,
        intent: Option<&ConversationQueryIntent>,
        correlation_id: CorrelationId,
    ) -> Result<ConversationTurn, ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        lock_conversation(&mut transaction, auth, conversation).await?;
        let active = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                SELECT 1 FROM conversation_turns
                WHERE conversation_id = $1 AND status = 'collecting'
             )",
        )
        .bind(conversation.id.as_uuid())
        .fetch_one(&mut *transaction)
        .await?;
        if active {
            return Err(ControlPlaneError::conflict_code(
                "conversation_query_in_progress",
                "only one read-only query may run per conversation",
            ));
        }
        let sequence = sqlx::query_scalar::<_, i32>(
            "SELECT COALESCE(MAX(sequence), 0) + 1
             FROM conversation_turns
             WHERE conversation_id = $1",
        )
        .bind(conversation.id.as_uuid())
        .fetch_one(&mut *transaction)
        .await?;
        let id = ConversationTurnId::new();
        let created_at = Utc::now();
        let query_intent = intent
            .map(serde_json::to_value)
            .transpose()
            .map_err(|_| ControlPlaneError::configuration("conversation query intent cannot be serialized"))?;
        sqlx::query(
            "INSERT INTO conversation_turns (
                id, conversation_id, tenant_id, cluster_id, sequence,
                question, resource, status, query_intent, correlation_id,
                created_at, completed_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, 'collecting', $8, $9, $10, NULL)",
        )
        .bind(id.as_uuid())
        .bind(conversation.id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .bind(conversation.cluster_id.as_uuid())
        .bind(sequence)
        .bind(request.question.trim())
        .bind(request.resource.as_deref().map(str::trim))
        .bind(query_intent)
        .bind(correlation_id.as_uuid())
        .bind(created_at)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(ConversationTurn {
            id,
            conversation_id: conversation.id,
            tenant_id: auth.tenant_id,
            cluster_id: conversation.cluster_id,
            sequence: u32::try_from(sequence)
                .map_err(|_| ControlPlaneError::configuration("conversation turn sequence is invalid"))?,
            question: request.question.trim().to_owned(),
            resource: request.resource.as_deref().map(str::trim).map(str::to_owned),
            status: ConversationTurnStatus::Collecting,
            query_intent: intent.cloned(),
            correlation_id,
            created_at,
            completed_at: None,
        })
    }

    pub(super) async fn complete_conversation_turn(
        &self,
        auth: &AuthContext,
        turn: &ConversationTurn,
        completion: ConversationCompletion,
    ) -> Result<ConversationTurnView, ControlPlaneError> {
        if completion.status == ConversationTurnStatus::Collecting {
            return Err(ControlPlaneError::configuration(
                "conversation completion requires a terminal turn status",
            ));
        }
        let mut transaction = self.pool.begin().await?;
        let completed_at = Utc::now();
        let query_intent = completion
            .intent
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|_| ControlPlaneError::configuration("conversation query intent cannot be serialized"))?;
        let updated = sqlx::query(
            "UPDATE conversation_turns
             SET status = $4, query_intent = $5, completed_at = $6
             WHERE id = $1 AND tenant_id = $2 AND cluster_id = $3 AND status = 'collecting'",
        )
        .bind(turn.id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .bind(turn.cluster_id.as_uuid())
        .bind(turn_status_name(completion.status))
        .bind(query_intent)
        .bind(completed_at)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        if updated != 1 {
            return Err(ControlPlaneError::conflict_code(
                "conversation_turn_not_active",
                "conversation turn is no longer collecting",
            ));
        }
        let revision_id = ConversationAnswerRevisionId::new();
        let citations = serde_json::to_value(&completion.citations)
            .map_err(|_| ControlPlaneError::configuration("conversation citations cannot be serialized"))?;
        let warnings = serde_json::to_value(&completion.warnings)
            .map_err(|_| ControlPlaneError::configuration("conversation warnings cannot be serialized"))?;
        let evidence_ids = completion
            .evidence_ids
            .iter()
            .map(|id| id.as_uuid())
            .collect::<Vec<_>>();
        sqlx::query(
            "INSERT INTO conversation_answer_revisions (
                id, conversation_id, turn_id, revision, answer, mode,
                citations, evidence_ids, model_invocation_id, partial,
                warnings, created_at
             ) VALUES ($1, $2, $3, 1, $4, $5, $6, $7, $8, $9, $10, $11)",
        )
        .bind(revision_id.as_uuid())
        .bind(turn.conversation_id.as_uuid())
        .bind(turn.id.as_uuid())
        .bind(completion.answer.trim())
        .bind(answer_mode_name(completion.mode))
        .bind(citations)
        .bind(evidence_ids)
        .bind(completion.model_invocation_id.map(ModelInvocationId::as_uuid))
        .bind(completion.partial)
        .bind(warnings)
        .bind(completed_at)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(ConversationTurnView {
            turn: ConversationTurn {
                status: completion.status,
                query_intent: completion.intent,
                completed_at: Some(completed_at),
                ..turn.clone()
            },
            answer: Some(ConversationAnswerRevision {
                id: revision_id,
                conversation_id: turn.conversation_id,
                turn_id: turn.id,
                revision: 1,
                answer: completion.answer,
                mode: completion.mode,
                citations: completion.citations,
                evidence_ids: completion.evidence_ids,
                model_invocation_id: completion.model_invocation_id,
                partial: completion.partial,
                warnings: completion.warnings,
                created_at: completed_at,
            }),
        })
    }

    pub(super) async fn link_conversation_evidence(
        &self,
        auth: &AuthContext,
        turn: &ConversationTurn,
        evidence_id: EvidenceId,
    ) -> Result<(), ControlPlaneError> {
        if turn.tenant_id != auth.tenant_id || !auth.clusters.contains(&turn.cluster_id) {
            return Err(ControlPlaneError::forbidden(
                "cluster_not_allowed",
                "conversation evidence is outside the caller scope",
            ));
        }
        sqlx::query(
            "INSERT INTO conversation_evidence_links (id, turn_id, evidence_id, linked_at)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (turn_id, evidence_id) DO NOTHING",
        )
        .bind(Uuid::new_v4())
        .bind(turn.id.as_uuid())
        .bind(evidence_id.as_uuid())
        .bind(Utc::now())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub(super) async fn conversation_turns(
        &self,
        auth: &AuthContext,
        conversation: &Conversation,
    ) -> Result<ConversationTurnPage, ControlPlaneError> {
        if conversation.tenant_id != auth.tenant_id || !auth.clusters.contains(&conversation.cluster_id) {
            return Err(ControlPlaneError::forbidden(
                "cluster_not_allowed",
                "conversation is outside the caller scope",
            ));
        }
        let rows = sqlx::query(
            "SELECT t.id, t.conversation_id, t.tenant_id, t.cluster_id, t.sequence,
                    t.question, t.resource, t.status, t.query_intent,
                    t.correlation_id, t.created_at, t.completed_at,
                    a.id AS answer_id, a.revision AS answer_revision,
                    a.answer, a.mode, a.citations, a.evidence_ids,
                    a.model_invocation_id, a.partial AS answer_partial,
                    a.warnings AS answer_warnings, a.created_at AS answer_created_at
             FROM conversation_turns t
             LEFT JOIN LATERAL (
                SELECT * FROM conversation_answer_revisions
                WHERE turn_id = t.id
                ORDER BY revision DESC
                LIMIT 1
             ) a ON TRUE
             WHERE t.conversation_id = $1 AND t.tenant_id = $2 AND t.cluster_id = $3
             ORDER BY t.sequence ASC
             LIMIT 200",
        )
        .bind(conversation.id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .bind(conversation.cluster_id.as_uuid())
        .fetch_all(&self.pool)
        .await?;
        Ok(ConversationTurnPage {
            schema_version: "rocketmq-sre.conversation-turn-page.v1",
            items: rows
                .iter()
                .map(conversation_turn_view_from_row)
                .collect::<Result<_, _>>()?,
            observed_at: Utc::now(),
        })
    }

    pub(super) async fn request_conversation_cancel(
        &self,
        auth: &AuthContext,
        conversation: &Conversation,
    ) -> Result<bool, ControlPlaneError> {
        if conversation.tenant_id != auth.tenant_id || !auth.clusters.contains(&conversation.cluster_id) {
            return Err(ControlPlaneError::forbidden(
                "cluster_not_allowed",
                "conversation is outside the caller scope",
            ));
        }
        let updated = sqlx::query(
            "UPDATE conversation_turns
             SET cancel_requested = TRUE
             WHERE id = (
                SELECT id FROM conversation_turns
                WHERE conversation_id = $1 AND tenant_id = $2 AND cluster_id = $3
                  AND status = 'collecting'
                ORDER BY sequence DESC
                LIMIT 1
             )",
        )
        .bind(conversation.id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .bind(conversation.cluster_id.as_uuid())
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(updated == 1)
    }

    pub(super) async fn conversation_cancel_requested(
        &self,
        auth: &AuthContext,
        turn: &ConversationTurn,
    ) -> Result<bool, ControlPlaneError> {
        let requested = sqlx::query_scalar::<_, bool>(
            "SELECT cancel_requested
             FROM conversation_turns
             WHERE id = $1 AND tenant_id = $2 AND cluster_id = $3",
        )
        .bind(turn.id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .bind(turn.cluster_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        Ok(requested)
    }
}

async fn lock_conversation(
    transaction: &mut Transaction<'_, Postgres>,
    auth: &AuthContext,
    conversation: &Conversation,
) -> Result<(), ControlPlaneError> {
    let found = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM conversations
         WHERE id = $1 AND tenant_id = $2 AND cluster_id = $3 AND status != 'closed'
         FOR UPDATE",
    )
    .bind(conversation.id.as_uuid())
    .bind(auth.tenant_id.as_uuid())
    .bind(conversation.cluster_id.as_uuid())
    .fetch_optional(&mut **transaction)
    .await?;
    if found.is_none() {
        return Err(ControlPlaneError::NotFound);
    }
    Ok(())
}

fn conversation_turn_view_from_row(row: &PgRow) -> Result<ConversationTurnView, ControlPlaneError> {
    let conversation_id = rocketmq_sre_contracts::ConversationId::from_uuid(row.try_get("conversation_id")?);
    let turn_id = ConversationTurnId::from_uuid(row.try_get("id")?);
    let answer_id = row.try_get::<Option<Uuid>, _>("answer_id")?;
    let answer = answer_id
        .map(|id| {
            Ok::<ConversationAnswerRevision, ControlPlaneError>(ConversationAnswerRevision {
                id: ConversationAnswerRevisionId::from_uuid(id),
                conversation_id,
                turn_id,
                revision: u32::try_from(row.try_get::<i32, _>("answer_revision")?)
                    .map_err(|_| ControlPlaneError::configuration("stored answer revision is invalid"))?,
                answer: row.try_get("answer")?,
                mode: parse_answer_mode(row.try_get("mode")?)?,
                citations: serde_json::from_value(row.try_get("citations")?)
                    .map_err(|_| ControlPlaneError::configuration("stored conversation citations are invalid"))?,
                evidence_ids: row
                    .try_get::<Vec<Uuid>, _>("evidence_ids")?
                    .into_iter()
                    .map(EvidenceId::from_uuid)
                    .collect(),
                model_invocation_id: row
                    .try_get::<Option<Uuid>, _>("model_invocation_id")?
                    .map(ModelInvocationId::from_uuid),
                partial: row.try_get("answer_partial")?,
                warnings: serde_json::from_value(row.try_get("answer_warnings")?)
                    .map_err(|_| ControlPlaneError::configuration("stored conversation warnings are invalid"))?,
                created_at: row.try_get("answer_created_at")?,
            })
        })
        .transpose()?;
    Ok(ConversationTurnView {
        turn: ConversationTurn {
            id: turn_id,
            conversation_id,
            tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?),
            cluster_id: rocketmq_sre_contracts::ClusterId::from_uuid(row.try_get("cluster_id")?),
            sequence: u32::try_from(row.try_get::<i32, _>("sequence")?)
                .map_err(|_| ControlPlaneError::configuration("stored conversation sequence is invalid"))?,
            question: row.try_get("question")?,
            resource: row.try_get("resource")?,
            status: parse_turn_status(row.try_get("status")?)?,
            query_intent: row
                .try_get::<Option<serde_json::Value>, _>("query_intent")?
                .map(serde_json::from_value)
                .transpose()
                .map_err(|_| ControlPlaneError::configuration("stored conversation intent is invalid"))?,
            correlation_id: CorrelationId::from_uuid(row.try_get("correlation_id")?),
            created_at: row.try_get("created_at")?,
            completed_at: row.try_get("completed_at")?,
        },
        answer,
    })
}

fn turn_status_name(value: ConversationTurnStatus) -> &'static str {
    match value {
        ConversationTurnStatus::Collecting => "collecting",
        ConversationTurnStatus::Answered => "answered",
        ConversationTurnStatus::NeedsScope => "needs_scope",
        ConversationTurnStatus::NeedsEvidence => "needs_evidence",
        ConversationTurnStatus::Cancelled => "cancelled",
        ConversationTurnStatus::Failed => "failed",
    }
}

fn parse_turn_status(value: &str) -> Result<ConversationTurnStatus, ControlPlaneError> {
    match value {
        "collecting" => Ok(ConversationTurnStatus::Collecting),
        "answered" => Ok(ConversationTurnStatus::Answered),
        "needs_scope" => Ok(ConversationTurnStatus::NeedsScope),
        "needs_evidence" => Ok(ConversationTurnStatus::NeedsEvidence),
        "cancelled" => Ok(ConversationTurnStatus::Cancelled),
        "failed" => Ok(ConversationTurnStatus::Failed),
        _ => Err(ControlPlaneError::configuration(
            "stored conversation status is invalid",
        )),
    }
}

fn answer_mode_name(value: ConversationAnswerMode) -> &'static str {
    match value {
        ConversationAnswerMode::ModelAssisted => "model_assisted",
        ConversationAnswerMode::RulesOnly => "rules_only",
    }
}

fn parse_answer_mode(value: &str) -> Result<ConversationAnswerMode, ControlPlaneError> {
    match value {
        "model_assisted" => Ok(ConversationAnswerMode::ModelAssisted),
        "rules_only" => Ok(ConversationAnswerMode::RulesOnly),
        _ => Err(ControlPlaneError::configuration(
            "stored conversation answer mode is invalid",
        )),
    }
}
