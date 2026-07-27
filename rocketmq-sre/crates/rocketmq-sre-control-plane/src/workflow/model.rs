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

use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::Conversation;
use rocketmq_sre_contracts::DiagnosisRevision;
use rocketmq_sre_contracts::Incident;
use rocketmq_sre_contracts::InspectionRun;
use rocketmq_sre_contracts::InspectionTemplate;
use rocketmq_sre_contracts::Investigation;
use rocketmq_sre_contracts::Recommendation;
use rocketmq_sre_contracts::RecommendationStatus;
use rocketmq_sre_contracts::TimelineEvent;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::time::Duration;
use uuid::Uuid;

use crate::ControlPlaneError;

const DEFAULT_PAGE_LIMIT: u32 = 50;
const MAX_PAGE_LIMIT: u32 = 200;
const WORKFLOW_PAGE_SCHEMA: &str = "rocketmq-sre.workflow-page.v1";
const PAGE_TRUNCATED_WARNING: &str = "additional workflow records are available; continue with next_cursor";

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct WorkflowListQuery {
    pub cluster_id: ClusterId,
    pub cursor: Option<String>,
    pub limit: Option<u32>,
}

impl WorkflowListQuery {
    pub(crate) fn bounded_limit(&self) -> Result<u32, ControlPlaneError> {
        let limit = self.limit.unwrap_or(DEFAULT_PAGE_LIMIT);
        if !(1..=MAX_PAGE_LIMIT).contains(&limit) {
            return Err(ControlPlaneError::validation(
                "invalid_request",
                "workflow page limit must be between 1 and 200",
            ));
        }
        Ok(limit)
    }

    pub(crate) fn cursor_uuid(&self) -> Result<Option<Uuid>, ControlPlaneError> {
        self.cursor
            .as_deref()
            .map(|value| {
                value
                    .parse()
                    .map_err(|_| ControlPlaneError::validation("invalid_request", "workflow cursor must be a UUID"))
            })
            .transpose()
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct WorkflowPage<T> {
    pub schema_version: &'static str,
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
    pub partial: bool,
    pub warnings: Vec<String>,
    pub observed_at: chrono::DateTime<chrono::Utc>,
}

impl<T> WorkflowPage<T> {
    pub(super) fn from_window<F>(mut items: Vec<T>, limit: u32, item_id: F) -> Self
    where
        F: Fn(&T) -> Uuid,
    {
        let has_more = items.len() > limit as usize;
        items.truncate(limit as usize);
        let next_cursor = has_more
            .then(|| items.last().map(|item| item_id(item).to_string()))
            .flatten();
        let warnings = has_more
            .then(|| PAGE_TRUNCATED_WARNING.to_owned())
            .into_iter()
            .collect();
        Self {
            schema_version: WORKFLOW_PAGE_SCHEMA,
            items,
            next_cursor,
            partial: has_more,
            warnings,
            observed_at: chrono::Utc::now(),
        }
    }

    pub(super) fn map<U, F>(self, map: F) -> WorkflowPage<U>
    where
        F: FnMut(T) -> U,
    {
        WorkflowPage {
            schema_version: self.schema_version,
            items: self.items.into_iter().map(map).collect(),
            next_cursor: self.next_cursor,
            partial: self.partial,
            warnings: self.warnings,
            observed_at: self.observed_at,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct ConversationCreateRequest {
    pub cluster_id: ClusterId,
    pub question: String,
    pub resource: Option<String>,
    #[serde(default)]
    pub persist_investigation: bool,
}

impl ConversationCreateRequest {
    pub(crate) fn validate(&self) -> Result<(), ControlPlaneError> {
        validate_text("question", &self.question, 1, 8_192)?;
        if let Some(resource) = &self.resource {
            validate_text("resource", resource, 1, 1_024)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct InvestigationCreateRequest {
    pub cluster_id: ClusterId,
    pub conversation_id: Option<rocketmq_sre_contracts::ConversationId>,
    pub title: String,
    pub resource: Option<String>,
    pub symptom_family: String,
}

impl InvestigationCreateRequest {
    pub(crate) fn validate(&self) -> Result<(), ControlPlaneError> {
        validate_text("title", &self.title, 1, 512)?;
        validate_text("symptom_family", &self.symptom_family, 1, 128)?;
        if let Some(resource) = &self.resource {
            validate_text("resource", resource, 1, 1_024)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct PromoteInvestigationRequest {
    pub title: Option<String>,
    pub reason: String,
}

impl PromoteInvestigationRequest {
    pub(crate) fn validate(&self) -> Result<(), ControlPlaneError> {
        if let Some(title) = &self.title {
            validate_text("title", title, 1, 512)?;
        }
        validate_text("reason", &self.reason, 1, 2_048)
    }
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct IncidentCreateRequest {
    pub cluster_id: ClusterId,
    pub title: String,
    pub resource: Option<String>,
    pub symptom_family: String,
}

impl IncidentCreateRequest {
    pub(crate) fn validate(&self) -> Result<(), ControlPlaneError> {
        validate_text("title", &self.title, 1, 512)?;
        validate_text("symptom_family", &self.symptom_family, 1, 128)?;
        if let Some(resource) = &self.resource {
            validate_text("resource", resource, 1, 1_024)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct InspectionCreateRequest {
    pub cluster_id: ClusterId,
    pub template: InspectionTemplate,
    pub schedule: Option<String>,
}

impl InspectionCreateRequest {
    pub(crate) fn validate(&self) -> Result<(), ControlPlaneError> {
        if let Some(schedule) = &self.schedule {
            validate_text("schedule", schedule, 1, 128)?;
            schedule_interval_from_expression(schedule)?;
        }
        Ok(())
    }

    pub(crate) fn schedule_interval(&self) -> Result<Option<Duration>, ControlPlaneError> {
        self.schedule
            .as_deref()
            .map(schedule_interval_from_expression)
            .transpose()
    }
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct RecommendationDispositionRequest {
    pub status: RecommendationStatus,
    pub assignee: Option<String>,
    pub reason: String,
    pub promote_to: Option<RecommendationPromotionTarget>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RecommendationPromotionTarget {
    Investigation,
    Incident,
}

impl RecommendationDispositionRequest {
    pub(crate) fn validate(&self) -> Result<(), ControlPlaneError> {
        validate_text("reason", &self.reason, 1, 2_048)?;
        if self.status == RecommendationStatus::Assigned
            && self.assignee.as_ref().is_none_or(|value| value.trim().is_empty())
        {
            return Err(ControlPlaneError::validation(
                "invalid_recommendation",
                "assigned recommendations require an assignee",
            ));
        }
        if self.promote_to.is_some() && self.status != RecommendationStatus::Promoted {
            return Err(ControlPlaneError::validation(
                "invalid_recommendation",
                "promote_to is only valid for a promoted recommendation",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ConversationView {
    pub conversation: Conversation,
    pub investigation: Option<Investigation>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct InvestigationView {
    pub investigation: Investigation,
    pub timeline: Vec<TimelineEvent>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct IncidentView {
    pub incident: Incident,
    pub investigation: Option<Investigation>,
    pub timeline: Vec<TimelineEvent>,
    pub diagnosis_revisions: Vec<DiagnosisRevision>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct InspectionView {
    pub run: InspectionRun,
    pub recommendations: Vec<Recommendation>,
    pub pack_diffs: Vec<Value>,
}

fn validate_text(name: &'static str, value: &str, min: usize, max: usize) -> Result<(), ControlPlaneError> {
    let length = value.trim().chars().count();
    if !(min..=max).contains(&length) {
        return Err(ControlPlaneError::validation(
            "invalid_request",
            format!("{name} length must be between {min} and {max} characters"),
        ));
    }
    Ok(())
}

pub(crate) fn schedule_interval_from_expression(value: &str) -> Result<Duration, ControlPlaneError> {
    let seconds = match value.trim() {
        "@hourly" => 60 * 60,
        "@daily" => 24 * 60 * 60,
        "@weekly" => 7 * 24 * 60 * 60,
        schedule => {
            let raw = schedule.strip_prefix("every ").ok_or_else(|| {
                ControlPlaneError::validation(
                    "invalid_schedule",
                    "schedule must be @hourly, @daily, @weekly, or `every <duration>`",
                )
            })?;
            parse_duration_seconds(raw)?
        }
    };
    if !(60..=30 * 24 * 60 * 60).contains(&seconds) {
        return Err(ControlPlaneError::validation(
            "invalid_schedule",
            "inspection interval must be between one minute and thirty days",
        ));
    }
    Ok(Duration::from_secs(seconds))
}

fn parse_duration_seconds(value: &str) -> Result<u64, ControlPlaneError> {
    let value = value.trim();
    let split = value
        .find(|character: char| !character.is_ascii_digit())
        .ok_or_else(|| {
            ControlPlaneError::validation("invalid_schedule", "duration requires an explicit s, m, h, or d unit")
        })?;
    let (amount, unit) = value.split_at(split);
    let amount = amount
        .parse::<u64>()
        .map_err(|_| ControlPlaneError::validation("invalid_schedule", "inspection duration is invalid"))?;
    let multiplier = match unit {
        "s" => 1,
        "m" => 60,
        "h" => 60 * 60,
        "d" => 24 * 60 * 60,
        _ => {
            return Err(ControlPlaneError::validation(
                "invalid_schedule",
                "duration unit must be s, m, h, or d",
            ));
        }
    };
    amount.checked_mul(multiplier).ok_or_else(|| {
        ControlPlaneError::validation("invalid_schedule", "inspection duration exceeds the supported range")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conversation_requires_a_bounded_question() {
        let request = ConversationCreateRequest {
            cluster_id: ClusterId::new(),
            question: String::new(),
            resource: None,
            persist_investigation: true,
        };
        assert!(request.validate().is_err());
    }

    #[test]
    fn ordinary_question_does_not_create_an_investigation_by_default() {
        let request: ConversationCreateRequest = serde_json::from_value(serde_json::json!({
            "cluster_id": ClusterId::new(),
            "question": "Why is this consumer slow?"
        }))
        .expect("valid request");

        assert!(!request.persist_investigation);
    }

    #[test]
    fn workflow_page_rejects_invalid_bounds_and_cursor() {
        let cluster_id = ClusterId::new();
        let invalid_limit = WorkflowListQuery {
            cluster_id,
            cursor: None,
            limit: Some(201),
        };
        assert!(invalid_limit.bounded_limit().is_err());

        let invalid_cursor = WorkflowListQuery {
            cluster_id,
            cursor: Some("not-a-uuid".to_owned()),
            limit: None,
        };
        assert!(invalid_cursor.cursor_uuid().is_err());
    }

    #[test]
    fn workflow_page_exposes_a_stable_uuid_cursor_and_partial_signal() {
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        let page = WorkflowPage::from_window(vec![first, second], 1, |id| *id);

        assert_eq!(page.items, vec![first]);
        assert_eq!(page.next_cursor.as_deref(), Some(first.to_string().as_str()));
        assert!(page.partial);
        assert_eq!(page.warnings, vec![PAGE_TRUNCATED_WARNING]);
        let encoded = serde_json::to_value(page).expect("page should serialize");
        assert_eq!(encoded["schema_version"], WORKFLOW_PAGE_SCHEMA);
        assert!(encoded.get("observed_at").is_some());
    }

    #[test]
    fn assigned_recommendation_requires_an_assignee() {
        let request = RecommendationDispositionRequest {
            status: RecommendationStatus::Assigned,
            assignee: None,
            reason: "take ownership".to_owned(),
            promote_to: None,
        };
        assert!(request.validate().is_err());
    }

    #[test]
    fn recommendation_promotion_target_is_scoped_to_promoted_status() {
        let invalid = RecommendationDispositionRequest {
            status: RecommendationStatus::Acknowledged,
            assignee: None,
            reason: "reviewed".to_owned(),
            promote_to: Some(RecommendationPromotionTarget::Incident),
        };
        assert!(invalid.validate().is_err());

        let valid = RecommendationDispositionRequest {
            status: RecommendationStatus::Promoted,
            assignee: None,
            reason: "operator escalation".to_owned(),
            promote_to: Some(RecommendationPromotionTarget::Incident),
        };
        assert!(valid.validate().is_ok());
    }

    #[test]
    fn inspection_schedule_is_bounded_and_explicit() {
        let request = InspectionCreateRequest {
            cluster_id: ClusterId::new(),
            template: InspectionTemplate::ClusterHealth,
            schedule: Some("every 15m".to_owned()),
        };
        assert_eq!(
            request.schedule_interval().expect("valid schedule"),
            Some(Duration::from_secs(900))
        );

        let too_fast = InspectionCreateRequest {
            schedule: Some("every 5s".to_owned()),
            ..request
        };
        assert!(too_fast.validate().is_err());
    }
}
