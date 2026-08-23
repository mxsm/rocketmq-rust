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
use crate::error::DashboardError;
use crate::model::{
    ConsumerMonitorMutationResult, ConsumerMonitorRule, ConsumerMonitorUpsertRequest, ConsumerMonitorView,
    EnvironmentId,
};
use crate::persistence::Revision;
use crate::state::AppState;

pub async fn list_consumer_monitors(
    state: &AppState,
    environment_id: &EnvironmentId,
) -> Result<Vec<ConsumerMonitorView>, DashboardError> {
    state
        .persistence
        .list_monitor_rules(environment_id)
        .await
        .map(|rules| rules.into_iter().map(ConsumerMonitorView::from).collect())
        .map_err(Into::into)
}

pub async fn create_or_update_consumer_monitor(
    state: &AppState,
    request: ConsumerMonitorUpsertRequest,
) -> Result<ConsumerMonitorMutationResult, DashboardError> {
    let rule = ConsumerMonitorRule {
        environment_id: request.environment_id,
        consumer_group: request.consumer_group.trim().to_string(),
        min_count: request.min_count,
        max_diff_total: request.max_diff_total,
        revision: Revision(0),
        created_at_ms: 0,
        updated_at_ms: 0,
    };
    let expected_revision = request.expected_revision;
    state
        .run_persisted_mutation("dashboard-monitor-candidate-persist", move |state| async move {
            let saved = state.persistence.upsert_monitor_rule(rule, expected_revision).await?;
            let item = ConsumerMonitorView::from(saved);
            Ok(ConsumerMonitorMutationResult {
                message: format!("Consumer monitor {} saved", item.consumer_group),
                item: Some(item),
            })
        })
        .await
}

pub async fn delete_consumer_monitor(
    state: &AppState,
    environment_id: &EnvironmentId,
    consumer_group: &str,
    expected_revision: Revision,
) -> Result<ConsumerMonitorMutationResult, DashboardError> {
    let environment_id = environment_id.clone();
    let consumer_group = consumer_group.to_string();
    state
        .run_persisted_mutation("dashboard-monitor-delete", move |state| async move {
            let removed = state
                .persistence
                .delete_monitor_rule(&environment_id, &consumer_group, expected_revision)
                .await?;
            Ok(ConsumerMonitorMutationResult {
                message: if removed {
                    format!("Consumer monitor {consumer_group} deleted")
                } else {
                    format!("Consumer monitor {consumer_group} did not exist")
                },
                item: None,
            })
        })
        .await
}
