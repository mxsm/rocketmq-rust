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

//! Applied local Audit query and immutable pages within its retained cursor window.

use rocketmq_dashboard_common::{AuditAcceptedOperation, AuditFilter, AuditPage, AuditQuery};

use crate::state::UiError;

#[derive(Default)]
pub(crate) struct AuditQueryStore {
    pub applied: Option<AuditFilter>,
    pub pages: Vec<AuditPage>,
    pub page_index: usize,
    pub pending: Vec<AuditAcceptedOperation>,
    pub loading: bool,
    pub error: Option<UiError>,
}

impl AuditQueryStore {
    pub fn apply(&mut self, filter: AuditFilter) -> AuditQuery {
        self.applied = Some(filter.clone());
        self.pages.clear();
        self.page_index = 0;
        self.pending.clear();
        self.error = None;
        AuditQuery {
            filter,
            limit: 25,
            cursor: None,
        }
    }

    pub fn page(&self) -> Option<&AuditPage> {
        self.pages.get(self.page_index)
    }

    pub fn next(&self) -> Option<AuditQuery> {
        Some(AuditQuery {
            filter: self.applied.clone()?,
            limit: 25,
            cursor: Some(self.page()?.next_cursor.clone()?),
        })
    }

    pub fn finish(&mut self, query: &AuditQuery, result: Result<(AuditPage, Vec<AuditAcceptedOperation>), UiError>) {
        self.loading = false;
        match result {
            Ok((page, pending)) => {
                self.error = None;
                self.pending = pending
                    .into_iter()
                    .filter(|operation| pending_matches(&query.filter, operation))
                    .collect();
                if query.cursor.is_none() {
                    self.pages = vec![page];
                    self.page_index = 0;
                } else {
                    self.pages.truncate(self.page_index + 1);
                    self.pages.push(page);
                    self.page_index += 1;
                }
            }
            Err(error) => self.error = Some(error),
        }
    }

    pub fn clear(&mut self) {
        *self = Self::default();
    }
}

fn pending_matches(filter: &AuditFilter, operation: &AuditAcceptedOperation) -> bool {
    filter.outcome.is_none()
        && operation.accepted_at_epoch_ms >= filter.start_epoch_ms
        && operation.accepted_at_epoch_ms < filter.end_epoch_ms
        && filter
            .actor
            .as_ref()
            .is_none_or(|actor| actor == &operation.context.actor)
        && filter.action.is_none_or(|action| action == operation.action)
        && filter
            .environment_id
            .as_ref()
            .is_none_or(|id| id == &operation.context.environment_id)
}
