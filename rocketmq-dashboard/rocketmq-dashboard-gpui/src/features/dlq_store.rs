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

//! Applied DLQ pages, stable current-page selection, and reviewed failed-only retries.

use crate::{
    services::messages::MessageRequestScope,
    state::{Loadable, RequestEpoch, UiError},
};
use rocketmq_dashboard_common::{
    DlqBatchResult, DlqPage, DlqQuery, DlqResendTarget, MessageIdentity, dlq_selected_targets,
};
use std::collections::BTreeSet;

#[derive(Clone)]
pub(super) struct DlqRequest {
    pub scope: MessageRequestScope,
    pub query: DlqQuery,
}
pub(super) struct DlqStore {
    pub applied: Option<DlqQuery>,
    pub rows: Loadable<DlqPage>,
    pub selected: BTreeSet<MessageIdentity>,
    pub results: Option<DlqBatchResult>,
    pub draft_changed: bool,
    epoch: RequestEpoch,
}
impl Default for DlqStore {
    fn default() -> Self {
        Self {
            applied: None,
            rows: Loadable::Idle,
            selected: BTreeSet::new(),
            results: None,
            draft_changed: false,
            epoch: RequestEpoch::initial(),
        }
    }
}
impl DlqStore {
    pub fn epoch(&self) -> u64 {
        self.epoch.value()
    }
    pub fn apply(&mut self, query: DlqQuery, revision: u64) -> Option<DlqRequest> {
        self.clear_reads();
        self.applied = Some(query);
        self.draft_changed = false;
        self.begin(revision)
    }
    pub fn begin(&mut self, revision: u64) -> Option<DlqRequest> {
        let query = self.applied.clone()?;
        let epoch = self.epoch.advance().ok()?;
        self.selected.clear();
        self.results = None;
        self.rows = std::mem::replace(&mut self.rows, Loadable::Idle).begin();
        Some(DlqRequest {
            scope: MessageRequestScope {
                revision,
                epoch: epoch.value(),
            },
            query,
        })
    }
    pub fn finish(&mut self, request: &DlqRequest, revision: u64, result: Result<DlqPage, UiError>) -> bool {
        if request.scope.revision != revision
            || request.scope.epoch != self.epoch.value()
            || self.applied.as_ref() != Some(&request.query)
        {
            return false;
        }
        self.rows = match result {
            Ok(page) => Loadable::Ready(page),
            Err(error) => std::mem::replace(&mut self.rows, Loadable::Idle).fail(error),
        };
        true
    }
    pub fn clear_reads(&mut self) {
        let _ = self.epoch.advance();
        self.rows = Loadable::Idle;
        self.selected.clear();
        self.results = None;
    }
    pub fn edit(&mut self) {
        self.clear_reads();
        self.draft_changed = true;
    }
    pub fn clear_context(&mut self) {
        self.clear_reads();
        self.applied = None;
        self.draft_changed = false;
    }
    pub fn selectable(&self, identity: &MessageIdentity) -> bool {
        let Some(target) = self
            .rows
            .value()
            .and_then(|page| page.rows.iter().find(|row| &row.metadata.identity == identity))
            .and_then(|row| row.target.as_ref().ok())
        else {
            return false;
        };
        // Every attempted tuple remains locked in the table. Failed entries have a separate,
        // explicitly reviewed retry action; successes and unknowns are never replayed by it.
        !self.results.as_ref().is_some_and(|result| {
            result.entries.iter().any(|entry| {
                entry.target.original() == target.original() && entry.target.consumer_group() == target.consumer_group()
            })
        })
    }
    pub fn toggle(&mut self, identity: MessageIdentity, checked: bool) {
        if checked && self.selectable(&identity) {
            self.selected.insert(identity);
        } else {
            self.selected.remove(&identity);
        }
    }
    pub fn targets(&self) -> Vec<DlqResendTarget> {
        self.rows
            .value()
            .map_or_else(Vec::new, |page| dlq_selected_targets(&page.rows, &self.selected))
    }
    pub fn merge_results(&mut self, result: DlqBatchResult) {
        self.selected.clear();
        let previous = self
            .results
            .get_or_insert_with(|| DlqBatchResult { entries: Vec::new() });
        for entry in result.entries {
            if let Some(existing) = previous.entries.iter_mut().find(|existing| {
                existing.target.original() == entry.target.original()
                    && existing.target.consumer_group() == entry.target.consumer_group()
            }) {
                *existing = entry;
            } else {
                previous.entries.push(entry);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::dlq::test_support::{page, query};
    use rocketmq_dashboard_common::{DlqAttempt, DlqEntryResult, DlqEntryState};
    #[test]
    fn dlq_page_selection_epochs_and_reviewed_retry_exclude_prior_success_and_unknown() {
        let mut store = DlqStore::default();
        let first = store.apply(query(), 0).unwrap();
        let page = page();
        let id = page.rows[0].metadata.identity.clone();
        let target = page.rows[0].target.clone().unwrap();
        store.finish(&first, 0, Ok(page.clone()));
        store.toggle(id.clone(), true);
        assert_eq!(store.targets().len(), 1);
        for state in [
            DlqEntryState::BusinessFailure,
            DlqEntryState::Succeeded,
            DlqEntryState::Unknown,
        ] {
            store.merge_results(DlqBatchResult {
                entries: vec![DlqEntryResult {
                    target: target.clone(),
                    attempt: DlqAttempt { state, receipt: None },
                }],
            });
            store.toggle(id.clone(), true);
            assert!(store.selected.is_empty());
            assert_eq!(
                store.results.as_ref().unwrap().retryable_targets().len(),
                usize::from(state.can_retry())
            );
        }
        let refresh = store.begin(0).unwrap();
        store.edit();
        assert!(store.results.is_none());
        assert!(!store.finish(&refresh, 0, Ok(page.clone())));
        let next = store.apply(query().with_page(2).unwrap(), 0).unwrap();
        assert!(store.selected.is_empty());
        store.clear_context();
        assert!(!store.finish(&next, 1, Ok(page)));
        assert!(store.applied.is_none());
    }
}
