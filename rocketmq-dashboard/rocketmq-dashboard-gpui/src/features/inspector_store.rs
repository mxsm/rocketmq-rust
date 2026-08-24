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

//! Independent Broker Inspector resources and draft-safe CAS mutation state.

use std::collections::BTreeMap;

use rocketmq_dashboard_common::{
    BrokerConfigPatch, BrokerConfigSnapshot, BrokerIdentity, BrokerInventoryItem, RuntimeEntry, broker_config_diff,
    broker_config_patch, is_sensitive_key,
};

use crate::{
    features::dashboard_store::{ResourceRequest, ResourceSlot},
    services::brokers::BrokerConfigMutationResult,
    state::{Loadable, UiError, UiErrorCode},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigConfirmation {
    pub broker_name: String,
    pub address: String,
    pub changed_keys: Vec<String>,
}

impl ConfigConfirmation {
    pub fn change_count(&self) -> usize {
        self.changed_keys.len()
    }
}

pub struct ConfigSubmission {
    pub patch: BrokerConfigPatch,
    pub confirmation: ConfigConfirmation,
}

impl std::fmt::Debug for ConfigSubmission {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConfigSubmission")
            .field("patch", &self.patch)
            .field("confirmation", &self.confirmation)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum ConfigSubmissionState {
    Idle,
    Submitting,
    Succeeded {
        generation: u64,
    },
    AppliedReloadFailed {
        reported_generation: u64,
        error: UiError,
    },
    GenerationConflict {
        expected_generation: u64,
        actual_generation: u64,
    },
    Failed(UiError),
}

impl std::fmt::Debug for ConfigSubmissionState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (name, generation) = match self {
            Self::Idle => ("Idle", None),
            Self::Submitting => ("Submitting", None),
            Self::Succeeded { generation } => ("Succeeded", Some(*generation)),
            Self::AppliedReloadFailed {
                reported_generation, ..
            } => ("AppliedReloadFailed", Some(*reported_generation)),
            Self::GenerationConflict { actual_generation, .. } => ("GenerationConflict", Some(*actual_generation)),
            Self::Failed(_) => ("Failed", None),
        };
        formatter
            .debug_struct("ConfigSubmissionState")
            .field("variant", &name)
            .field("generation", &generation)
            .finish()
    }
}

pub struct InspectorStore {
    pub identity: BrokerIdentity,
    pub stale: bool,
    pub overview: ResourceSlot<BrokerInventoryItem>,
    pub runtime: ResourceSlot<Vec<RuntimeEntry>>,
    pub config: ResourceSlot<BrokerConfigSnapshot>,
    pub runtime_filter: String,
    draft: BTreeMap<String, String>,
    validated_revision: Option<u64>,
    config_revision: Option<u64>,
    preserve_draft_on_config_load: bool,
    pending_patch: Option<BrokerConfigPatch>,
    submission_revision: Option<u64>,
    pub submission: ConfigSubmissionState,
}

impl InspectorStore {
    pub fn new(identity: BrokerIdentity, revision: u64) -> Self {
        Self {
            identity,
            stale: false,
            overview: ResourceSlot::default(),
            runtime: ResourceSlot::default(),
            config: ResourceSlot::default(),
            runtime_filter: String::new(),
            draft: BTreeMap::new(),
            validated_revision: Some(revision),
            config_revision: None,
            preserve_draft_on_config_load: false,
            pending_patch: None,
            submission_revision: None,
            submission: ConfigSubmissionState::Idle,
        }
    }

    pub fn begin_overview(&mut self, revision: u64) -> Option<ResourceRequest> {
        self.overview.begin(revision)
    }

    pub fn finish_overview(
        &mut self,
        request: ResourceRequest,
        revision: u64,
        result: Result<Option<BrokerInventoryItem>, UiError>,
    ) -> bool {
        let valid_identity = matches!(&result, Ok(Some(item)) if item.identity == self.identity);
        let stale = !valid_identity;
        let accepted = self.overview.finish(request, revision, result);
        if accepted {
            self.stale = stale;
            self.validated_revision = valid_identity.then_some(revision);
            if stale {
                self.clear_sensitive_resources();
            }
        }
        accepted
    }

    pub fn begin_runtime(&mut self, revision: u64) -> Option<ResourceRequest> {
        self.is_validated_for(revision)
            .then(|| self.runtime.begin(revision))
            .flatten()
    }

    pub fn finish_runtime(
        &mut self,
        request: ResourceRequest,
        revision: u64,
        result: Result<Vec<RuntimeEntry>, UiError>,
    ) -> bool {
        self.runtime.finish(
            request,
            revision,
            result.map(|entries| (!entries.is_empty()).then_some(entries)),
        )
    }

    pub fn begin_config(&mut self, revision: u64, preserve_draft: bool) -> Option<ResourceRequest> {
        if !self.is_validated_for(revision) {
            return None;
        }
        self.preserve_draft_on_config_load = preserve_draft;
        self.config.begin(revision)
    }

    pub fn finish_config(
        &mut self,
        request: ResourceRequest,
        revision: u64,
        result: Result<BrokerConfigSnapshot, UiError>,
    ) -> bool {
        let result = result.and_then(|snapshot| {
            if self.is_validated_for(revision) && snapshot.identity == self.identity {
                Ok(snapshot)
            } else {
                Err(UiError::new(
                    "The Broker configuration response no longer matches the selected revision.",
                    UiErrorCode::Connection,
                    true,
                ))
            }
        });
        let loaded = result.as_ref().ok().cloned();
        if !self.config.finish(request, revision, result.map(Some)) {
            return false;
        }
        if let Some(snapshot) = loaded {
            let loaded_draft = editable_entries(&snapshot);
            self.draft = if self.preserve_draft_on_config_load && !self.draft.is_empty() {
                loaded_draft
                    .into_iter()
                    .map(|(key, value)| {
                        let value = self.draft.get(&key).cloned().unwrap_or(value);
                        (key, value)
                    })
                    .collect()
            } else {
                loaded_draft
            };
            self.config_revision = Some(revision);
            self.submission = ConfigSubmissionState::Idle;
            self.pending_patch = None;
            self.submission_revision = None;
        }
        true
    }

    pub fn draft(&self) -> &BTreeMap<String, String> {
        &self.draft
    }

    pub fn set_draft_value(&mut self, key: &str, value: String) -> Result<(), UiError> {
        let Some(snapshot) = self.config.state.value() else {
            return Err(UiError::new(
                "Broker configuration has not loaded.",
                UiErrorCode::Validation,
                false,
            ));
        };
        if !self.write_ready() || is_sensitive_key(key) || !snapshot.entries().contains_key(key) {
            return Err(UiError::new(
                "This Broker configuration key is not editable.",
                UiErrorCode::Validation,
                false,
            ));
        }
        self.draft.insert(key.to_owned(), value);
        self.submission = ConfigSubmissionState::Idle;
        Ok(())
    }

    pub fn begin_submit(&mut self, revision: u64) -> Result<Option<ConfigSubmission>, UiError> {
        if matches!(self.submission, ConfigSubmissionState::Submitting) {
            return Ok(None);
        }
        if !self.is_write_ready_for(revision) {
            return Err(stale_write_error());
        }
        let submission = self.prepare_submission()?;
        let Some(submission) = submission else {
            return Ok(None);
        };
        self.pending_patch = Some(submission.patch.clone());
        self.submission_revision = Some(revision);
        self.submission = ConfigSubmissionState::Submitting;
        Ok(Some(submission))
    }

    pub fn prepare_submission(&self) -> Result<Option<ConfigSubmission>, UiError> {
        if !self.write_ready() {
            return Err(stale_write_error());
        }
        let Some(snapshot) = self.config.state.value() else {
            return Err(UiError::new(
                "Broker configuration has not loaded.",
                UiErrorCode::Validation,
                false,
            ));
        };
        let diff = broker_config_diff(snapshot, &self.draft);
        if diff.is_empty() {
            return Ok(None);
        }
        let patch = broker_config_patch(snapshot, &diff);
        let confirmation = ConfigConfirmation {
            broker_name: snapshot.identity.broker_name.clone(),
            address: snapshot.identity.address.clone(),
            changed_keys: diff.into_iter().map(|change| change.key).collect(),
        };
        Ok(Some(ConfigSubmission { patch, confirmation }))
    }

    pub fn finish_submit(
        &mut self,
        current_revision: u64,
        result: Result<BrokerConfigMutationResult, UiError>,
    ) -> bool {
        if self.submission_revision.take() != Some(current_revision) || !self.is_validated_for(current_revision) {
            self.pending_patch = None;
            return false;
        }
        match result {
            Ok(BrokerConfigMutationResult::Applied { snapshot, .. }) if snapshot.identity == self.identity => {
                let generation = snapshot.generation;
                self.draft = editable_entries(&snapshot);
                self.config.state = Loadable::ready(snapshot);
                self.config_revision = Some(current_revision);
                self.pending_patch = None;
                self.submission = ConfigSubmissionState::Succeeded { generation };
            }
            Ok(BrokerConfigMutationResult::Applied { .. }) => {
                self.pending_patch = None;
                self.config_revision = None;
                let error = UiError::new(
                    "The reloaded Broker configuration did not match the selected target.",
                    UiErrorCode::Connection,
                    true,
                );
                self.config.clear_with_error(error.clone());
                self.submission = ConfigSubmissionState::Failed(error);
            }
            Ok(BrokerConfigMutationResult::AppliedReloadFailed {
                reported_generation,
                error,
                ..
            }) => {
                self.pending_patch = None;
                self.config_revision = None;
                self.config.clear_with_error(error.clone());
                self.submission = ConfigSubmissionState::AppliedReloadFailed {
                    reported_generation,
                    error,
                };
            }
            Ok(BrokerConfigMutationResult::GenerationConflict {
                expected_generation,
                actual_generation,
            }) => {
                self.pending_patch = None;
                self.submission = ConfigSubmissionState::GenerationConflict {
                    expected_generation,
                    actual_generation,
                };
            }
            Err(error) => {
                self.pending_patch = None;
                self.submission = ConfigSubmissionState::Failed(error);
            }
        }
        true
    }

    pub fn cancel_draft(&mut self) {
        if let Some(snapshot) = self.config.state.value() {
            self.draft = editable_entries(snapshot);
        }
        self.pending_patch = None;
        self.submission_revision = None;
        self.submission = ConfigSubmissionState::Idle;
    }

    pub fn mark_stale(&mut self) {
        self.stale = true;
        self.validated_revision = None;
        self.overview.clear();
        self.clear_sensitive_resources();
    }

    pub fn is_validated_for(&self, revision: u64) -> bool {
        !self.stale && self.validated_revision == Some(revision)
    }

    pub fn confirm_identity_revision(&mut self, revision: u64) {
        self.stale = false;
        self.validated_revision = Some(revision);
    }

    pub fn is_write_ready_for(&self, revision: u64) -> bool {
        self.is_validated_for(revision) && self.config_revision == Some(revision) && self.config.state.value().is_some()
    }

    pub fn write_ready(&self) -> bool {
        self.validated_revision
            .is_some_and(|revision| self.is_write_ready_for(revision))
    }

    fn clear_sensitive_resources(&mut self) {
        self.runtime.clear();
        self.config.clear();
        self.config_revision = None;
        self.draft.clear();
        self.pending_patch = None;
        self.submission_revision = None;
        self.submission = ConfigSubmissionState::Idle;
    }
}

fn editable_entries(snapshot: &BrokerConfigSnapshot) -> BTreeMap<String, String> {
    snapshot
        .entries()
        .iter()
        .filter(|(key, _)| !is_sensitive_key(key))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn stale_write_error() -> UiError {
    UiError::new(
        "Reload and revalidate this Broker before editing its configuration.",
        UiErrorCode::Validation,
        false,
    )
}

#[cfg(test)]
mod tests {
    use rocketmq_dashboard_common::{BrokerRole, EndpointAvailability, Observed};

    use super::*;

    fn identity() -> BrokerIdentity {
        BrokerIdentity {
            cluster: "c".into(),
            broker_name: "b".into(),
            broker_id: 0,
            address: "127.0.0.1:10911".into(),
        }
    }

    fn overview() -> BrokerInventoryItem {
        BrokerInventoryItem {
            identity: identity(),
            role: BrokerRole::Master,
            version: Observed::Unknown,
            availability: EndpointAvailability::Available,
            produce_tps: Observed::Unknown,
            consume_tps: Observed::Unknown,
        }
    }

    fn config(generation: u64) -> BrokerConfigSnapshot {
        BrokerConfigSnapshot::new(
            identity(),
            generation,
            BTreeMap::from([
                ("flushDiskType".into(), "ASYNC_FLUSH".into()),
                ("removedSetting".into(), "old".into()),
                ("accessKey".into(), "must-never-copy".into()),
            ]),
        )
    }

    #[test]
    fn overview_runtime_and_config_are_independent_loadables() {
        let mut store = InspectorStore::new(identity(), 2);
        let overview_request = store.begin_overview(2).expect("overview");
        let runtime_request = store.begin_runtime(2).expect("runtime");
        assert!(store.finish_overview(overview_request, 2, Ok(Some(overview()))));
        assert!(store.finish_runtime(
            runtime_request,
            2,
            Err(UiError::new("runtime unavailable", UiErrorCode::Connection, true))
        ));
        assert!(matches!(store.overview.state, Loadable::Ready(_)));
        assert!(matches!(store.runtime.state, Loadable::Failed { .. }));
        assert!(matches!(store.config.state, Loadable::Idle));
    }

    #[test]
    fn conflict_and_failure_retain_draft_reload_can_preserve_it_and_duplicate_submit_is_blocked() {
        let mut store = InspectorStore::new(identity(), 2);
        let request = store.begin_config(2, false).expect("config");
        assert!(store.finish_config(request, 2, Ok(config(7))));
        assert!(!store.draft().contains_key("accessKey"));
        store
            .set_draft_value("flushDiskType", "SYNC_FLUSH".into())
            .expect("editable");
        let submission = store.begin_submit(2).expect("submit").expect("changed patch");
        assert_eq!(submission.confirmation.changed_keys, ["flushDiskType"]);
        assert!(!format!("{submission:?}").contains("SYNC_FLUSH"));
        assert!(store.begin_submit(2).expect("duplicate ignored").is_none());
        assert!(store.finish_submit(
            2,
            Ok(BrokerConfigMutationResult::GenerationConflict {
                expected_generation: 7,
                actual_generation: 8,
            })
        ));
        assert_eq!(store.draft()["flushDiskType"], "SYNC_FLUSH");

        let reload = store.begin_config(2, true).expect("reload");
        let mut reloaded = config(8);
        let mut reloaded_entries = reloaded.entries().clone();
        reloaded_entries.remove("removedSetting");
        reloaded_entries.insert("brokerRole".into(), "ASYNC_MASTER".into());
        reloaded = BrokerConfigSnapshot::new(identity(), 8, reloaded_entries);
        assert!(store.finish_config(reload, 2, Ok(reloaded)));
        assert_eq!(store.draft()["flushDiskType"], "SYNC_FLUSH");
        assert!(!store.draft().contains_key("removedSetting"));
        assert_eq!(store.draft()["brokerRole"], "ASYNC_MASTER");
        let retry = store.begin_submit(2).expect("retry").expect("retry patch");
        assert_eq!(retry.patch.expected_generation, 8);
        assert!(store.finish_submit(2, Err(UiError::new("failed", UiErrorCode::Connection, true))));
        assert_eq!(store.draft()["flushDiskType"], "SYNC_FLUSH");
        assert!(matches!(store.submission, ConfigSubmissionState::Failed(_)));
        assert!(store.begin_submit(2).expect("retry after failure").is_some());
    }

    #[test]
    fn revision_change_erases_write_state_until_exact_identity_and_new_config_reload() {
        let mut store = InspectorStore::new(identity(), 2);
        let request = store.begin_config(2, false).expect("config");
        assert!(store.finish_config(request, 2, Ok(config(7))));
        store
            .set_draft_value("flushDiskType", "SYNC_FLUSH".into())
            .expect("draft");
        assert!(store.begin_submit(2).expect("submit").is_some());

        store.mark_stale();
        assert!(store.stale);
        assert!(store.draft().is_empty());
        assert!(store.runtime.state.value().is_none());
        assert!(store.config.state.value().is_none());
        assert!(store.begin_config(3, false).is_none());
        assert!(store.begin_submit(3).is_err());

        let inventory = store.begin_overview(3).expect("inventory revalidation");
        assert!(store.finish_overview(inventory, 3, Ok(Some(overview()))));
        assert!(!store.stale);
        assert!(store.begin_submit(3).is_err());

        // The address and generation may be reused, but only this new-revision response unlocks writes.
        let config_request = store.begin_config(3, false).expect("new revision config");
        assert!(store.finish_config(config_request, 3, Ok(config(7))));
        store
            .set_draft_value("flushDiskType", "SYNC_FLUSH".into())
            .expect("new revision draft");
        assert!(store.begin_submit(3).expect("new revision submit").is_some());
        assert!(!store.finish_submit(
            4,
            Ok(BrokerConfigMutationResult::GenerationConflict {
                expected_generation: 7,
                actual_generation: 8,
            })
        ));
    }
}
