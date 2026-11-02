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

//! P11 journal facade over the same selected authority as configuration and Sessions.

use rocketmq_dashboard_common::{AuditAcceptedOperation, AuditFacts, AuditOutcome, AuditPage, AuditQuery};
pub(super) use rocketmq_dashboard_storage::AuditJournal;
use std::sync::Arc;
pub(crate) const MAX_PENDING: usize = rocketmq_dashboard_storage::MAX_PENDING_AUDIT_OPERATIONS;
use super::config_store::{ConfigStoreFailure, DesktopConfigStore};

#[derive(Clone)]
pub(crate) struct AuditStore {
    config: Arc<DesktopConfigStore>,
}

impl AuditStore {
    pub(crate) fn new(config: Arc<DesktopConfigStore>) -> Self {
        Self { config }
    }

    pub(crate) async fn accept(&self, operation: AuditAcceptedOperation) -> Result<(), ConfigStoreFailure> {
        self.config.accept_audit(operation).await
    }

    pub(crate) async fn accept_batch(&self, operations: Vec<AuditAcceptedOperation>) -> Result<(), ConfigStoreFailure> {
        self.config.accept_audit_batch(operations).await
    }

    pub(crate) async fn finish(
        &self,
        operation: AuditAcceptedOperation,
        outcome: AuditOutcome,
        facts: AuditFacts,
        timestamp_epoch_ms: u64,
    ) -> Result<(), ConfigStoreFailure> {
        self.config
            .update_audit("gpui-audit-terminal", move |journal| {
                journal
                    .finish(operation, outcome, facts, timestamp_epoch_ms)
                    .map_err(ConfigStoreFailure::from)
            })
            .await
    }

    #[cfg(test)]
    pub(crate) async fn query(&self, query: AuditQuery) -> Result<AuditPage, ConfigStoreFailure> {
        self.query_with_pending(query).await.map(|(page, _)| page)
    }

    pub(crate) async fn query_with_pending(
        &self,
        query: AuditQuery,
    ) -> Result<(AuditPage, Vec<AuditAcceptedOperation>), ConfigStoreFailure> {
        let snapshot = self.config.audit_snapshot().await?;
        let page = snapshot.query(query).map_err(ConfigStoreFailure::from)?;
        Ok((page, snapshot.pending().to_vec()))
    }

    pub(crate) async fn pending(&self) -> Result<Vec<AuditAcceptedOperation>, ConfigStoreFailure> {
        Ok(self.config.audit_snapshot().await?.pending().to_vec())
    }
}

/// File fixtures inspect durable facts independently of a signed-out or drained service scope.
#[cfg(test)]
pub(crate) fn persisted_journal(path: &std::path::Path) -> AuditJournal {
    let document: rocketmq_dashboard_storage::DesktopDocument =
        serde_json::from_slice(&std::fs::read(path).expect("native file fixture")).expect("native persisted document");
    document.audit
}

#[cfg(test)]
#[path = "audit_store_tests.rs"]
mod tests;
