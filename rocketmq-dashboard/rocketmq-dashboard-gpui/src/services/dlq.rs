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

//! One-page reads and application-owned sequential DLQ recovery, attribution, and export.

use super::{
    AppServices, ServiceFuture, audit::MutationAudit, consumers::ConsumerRequestScope, messages::MessageRequestScope,
};
use crate::{
    infrastructure::{
        admin_provider::{GpuiAdminProvider, ProviderFailure},
        export_service,
    },
    state::{UiError, UiErrorCode},
};
use rocketmq_dashboard_common::{
    AuditAction, AuditFacts, AuditOutcome, ConnectionScope, ConsumerInventory, ConsumerQueryScope, DlqAttempt,
    DlqBatchResult, DlqCsv, DlqEntryResult, DlqEntryState, DlqIdentityError, DlqPage, DlqQuery, DlqResendTarget,
    MessageIdentity, OperationResource,
};
use std::{collections::BTreeSet, path::PathBuf, sync::Arc};

pub(crate) trait DlqBackend: Send + Sync {
    fn query(&self, scope: MessageRequestScope, query: DlqQuery) -> ServiceFuture<'_, Result<DlqPage, UiError>>;
    fn consume(&self, scope: MessageRequestScope, target: DlqResendTarget) -> ServiceFuture<'_, DlqAttempt>;
    fn csv(
        &self,
        scope: MessageRequestScope,
        query: DlqQuery,
        expected: Vec<MessageIdentity>,
    ) -> ServiceFuture<'_, Result<DlqCsv, UiError>>;
}
pub(super) struct RealDlqBackend {
    provider: Option<Arc<GpuiAdminProvider>>,
}
impl RealDlqBackend {
    pub(super) fn new(provider: Arc<GpuiAdminProvider>) -> Arc<Self> {
        Arc::new(Self {
            provider: Some(provider),
        })
    }
    pub(super) fn unavailable() -> Arc<Self> {
        Arc::new(Self { provider: None })
    }
    fn provider(&self) -> Result<&Arc<GpuiAdminProvider>, UiError> {
        self.provider.as_ref().ok_or_else(|| {
            UiError::new(
                "DLQ operations are unavailable.",
                UiErrorCode::CapabilityUnavailable,
                false,
            )
        })
    }
}
impl DlqBackend for RealDlqBackend {
    fn query(&self, scope: MessageRequestScope, query: DlqQuery) -> ServiceFuture<'_, Result<DlqPage, UiError>> {
        Box::pin(async move {
            self.provider()?
                .query_dlq(scope.revision, query)
                .await
                .map_err(provider_error)
        })
    }
    fn consume(&self, scope: MessageRequestScope, target: DlqResendTarget) -> ServiceFuture<'_, DlqAttempt> {
        Box::pin(async move {
            let Ok(provider) = self.provider() else {
                return DlqAttempt {
                    state: DlqEntryState::Unattempted,
                    receipt: None,
                };
            };
            provider
                .consume_dlq(scope.revision, target)
                .await
                .unwrap_or(DlqAttempt {
                    state: DlqEntryState::Unknown,
                    receipt: None,
                })
        })
    }
    fn csv(
        &self,
        scope: MessageRequestScope,
        query: DlqQuery,
        expected: Vec<MessageIdentity>,
    ) -> ServiceFuture<'_, Result<DlqCsv, UiError>> {
        Box::pin(async move {
            self.provider()?
                .dlq_csv(scope.revision, query, expected)
                .await
                .map_err(provider_error)
        })
    }
}
fn provider_error(error: ProviderFailure) -> UiError {
    let summary = error.summary();
    let retryable = error.is_retryable();
    let code = if error.is_invalid_request() {
        UiErrorCode::Validation
    } else {
        UiErrorCode::Connection
    };
    match error.into_operational() {
        Some(source) => UiError::caused_by(summary, code, retryable, source),
        None => UiError::new(summary, code, retryable),
    }
}
pub(crate) fn validation(error: DlqIdentityError) -> UiError {
    UiError::new(error.summary(), UiErrorCode::Validation, false)
}

impl AppServices {
    fn require_dlq_scope(&self, revision: u64) -> Result<(), UiError> {
        let state = self.connection_state();
        if state.config.scope != ConnectionScope::NameServer {
            return Err(UiError::new(
                "DLQ operations require NameServer Direct scope.",
                UiErrorCode::CapabilityUnavailable,
                false,
            ));
        }
        if state.session.revision != revision {
            return Err(UiError::new(
                "The connection changed. Apply the query again.",
                UiErrorCode::Validation,
                false,
            ));
        }
        Ok(())
    }
    pub(crate) async fn dlq_group_inventory(&self, scope: ConsumerRequestScope) -> Result<ConsumerInventory, UiError> {
        self.require_dlq_scope(scope.revision)?;
        let inventory = self
            .consumer_inventory(ConsumerRequestScope {
                query_scope: ConsumerQueryScope::Configured,
                ..scope
            })
            .await?;
        self.require_dlq_scope(scope.revision)?;
        Ok(inventory)
    }
    pub(crate) async fn query_dlq(&self, scope: MessageRequestScope, query: DlqQuery) -> Result<DlqPage, UiError> {
        query.validate().map_err(validation)?;
        self.require_dlq_scope(scope.revision)?;
        let backend = Arc::clone(&self.dlq);
        let work = async move { backend.query(scope, query).await };
        match &self.runtime_bridge {
            Some(runtime) => runtime.run("gpui-service-dlq-query", work).await,
            None => work.await,
        }
    }
    pub(crate) async fn recover_dlq(
        &self,
        scope: MessageRequestScope,
        targets: Vec<DlqResendTarget>,
    ) -> Result<DlqBatchResult, UiError> {
        self.require_dlq_scope(scope.revision)?;
        if targets.is_empty() || targets.len() > rocketmq_dashboard_common::MESSAGE_PAGE_SIZE_MAX as usize {
            return Err(UiError::new(
                "Select valid entries from one DLQ query page.",
                UiErrorCode::Validation,
                false,
            ));
        }
        let mut seen = BTreeSet::new();
        let targets = targets
            .into_iter()
            .filter(|target| seen.insert((target.original().clone(), target.consumer_group().to_string())))
            .collect::<Vec<_>>();
        // Capture the actor/environment once, then derive per-entry operation IDs locally. No form
        // supplies attribution and a logout or profile switch cannot relabel later batch entries.
        let accepted = self.accept_operation(
            scope.revision,
            AuditAction::DlqResent,
            OperationResource::Message(targets[0].original().message_id.clone()),
        )?;
        let operations = targets
            .iter()
            .enumerate()
            .map(|(index, target)| {
                accepted.clone().map(|mut operation| {
                    operation.context.request_id = format!("{}-entry-{index}", operation.context.request_id);
                    operation.context.resource = OperationResource::Message(target.original().message_id.clone());
                    operation
                })
            })
            .collect::<Vec<_>>();
        let services = self.clone();
        let backend = Arc::clone(&self.dlq);
        let work = async move {
            let mut entries = Vec::with_capacity(targets.len());
            for (target, operation) in targets.into_iter().zip(operations) {
                let attempt = services
                    .record_mutation(operation, async { Ok(backend.consume(scope, target.clone()).await) })
                    .await
                    .unwrap_or(DlqAttempt {
                        state: DlqEntryState::Unattempted,
                        receipt: None,
                    });
                entries.push(DlqEntryResult { target, attempt });
            }
            Ok(DlqBatchResult { entries })
        };
        // The entire sequence is admitted here, before its UI receiver can be dropped.
        match &self.runtime_bridge {
            Some(runtime) => runtime.run_owned("gpui-service-dlq-batch", work).await,
            None => work.await,
        }
    }
    pub(crate) async fn export_dlq_page(
        &self,
        scope: MessageRequestScope,
        query: DlqQuery,
        expected: Vec<MessageIdentity>,
        destination: PathBuf,
    ) -> Result<usize, UiError> {
        query.validate().map_err(validation)?;
        self.require_dlq_scope(scope.revision)?;
        let runtime = self.runtime_bridge.as_ref().ok_or_else(|| {
            UiError::new(
                "The CSV writer is unavailable.",
                UiErrorCode::CapabilityUnavailable,
                false,
            )
        })?;
        let context = runtime.context.clone();
        let backend = Arc::clone(&self.dlq);
        runtime
            .run_owned("gpui-service-dlq-export", async move {
                let csv = backend.csv(scope, query, expected).await?;
                export_service::write_csv(context, destination, csv).await
            })
            .await
    }
    #[cfg(test)]
    pub(crate) fn with_dlq_backend(mut self, backend: Arc<dyn DlqBackend>) -> Self {
        self.dlq = backend;
        self
    }
}
impl MutationAudit for DlqAttempt {
    fn audit(&self) -> (AuditOutcome, AuditFacts) {
        let outcome = match self.state {
            DlqEntryState::Succeeded => AuditOutcome::Succeeded,
            DlqEntryState::BusinessFailure | DlqEntryState::CallFailed => AuditOutcome::Failed,
            DlqEntryState::Unknown => AuditOutcome::Unknown,
            DlqEntryState::Unattempted => AuditOutcome::Rejected,
        };
        (
            outcome,
            AuditFacts {
                applied_targets: u32::from(self.state == DlqEntryState::Succeeded),
                unconfirmed_targets: u32::from(self.state == DlqEntryState::Unknown),
                ..Default::default()
            },
        )
    }
}

#[cfg(test)]
#[path = "dlq_test_support.rs"]
pub(crate) mod test_support;
#[cfg(test)]
#[path = "dlq_tests.rs"]
mod tests;
