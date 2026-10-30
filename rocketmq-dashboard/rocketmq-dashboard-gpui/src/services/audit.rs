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

//! Application-owned mutation attribution, terminal writes, and audit-only recovery.

use std::{
    collections::BTreeMap,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::SystemTime,
};

use rocketmq_dashboard_common::{
    AuditAcceptedOperation, AuditAction, AuditFacts, AuditFailureCode, AuditOutcome, AuditPage, AuditQuery,
    OperationActor, OperationResource,
};

use super::AppServices;
use crate::{
    infrastructure::{
        audit_store::AuditStore,
        auth_state::{DesktopAuthState, LocalSession},
        config_store::{ConfigStoreFailure, DesktopConfigStore},
    },
    state::{UiError, UiErrorCode},
};

#[derive(Clone)]
struct TerminalWrite {
    operation: AuditAcceptedOperation,
    outcome: AuditOutcome,
    facts: AuditFacts,
    timestamp_epoch_ms: u64,
}

struct FailedTerminalWrite {
    write: TerminalWrite,
    // Retain the typed cause for recovery diagnostics without exposing its path or source text.
    _failure: ConfigStoreFailure,
}

/// Read attribution only; every use must compare it with the live local authority.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct AuditReadAccess {
    // Authentication-disabled mode intentionally has no operator identity requirement.
    session: Option<LocalSession>,
}

#[cfg(test)]
type TerminalGate = (tokio::sync::oneshot::Sender<()>, tokio::sync::oneshot::Receiver<()>);

pub(super) struct AuditService {
    store: AuditStore,
    config: Arc<DesktopConfigStore>,
    auth: Arc<DesktopAuthState>,
    // Only safe terminal facts are retained. Commands, credentials, and message bodies are dropped
    // with the executing future and can never be replayed by this recovery path.
    failed_writes: parking_lot::Mutex<BTreeMap<String, FailedTerminalWrite>>,
    // An exhausted audit queue must never keep an established local session signed in. This
    // reserved slot retains its logout fact without displacing any admitted Broker outcome.
    failed_logout: parking_lot::Mutex<Option<FailedTerminalWrite>>,
    changes: tokio::sync::watch::Sender<usize>,
    recovery_loaded: AtomicBool,
    recovered_pending: AtomicUsize,
    #[cfg(test)]
    terminal_gate: parking_lot::Mutex<Option<TerminalGate>>,
}

impl AuditService {
    pub(super) fn new(config: Arc<DesktopConfigStore>, auth: Arc<DesktopAuthState>) -> Arc<Self> {
        let (changes, _) = tokio::sync::watch::channel(0);
        Arc::new(Self {
            store: AuditStore::new(Arc::clone(&config)),
            config,
            auth,
            failed_writes: parking_lot::Mutex::new(BTreeMap::new()),
            failed_logout: parking_lot::Mutex::new(None),
            changes,
            recovery_loaded: AtomicBool::new(false),
            recovered_pending: AtomicUsize::new(0),
            #[cfg(test)]
            terminal_gate: parking_lot::Mutex::new(None),
        })
    }

    pub(super) fn recover_pending(&self) {
        if !self.recovery_loaded.swap(true, Ordering::AcqRel) {
            let count = self.store.pending().map_or(0, |pending| pending.len());
            self.recovered_pending.store(count, Ordering::Release);
            self.changes.send_replace(count);
        }
    }

    fn capture(
        &self,
        revision: u64,
        action: AuditAction,
        resource: OperationResource,
    ) -> Result<AuditAcceptedOperation, UiError> {
        let session = self.auth.session();
        if action == AuditAction::LocalLogout
            && !session.is_authenticated()
            && let Some(pending) = self.failed_logout.lock().as_ref()
        {
            // Repeating logout of the already-cleared session is idempotent. Keep the original
            // operator and request ID while its terminal write still needs persistence.
            return Ok(pending.write.operation.clone());
        }
        if action != AuditAction::LocalLogout
            && (self.failed_writes.lock().len() >= crate::infrastructure::audit_store::MAX_PENDING
                || self.failed_logout.lock().is_some())
        {
            return Err(UiError::new(
                "Retry pending audit writes before submitting another operation.",
                UiErrorCode::Configuration,
                false,
            ));
        }
        let actor = session
            .username()
            .map(|username| OperationActor::LocalOperator {
                username: username.to_owned(),
            })
            .unwrap_or(OperationActor::LocalDesktop);
        let operation = AuditAcceptedOperation {
            context: self
                .config
                .operation_context(revision, actor, resource)
                .map_err(super::config_ui_error)?,
            action,
            accepted_at_epoch_ms: now_epoch_ms()?,
            topic_fields: Default::default(),
            consumer_fields: Default::default(),
        };
        operation.validate().map_err(|source| {
            UiError::caused_by(
                "Operation attribution is invalid.",
                UiErrorCode::Validation,
                false,
                source,
            )
        })?;
        Ok(operation)
    }

    async fn complete(&self, write: TerminalWrite) {
        #[cfg(test)]
        {
            let gate = { self.terminal_gate.lock().take() };
            if let Some((started, release)) = gate {
                let _ = started.send(());
                let _ = release.await;
            }
        }
        let request_id = write.operation.context.request_id.clone();
        let result = self
            .store
            .finish(
                write.operation.clone(),
                write.outcome,
                write.facts.clone(),
                write.timestamp_epoch_ms,
            )
            .await;
        let count = {
            let mut failed = self.failed_writes.lock();
            match result {
                Err(failure) => {
                    let logout_overflow = write.operation.action == AuditAction::LocalLogout
                        && failed.len() >= crate::infrastructure::audit_store::MAX_PENDING
                        && !failed.contains_key(&request_id);
                    let pending = FailedTerminalWrite {
                        write,
                        _failure: failure,
                    };
                    if logout_overflow {
                        *self.failed_logout.lock() = Some(pending);
                    } else {
                        let mut logout = self.failed_logout.lock();
                        if logout
                            .as_ref()
                            .is_some_and(|pending| pending.write.operation.context.request_id == request_id)
                        {
                            *logout = None;
                        }
                        failed.insert(request_id, pending);
                    }
                }
                Ok(()) => {
                    failed.remove(&request_id);
                    let mut logout = self.failed_logout.lock();
                    if logout
                        .as_ref()
                        .is_some_and(|pending| pending.write.operation.context.request_id == request_id)
                    {
                        *logout = None;
                    }
                }
            }
            failed.len()
        };
        self.changes.send_replace(
            count + usize::from(self.failed_logout.lock().is_some()) + self.recovered_pending.load(Ordering::Acquire),
        );
    }

    async fn retry(&self) {
        let mut writes: Vec<_> = self
            .failed_writes
            .lock()
            .values()
            .map(|failed| failed.write.clone())
            .collect();
        if let Some(logout) = self.failed_logout.lock().as_ref() {
            writes.push(logout.write.clone());
        }
        for write in writes {
            self.complete(write).await;
        }
    }
}

pub(crate) fn now_epoch_ms() -> Result<u64, UiError> {
    let elapsed = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|source| {
            UiError::caused_by(
                "The system clock is unavailable.",
                UiErrorCode::Configuration,
                false,
                source,
            )
        })?;
    u64::try_from(elapsed.as_millis()).map_err(|source| {
        UiError::caused_by(
            "The system clock is out of range.",
            UiErrorCode::Configuration,
            false,
            source,
        )
    })
}

/// Domain results project their actual business outcome, never just `Result::is_ok()`.
pub(super) trait MutationAudit {
    fn audit(&self) -> (AuditOutcome, AuditFacts);
}

impl MutationAudit for () {
    fn audit(&self) -> (AuditOutcome, AuditFacts) {
        (AuditOutcome::Succeeded, AuditFacts::default())
    }
}

impl MutationAudit for super::SessionState {
    fn audit(&self) -> (AuditOutcome, AuditFacts) {
        (
            AuditOutcome::Succeeded,
            AuditFacts {
                authenticated_actor: self.username.as_ref().map(|username| OperationActor::LocalOperator {
                    username: username.clone(),
                }),
                ..AuditFacts::default()
            },
        )
    }
}

impl MutationAudit for super::ConfigUpdate {
    fn audit(&self) -> (AuditOutcome, AuditFacts) {
        (AuditOutcome::Succeeded, AuditFacts::default())
    }
}

pub(super) fn rejected_conflict() -> (AuditOutcome, AuditFacts) {
    (
        AuditOutcome::Rejected,
        AuditFacts {
            failure_code: Some(AuditFailureCode::Conflict),
            ..AuditFacts::default()
        },
    )
}

pub(super) fn target_outcome(applied: usize, unconfirmed: usize, refresh_failed: bool) -> (AuditOutcome, AuditFacts) {
    let outcome = match (applied, unconfirmed) {
        (0, 0) => AuditOutcome::Rejected,
        (0, _) => AuditOutcome::Unknown,
        (_, 0) => AuditOutcome::Succeeded,
        _ => AuditOutcome::Partial,
    };
    (
        outcome,
        AuditFacts {
            applied_targets: u32::try_from(applied).unwrap_or(u32::MAX),
            unconfirmed_targets: u32::try_from(unconfirmed).unwrap_or(u32::MAX),
            refresh_failed,
            failure_code: None,
            consumer_cleanup: None,
            authenticated_actor: None,
        },
    )
}

fn error_outcome(error: &UiError) -> (AuditOutcome, AuditFacts) {
    let (outcome, code) = match error.code() {
        UiErrorCode::Validation => (AuditOutcome::Rejected, AuditFailureCode::Validation),
        UiErrorCode::Authentication => (AuditOutcome::Rejected, AuditFailureCode::Authentication),
        UiErrorCode::CapabilityUnavailable => (AuditOutcome::Rejected, AuditFailureCode::CapabilityUnavailable),
        UiErrorCode::Configuration => (AuditOutcome::Unknown, AuditFailureCode::Configuration),
        UiErrorCode::Connection => (AuditOutcome::Unknown, AuditFailureCode::Connection),
        UiErrorCode::Unknown => (AuditOutcome::Unknown, AuditFailureCode::Unknown),
    };
    (
        outcome,
        AuditFacts {
            failure_code: Some(code),
            ..AuditFacts::default()
        },
    )
}

impl AppServices {
    pub(super) fn require_acl_access(&self, revision: u64) -> Result<(), UiError> {
        let state = self.connection_state();
        if state.config.scope != rocketmq_dashboard_common::ConnectionScope::NameServer {
            return Err(UiError::new(
                "ACL management requires NameServer Direct scope.",
                UiErrorCode::CapabilityUnavailable,
                false,
            ));
        }
        if state.config.revision != revision {
            return Err(UiError::new(
                "The connection changed. Confirm the ACL scope again.",
                UiErrorCode::Validation,
                false,
            ));
        }
        if let Some(audit) = &self.audit
            && audit.config.authentication_enabled().map_err(super::config_ui_error)?
            && !audit.auth.session().is_authenticated()
        {
            return Err(UiError::new(
                "Sign in before accessing ACL data.",
                UiErrorCode::Authentication,
                false,
            ));
        }
        Ok(())
    }
    /// Reads only the canonical in-memory authentication setting and existing local session.
    pub(crate) fn audit_read_access(&self) -> Result<AuditReadAccess, UiError> {
        let audit = self
            .audit
            .as_ref()
            .ok_or_else(|| UiError::new("Local audit is unavailable.", UiErrorCode::CapabilityUnavailable, false))?;
        let session = if audit.config.authentication_enabled().map_err(super::config_ui_error)? {
            let session = audit.auth.session();
            if !session.is_authenticated() {
                return Err(UiError::new(
                    "Sign in to read local audit records.",
                    UiErrorCode::Authentication,
                    false,
                ));
            }
            Some(session)
        } else {
            None
        };
        Ok(AuditReadAccess { session })
    }

    #[cfg(test)]
    pub(crate) fn hold_next_audit_terminal(
        &self,
    ) -> (tokio::sync::oneshot::Receiver<()>, tokio::sync::oneshot::Sender<()>) {
        let (started, observed) = tokio::sync::oneshot::channel();
        let (release, wait) = tokio::sync::oneshot::channel();
        *self.audit.as_ref().unwrap().terminal_gate.lock() = Some((started, wait));
        (observed, release)
    }

    pub(super) fn accept_operation(
        &self,
        revision: u64,
        action: AuditAction,
        resource: OperationResource,
    ) -> Result<Option<AuditAcceptedOperation>, UiError> {
        self.audit
            .as_ref()
            .map(|audit| audit.capture(revision, action, resource))
            .transpose()
    }

    pub(super) async fn run_mutation<T: MutationAudit + Send + 'static>(
        &self,
        name: &'static str,
        operation: Option<AuditAcceptedOperation>,
        future: impl Future<Output = Result<T, UiError>> + Send + 'static,
    ) -> Result<T, UiError> {
        let services = self.clone();
        let work = async move { services.record_mutation(operation, future).await };
        match &self.runtime_bridge {
            Some(runtime) => runtime.run_owned(name, work).await,
            None => work.await,
        }
    }

    /// Reuses the audit boundary inside an already application-owned sequential batch.
    pub(super) async fn record_mutation<T: MutationAudit + Send>(
        &self,
        operation: Option<AuditAcceptedOperation>,
        future: impl Future<Output = Result<T, UiError>> + Send,
    ) -> Result<T, UiError> {
        let audit = self.audit.clone();
        if let (Some(audit), Some(operation)) = (&audit, &operation)
            && !matches!(operation.action, AuditAction::LocalLogin | AuditAction::LocalLogout)
        {
            audit.store.accept(operation.clone()).await.map_err(|source| {
                super::config_ui_error_with_summary(source, "The operation could not be recorded and was not started.")
            })?;
        }
        let result = future.await;
        if let (Some(audit), Some(operation)) = (audit, operation) {
            let (outcome, facts) = result.as_ref().map_or_else(error_outcome, MutationAudit::audit);
            // Preserve a terminal fact even if the wall clock fails after acceptance.
            let timestamp_epoch_ms = now_epoch_ms().unwrap_or(operation.accepted_at_epoch_ms);
            audit
                .complete(TerminalWrite {
                    operation,
                    outcome,
                    facts,
                    timestamp_epoch_ms,
                })
                .await;
        }
        result
    }

    pub(crate) fn audit_changes(&self) -> Option<tokio::sync::watch::Receiver<usize>> {
        self.audit.as_ref().map(|audit| audit.changes.subscribe())
    }

    pub(crate) fn audit_write_warning(&self) -> bool {
        self.audit
            .as_ref()
            .is_some_and(|audit| !audit.failed_writes.lock().is_empty() || audit.failed_logout.lock().is_some())
    }

    pub(crate) fn audit_recovery_warning(&self) -> bool {
        self.audit
            .as_ref()
            .is_some_and(|audit| audit.recovered_pending.load(Ordering::Acquire) > 0)
    }

    /// Retries only terminal persistence. No mutation command is retained or executed here.
    pub(crate) async fn retry_audit_writes(&self) -> Result<(), UiError> {
        let Some(audit) = self.audit.clone() else {
            return Ok(());
        };
        let work = async move {
            audit.retry().await;
            Ok(())
        };
        match &self.runtime_bridge {
            Some(runtime) => runtime.run_owned("gpui-audit-retry", work).await,
            None => work.await,
        }
    }

    pub(crate) fn query_audit(&self, query: AuditQuery) -> Result<AuditPage, UiError> {
        self.audit_read_access()?;
        let audit = self
            .audit
            .as_ref()
            .ok_or_else(|| UiError::new("Local audit is unavailable.", UiErrorCode::CapabilityUnavailable, false))?;
        audit.store.query(query).map_err(|error| {
            super::config_ui_error_with_summary(
                error,
                "Local audit records could not be read. Restart the applied query if its retained page expired.",
            )
        })
    }

    pub(crate) fn audit_pending(&self) -> Result<Vec<AuditAcceptedOperation>, UiError> {
        let Some(audit) = &self.audit else {
            return Ok(Vec::new());
        };
        self.audit_read_access()?;
        audit.store.pending().map_err(super::config_ui_error)
    }
}

#[cfg(test)]
#[path = "audit_tests.rs"]
mod tests;
