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

use std::future::Future;

use tokio_util::sync::CancellationToken;

use super::DeferredSessionCleanupRegistration;
use super::LegacySessionCleanupCapability;
use super::LegacySessionCleanupEnrollment;
use super::LegacySessionCleanupInstallError;
use crate::admission::AdmissionClass;
use crate::request_ordering::RequestOrdering;
use crate::session_executor::DeferredResumeExecutor;
use crate::session_executor::LegacySessionExecutor;

/// Failure to submit a claimed legacy waiter through its original session.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum LegacySessionExecutionSubmitError {
    /// The canonical session was already closing or its executor was retired.
    #[error("legacy session execution rejected a closed session")]
    SessionClosed,
    /// The canonical session could not admit the resumed execution.
    #[error("legacy session execution admission was exhausted")]
    Admission,
}

/// Affine ownership of one claimed legacy waiter's canonical session execution.
///
/// This value exposes neither the session task group nor its raw executor. A
/// successful submission runs under the original session's ordering and
/// admission limits. Session close cancels the submitted future at every await
/// point, including while the canonical response writer is pending. Dropping
/// the value before submission deregisters its exact session-close callback.
#[must_use = "dropping the enrollment releases the legacy session execution claim"]
pub struct LegacySessionExecutionEnrollment {
    cleanup: LegacySessionCleanupEnrollment,
    executor: LegacySessionExecutor,
    cancellation: CancellationToken,
}

impl std::fmt::Debug for LegacySessionExecutionEnrollment {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LegacySessionExecutionEnrollment")
            .field("active", &true)
            .field("session_closed", &self.cancellation.is_cancelled())
            .finish()
    }
}

impl LegacySessionExecutionEnrollment {
    /// Submits one claimed legacy waiter through its original session owner.
    ///
    /// The enrollment remains live until `execute` reaches its terminal or is
    /// cancelled by session close. Submission failure consumes and releases
    /// the enrollment without polling `execute`.
    ///
    /// # Errors
    ///
    /// Returns a closed-session or admission error when the original session
    /// cannot own the execution.
    pub fn try_execute<F>(self, execute: F) -> Result<(), LegacySessionExecutionSubmitError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let executor = self.executor.clone();
        let cancellation = self.cancellation.clone();
        executor.try_execute(Box::pin(async move {
            let _enrollment = self;
            tokio::select! {
                biased;
                () = cancellation.cancelled() => {}
                () = execute => {}
            }
        }))
    }
}

#[derive(Clone)]
pub(crate) struct LegacySessionExecutionCapability {
    cleanup: LegacySessionCleanupCapability,
    executor: LegacySessionExecutor,
}

impl LegacySessionExecutionCapability {
    pub(crate) fn new(seed: LegacySessionExecutionSeed) -> Self {
        Self {
            cleanup: LegacySessionCleanupCapability::new(seed.cleanup),
            executor: seed.executor,
        }
    }

    pub(crate) fn cleanup_capability(&self) -> LegacySessionCleanupCapability {
        self.cleanup.clone()
    }

    pub(crate) fn install<T, E>(
        &self,
        cleanup: impl Fn() + Send + Sync + 'static,
        install: impl FnOnce(LegacySessionExecutionEnrollment) -> Result<T, (E, LegacySessionExecutionEnrollment)>,
    ) -> Result<T, LegacySessionCleanupInstallError<E>> {
        let cancellation = CancellationToken::new();
        let close_cancellation = cancellation.clone();
        let executor = self.executor.clone();
        self.cleanup.install(
            move || {
                close_cancellation.cancel();
                cleanup();
            },
            move |cleanup| {
                install(LegacySessionExecutionEnrollment {
                    cleanup,
                    executor,
                    cancellation,
                })
                .map_err(|(error, enrollment)| {
                    let LegacySessionExecutionEnrollment {
                        cleanup,
                        executor: _,
                        cancellation: _,
                    } = enrollment;
                    (error, cleanup)
                })
            },
        )
    }
}

pub(crate) struct LegacySessionExecutionSeed {
    cleanup: DeferredSessionCleanupRegistration,
    executor: LegacySessionExecutor,
}

impl LegacySessionExecutionSeed {
    pub(crate) fn new(
        cleanup: DeferredSessionCleanupRegistration,
        resume_executor: DeferredResumeExecutor,
        retained_bytes: usize,
        class: AdmissionClass,
        ordering: RequestOrdering,
    ) -> Self {
        Self {
            cleanup,
            executor: resume_executor.legacy_session_executor(retained_bytes, class, ordering),
        }
    }
}
