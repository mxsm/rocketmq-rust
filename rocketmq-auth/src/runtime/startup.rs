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

use std::sync::Arc;
use std::time::Duration;

use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::MetadataDeadline;
use rocketmq_runtime::MetadataIoActor;
use rocketmq_runtime::ShutdownDeadline;
use tokio::sync::Mutex;

use crate::provider_owner::ProviderOwner;
use crate::AuthFailureKind;
use crate::AuthOperation;
use crate::AuthServiceError;
use crate::AuthServiceResult;

/// Startup failure with the original cause and any incomplete cleanup retained.
///
/// Obtain this value from the source of an [`AuthServiceError`]. A failed or
/// timed-out rollback retains its providers and I/O owner for an explicit retry;
/// dropping the error does not perform asynchronous cleanup.
pub struct AuthStartupFailure {
    primary: AuthServiceError,
    cleanup_error: Option<AuthServiceError>,
    cleanup: StartupCleanup,
}

impl AuthStartupFailure {
    pub fn primary(&self) -> &AuthServiceError {
        &self.primary
    }

    pub fn cleanup_error(&self) -> Option<&AuthServiceError> {
        self.cleanup_error.as_ref()
    }

    /// Resumes rollback using the retained owner and one absolute deadline.
    ///
    /// # Errors
    ///
    /// Returns a typed timeout or cleanup failure while resources still need
    /// finalization. Successful controls are not closed again on subsequent calls.
    pub async fn retry_cleanup_until(&self, deadline: ShutdownDeadline) -> AuthServiceResult<()> {
        self.cleanup.run(deadline).await
    }
}

impl std::fmt::Debug for AuthStartupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthStartupFailure")
            .field("primary", &self.primary)
            .field("cleanup_error", &self.cleanup_error)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for AuthStartupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("authentication runtime startup failed")
    }
}

impl std::error::Error for AuthStartupFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.primary)
    }
}

struct StartupCleanup {
    provider: Option<Arc<ProviderOwner>>,
    metadata_io: Option<MetadataIoActor>,
    context: ChildServiceContext,
    completed: Mutex<bool>,
}

impl StartupCleanup {
    async fn run(&self, deadline: ShutdownDeadline) -> AuthServiceResult<()> {
        let at = tokio::time::Instant::from_std(deadline.instant());
        let timeout = || AuthServiceError::new(AuthOperation::MaintainService, AuthFailureKind::Timeout);
        let mut completed = tokio::time::timeout_at(at, self.completed.lock())
            .await
            .map_err(|_| timeout())?;
        if *completed {
            return Ok(());
        }
        if let Some(provider) = &self.provider {
            // Keep the I/O lane alive until provider cleanup finishes, including
            // after a timeout. Providers may need it to release their resources.
            provider.shutdown_until(deadline, false).await?;
        }
        if let Some(metadata_io) = &self.metadata_io {
            let report = metadata_io.shutdown_until(MetadataDeadline::at(at)).await;
            if report.timed_out || report.pending_operations != 0 {
                return Err(timeout());
            }
        }
        let report = self.context.task_group().shutdown(deadline.remaining()).await;
        if !report.is_healthy() {
            return Err(timeout());
        }
        *completed = true;
        Ok(())
    }
}

pub(super) async fn rollback(
    primary: AuthServiceError,
    provider: Option<Arc<ProviderOwner>>,
    metadata_io: Option<MetadataIoActor>,
    context: ChildServiceContext,
) -> AuthServiceError {
    let cleanup = StartupCleanup {
        provider,
        metadata_io,
        context,
        completed: Mutex::new(false),
    };
    let cleanup_error = cleanup.run(ShutdownDeadline::after(Duration::from_secs(5))).await.err();
    AuthServiceError::with_source(
        primary.operation(),
        primary.kind(),
        AuthStartupFailure {
            primary,
            cleanup_error,
            cleanup,
        },
    )
}
