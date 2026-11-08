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

//! Session admission for the existing owned History collector.

use std::sync::Arc;

use rocketmq_dashboard_common::EnvironmentConnectionSnapshot;

use super::sessions::SessionService;
use crate::{
    infrastructure::history_collector::{EnvironmentHistorySampleFuture, HistorySampleFuture, HistorySampler},
    state::{UiError, UiErrorCode},
};

/// Reuses the foreground session authority without introducing another polling task. The
/// collector still owns sampling, cancellation, environment fencing, and lease renewal.
pub(super) struct ProtectedHistorySampler {
    sessions: Arc<SessionService>,
    inner: Arc<dyn HistorySampler>,
}

impl ProtectedHistorySampler {
    pub(super) fn new(sessions: Arc<SessionService>, inner: Arc<dyn HistorySampler>) -> Arc<Self> {
        Arc::new(Self { sessions, inner })
    }

    async fn admit(&self) -> Result<(), UiError> {
        let expected = self.sessions.snapshot();
        let access = self.sessions.validate_marker(&expected).await?;
        if self.sessions.snapshot() != expected || !self.sessions.access_is_current(&access) {
            return Err(UiError::new(
                "The session changed before History sampling. Sign in again.",
                UiErrorCode::Authentication,
                false,
            ));
        }
        Ok(())
    }
}

impl HistorySampler for ProtectedHistorySampler {
    fn sample_topics(&self) -> HistorySampleFuture<'_> {
        Box::pin(async move {
            self.admit().await?;
            self.inner.sample_topics().await
        })
    }

    fn sample_brokers(&self) -> HistorySampleFuture<'_> {
        Box::pin(async move {
            self.admit().await?;
            self.inner.sample_brokers().await
        })
    }

    fn sample_topics_scoped(&self, scope: Option<EnvironmentConnectionSnapshot>) -> EnvironmentHistorySampleFuture<'_> {
        Box::pin(async move {
            self.admit().await?;
            self.inner.sample_topics_scoped(scope).await
        })
    }

    fn sample_brokers_scoped(
        &self,
        scope: Option<EnvironmentConnectionSnapshot>,
    ) -> EnvironmentHistorySampleFuture<'_> {
        Box::pin(async move {
            self.admit().await?;
            self.inner.sample_brokers_scoped(scope).await
        })
    }
}
