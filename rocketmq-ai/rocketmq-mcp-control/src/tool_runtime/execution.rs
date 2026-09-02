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
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use futures_util::FutureExt;
use tokio_util::sync::CancellationToken;

use super::UpsertRequest;
use super::UpsertResponse;
use super::UpsertSessionFactory;
use super::SESSION_SHUTDOWN_TIMEOUT;
use crate::error::ControlError;
use crate::model::ClusterName;

pub(super) async fn supervise_session(
    factory: Arc<dyn UpsertSessionFactory>,
    cluster: ClusterName,
    request: UpsertRequest,
    timeout: Duration,
    request_cancellation: CancellationToken,
    owner_cancellation: CancellationToken,
) -> Result<UpsertResponse, ControlError> {
    let deadline = tokio::time::Instant::now() + timeout;
    let opened = tokio::time::timeout_at(deadline, AssertUnwindSafe(factory.open(&cluster)).catch_unwind());
    tokio::pin!(opened);
    let opened = tokio::select! {
        biased;
        result = &mut opened => result,
        _ = request_cancellation.cancelled() => return Err(ControlError::cancelled()),
        _ = owner_cancellation.cancelled() => return Err(ControlError::cancelled()),
    };
    let mut session = match opened {
        Ok(Ok(Ok(session))) => session,
        Ok(Ok(Err(error))) => return Err(error),
        Ok(Err(_)) => return Err(ControlError::execution_failed()),
        Err(_) => return Err(ControlError::timeout()),
    };
    let run = tokio::select! {
        biased;
        _ = request_cancellation.cancelled() => Err(ControlError::cancelled()),
        _ = owner_cancellation.cancelled() => Err(ControlError::cancelled()),
        result = tokio::time::timeout_at(deadline, AssertUnwindSafe(session.run(request)).catch_unwind()) => {
            match result {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => Err(ControlError::execution_failed()),
                Err(_) => Err(ControlError::timeout()),
            }
        }
    };
    let shutdown = tokio::time::timeout(
        SESSION_SHUTDOWN_TIMEOUT,
        AssertUnwindSafe(session.shutdown()).catch_unwind(),
    )
    .await;
    match shutdown {
        Ok(Ok(Ok(()))) => run,
        _ => Err(ControlError::shutdown_failed()),
    }
}
