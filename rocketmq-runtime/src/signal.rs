// Copyright 2023 The RocketMQ Rust Authors
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

use tracing::warn;

use crate::RuntimeError;
use crate::RuntimeResult;

/// Waits for the process termination signal and logs registration failures.
pub async fn wait_for_signal() {
    if let Err(error) = wait_for_signal_result().await {
        warn!(error = %error, "Failed to wait for the process termination signal");
    }
}

/// Waits for the process termination signal.
///
/// # Errors
///
/// Returns [`RuntimeError::LifecycleOperation`] when the platform signal
/// handler cannot be registered or observed.
#[cfg(unix)]
pub async fn wait_for_signal_result() -> RuntimeResult<()> {
    use tokio::signal::unix::signal;
    use tokio::signal::unix::SignalKind;
    use tracing::info;

    let mut term = signal(SignalKind::terminate()).map_err(|error| RuntimeError::LifecycleOperation {
        operation: "wait_for_signal",
        message: format!("failed to register SIGTERM handler: {error}"),
    })?;
    let mut int = signal(SignalKind::interrupt()).map_err(|error| RuntimeError::LifecycleOperation {
        operation: "wait_for_signal",
        message: format!("failed to register SIGINT handler: {error}"),
    })?;

    tokio::select! {
        _ = term.recv() => info!("Received SIGTERM"),
        _ = int.recv() => info!("Received SIGINT"),
    }
    Ok(())
}

/// Waits for the process termination signal.
///
/// # Errors
///
/// Returns [`RuntimeError::LifecycleOperation`] when the Ctrl-C handler
/// cannot be registered or observed.
#[cfg(windows)]
pub async fn wait_for_signal_result() -> RuntimeResult<()> {
    tokio::signal::ctrl_c()
        .await
        .map_err(|error| RuntimeError::LifecycleOperation {
            operation: "wait_for_signal",
            message: error.to_string(),
        })
}
