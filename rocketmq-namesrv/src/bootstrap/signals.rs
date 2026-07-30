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

use std::future::Future;

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing::error;
use tracing::info;

#[inline]
pub(super) async fn relay<F>(shutdown_tx: watch::Sender<bool>, shutdown_signal: F, cancellation: CancellationToken)
where
    F: Future<Output = ()>,
{
    tokio::select! {
        _ = shutdown_signal => {
            info!("Shutdown signal received, broadcasting to all components...");
            if let Err(error) = shutdown_tx.send(true) {
                error!("Failed to broadcast shutdown signal: {error}");
            }
        }
        _ = cancellation.cancelled() => {
            debug!("NameServer shutdown relay cancelled by its lifecycle owner");
        }
    }
}

pub(super) async fn wait(shutdown_rx: &mut watch::Receiver<bool>) {
    if *shutdown_rx.borrow() {
        return;
    }
    while shutdown_rx.changed().await.is_ok() {
        if *shutdown_rx.borrow() {
            return;
        }
    }
}
