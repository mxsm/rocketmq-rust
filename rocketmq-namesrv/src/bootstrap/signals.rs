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

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::pin::pin;

    use tokio::sync::oneshot;

    use super::*;

    #[tokio::test]
    async fn relay_broadcasts_shutdown_when_the_signal_completes() {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (signal_tx, signal_rx) = oneshot::channel::<()>();
        signal_tx
            .send(())
            .expect("shutdown signal receiver should still be alive");

        relay(
            shutdown_tx,
            async {
                signal_rx.await.expect("shutdown signal sender should not be dropped");
            },
            CancellationToken::new(),
        )
        .await;

        assert!(*shutdown_rx.borrow(), "relay should broadcast the shutdown signal");
    }

    #[tokio::test]
    async fn relay_cancelled_before_the_signal_leaves_the_value_unchanged() {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let cancellation = CancellationToken::new();
        cancellation.cancel();

        relay(shutdown_tx, pending::<()>(), cancellation).await;

        assert!(!*shutdown_rx.borrow(), "a cancelled relay must not broadcast shutdown");
        assert!(
            shutdown_rx.changed().await.is_err(),
            "a returning relay drops the sender, closing the channel"
        );
    }

    #[tokio::test]
    async fn wait_returns_immediately_when_shutdown_already_broadcast() {
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(true);

        assert!(
            futures::poll!(pin!(wait(&mut shutdown_rx))).is_ready(),
            "wait should not suspend when the current value is already true"
        );
    }

    #[tokio::test]
    async fn wait_suspends_until_shutdown_is_broadcast() {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let mut waiter = pin!(wait(&mut shutdown_rx));

        assert!(
            futures::poll!(waiter.as_mut()).is_pending(),
            "wait should suspend while the value is still false"
        );

        shutdown_tx.send(true).expect("waiter keeps a receiver alive");
        waiter.await;
    }

    #[tokio::test]
    async fn wait_returns_when_the_sender_is_dropped_without_a_change() {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        drop(shutdown_tx);

        wait(&mut shutdown_rx).await;

        assert!(!*shutdown_rx.borrow(), "a closed channel keeps its last value");
    }
}
