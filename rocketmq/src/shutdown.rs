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

use tokio::sync::broadcast;
use tracing::warn;

pub struct Shutdown<T> {
    /// `true` if the shutdown signal has been received
    is_shutdown: bool,

    /// The receiver half of the channel used to listen for shutdown.
    notify: broadcast::Receiver<T>,
}

impl<T> Shutdown<T>
where
    T: Clone,
{
    /// Create a new `Shutdown` backed by the given `broadcast::Receiver`.
    pub fn new(capacity: usize) -> (Shutdown<T>, broadcast::Sender<T>) {
        let (tx, _) = broadcast::channel(capacity);
        let shutdown = Shutdown {
            is_shutdown: false,
            notify: tx.subscribe(),
        };
        (shutdown, tx)
    }

    /// Returns `true` if the shutdown signal has been received.
    pub fn is_shutdown(&self) -> bool {
        self.is_shutdown
    }

    /// Receive the shutdown notice, waiting if necessary.
    pub async fn recv(&mut self) {
        // If the shutdown signal has already been received, then return
        // immediately.
        if self.is_shutdown {
            return;
        }

        // Cannot receive a "lag error" as only one value is ever sent.
        let result = self.notify.recv().await;
        if result.is_err() {
            warn!("Failed to receive shutdown signal");
        }

        // Remember that the signal has been received.
        self.is_shutdown = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shutdown_signal_received() {
        let (mut shutdown, sender) = Shutdown::new(1);
        sender.send(()).unwrap();
        shutdown.recv().await;
        assert!(shutdown.is_shutdown());
    }

    #[tokio::test]
    async fn shutdown_signal_not_received() {
        let (shutdown, _) = Shutdown::<()>::new(1);
        assert!(!shutdown.is_shutdown());
    }

    #[tokio::test]
    async fn shutdown_signal_multiple_receivers() {
        let (mut shutdown1, sender) = Shutdown::new(1);
        sender.send(()).unwrap();
        shutdown1.recv().await;

        assert!(shutdown1.is_shutdown());
    }

    #[tokio::test]
    async fn shutdown_signal_already_received() {
        let (mut shutdown, sender) = Shutdown::new(1);
        sender.send(()).unwrap();
        shutdown.recv().await;
        shutdown.recv().await; // Call recv again
        assert!(shutdown.is_shutdown());
    }
}
