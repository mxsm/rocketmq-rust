/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
    pub fn new(notify: broadcast::Receiver<T>) -> Shutdown<T> {
        Shutdown {
            is_shutdown: false,
            notify,
        }
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
    use tokio::sync::broadcast;

    use super::*;

    #[tokio::test]
    async fn shutdown_initial_state() {
        let (_, rx) = broadcast::channel::<()>(1);
        let shutdown = Shutdown::new(rx);
        assert!(!shutdown.is_shutdown());
    }

    #[tokio::test]
    async fn shutdown_signal_received() {
        let (tx, rx) = broadcast::channel::<()>(1);
        let mut shutdown = Shutdown::new(rx);
        tx.send(()).unwrap();
        shutdown.recv().await;
        assert!(shutdown.is_shutdown());
    }

    #[tokio::test]
    async fn shutdown_signal_already_received() {
        let (tx, rx) = broadcast::channel::<()>(1);
        let mut shutdown = Shutdown::new(rx);
        tx.send(()).unwrap();
        shutdown.recv().await;
        shutdown.recv().await;
        assert!(shutdown.is_shutdown());
    }
}
