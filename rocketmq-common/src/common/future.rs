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

use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;

/// Enumeration representing the state of a CompletableFuture.
#[derive(Copy, Clone, PartialEq)]
enum State {
    /// The default state, indicating that the future value is pending.
    Pending,
    /// Indicates that the future value is ready.
    Ready,
}

/// The internal state of a CompletableFuture.
struct CompletableFutureState<T> {
    /// The current completion status.
    completed: State,
    /// An optional waker to be notified upon completion.
    waker: Option<Waker>,
    /// The data value contained within the CompletableFuture upon completion.
    data: Option<T>,

    /// An optional error value contained within the CompletableFuture upon completion.
    error: Option<Box<dyn std::error::Error + Send + Sync>>,
}

/// A CompletableFuture represents a future value that may be completed or pending.
pub struct CompletableFuture<T> {
    /// The shared state of the CompletableFuture.
    state: Arc<std::sync::Mutex<CompletableFutureState<T>>>,
    /// The sender part of a channel used for communication with the CompletableFuture.
    tx_rx: Sender<T>,
}

impl<T: Send + 'static> Default for CompletableFuture<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Send + 'static> CompletableFuture<T> {
    /// Constructs a new CompletableFuture.
    pub fn new() -> Self {
        // Create the shared state.
        let status = Arc::new(std::sync::Mutex::new(CompletableFutureState {
            completed: State::Pending,
            waker: None,
            data: None,
            error: None,
        }));
        let arc = status.clone();

        // Spawn a Tokio task to handle completion.
        let (tx, mut rx) = mpsc::channel::<T>(1);
        tokio::spawn(async move {
            if let Some(data) = rx.recv().await {
                let mut state = arc.lock().unwrap();
                state.data = Some(data);
                state.completed = State::Ready;
                if let Some(waker) = state.waker.take() {
                    waker.wake();
                }
                rx.close();
            }
        });

        Self {
            state: status,
            tx_rx: tx,
        }
    }

    /// Returns the sender part of the channel used for communication.
    pub fn get_sender(&self) -> Sender<T> {
        self.tx_rx.clone()
    }

    // Rust code to complete a future task by updating the state and waking up the associated waker
    pub fn complete(&mut self, result: T) {
        let mut state = self.state.lock().unwrap();
        state.completed = State::Ready;
        state.data = Some(result);
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
    }

    pub fn complete_exceptionally(&mut self, error: Box<dyn std::error::Error + Send + Sync>) {
        let mut state = self.state.lock().unwrap();
        state.completed = State::Ready;
        state.error = Some(error);
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
    }
}

impl<T> std::future::Future for CompletableFuture<T> {
    type Output = Option<T>;

    /// Polls the CompletableFuture to determine if the future value is ready.
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut shared_state = self.state.lock().unwrap();
        if shared_state.completed == State::Ready {
            // If the future value is ready, return it.
            Poll::Ready(shared_state.data.take())
        } else {
            // Otherwise, set the waker to be notified upon completion and return pending.
            shared_state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::runtime::Runtime;

    use super::CompletableFuture;

    #[test]
    fn test_completable_future() {
        Runtime::new().unwrap().block_on(async move {
            let cf = CompletableFuture::new();
            let sender = cf.get_sender();

            // Send data to the CompletableFuture
            sender.send(42).await.expect("Failed to send data");

            // Wait for the CompletableFuture to complete
            let result = cf.await;

            // Ensure that the result is Some(42)
            assert_eq!(result, Some(42));
        });
    }
}
