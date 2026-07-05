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
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;

use rocketmq_error::RocketMQError;

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
    error: Option<RocketMQError>,
}

fn lock_state<T>(state: &Mutex<CompletableFutureState<T>>) -> MutexGuard<'_, CompletableFutureState<T>> {
    state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn complete_state<T>(state: &Mutex<CompletableFutureState<T>>, data: T) {
    let mut state = lock_state(state);
    state.data = Some(data);
    state.completed = State::Ready;
    if let Some(waker) = state.waker.take() {
        waker.wake();
    }
}

fn complete_exceptionally<T>(state: &Mutex<CompletableFutureState<T>>, error: RocketMQError) {
    let mut state = lock_state(state);
    state.completed = State::Ready;
    state.error = Some(error);
    if let Some(waker) = state.waker.take() {
        waker.wake();
    }
}

/// A CompletableFuture represents a future value that may be completed or pending.
pub struct CompletableFuture<T> {
    /// The shared state of the CompletableFuture.
    state: Arc<std::sync::Mutex<CompletableFutureState<T>>>,
    /// The sender part of a channel used for communication with the CompletableFuture.
    tx_rx: Sender<T>,
    /// Receiver polled by this future. Polling directly avoids per-future background tasks.
    rx: Receiver<T>,
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
        let (tx, rx) = mpsc::channel::<T>(1);

        Self {
            state: status,
            tx_rx: tx,
            rx,
        }
    }

    /// Returns the sender part of the channel used for communication.
    pub fn get_sender(&self) -> Sender<T> {
        self.tx_rx.clone()
    }

    // Rust code to complete a future task by updating the state and waking up the associated waker
    pub fn complete(&mut self, result: T) {
        complete_state(&self.state, result);
    }

    pub fn complete_exceptionally(&mut self, error: RocketMQError) {
        complete_exceptionally(&self.state, error);
    }
}

impl<T> Unpin for CompletableFuture<T> {}

impl<T> std::future::Future for CompletableFuture<T> {
    type Output = Option<T>;

    /// Polls the CompletableFuture to determine if the future value is ready.
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        {
            let mut shared_state = lock_state(&self.state);
            if shared_state.completed == State::Ready {
                // If the future value is ready, return it.
                return Poll::Ready(shared_state.data.take());
            }

            // Otherwise, set the waker for direct complete()/complete_exceptionally().
            shared_state.waker = Some(cx.waker().clone());
        }

        match Pin::new(&mut self.rx).poll_recv(cx) {
            Poll::Ready(Some(data)) => Poll::Ready(Some(data)),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::runtime::Runtime;

    use super::*;

    fn poison_state<T>(state: Arc<Mutex<CompletableFutureState<T>>>) {
        let state_for_panic = state.clone();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _guard = state_for_panic.lock().unwrap();
            panic!("poison future state");
        }));

        assert!(result.is_err());
        assert!(state.lock().is_err());
    }

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

    #[test]
    fn completable_future_recovers_from_poisoned_state_lock() {
        Runtime::new().unwrap().block_on(async move {
            let cf = CompletableFuture::new();
            let state = cf.state.clone();
            let sender = cf.get_sender();

            poison_state(state);
            sender.send(42).await.expect("Failed to send data");

            assert_eq!(cf.await, Some(42));
        });
    }

    #[test]
    fn completable_future_new_without_tokio_runtime_does_not_panic() {
        let cf = CompletableFuture::new();
        let sender = cf.get_sender();

        sender
            .blocking_send(42)
            .expect("completable future receiver should accept data");

        assert_eq!(futures::executor::block_on(cf), Some(42));
    }

    #[test]
    fn completable_future_direct_complete_wakes_without_channel_send() {
        let mut cf = CompletableFuture::new();
        cf.complete(42);

        assert_eq!(futures::executor::block_on(cf), Some(42));
    }

    #[test]
    fn completable_future_exceptional_completion_accepts_typed_error() {
        let mut cf: CompletableFuture<i32> = CompletableFuture::new();
        cf.complete_exceptionally(RocketMQError::response_process_failed(
            "completable_future",
            "test failure",
        ));

        assert_eq!(futures::executor::block_on(cf), None);
    }
}
