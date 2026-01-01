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

use tokio::sync::oneshot;
use tokio::sync::Mutex;

use crate::ha::ha_connection_state::HAConnectionState;

/// Request to be notified when a connection reaches a specific state
///
/// This struct is used to register interest in a connection state change and
/// receive notification when the expected state is reached.
pub struct HAConnectionStateNotificationRequest {
    /// Channel for receiving the notification result
    notification_sender: Mutex<Option<oneshot::Sender<bool>>>,

    /// The connection state we're waiting for
    expect_state: HAConnectionState,

    /// Remote address of the connection we're interested in
    remote_addr: String,

    /// Whether to notify when the connection is shut down, even if it doesn't reach the expected
    /// state
    notify_when_shutdown: bool,
}

impl HAConnectionStateNotificationRequest {
    /// Create a new notification request
    ///
    /// # Parameters
    /// * `expect_state` - The connection state we're waiting for
    /// * `remote_addr` - Remote address of the connection we're interested in
    /// * `notify_when_shutdown` - Whether to notify when the connection is shut down
    pub fn new(
        expect_state: HAConnectionState,
        remote_addr: &str,
        notify_when_shutdown: bool,
    ) -> (Self, oneshot::Receiver<bool>) {
        let (sender, receiver) = oneshot::channel();

        (
            Self {
                notification_sender: Mutex::new(Some(sender)),
                expect_state,
                remote_addr: remote_addr.to_string(),
                notify_when_shutdown,
            },
            receiver,
        )
    }

    /// Get the expected state
    pub fn expect_state(&self) -> HAConnectionState {
        self.expect_state
    }

    /// Get the remote address
    pub fn remote_addr(&self) -> &str {
        &self.remote_addr
    }

    /// Check if notification should be sent when connection is shut down
    pub fn notify_when_shutdown(&self) -> bool {
        self.notify_when_shutdown
    }

    /// Complete the request with the given result
    ///
    /// This will send the notification to the waiting receiver.
    ///
    /// # Returns
    /// `true` if the notification was successfully sent, `false` if the receiver was dropped
    pub async fn complete(&self, result: bool) -> bool {
        let mut sender_guard = self.notification_sender.lock().await;
        if let Some(sender) = sender_guard.take() {
            sender.send(result).is_ok()
        } else {
            false // Sender was already consumed
        }
    }

    /// Check if the request has already been completed
    ///
    ///
    /// # Returns
    /// `true` if the request has been completed, `false` otherwise
    ///
    /// # Notes
    /// This method is not async and does not require locking
    pub fn is_completed(&self) -> bool {
        self.notification_sender.try_lock().is_ok_and(|guard| guard.is_none())
    }
}

// Example usage:
#[cfg(test)]
mod tests {
    use super::*;
    use crate::ha::ha_connection_state::HAConnectionState;

    #[tokio::test]
    async fn test_notification_request() {
        // Create a notification request
        let (request, receiver) =
            HAConnectionStateNotificationRequest::new(HAConnectionState::Transfer, "127.0.0.1:9876", true);

        // Check getters
        assert_eq!(request.expect_state(), HAConnectionState::Transfer);
        assert_eq!(request.remote_addr(), "127.0.0.1:9876");
        assert!(request.notify_when_shutdown());

        // Complete the request
        assert!(request.complete(true).await);

        // Verify the result was received
        assert!(receiver.await.unwrap());

        // Completing again should fail
        assert!(!request.complete(false).await);
    }
}
