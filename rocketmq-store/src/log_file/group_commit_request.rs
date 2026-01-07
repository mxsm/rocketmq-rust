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

use std::time::Duration;
use std::time::Instant;

use tokio::sync::oneshot;
use tracing::warn;

use crate::base::message_status_enum::PutMessageStatus;

pub struct GroupCommitResponse {
    flush_ok_receiver: Option<oneshot::Receiver<PutMessageStatus>>,
    deadline: Instant,
}

impl GroupCommitResponse {
    /// Get the deadline for this request
    pub fn get_deadline(&self) -> Instant {
        self.deadline
    }

    /// Check if the request has expired
    pub fn is_expired(&self) -> bool {
        Instant::now() > self.deadline
    }

    /// Get a future that resolves when the flush operation completes
    pub async fn wait_for_result(mut self) -> Result<PutMessageStatus, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(receiver) = self.flush_ok_receiver.take() {
            match receiver.await {
                Ok(status) => Ok(status),
                Err(_) => Err("Sender was dropped before sending result".into()),
            }
        } else {
            Err("Receiver was already consumed".into())
        }
    }

    /// Get a future that resolves when the flush operation completes with timeout
    pub async fn wait_for_result_with_timeout(
        &mut self,
    ) -> Result<PutMessageStatus, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(receiver) = self.flush_ok_receiver.take() {
            let timeout_duration = if self.deadline > Instant::now() {
                self.deadline - Instant::now()
            } else {
                Duration::from_millis(0)
            };

            match tokio::time::timeout(timeout_duration, receiver).await {
                Ok(Ok(status)) => Ok(status),
                Ok(Err(_)) => Err("Sender was dropped before sending result".into()),
                Err(_) => Ok(PutMessageStatus::FlushDiskTimeout),
            }
        } else {
            Err("Receiver was already consumed".into())
        }
    }
}

pub struct GroupCommitRequest {
    next_offset: i64,
    flush_ok_sender: Option<oneshot::Sender<PutMessageStatus>>,
    ack_nums: i32,
    deadline: Instant,
}

impl GroupCommitRequest {
    /// Get the deadline for this request
    pub fn get_deadline(&self) -> Instant {
        self.deadline
    }

    /// Check if the request has expired
    pub fn is_expired(&self) -> bool {
        Instant::now() > self.deadline
    }

    /// Create a new GroupCommitRequest with timeout in milliseconds
    pub fn new(next_offset: i64, timeout_millis: u64) -> (Self, GroupCommitResponse) {
        Self::create_request(next_offset, timeout_millis, 1)
    }

    /// Create a new GroupCommitRequest with timeout and ack numbers
    pub fn with_ack_nums(next_offset: i64, timeout_millis: u64, ack_nums: i32) -> (Self, GroupCommitResponse) {
        Self::create_request(next_offset, timeout_millis, ack_nums)
    }

    #[inline]
    fn create_request(next_offset: i64, timeout_millis: u64, ack_nums: i32) -> (Self, GroupCommitResponse) {
        let (sender, receiver) = oneshot::channel();
        let instant = Instant::now() + Duration::from_millis(timeout_millis);
        (
            Self {
                next_offset,
                flush_ok_sender: Some(sender),
                ack_nums,
                deadline: instant,
            },
            GroupCommitResponse {
                flush_ok_receiver: Some(receiver),
                deadline: instant,
            },
        )
    }

    /// Get the next offset
    pub fn get_next_offset(&self) -> i64 {
        self.next_offset
    }

    /// Get the number of acknowledgments needed
    pub fn get_ack_nums(&self) -> i32 {
        self.ack_nums
    }

    /// Wake up the customer/caller with the result
    pub fn wakeup_customer(&mut self, status: PutMessageStatus) {
        if let Some(sender) = self.flush_ok_sender.take() {
            if sender.send(status).is_err() {
                warn!("Failed to send flush result - receiver may have been dropped");
            }
        } else {
            warn!("Attempted to wakeup customer but sender was already consumed");
        }
    }
}

impl std::fmt::Debug for GroupCommitRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupCommitRequest")
            .field("next_offset", &self.next_offset)
            .field("ack_nums", &self.ack_nums)
            .field("has_sender", &self.flush_ok_sender.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::Duration;

    use super::*;

    #[tokio::test]
    async fn test_group_commit_request_creation() {
        let request = GroupCommitRequest::new(12345, 5000);

        assert_eq!(request.0.get_next_offset(), 12345);
        assert_eq!(request.0.get_ack_nums(), 1);
        assert!(!request.1.is_expired());
    }

    #[tokio::test]
    async fn test_group_commit_request_with_ack_nums() {
        let request = GroupCommitRequest::with_ack_nums(67890, 3000, 3);

        assert_eq!(request.0.get_next_offset(), 67890);
        assert_eq!(request.0.get_ack_nums(), 3);
    }

    #[tokio::test]
    async fn test_wakeup_customer() {
        let mut request = GroupCommitRequest::new(12345, 5000);

        // Wakeup with success status
        request.0.wakeup_customer(PutMessageStatus::PutOk);
    }

    #[tokio::test]
    async fn test_timeout() {
        let mut request = GroupCommitRequest::new(12345, 100); // 100ms timeout

        let start = Instant::now();
        let result = request.1.wait_for_result_with_timeout().await;
        let elapsed = start.elapsed();

        assert!(elapsed >= Duration::from_millis(90)); // Allow some tolerance
        assert!(matches!(result, Ok(PutMessageStatus::FlushDiskTimeout)));
    }
}
