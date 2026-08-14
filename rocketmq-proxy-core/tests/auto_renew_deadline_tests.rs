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

use std::time::Duration;

use rocketmq_proxy_core::ClientSessionRegistry;
use rocketmq_proxy_core::ReceiptHandleRegistration;
use rocketmq_proxy_core::ResourceIdentity;
use rocketmq_proxy_core::SettingsBackoffPolicy;

fn registration(invisible_duration: Duration) -> ReceiptHandleRegistration {
    ReceiptHandleRegistration {
        client_id: "client-a".to_owned(),
        group: ResourceIdentity::new("", "GroupA"),
        topic: ResourceIdentity::new("", "TopicA"),
        message_id: "msg-1".to_owned(),
        receipt_handle: "handle-1".to_owned(),
        invisible_duration,
        delivery_attempt: 1,
        retry_backoff: SettingsBackoffPolicy::default(),
    }
}

#[tokio::test(start_paused = true)]
async fn default_invisible_window_is_claimed_at_half_time() {
    let registry = ClientSessionRegistry::<()>::default();
    registry.track_receipt_handle(registration(Duration::from_secs(15)));

    tokio::time::advance(Duration::from_millis(7_499)).await;
    assert!(registry
        .claim_due_receipt_handles(32, Duration::from_secs(5))
        .is_empty());

    tokio::time::advance(Duration::from_millis(1)).await;
    let claims = registry.claim_due_receipt_handles(32, Duration::from_secs(5));
    assert_eq!(claims.len(), 1);
    assert!(claims[0].due_lag() <= Duration::from_millis(1));
    assert!(claims[0].remaining_visibility() >= Duration::from_millis(7_499));
    assert!(registry.complete_receipt_handle_renewal(&claims[0], "handle-2", Duration::from_secs(15)));

    let tracked = registry
        .tracked_receipt_handle(
            "client-a",
            &ResourceIdentity::new("", "GroupA"),
            &ResourceIdentity::new("", "TopicA"),
            "msg-1",
        )
        .expect("renewed receipt");
    assert_eq!(tracked.receipt_handle, "handle-2");
}

#[tokio::test(start_paused = true)]
async fn acknowledged_receipt_never_becomes_due() {
    let registry = ClientSessionRegistry::<()>::default();
    registry.track_receipt_handle(registration(Duration::from_secs(15)));
    let removed = registry.remove_receipt_handle_matching(
        "client-a",
        &ResourceIdentity::new("", "GroupA"),
        &ResourceIdentity::new("", "TopicA"),
        "msg-1",
        "handle-1",
    );
    assert!(removed.is_some());

    tokio::time::advance(Duration::from_secs(15)).await;
    assert!(registry
        .claim_due_receipt_handles(32, Duration::from_secs(5))
        .is_empty());
    assert_eq!(registry.receipt_renewal_metrics_snapshot().live, 0);
}

#[tokio::test(start_paused = true)]
async fn transient_failure_retries_before_expiry() {
    let registry = ClientSessionRegistry::<()>::default();
    registry.track_receipt_handle(registration(Duration::from_secs(4)));
    tokio::time::advance(Duration::from_secs(2)).await;
    let first = registry.claim_due_receipt_handles(1, Duration::from_secs(1));
    assert_eq!(first.len(), 1);
    assert!(registry.retry_receipt_handle_renewal(&first[0], Duration::from_millis(500)));

    tokio::time::advance(Duration::from_millis(499)).await;
    assert!(registry.claim_due_receipt_handles(1, Duration::from_secs(1)).is_empty());
    tokio::time::advance(Duration::from_millis(1)).await;
    let retry = registry.claim_due_receipt_handles(1, Duration::from_secs(1));
    assert_eq!(retry.len(), 1);
    assert_eq!(retry[0].tracked.receipt_handle, "handle-1");
    assert_eq!(registry.receipt_renewal_metrics_snapshot().retries, 1);
}

#[tokio::test(start_paused = true)]
async fn replacement_invalidates_an_already_claimed_generation() {
    let registry = ClientSessionRegistry::<()>::default();
    registry.track_receipt_handle(registration(Duration::from_secs(4)));
    tokio::time::advance(Duration::from_secs(2)).await;
    let claim = registry.claim_due_receipt_handles(1, Duration::from_secs(1)).remove(0);

    let updated = registry.update_receipt_handle_matching(
        "client-a",
        &ResourceIdentity::new("", "GroupA"),
        &ResourceIdentity::new("", "TopicA"),
        "msg-1",
        "handle-1",
        "handle-2",
        Duration::from_secs(15),
    );
    assert!(updated.is_some());
    assert!(!registry.receipt_renewal_claim_is_current(&claim));
    assert!(!registry.complete_receipt_handle_renewal(&claim, "stale", Duration::from_secs(4)));
}
