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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;
use rocketmq_store_api::AppendReceipt;
use rocketmq_store_api::AppendStatus;
use rocketmq_store_api::DerivedProgress;
use rocketmq_store_api::Durability;
use rocketmq_store_api::LeasedBytes;
use rocketmq_store_api::StoreHealthSnapshot;

#[test]
fn receipt_keeps_appended_range_and_durable_progress_independent() {
    let receipt = AppendReceipt::new(AppendStatus::FlushDiskTimeout, 40..60, 80, 48, Durability::Memory);

    assert_eq!(Some(40..60), receipt.appended_range());
    assert_eq!(Some(40), receipt.first_appended_offset());
    assert_eq!(Some(59), receipt.last_appended_offset());
    assert_eq!(80, receipt.appended_watermark());
    assert_eq!(48, receipt.durable_watermark());
    assert_eq!(Durability::Memory, receipt.durability());
    assert!(receipt.is_accepted());
    assert!(!receipt.is_durable());
}

#[test]
fn rejected_receipt_has_no_synthetic_appended_range() {
    let receipt = AppendReceipt::rejected(AppendStatus::ServiceUnavailable, 80, 48);

    assert_eq!(None, receipt.appended_range());
    assert_eq!(None, receipt.first_appended_offset());
    assert_eq!(None, receipt.last_appended_offset());
    assert!(!receipt.is_accepted());
    assert!(!receipt.is_durable());
}

#[test]
fn derived_progress_cannot_ack_or_satisfy_primary_durability() {
    let progress = DerivedProgress::new(64, 48);

    assert_eq!(64, progress.source_watermark());
    assert_eq!(48, progress.derived_watermark());
    assert!(!progress.acknowledges_primary_append());
    assert!(!progress.satisfies_primary_durability());
}

#[derive(Clone)]
struct ReleaseProbe(Arc<AtomicUsize>);

impl Drop for ReleaseProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn leased_bytes_releases_guard_only_when_the_result_is_dropped() {
    let releases = Arc::new(AtomicUsize::new(0));
    let leased = LeasedBytes::new(Bytes::from_static(b"message"), ReleaseProbe(releases.clone()));

    assert_eq!(b"message", leased.bytes().as_ref());
    assert_eq!(0, releases.load(Ordering::SeqCst));
    drop(leased);
    assert_eq!(1, releases.load(Ordering::SeqCst));
}

#[test]
fn neutral_health_defaults_to_writable_without_progress_or_pressure() {
    let health = StoreHealthSnapshot::default();

    assert!(health.writable());
    assert_eq!(None, health.last_error());
    assert_eq!(0, health.appended_watermark());
    assert_eq!(0, health.durable_watermark());
}
