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

mod admission;
mod due_scanner;
mod engine;
mod gc;
mod ha;
pub(crate) mod index_migration;
mod materializer;
mod ready_outbox;
mod recall;
mod receipt;
mod receipt_reconciler;
mod rocksdb_index;
pub(crate) mod segmented_index;
mod shadow;
mod snapshot;

pub(crate) use admission::usage_summary_keys;
pub(crate) use admission::TimelineAdmissionController;
pub use admission::TimelineAdmissionOutcome;
pub(crate) use due_scanner::TimelineDueScanner;
pub(crate) use due_scanner::TimelineDueScannerError;
pub(crate) use engine::ExtendedTimelineEngine;
pub(crate) use gc::TimelineGcService;
pub(crate) use ha::TimelinePromotionGate;
pub(crate) use ha::TimelinePromotionObservation;
pub use ha::TimelinePromotionOutcome;
pub(crate) use index_migration::TimelineIndexMigrationManager;
pub(crate) use materializer::ShadowTimelineMaterializer;
pub(crate) use materializer::TimelineMaterializerError;
pub(crate) use ready_outbox::TimelineReadyOutbox;
pub(crate) use recall::RecallResult;
pub(crate) use recall::TimelineRecallService;
pub(crate) use receipt::TimelineCompletionReceiptV1;
pub(crate) use receipt::TimelineReceiptStore;
pub(crate) use receipt_reconciler::TimelineCompletionError;
pub(crate) use receipt_reconciler::TimelineCompletionReconciler;
pub(crate) use receipt_reconciler::TimelineCompletionWake;
#[cfg(test)]
pub(crate) use rocksdb_index::RocksDbTimerIndex;
pub(crate) use shadow::ShadowReconciler;
pub(crate) use snapshot::TimelineSnapshotManager;
