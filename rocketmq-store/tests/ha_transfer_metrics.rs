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

use rocketmq_store::ha::transfer_engine::TransferEngineKind;
use rocketmq_store::ha::transfer_engine::TransferStats;
use rocketmq_store::ha::transfer_metrics::HaTransferMetrics;
use rocketmq_store::ha::transfer_metrics::TransferFallbackSnapshot;

#[test]
fn ha_transfer_metrics_records_transfer_stats_by_engine() {
    let metrics = HaTransferMetrics::default();
    metrics.record_transfer(&TransferStats {
        engine: TransferEngineKind::Vectored,
        bytes_written: 128,
        body_bytes: 116,
        frame_count: 1,
        write_call_count: 2,
        sendfile_call_count: 0,
        sendfile_bytes: 0,
        fallback_bytes: 0,
        partial_write_count: 1,
    });
    metrics.record_transfer(&TransferStats {
        engine: TransferEngineKind::Sendfile,
        bytes_written: 256,
        body_bytes: 244,
        frame_count: 1,
        write_call_count: 1,
        sendfile_call_count: 3,
        sendfile_bytes: 244,
        fallback_bytes: 0,
        partial_write_count: 2,
    });

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.batch_total, 2);
    assert_eq!(snapshot.bytes_total, 384);
    assert_eq!(snapshot.body_bytes_total, 360);
    assert_eq!(snapshot.write_call_total, 3);
    assert_eq!(snapshot.sendfile_call_total, 3);
    assert_eq!(snapshot.sendfile_bytes_total, 244);
    assert_eq!(snapshot.partial_write_total, 3);
    assert_eq!(snapshot.engine_totals.vectored, 1);
    assert_eq!(snapshot.engine_totals.sendfile, 1);
}

#[test]
fn ha_transfer_metrics_records_fallback_labels() {
    let metrics = HaTransferMetrics::default();

    metrics.record_fallback(
        TransferEngineKind::IoUring,
        TransferEngineKind::Vectored,
        "io_uring unavailable",
    );
    metrics.record_fallback(
        TransferEngineKind::IoUring,
        TransferEngineKind::Vectored,
        "io_uring unavailable",
    );

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.fallback_total, 2);
    assert_eq!(
        snapshot.fallbacks,
        vec![TransferFallbackSnapshot {
            from: TransferEngineKind::IoUring,
            to: TransferEngineKind::Vectored,
            reason: "io_uring unavailable",
            count: 2,
        }]
    );
}
