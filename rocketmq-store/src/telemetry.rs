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

//! Store-owned telemetry capabilities constructed at the Broker composition root.

use rocketmq_observability::metrics::rocksdb::RocksDbMetricsRecorder;
use rocketmq_observability::metrics::store::StoreMetricsRecorder;
use rocketmq_observability::metrics::tiered_store::TieredStoreMetricsRecorder;
use rocketmq_observability::metrics::timer::TimerMetricsRecorder;
use rocketmq_observability::TelemetryHandle;

/// Cloneable typed recorders shared by one message-store instance.
#[derive(Clone)]
pub struct StoreTelemetry {
    handle: TelemetryHandle,
    store: StoreMetricsRecorder,
    timer: TimerMetricsRecorder,
    rocksdb: RocksDbMetricsRecorder,
    tiered_store: TieredStoreMetricsRecorder,
}

impl Default for StoreTelemetry {
    fn default() -> Self {
        Self::noop()
    }
}

impl StoreTelemetry {
    /// Creates a Store telemetry bundle from one explicit runtime handle.
    #[must_use]
    pub fn from_handle(handle: &TelemetryHandle) -> Self {
        Self {
            handle: handle.clone(),
            store: StoreMetricsRecorder::from_handle(handle),
            timer: TimerMetricsRecorder::from_handle(handle),
            rocksdb: RocksDbMetricsRecorder::from_handle(handle),
            tiered_store: TieredStoreMetricsRecorder::from_handle(handle),
        }
    }

    /// Creates a bundle whose recorders are all no-ops.
    #[must_use]
    pub fn noop() -> Self {
        Self::from_handle(&TelemetryHandle::noop())
    }

    #[must_use]
    pub fn handle(&self) -> &TelemetryHandle {
        &self.handle
    }

    #[must_use]
    pub fn store(&self) -> &StoreMetricsRecorder {
        &self.store
    }

    #[must_use]
    pub fn timer(&self) -> &TimerMetricsRecorder {
        &self.timer
    }

    #[must_use]
    pub fn rocksdb(&self) -> &RocksDbMetricsRecorder {
        &self.rocksdb
    }

    #[must_use]
    pub fn tiered_store(&self) -> &TieredStoreMetricsRecorder {
        &self.tiered_store
    }
}
