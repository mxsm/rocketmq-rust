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

use std::sync::Arc;

use parking_lot::Mutex;
use rocketmq_runtime::{MetadataIoObserver, RuntimeDiagnosticsInputs, ScheduledTaskObserver, ShutdownReport};

use super::RuntimeDiagnosticsDataProvider;

#[derive(Default)]
struct Sources {
    schedules: Option<ScheduledTaskObserver>,
    metadata: Option<MetadataIoObserver>,
    shutdown: Option<ShutdownReport>,
}

/// Caller-owned, read-only component sources and a retained shutdown summary.
///
/// Each source slot replaces its previous value. Schedule observers select fixed
/// component jobs, and the metadata registry bounds its resource population.
/// Neither observer keeps execution or write authority alive. Keep a clone at
/// the composition root to read the final summary after the endpoint has stopped.
#[derive(Clone, Default)]
pub struct RuntimeDiagnosticsSources {
    inner: Arc<Mutex<Sources>>,
}

impl RuntimeDiagnosticsSources {
    pub fn set_schedules(&self, observer: ScheduledTaskObserver) {
        self.inner.lock().schedules = Some(observer);
    }

    pub fn set_metadata(&self, observer: MetadataIoObserver) {
        self.inner.lock().metadata = Some(observer);
    }

    /// Retains only local numeric fields; task names, children and details are discarded.
    ///
    /// V2 continues to report the supplied report's local counters. This method
    /// does not flatten a subtree or reinterpret wrapper completion as success.
    pub fn retain_shutdown(&self, report: &ShutdownReport) {
        let mut summary = ShutdownReport::new("", report.elapsed);
        summary.completed = report.completed;
        summary.cancelled = report.cancelled;
        summary.aborted = report.aborted;
        summary.failed = report.failed;
        summary.panicked = report.panicked;
        summary.timed_out = report.timed_out;
        summary.leaked = report.leaked;
        summary.blocking_still_running = report.blocking_still_running;
        self.inner.lock().shutdown = Some(summary);
    }
}

impl RuntimeDiagnosticsDataProvider for RuntimeDiagnosticsSources {
    fn snapshot(&self) -> RuntimeDiagnosticsInputs {
        let (schedules, metadata, shutdown) = {
            let sources = self.inner.lock();
            (
                sources.schedules.clone(),
                sources.metadata.clone(),
                sources.shutdown.clone(),
            )
        };
        RuntimeDiagnosticsInputs {
            schedule: schedules.map_or_else(Vec::new, |observer| observer.snapshot()),
            metadata: metadata.and_then(|observer| observer.snapshot()),
            shutdown,
        }
    }
}
