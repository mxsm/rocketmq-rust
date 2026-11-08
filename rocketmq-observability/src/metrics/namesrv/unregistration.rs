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

/// Live pending age sampled during metrics collection, including in-flight work.
///
/// Implementations must only read application state, without recording metrics
/// or creating instruments. The collector can call this from another thread.
pub trait UnregistrationAgeSource: Send + Sync {
    fn oldest_age(&self) -> Duration;
}

#[cfg(feature = "otel-metrics")]
mod enabled {
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::sync::Weak;

    use parking_lot::Mutex;

    use super::*;
    use crate::metrics::namesrv::duration_millis_u64;
    use crate::metrics::namesrv::NAMESRV_UNREGISTRATION_OLDEST_AGE;

    #[derive(Default)]
    pub(crate) struct UnregistrationAge {
        recorded: AtomicU64,
        sources: Mutex<Vec<Weak<dyn UnregistrationAgeSource>>>,
    }

    impl UnregistrationAge {
        pub(crate) fn new(meter: &opentelemetry::metrics::Meter) -> Arc<Self> {
            let age = Arc::new(Self::default());
            let observed = Arc::clone(&age);
            meter
                .u64_observable_gauge(NAMESRV_UNREGISTRATION_OLDEST_AGE)
                .with_description("Age of the oldest NameServer pending broker unregistration")
                .with_unit("ms")
                .with_callback(move |observer| observer.observe(observed.sample(), &[]))
                .build();
            age
        }

        pub(crate) fn register<S: UnregistrationAgeSource + 'static>(&self, source: &Arc<S>) {
            let source: Arc<dyn UnregistrationAgeSource> = source.clone();
            let mut sources = self.sources.lock();
            sources.retain(|source| source.strong_count() != 0);
            sources.push(Arc::downgrade(&source));
            self.recorded.store(0, Ordering::Relaxed);
        }

        pub(crate) fn record(&self, age: Duration) {
            self.recorded.store(duration_millis_u64(age), Ordering::Relaxed);
        }

        fn sample(&self) -> u64 {
            let sources = {
                let mut sources = self.sources.lock();
                sources.retain(|source| source.strong_count() != 0);
                sources.clone()
            };
            // Release the registry before reading queue state. Sources are weak:
            // neither the callback nor the provider owns a queue or its worker.
            sources
                .iter()
                .filter_map(Weak::upgrade)
                .map(|source| duration_millis_u64(source.oldest_age()))
                .max()
                .unwrap_or_else(|| self.recorded.load(Ordering::Relaxed))
        }
    }
}

#[cfg(feature = "otel-metrics")]
pub(crate) use enabled::UnregistrationAge;

#[cfg(all(test, feature = "otel-metrics"))]
mod tests;
