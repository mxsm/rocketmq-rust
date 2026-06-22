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

/// Legacy compatibility wrapper around a directly owned Tokio runtime.
///
/// New RocketMQ code should prefer [`RuntimeOwner`](crate::RuntimeOwner) when
/// it owns the runtime, or [`RuntimeContext`](crate::RuntimeContext) /
/// [`ServiceContext`](crate::ServiceContext) when it is borrowing an existing
/// Tokio runtime. `RocketMQRuntime` remains available for older synchronous
/// builder and scheduler APIs while those call sites are migrated behind
/// explicit compatibility adapters.
pub enum RocketMQRuntime {
    Multi(tokio::runtime::Runtime),
}

impl RocketMQRuntime {
    #[inline]
    pub fn new_multi(threads: usize, name: &str) -> Self {
        Self::Multi(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(threads)
                .thread_name(name)
                .enable_all()
                .build()
                .unwrap(),
        )
    }

    #[inline]
    pub fn get_handle(&self) -> &tokio::runtime::Handle {
        match self {
            Self::Multi(runtime) => runtime.handle(),
        }
    }

    #[inline]
    pub fn get_runtime(&self) -> &tokio::runtime::Runtime {
        match self {
            Self::Multi(runtime) => runtime,
        }
    }

    #[inline]
    pub fn shutdown(self) {
        match self {
            Self::Multi(runtime) => runtime.shutdown_background(),
        }
    }

    #[inline]
    pub fn shutdown_timeout(self, timeout: Duration) {
        match self {
            Self::Multi(runtime) => runtime.shutdown_timeout(timeout),
        }
    }

    #[inline]
    pub fn schedule_at_fixed_rate<F>(&self, task: F, initial_delay: Option<Duration>, period: Duration)
    where
        F: Fn() + Send + 'static,
    {
        match self {
            RocketMQRuntime::Multi(runtime) => {
                runtime.handle().spawn(async move {
                    if let Some(initial_delay_inner) = initial_delay {
                        tokio::time::sleep(initial_delay_inner).await;
                    }

                    loop {
                        let current_execution_time = tokio::time::Instant::now();
                        task();
                        let next_execution_time = current_execution_time + period;
                        let delay = next_execution_time.saturating_duration_since(tokio::time::Instant::now());
                        tokio::time::sleep(delay).await;
                    }
                });
            }
        }
    }

    #[inline]
    pub fn schedule_at_fixed_rate_mut<F>(&self, mut task: F, initial_delay: Option<Duration>, period: Duration)
    where
        F: FnMut() + Send + 'static,
    {
        match self {
            RocketMQRuntime::Multi(runtime) => {
                runtime.handle().spawn(async move {
                    if let Some(initial_delay_inner) = initial_delay {
                        tokio::time::sleep(initial_delay_inner).await;
                    }

                    loop {
                        let current_execution_time = tokio::time::Instant::now();
                        task();
                        let next_execution_time = current_execution_time + period;
                        let delay = next_execution_time.saturating_duration_since(tokio::time::Instant::now());
                        tokio::time::sleep(delay).await;
                    }
                });
            }
        }
    }
}
