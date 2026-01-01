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

use std::cmp;
use std::future::Future;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::runtime::Handle;
use tokio::task::JoinHandle;

pub struct TokioExecutorService {
    inner: tokio::runtime::Runtime,
}

impl Default for TokioExecutorService {
    fn default() -> Self {
        Self::new()
    }
}

impl TokioExecutorService {
    pub fn shutdown(self) {
        self.inner.shutdown_background();
    }

    pub fn shutdown_timeout(self, timeout: Duration) {
        self.inner.shutdown_timeout(timeout);
    }
}

impl TokioExecutorService {
    pub fn new() -> TokioExecutorService {
        TokioExecutorService {
            inner: tokio::runtime::Builder::new_multi_thread()
                .worker_threads(num_cpus::get())
                .enable_all()
                .build()
                .unwrap(),
        }
    }

    pub fn new_with_config(
        thread_num: usize,
        thread_prefix: Option<impl Into<String>>,
        keep_alive: Duration,
        max_blocking_threads: usize,
    ) -> TokioExecutorService {
        let thread_prefix_inner = if let Some(thread_prefix) = thread_prefix {
            thread_prefix.into()
        } else {
            "rocketmq-thread-".to_string()
        };
        TokioExecutorService {
            inner: tokio::runtime::Builder::new_multi_thread()
                .worker_threads(thread_num)
                .thread_keep_alive(keep_alive)
                .max_blocking_threads(max_blocking_threads)
                .thread_name_fn(move || {
                    static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                    let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
                    format!("{thread_prefix_inner}{id}")
                })
                .enable_all()
                .build()
                .unwrap(),
        }
    }
}

impl TokioExecutorService {
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.inner.spawn(future)
    }

    pub fn get_handle(&self) -> &Handle {
        self.inner.handle()
    }

    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.inner.block_on(future)
    }
}

pub struct FuturesExecutorService {
    inner: futures::executor::ThreadPool,
}
impl FuturesExecutorService {
    pub fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.inner.spawn_ok(future);
    }
}

#[derive(Debug, Default)]
pub struct FuturesExecutorServiceBuilder {
    pool_size: usize,
    stack_size: usize,
    thread_name_prefix: Option<String>,
}

impl FuturesExecutorServiceBuilder {
    pub fn new() -> FuturesExecutorServiceBuilder {
        FuturesExecutorServiceBuilder {
            pool_size: cmp::max(1, num_cpus::get()),
            stack_size: 0,
            thread_name_prefix: None,
        }
    }

    pub fn pool_size(mut self, pool_size: usize) -> Self {
        self.pool_size = pool_size;
        self
    }

    pub fn stack_size(mut self, stack_size: usize) -> Self {
        self.stack_size = stack_size;
        self
    }

    pub fn create(&mut self) -> anyhow::Result<FuturesExecutorService> {
        let thread_pool = futures::executor::ThreadPool::builder()
            .stack_size(self.stack_size)
            .pool_size(self.pool_size)
            .name_prefix(
                self.thread_name_prefix
                    .as_ref()
                    .unwrap_or(&String::from("Default-Executor")),
            )
            .create()
            .unwrap();
        Ok(FuturesExecutorService { inner: thread_pool })
    }
}

pub struct ScheduledExecutorService {
    inner: tokio::runtime::Runtime,
}

impl Default for ScheduledExecutorService {
    fn default() -> Self {
        Self::new()
    }
}
impl ScheduledExecutorService {
    pub fn new() -> ScheduledExecutorService {
        ScheduledExecutorService {
            inner: tokio::runtime::Builder::new_multi_thread()
                .worker_threads(num_cpus::get())
                .enable_all()
                .build()
                .unwrap(),
        }
    }

    pub fn new_with_config(
        thread_num: usize,
        thread_prefix: Option<impl Into<String>>,
        keep_alive: Duration,
        max_blocking_threads: usize,
    ) -> ScheduledExecutorService {
        let thread_prefix_inner = if let Some(thread_prefix) = thread_prefix {
            thread_prefix.into()
        } else {
            "rocketmq-thread-".to_string()
        };
        ScheduledExecutorService {
            inner: tokio::runtime::Builder::new_multi_thread()
                .worker_threads(thread_num)
                .thread_keep_alive(keep_alive)
                .max_blocking_threads(max_blocking_threads)
                .thread_name_fn(move || {
                    static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                    let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
                    format!("{thread_prefix_inner}{id}")
                })
                .enable_all()
                .build()
                .unwrap(),
        }
    }

    pub fn schedule_at_fixed_rate<F>(&self, mut task: F, initial_delay: Option<Duration>, period: Duration)
    where
        F: FnMut() + Send + 'static,
    {
        self.inner.spawn(async move {
            // initial delay

            if let Some(initial_delay_inner) = initial_delay {
                tokio::time::sleep(initial_delay_inner).await;
            }

            loop {
                // record current execution time
                let current_execution_time = tokio::time::Instant::now();
                // execute task
                task();
                // Calculate the time of the next execution
                let next_execution_time = current_execution_time + period;

                // Wait until the next execution
                let delay = next_execution_time.saturating_duration_since(tokio::time::Instant::now());
                tokio::time::sleep(delay).await;
            }
        });
    }
}

impl ScheduledExecutorService {
    pub fn shutdown(self) {
        self.inner.shutdown_background();
    }

    pub fn shutdown_timeout(self, timeout: Duration) {
        self.inner.shutdown_timeout(timeout);
    }
}
