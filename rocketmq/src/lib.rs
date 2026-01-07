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

#![feature(sync_unsafe_cell)]
#![feature(async_fn_traits)]
#![feature(unboxed_closures)]
#![allow(dead_code)]

mod arc_mut;
mod blocking_queue;
pub mod count_down_latch;
pub mod rocketmq_tokio_lock;

mod shutdown;
pub mod task;

pub mod schedule;

pub use arc_mut::ArcMut;
pub use arc_mut::SyncUnsafeCellWrapper;
pub use arc_mut::WeakArcMut;
pub use blocking_queue::BlockingQueue as RocketMQBlockingQueue;
pub use count_down_latch::CountDownLatch;
/// Re-export rocketmq main.
pub use rocketmq::main;
pub use rocketmq_tokio_lock::RocketMQTokioMutex;
pub use rocketmq_tokio_lock::RocketMQTokioRwLock;
pub use schedule::executor::ExecutorConfig;
pub use schedule::executor::ExecutorPool;
pub use schedule::executor::TaskExecutor;
pub use schedule::scheduler::SchedulerConfig;
pub use schedule::scheduler::TaskScheduler;
pub use schedule::task::Task;
pub use schedule::task::TaskContext;
pub use schedule::task::TaskResult;
pub use schedule::task::TaskStatus;
pub use schedule::trigger::CronTrigger;
pub use schedule::trigger::DelayTrigger;
pub use schedule::trigger::DelayedIntervalTrigger;
pub use schedule::trigger::IntervalTrigger;
pub use schedule::trigger::Trigger;
pub use shutdown::Shutdown;
/// Re-export tokio module.
pub use tokio as rocketmq;

/// On unix platforms we want to intercept SIGINT and SIGTERM
/// This method returns if either are signalled
#[cfg(unix)]
pub async fn wait_for_signal() {
    use tokio::signal::unix::signal;
    use tokio::signal::unix::SignalKind;
    use tracing::info;
    let mut term = signal(SignalKind::terminate()).expect("failed to register signal handler");
    let mut int = signal(SignalKind::interrupt()).expect("failed to register signal handler");

    tokio::select! {
        _ = term.recv() => info!("Received SIGTERM"),
        _ = int.recv() => info!("Received SIGINT"),
    }
}

#[cfg(windows)]
/// ctrl_c is the cross-platform way to intercept the equivalent of SIGINT
/// This method returns if this occurs
pub async fn wait_for_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
