/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#![feature(sync_unsafe_cell)]

mod arc_mut;
mod blocking_queue;
pub mod count_down_latch;
pub mod rocketmq_tokio_lock;
mod shutdown;

pub use arc_mut::ArcMut;
pub use arc_mut::SyncUnsafeCellWrapper;
pub use arc_mut::WeakArcMut;
pub use blocking_queue::BlockingQueue as RocketMQBlockingQueue;
pub use count_down_latch::CountDownLatch;
/// Re-export rocketmq main.
pub use rocketmq::main;
pub use rocketmq_tokio_lock::RocketMQTokioMutex;
pub use rocketmq_tokio_lock::RocketMQTokioRwLock;
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
