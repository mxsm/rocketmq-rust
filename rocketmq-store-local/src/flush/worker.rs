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

//! Canonical asynchronous flush and transient-buffer commit worker loops.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::flush::FlushProgress;

/// Frozen R0 configuration for the asynchronous CommitLog flush worker.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FlushRealTimeWorkerConfig {
    timed: bool,
    interval: Duration,
    least_pages: i32,
    thorough_interval_millis: u64,
}

impl FlushRealTimeWorkerConfig {
    /// Projects the existing Store configuration without changing defaults.
    #[doc(hidden)]
    pub const fn legacy(timed: bool, interval_millis: u64, least_pages: i32, thorough_interval_millis: u64) -> Self {
        Self {
            timed,
            interval: Duration::from_millis(interval_millis),
            least_pages,
            thorough_interval_millis,
        }
    }
}

/// Identifies which asynchronous flush attempt failed.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FlushWorkerFailurePhase {
    Periodic,
    Final,
}

/// Store-owned side-effect adapters for the Local asynchronous flush worker.
#[doc(hidden)]
pub struct FlushRealTimeWorkerPorts<Flush, Failure, Checkpoint, Delay> {
    flush: Flush,
    failure: Failure,
    checkpoint: Checkpoint,
    delay: Delay,
}

impl<Flush, Failure, Checkpoint, Delay> FlushRealTimeWorkerPorts<Flush, Failure, Checkpoint, Delay> {
    /// Creates the worker adapter bundle.
    #[doc(hidden)]
    pub fn new(flush: Flush, failure: Failure, checkpoint: Checkpoint, delay: Delay) -> Self {
        Self {
            flush,
            failure,
            checkpoint,
            delay,
        }
    }
}

/// Runs the canonical asynchronous flush interval, wakeup, thorough-flush, and final-flush loop.
#[doc(hidden)]
pub async fn run_flush_real_time_worker<E, Flush, FlushFuture, Failure, Checkpoint, Delay, DelayFuture>(
    notified: Arc<Notify>,
    shutdown_token: CancellationToken,
    config: FlushRealTimeWorkerConfig,
    mut ports: FlushRealTimeWorkerPorts<Flush, Failure, Checkpoint, Delay>,
) where
    Flush: FnMut(i32) -> FlushFuture,
    FlushFuture: Future<Output = Result<FlushProgress, Arc<E>>>,
    Failure: FnMut(FlushWorkerFailurePhase, Arc<E>),
    Checkpoint: FnMut(u64),
    Delay: FnMut(Duration) -> DelayFuture,
    DelayFuture: Future<Output = ()>,
{
    let mut last_flush_timestamp = 0;
    loop {
        if shutdown_token.is_cancelled() {
            break;
        }

        let mut flush_least_pages = config.least_pages;
        let now = current_millis();
        if now >= last_flush_timestamp + config.thorough_interval_millis {
            last_flush_timestamp = now;
            flush_least_pages = 0;
        }

        let delay = (ports.delay)(config.interval);
        if config.timed {
            tokio::select! {
                _ = shutdown_token.cancelled() => break,
                _ = delay => {}
            }
        } else {
            tokio::select! {
                _ = shutdown_token.cancelled() => break,
                _ = notified.notified() => {}
                _ = delay => {}
            }
        }

        match (ports.flush)(flush_least_pages).await {
            Ok(progress) => checkpoint_if_present(progress.store_timestamp, &mut ports.checkpoint),
            Err(error) => {
                (ports.failure)(FlushWorkerFailurePhase::Periodic, error);
                break;
            }
        }
    }

    match (ports.flush)(0).await {
        Ok(progress) => checkpoint_if_present(progress.store_timestamp, &mut ports.checkpoint),
        Err(error) => (ports.failure)(FlushWorkerFailurePhase::Final, error),
    }
}

/// Frozen R0 configuration for the transient-buffer commit worker.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommitRealTimeWorkerConfig {
    interval: Duration,
    least_pages: i32,
    thorough_interval_millis: u64,
}

impl CommitRealTimeWorkerConfig {
    /// Projects the existing Store configuration without changing defaults.
    #[doc(hidden)]
    pub const fn legacy(interval_millis: u64, least_pages: i32, thorough_interval_millis: u64) -> Self {
        Self {
            interval: Duration::from_millis(interval_millis),
            least_pages,
            thorough_interval_millis,
        }
    }
}

/// Canonical result of one transient-buffer commit worker attempt.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommitWorkerProgress {
    legacy_commit_result: bool,
    store_timestamp: u64,
}

impl CommitWorkerProgress {
    /// Captures the legacy queue result and its current store timestamp.
    #[doc(hidden)]
    pub const fn new(legacy_commit_result: bool, store_timestamp: u64) -> Self {
        Self {
            legacy_commit_result,
            store_timestamp,
        }
    }
}

/// Store-owned side-effect adapters for the Local transient-buffer commit worker.
#[doc(hidden)]
pub struct CommitRealTimeWorkerPorts<Commit, WakeFlush, Checkpoint, Delay> {
    commit: Commit,
    wake_flush: WakeFlush,
    checkpoint: Checkpoint,
    delay: Delay,
}

impl<Commit, WakeFlush, Checkpoint, Delay> CommitRealTimeWorkerPorts<Commit, WakeFlush, Checkpoint, Delay> {
    /// Creates the worker adapter bundle.
    #[doc(hidden)]
    pub fn new(commit: Commit, wake_flush: WakeFlush, checkpoint: Checkpoint, delay: Delay) -> Self {
        Self {
            commit,
            wake_flush,
            checkpoint,
            delay,
        }
    }
}

/// Runs the canonical transient-buffer commit, flush wakeup, interval, and final-commit loop.
#[doc(hidden)]
pub async fn run_commit_real_time_worker<Commit, CommitFuture, WakeFlush, Checkpoint, Delay, DelayFuture>(
    notified: Arc<Notify>,
    shutdown_token: CancellationToken,
    config: CommitRealTimeWorkerConfig,
    mut ports: CommitRealTimeWorkerPorts<Commit, WakeFlush, Checkpoint, Delay>,
) where
    Commit: FnMut(i32) -> CommitFuture,
    CommitFuture: Future<Output = Option<CommitWorkerProgress>>,
    WakeFlush: FnMut(),
    Checkpoint: FnMut(u64),
    Delay: FnMut(Duration) -> DelayFuture,
    DelayFuture: Future<Output = ()>,
{
    let mut last_commit_timestamp = 0;
    loop {
        if shutdown_token.is_cancelled() {
            break;
        }

        let mut commit_least_pages = config.least_pages;
        let now = current_millis();
        if now >= last_commit_timestamp + config.thorough_interval_millis {
            last_commit_timestamp = now;
            commit_least_pages = 0;
        }

        let Some(progress) = (ports.commit)(commit_least_pages).await else {
            break;
        };
        if !progress.legacy_commit_result {
            last_commit_timestamp = current_millis();
            (ports.wake_flush)();
        }

        let delay = (ports.delay)(config.interval);
        tokio::select! {
            _ = shutdown_token.cancelled() => break,
            _ = notified.notified() => {}
            _ = delay => {}
        }
    }

    if let Some(progress) = (ports.commit)(0).await {
        if !progress.legacy_commit_result {
            checkpoint_if_present(progress.store_timestamp, &mut ports.checkpoint);
        }
    }
}

fn checkpoint_if_present(timestamp: u64, checkpoint: &mut impl FnMut(u64)) {
    if timestamp > 0 {
        checkpoint(timestamp);
    }
}

fn current_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
