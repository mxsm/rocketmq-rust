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

use serde::Serialize;

use crate::error::RuntimeError;
use crate::error::RuntimeResult;

#[derive(Debug, Clone)]
pub struct BlockingPoolPolicy {
    pub name: String,
    pub max_concurrency: usize,
    pub max_queue_depth: usize,
    pub queue_timeout: Duration,
    pub task_timeout: Duration,
    pub warn_after: Duration,
}

impl BlockingPoolPolicy {
    pub fn validate(&self) -> RuntimeResult<()> {
        if self.max_concurrency == 0 {
            return Err(RuntimeError::InvalidConfig(
                "blocking max_concurrency must be greater than zero".to_string(),
            ));
        }
        if self.max_queue_depth == 0 {
            return Err(RuntimeError::InvalidConfig(
                "blocking max_queue_depth must be greater than zero".to_string(),
            ));
        }
        let Some(total_timeout) = self.queue_timeout.checked_add(self.task_timeout) else {
            return Err(RuntimeError::InvalidConfig(
                "blocking queue_timeout plus task_timeout exceeds the supported duration".to_string(),
            ));
        };
        if std::time::Instant::now().checked_add(total_timeout).is_none() {
            return Err(RuntimeError::InvalidConfig(
                "blocking queue_timeout plus task_timeout exceeds the platform Instant range".to_string(),
            ));
        }
        Ok(())
    }
}

impl Default for BlockingPoolPolicy {
    fn default() -> Self {
        Self {
            name: "rocketmq-blocking".to_string(),
            max_concurrency: 64,
            max_queue_depth: 256,
            queue_timeout: Duration::from_secs(5),
            task_timeout: Duration::from_secs(30),
            warn_after: Duration::from_secs(1),
        }
    }
}

/// Capacity-isolated blocking work owned by a service root.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum BlockingLane {
    StorageIo,
    MetadataIo,
    CpuCrypto,
}

impl BlockingLane {
    pub(crate) const ALL: [Self; 3] = [Self::StorageIo, Self::MetadataIo, Self::CpuCrypto];

    pub(crate) const fn index(self) -> usize {
        match self {
            Self::StorageIo => 0,
            Self::MetadataIo => 1,
            Self::CpuCrypto => 2,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BlockingLanePolicies {
    pub storage_io: BlockingPoolPolicy,
    pub metadata_io: BlockingPoolPolicy,
    pub cpu_crypto: BlockingPoolPolicy,
}

impl BlockingLanePolicies {
    pub(crate) fn for_parallelism(parallelism: usize) -> Self {
        let parallelism = parallelism.max(1);
        Self {
            storage_io: BlockingPoolPolicy {
                name: "rocketmq-blocking.storage-io".to_string(),
                max_concurrency: parallelism.saturating_mul(4).max(4),
                max_queue_depth: parallelism.saturating_mul(16).max(16),
                ..BlockingPoolPolicy::default()
            },
            metadata_io: BlockingPoolPolicy {
                name: "rocketmq-blocking.metadata-io".to_string(),
                max_concurrency: parallelism.saturating_mul(2).max(2),
                max_queue_depth: parallelism.saturating_mul(8).max(8),
                ..BlockingPoolPolicy::default()
            },
            cpu_crypto: BlockingPoolPolicy {
                name: "rocketmq-blocking.cpu-crypto".to_string(),
                max_concurrency: parallelism.max(2),
                max_queue_depth: parallelism.saturating_mul(4).max(8),
                ..BlockingPoolPolicy::default()
            },
        }
    }

    pub(crate) fn cap_concurrency(&mut self, global_capacity: usize) {
        self.storage_io.max_concurrency = self.storage_io.max_concurrency.min(global_capacity).max(1);
        self.metadata_io.max_concurrency = self.metadata_io.max_concurrency.min(global_capacity).max(1);
        self.cpu_crypto.max_concurrency = self.cpu_crypto.max_concurrency.min(global_capacity).max(1);
    }

    pub fn validate(&self) -> RuntimeResult<()> {
        self.storage_io.validate()?;
        self.metadata_io.validate()?;
        self.cpu_crypto.validate()
    }

    pub(crate) fn validate_for_global_capacity(&self, global_capacity: usize) -> RuntimeResult<()> {
        self.validate()?;
        if global_capacity < BlockingLane::ALL.len() {
            return Err(RuntimeError::InvalidConfig(format!(
                "max_blocking_threads must be at least {} to reserve capacity for every blocking lane",
                BlockingLane::ALL.len()
            )));
        }
        Ok(())
    }

    pub(crate) fn max_concurrency(&self, lane: BlockingLane) -> usize {
        match lane {
            BlockingLane::StorageIo => self.storage_io.max_concurrency,
            BlockingLane::MetadataIo => self.metadata_io.max_concurrency,
            BlockingLane::CpuCrypto => self.cpu_crypto.max_concurrency,
        }
    }

    pub(crate) fn total_max_concurrency(&self) -> usize {
        self.storage_io
            .max_concurrency
            .saturating_add(self.metadata_io.max_concurrency)
            .saturating_add(self.cpu_crypto.max_concurrency)
    }

    pub fn uniform(policy: BlockingPoolPolicy) -> Self {
        let mut storage_io = policy.clone();
        storage_io.name = format!("{}.storage-io", policy.name);
        let mut metadata_io = policy.clone();
        metadata_io.name = format!("{}.metadata-io", policy.name);
        let mut cpu_crypto = policy;
        cpu_crypto.name = format!("{}.cpu-crypto", cpu_crypto.name);
        Self {
            storage_io,
            metadata_io,
            cpu_crypto,
        }
    }
}

impl Default for BlockingLanePolicies {
    fn default() -> Self {
        let parallelism = std::thread::available_parallelism()
            .map(|value| value.get())
            .unwrap_or(4);
        Self::for_parallelism(parallelism)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum BlockingKind {
    ShortIo,
    CpuBound,
    LongRunning,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub struct BlockingTaskId(pub(crate) u64);

impl BlockingTaskId {
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum BlockingTaskState {
    Queued,
    Running,
    Completed,
    JoinFailed,
    TimedOutStillRunning,
}
