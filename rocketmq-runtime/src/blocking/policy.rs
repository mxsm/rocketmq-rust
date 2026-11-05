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

use crate::error::RuntimeContractViolation;
use crate::RuntimeContractPolicy;

/// Represents blocking pool policy.
#[derive(Debug, Clone)]
pub struct BlockingPoolPolicy {
    /// The name value.
    pub name: String,
    /// The max concurrency value.
    pub max_concurrency: usize,
    /// The max queue depth value.
    pub max_queue_depth: usize,
    /// The queue timeout value.
    pub queue_timeout: Duration,
    /// The task timeout value.
    pub task_timeout: Duration,
    /// The warn after value.
    pub warn_after: Duration,
}

impl BlockingPoolPolicy {
    /// Validates this value.
    ///
    /// # Errors
    ///
    /// Returns a contract violation when concurrency or queue depth is zero,
    /// or when the combined queue and task timeout cannot be represented.
    pub fn validate(&self) -> Result<(), RuntimeContractViolation> {
        if self.max_concurrency == 0 {
            return Err(RuntimeContractViolation::InvalidConfiguration {
                policy: RuntimeContractPolicy::BlockingMaxConcurrencyPositive,
            });
        }
        if self.max_queue_depth == 0 {
            return Err(RuntimeContractViolation::InvalidConfiguration {
                policy: RuntimeContractPolicy::BlockingMaxQueueDepthPositive,
            });
        }
        let Some(total_timeout) = self.queue_timeout.checked_add(self.task_timeout) else {
            return Err(RuntimeContractViolation::InvalidConfiguration {
                policy: RuntimeContractPolicy::BlockingTimeoutRepresentable,
            });
        };
        if std::time::Instant::now().checked_add(total_timeout).is_none() {
            return Err(RuntimeContractViolation::InvalidConfiguration {
                policy: RuntimeContractPolicy::BlockingTimeoutRepresentable,
            });
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
    /// Represents the storage io case.
    StorageIo,
    /// Represents the metadata io case.
    MetadataIo,
    /// Represents the cpu crypto case.
    CpuCrypto,
    /// User consume listeners, which may block for a whole consume timeout.
    ///
    /// Unlike the other lanes this one does not draw from the global blocking
    /// budget: it has a capacity of its own, and the Tokio blocking pool is
    /// sized to hold it as well, so a slow listener cannot take slots that
    /// storage or metadata I/O count on, and they cannot starve listeners.
    ConsumeListener,
}

impl BlockingLane {
    pub(crate) const ALL: [Self; 4] = [
        Self::StorageIo,
        Self::MetadataIo,
        Self::CpuCrypto,
        Self::ConsumeListener,
    ];

    /// Lanes that share the runtime's global blocking budget.
    pub(crate) const MANAGED: [Self; 3] = [Self::StorageIo, Self::MetadataIo, Self::CpuCrypto];

    pub(crate) const fn index(self) -> usize {
        match self {
            Self::StorageIo => 0,
            Self::MetadataIo => 1,
            Self::CpuCrypto => 2,
            Self::ConsumeListener => 3,
        }
    }
}

/// Default listener capacity: the default `consume_thread_max` of one push
/// consumer. Like Java's consume thread pool it does not depend on the CPU
/// count, because listeners usually wait on I/O.
const CONSUME_LISTENER_DEFAULT_CONCURRENCY: usize = 64;

/// Listener calls queued behind a busy lane wait rather than being rejected;
/// each consumer already bounds its own in-flight calls.
const CONSUME_LISTENER_DEFAULT_QUEUE_DEPTH: usize = 1024;

/// Java's default `consumeTimeout`. A call still queued after this long is
/// rejected, and a call running longer is logged when it finishes.
const CONSUME_LISTENER_DEFAULT_QUEUE_TIMEOUT: Duration = Duration::from_secs(15 * 60);

/// How long a caller keeps waiting on a running listener. A listener cannot be
/// interrupted; one that overruns `consumeTimeout` is handled by the consumer's
/// expiry cleanup, so the caller does not give up early and cause a duplicate.
const CONSUME_LISTENER_DEFAULT_TASK_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);

/// Represents blocking lane policies.
#[derive(Debug, Clone)]
pub struct BlockingLanePolicies {
    /// The storage io value.
    pub storage_io: BlockingPoolPolicy,
    /// The metadata io value.
    pub metadata_io: BlockingPoolPolicy,
    /// The cpu crypto value.
    pub cpu_crypto: BlockingPoolPolicy,
    /// The consume listener value. Its capacity is not part of the global
    /// blocking budget; see [`BlockingLane::ConsumeListener`].
    pub consume_listener: BlockingPoolPolicy,
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
            consume_listener: BlockingPoolPolicy {
                name: "rocketmq-blocking.consume-listener".to_string(),
                max_concurrency: CONSUME_LISTENER_DEFAULT_CONCURRENCY,
                max_queue_depth: CONSUME_LISTENER_DEFAULT_QUEUE_DEPTH,
                queue_timeout: CONSUME_LISTENER_DEFAULT_QUEUE_TIMEOUT,
                task_timeout: CONSUME_LISTENER_DEFAULT_TASK_TIMEOUT,
                warn_after: CONSUME_LISTENER_DEFAULT_QUEUE_TIMEOUT,
            },
        }
    }

    /// Caps the lanes that share the global budget. The consume listener lane
    /// keeps its own capacity.
    pub(crate) fn cap_concurrency(&mut self, global_capacity: usize) {
        self.storage_io.max_concurrency = self.storage_io.max_concurrency.min(global_capacity).max(1);
        self.metadata_io.max_concurrency = self.metadata_io.max_concurrency.min(global_capacity).max(1);
        self.cpu_crypto.max_concurrency = self.cpu_crypto.max_concurrency.min(global_capacity).max(1);
    }

    /// Validates this value.
    ///
    /// # Errors
    ///
    /// Returns the first contract violation from one of the configured lanes.
    pub fn validate(&self) -> Result<(), RuntimeContractViolation> {
        self.storage_io.validate()?;
        self.metadata_io.validate()?;
        self.cpu_crypto.validate()?;
        self.consume_listener.validate()
    }

    pub(crate) fn validate_for_global_capacity(&self, global_capacity: usize) -> Result<(), RuntimeContractViolation> {
        self.validate()?;
        if global_capacity < BlockingLane::MANAGED.len() {
            return Err(RuntimeContractViolation::InvalidConfiguration {
                policy: RuntimeContractPolicy::BlockingGlobalCapacityCoversLanes,
            });
        }
        Ok(())
    }

    /// Returns the policy in force for one lane.
    ///
    /// Lane capacity, phase timeouts, and the warn threshold are owned here for
    /// every consumer of the lane, so a component that accepts its own timeout
    /// fields can report which value actually applies.
    #[must_use]
    pub fn policy(&self, lane: BlockingLane) -> &BlockingPoolPolicy {
        match lane {
            BlockingLane::StorageIo => &self.storage_io,
            BlockingLane::MetadataIo => &self.metadata_io,
            BlockingLane::CpuCrypto => &self.cpu_crypto,
            BlockingLane::ConsumeListener => &self.consume_listener,
        }
    }

    pub(crate) fn max_concurrency(&self, lane: BlockingLane) -> usize {
        match lane {
            BlockingLane::StorageIo => self.storage_io.max_concurrency,
            BlockingLane::MetadataIo => self.metadata_io.max_concurrency,
            BlockingLane::CpuCrypto => self.cpu_crypto.max_concurrency,
            BlockingLane::ConsumeListener => self.consume_listener.max_concurrency,
        }
    }

    /// Sum of the ceilings of the lanes that share the global budget.
    pub(crate) fn total_max_concurrency(&self) -> usize {
        self.storage_io
            .max_concurrency
            .saturating_add(self.metadata_io.max_concurrency)
            .saturating_add(self.cpu_crypto.max_concurrency)
    }

    /// Creates the uniform value.
    pub fn uniform(policy: BlockingPoolPolicy) -> Self {
        let mut storage_io = policy.clone();
        storage_io.name = format!("{}.storage-io", policy.name);
        let mut metadata_io = policy.clone();
        metadata_io.name = format!("{}.metadata-io", policy.name);
        let mut cpu_crypto = policy.clone();
        cpu_crypto.name = format!("{}.cpu-crypto", policy.name);
        let mut consume_listener = policy;
        consume_listener.name = format!("{}.consume-listener", consume_listener.name);
        Self {
            storage_io,
            metadata_io,
            cpu_crypto,
            consume_listener,
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

/// Identifies the blocking kind state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum BlockingKind {
    /// Represents the short io case.
    ShortIo,
    /// Represents the cpu bound case.
    CpuBound,
    /// Represents the long running case.
    LongRunning,
}

/// Represents blocking task id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub struct BlockingTaskId(pub(crate) u64);

impl BlockingTaskId {
    /// Borrows this value as u64.
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

/// Identifies the blocking task state state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum BlockingTaskState {
    /// Represents the queued case.
    Queued,
    /// Represents the running case.
    Running,
    /// Represents the completed case.
    Completed,
    /// Represents the join failed case.
    JoinFailed,
    /// Represents the timed out still running case.
    TimedOutStillRunning,
}
