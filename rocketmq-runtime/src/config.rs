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

use crate::blocking::BlockingLanePolicies;
use crate::error::RuntimeError;
use crate::error::RuntimeResult;

pub const MIN_ENTRYPOINT_BLOCKING_THREADS: usize = 3;
pub const MAX_ENTRYPOINT_BLOCKING_THREADS: usize = 512;

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub worker_threads: usize,
    pub max_blocking_threads: usize,
    pub thread_name: String,
    pub thread_stack_size: Option<usize>,
    pub thread_keep_alive: Duration,
    pub shutdown_timeout: Duration,
    pub blocking_lane_policies: BlockingLanePolicies,
    pub enable_io: bool,
    pub enable_time: bool,
}

impl RuntimeConfig {
    /// Builds a validated entrypoint profile from the effective CPU quota.
    ///
    /// `available_parallelism` should be the cgroup-aware value reported by
    /// [`std::thread::available_parallelism`]. Keeping it explicit makes the
    /// derivation deterministic in bootstrap tests and container profiles.
    pub fn for_parallelism(thread_name: impl Into<String>, available_parallelism: usize) -> Self {
        let parallelism = available_parallelism.max(1);
        let max_blocking_threads = parallelism
            .saturating_mul(4)
            .clamp(MIN_ENTRYPOINT_BLOCKING_THREADS, MAX_ENTRYPOINT_BLOCKING_THREADS);
        let mut blocking_lane_policies = BlockingLanePolicies::for_parallelism(parallelism);
        blocking_lane_policies.cap_concurrency(max_blocking_threads);
        Self {
            worker_threads: parallelism,
            max_blocking_threads,
            thread_name: thread_name.into(),
            thread_stack_size: None,
            thread_keep_alive: Duration::from_secs(30),
            shutdown_timeout: Duration::from_secs(30),
            blocking_lane_policies,
            enable_io: true,
            enable_time: true,
        }
    }

    /// Applies an explicit global blocking limit and aligns every lane ceiling.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::InvalidConfig`] when `limit` cannot reserve one
    /// slot for each blocking lane or exceeds the supported process bound.
    pub fn with_max_blocking_threads(mut self, limit: usize) -> RuntimeResult<Self> {
        if !(MIN_ENTRYPOINT_BLOCKING_THREADS..=MAX_ENTRYPOINT_BLOCKING_THREADS).contains(&limit) {
            return Err(RuntimeError::InvalidConfig(format!(
                "max_blocking_threads must be between {MIN_ENTRYPOINT_BLOCKING_THREADS} and \
                 {MAX_ENTRYPOINT_BLOCKING_THREADS}"
            )));
        }
        self.max_blocking_threads = limit;
        self.blocking_lane_policies.cap_concurrency(limit);
        self.validate()?;
        Ok(self)
    }

    pub fn server_default(thread_name: impl Into<String>) -> Self {
        Self {
            thread_name: thread_name.into(),
            ..Self::default()
        }
    }

    pub fn broker_default() -> Self {
        Self::server_default("rocketmq-broker")
    }

    pub fn namesrv_default() -> Self {
        Self::server_default("rocketmq-namesrv")
    }

    pub fn proxy_default() -> Self {
        Self::server_default("rocketmq-proxy")
    }

    pub fn controller_default() -> Self {
        Self::server_default("rocketmq-controller")
    }

    pub fn validate(&self) -> RuntimeResult<()> {
        if self.worker_threads == 0 {
            return Err(RuntimeError::InvalidConfig(
                "worker_threads must be greater than zero".to_string(),
            ));
        }
        if self.max_blocking_threads == 0 {
            return Err(RuntimeError::InvalidConfig(
                "max_blocking_threads must be greater than zero".to_string(),
            ));
        }
        if self.max_blocking_threads > MAX_ENTRYPOINT_BLOCKING_THREADS {
            return Err(RuntimeError::InvalidConfig(format!(
                "max_blocking_threads must not exceed {MAX_ENTRYPOINT_BLOCKING_THREADS}"
            )));
        }
        if self.thread_name.trim().is_empty() {
            return Err(RuntimeError::InvalidConfig("thread_name must not be empty".to_string()));
        }
        if matches!(self.thread_stack_size, Some(0)) {
            return Err(RuntimeError::InvalidConfig(
                "thread_stack_size must be greater than zero when set".to_string(),
            ));
        }
        self.blocking_lane_policies
            .validate_for_global_capacity(self.max_blocking_threads)?;
        Ok(())
    }
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        let parallelism = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
        Self::for_parallelism("rocketmq-runtime", parallelism)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entrypoint_profiles_derive_bounded_cpu_and_lane_budgets() {
        for (parallelism, expected_workers, expected_blocking) in [(0, 1, 4), (1, 1, 4), (2, 2, 8), (32, 32, 128)] {
            let config = RuntimeConfig::for_parallelism("profile-test", parallelism);
            assert_eq!(config.worker_threads, expected_workers);
            assert_eq!(config.max_blocking_threads, expected_blocking);
            assert!(config.blocking_lane_policies.storage_io.max_concurrency <= expected_blocking);
            assert!(config.blocking_lane_policies.metadata_io.max_concurrency <= expected_blocking);
            assert!(config.blocking_lane_policies.cpu_crypto.max_concurrency <= expected_blocking);
            config.validate().expect("derived entrypoint profile is valid");
        }

        let capped = RuntimeConfig::for_parallelism("large-profile-test", usize::MAX);
        assert_eq!(capped.max_blocking_threads, MAX_ENTRYPOINT_BLOCKING_THREADS);
    }

    #[test]
    fn explicit_blocking_limit_has_safe_bounds_and_caps_lanes() {
        let config = RuntimeConfig::for_parallelism("override-test", 16)
            .with_max_blocking_threads(6)
            .expect("explicit safe limit");
        assert_eq!(config.max_blocking_threads, 6);
        assert_eq!(config.blocking_lane_policies.storage_io.max_concurrency, 6);
        assert_eq!(config.blocking_lane_policies.metadata_io.max_concurrency, 6);
        assert!(config.blocking_lane_policies.cpu_crypto.max_concurrency <= 6);

        for invalid in [0, 1, 2, MAX_ENTRYPOINT_BLOCKING_THREADS + 1] {
            assert!(RuntimeConfig::for_parallelism("invalid-override", 4)
                .with_max_blocking_threads(invalid)
                .is_err());
        }
    }
}
