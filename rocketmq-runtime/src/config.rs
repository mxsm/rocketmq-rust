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

use crate::blocking::BlockingPoolPolicy;
use crate::error::RuntimeError;
use crate::error::RuntimeResult;

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub worker_threads: usize,
    pub max_blocking_threads: usize,
    pub thread_name: String,
    pub thread_keep_alive: Duration,
    pub shutdown_timeout: Duration,
    pub blocking_pool_policy: BlockingPoolPolicy,
    pub enable_io: bool,
    pub enable_time: bool,
}

impl RuntimeConfig {
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
        if self.thread_name.trim().is_empty() {
            return Err(RuntimeError::InvalidConfig("thread_name must not be empty".to_string()));
        }
        self.blocking_pool_policy.validate()?;
        Ok(())
    }
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        let parallelism = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);

        Self {
            worker_threads: parallelism,
            max_blocking_threads: parallelism * 4,
            thread_name: "rocketmq-runtime".to_string(),
            thread_keep_alive: Duration::from_secs(30),
            shutdown_timeout: Duration::from_secs(30),
            blocking_pool_policy: BlockingPoolPolicy {
                max_concurrency: parallelism * 4,
                ..BlockingPoolPolicy::default()
            },
            enable_io: true,
            enable_time: true,
        }
    }
}
