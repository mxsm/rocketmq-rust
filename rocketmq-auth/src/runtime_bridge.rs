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

use rocketmq_runtime::BlockingExecutor;
#[cfg(test)]
use rocketmq_runtime::BlockingExecutorSnapshot;
use rocketmq_runtime::RuntimeError;
use rocketmq_runtime::RuntimeOperation;
use rocketmq_runtime::RuntimeResult;

/// Narrow access to the root-owned metadata blocking lane.
///
/// The executor is never discovered or created by the Auth crate. Production
/// composition injects it from a [`rocketmq_runtime::ChildServiceContext`].
#[derive(Clone, Debug, Default)]
pub(crate) struct AuthBlockingExecutor {
    executor: Option<BlockingExecutor>,
}

impl AuthBlockingExecutor {
    pub(crate) fn new(executor: BlockingExecutor) -> Self {
        Self {
            executor: Some(executor),
        }
    }

    pub(crate) async fn spawn_io<F, R>(&self, name: &'static str, operation: F) -> RuntimeResult<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.executor
            .as_ref()
            .ok_or_else(|| RuntimeError::context_unavailable(RuntimeOperation::AuthMetadataIoLane))?
            .spawn_io(name, operation)
            .await
    }

    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> Option<BlockingExecutorSnapshot> {
        self.executor.as_ref().map(BlockingExecutor::snapshot)
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_runtime::RuntimeContext;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn auth_blocking_executor_uses_injected_metadata_lane() {
        let runtime = RuntimeContext::from_current("auth-blocking-executor-test");
        let service = runtime.service_context("auth-blocking-executor");
        let executor = AuthBlockingExecutor::new(service.metadata_io().clone());

        let value = executor
            .spawn_io("auth.blocking.counter", || 42usize)
            .await
            .expect("auth blocking task should complete");
        assert_eq!(value, 42);

        let snapshot = executor.snapshot().expect("injected executor has a snapshot");
        assert_eq!(snapshot.name, "rocketmq-blocking.metadata-io");
        assert_eq!(snapshot.blocking_still_running, 0);
    }

    #[tokio::test]
    async fn auth_blocking_executor_rejects_missing_capability() {
        let error = AuthBlockingExecutor::default()
            .spawn_io("auth.blocking.missing", || ())
            .await
            .expect_err("missing metadata lane must fail closed");
        assert!(error.to_string().contains("injected ChildServiceContext"));
    }
}
