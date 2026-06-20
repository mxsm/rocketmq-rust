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

use std::future::Future;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use rocketmq_runtime::BlockingExecutor;
use rocketmq_runtime::BlockingPoolPolicy;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeError;
use rocketmq_runtime::RuntimeHandle;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::RuntimeResult;
use rocketmq_runtime::TaskGroup;

static AUTH_SYNC_RUNTIME: OnceLock<Result<RuntimeOwner, String>> = OnceLock::new();

#[derive(Clone, Debug, Default)]
pub(crate) struct AuthBlockingExecutor {
    inner: Arc<OnceLock<BlockingExecutor>>,
}

impl AuthBlockingExecutor {
    pub(crate) async fn spawn_io<F, R>(&self, name: &'static str, operation: F) -> RuntimeResult<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.executor()?.spawn_io(name, operation).await
    }

    fn executor(&self) -> RuntimeResult<BlockingExecutor> {
        if let Some(executor) = self.inner.get() {
            return Ok(executor.clone());
        }

        let handle = tokio::runtime::Handle::try_current().map_err(|_error| RuntimeError::NoCurrentRuntime)?;
        let group = TaskGroup::root("rocketmq-auth.blocking", RuntimeHandle::new(handle));
        let executor = BlockingExecutor::new(
            BlockingPoolPolicy {
                name: "rocketmq-auth.blocking".to_string(),
                ..BlockingPoolPolicy::default()
            },
            group.child("rocketmq-auth.blocking-reaper"),
        )?;

        if self.inner.set(executor.clone()).is_err() {
            return Ok(self
                .inner
                .get()
                .expect("auth blocking executor must be initialized")
                .clone());
        }
        Ok(executor)
    }
}

pub(crate) fn block_on_sync_bridge<F, Fut, T, E, BuildError, ThreadPanic>(
    future: F,
    build_error: BuildError,
    thread_panic: ThreadPanic,
) -> Result<T, E>
where
    F: FnOnce() -> Fut + Send,
    Fut: Future<Output = Result<T, E>>,
    T: Send,
    E: Send,
    BuildError: FnOnce(String) -> E + Send,
    ThreadPanic: FnOnce() -> E + Send,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| handle.block_on(future()))
        }
        Ok(_) => std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let runtime = auth_sync_runtime().map_err(build_error)?;
                    runtime.block_on(future())
                })
                .join()
                .map_err(|_| thread_panic())?
        }),
        Err(_) => {
            let runtime = auth_sync_runtime().map_err(build_error)?;
            runtime.block_on(future())
        }
    }
}

fn auth_sync_runtime() -> Result<&'static RuntimeOwner, String> {
    match AUTH_SYNC_RUNTIME.get_or_init(|| {
        let parallelism = std::thread::available_parallelism()
            .map(|parallelism| parallelism.get())
            .unwrap_or(2);
        let worker_threads = parallelism.clamp(2, 4);
        RuntimeOwner::new(RuntimeConfig {
            worker_threads,
            max_blocking_threads: worker_threads * 2,
            thread_name: "rocketmq-auth-sync".to_string(),
            shutdown_timeout: Duration::from_secs(10),
            ..RuntimeConfig::default()
        })
        .map_err(|error| error.to_string())
    }) {
        Ok(runtime) => Ok(runtime),
        Err(error) => Err(error.clone()),
    }
}
