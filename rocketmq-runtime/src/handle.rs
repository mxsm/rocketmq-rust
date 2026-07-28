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

#[derive(Debug, Clone)]
pub struct RuntimeHandle {
    handle: tokio::runtime::Handle,
}

impl RuntimeHandle {
    pub fn new(handle: tokio::runtime::Handle) -> Self {
        Self { handle }
    }

    #[deprecated(
        since = "1.1.0",
        note = "raw Tokio handles are a compatibility boundary; use an injected task capability"
    )]
    pub fn inner(&self) -> &tokio::runtime::Handle {
        &self.handle
    }

    #[deprecated(
        since = "1.1.0",
        note = "raw spawning bypasses TaskGroup ownership; use TaskSpawner or TaskGroup"
    )]
    pub fn spawn<F>(&self, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.handle.spawn(future)
    }

    pub(crate) fn tokio_handle(&self) -> &tokio::runtime::Handle {
        &self.handle
    }

    pub(crate) fn spawn_owned<F>(&self, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.handle.spawn(future)
    }
}
