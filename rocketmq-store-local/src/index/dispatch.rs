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

//! Backend-neutral CommitLog-to-index dispatch root.

/// Root that owns index feature gating and progress projection.
#[derive(Clone, Debug)]
pub struct IndexDispatchRoot<A> {
    adapter: A,
}

impl<A> IndexDispatchRoot<A> {
    pub fn new(adapter: A) -> Self {
        Self { adapter }
    }

    pub fn adapter(&self) -> &A {
        &self.adapter
    }

    /// Dispatches only when the Store configuration enables message indexing.
    pub fn dispatch<Request, Build>(&self, enabled: bool, request: &mut Request, build: Build) -> bool
    where
        Build: FnOnce(&A, &mut Request),
    {
        if !enabled {
            return false;
        }
        build(&self.adapter, request);
        true
    }

    /// Reports index progress only while message indexing is enabled.
    pub fn progress<ReadProgress>(&self, enabled: bool, read_progress: ReadProgress) -> Option<i64>
    where
        ReadProgress: FnOnce(&A) -> Option<i64>,
    {
        enabled.then(|| read_progress(&self.adapter)).flatten()
    }
}
