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

//! Canonical root ownership for the Local CommitLog implementation.

/// Owns the complete adapter state used by one Local CommitLog instance.
///
/// The generic adapter keeps composition dependencies outside the Local crate while ensuring
/// there is only one canonical owner for the CommitLog root. The legacy Store type is a narrow
/// facade over this owner.
#[doc(hidden)]
#[derive(Debug)]
pub struct CommitLogRoot<A> {
    adapter: A,
}

impl<A> CommitLogRoot<A> {
    /// Creates a CommitLog root that exclusively owns `adapter`.
    #[doc(hidden)]
    pub fn new(adapter: A) -> Self {
        Self { adapter }
    }

    /// Borrows the composition adapter.
    #[doc(hidden)]
    pub fn adapter(&self) -> &A {
        &self.adapter
    }

    /// Mutably borrows the composition adapter.
    #[doc(hidden)]
    pub fn adapter_mut(&mut self) -> &mut A {
        &mut self.adapter
    }

    /// Returns the owned adapter when dismantling the CommitLog root.
    #[doc(hidden)]
    pub fn into_adapter(self) -> A {
        self.adapter
    }
}
