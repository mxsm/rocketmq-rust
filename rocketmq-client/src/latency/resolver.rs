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

use cheetah_string::CheetahString;

/// Resolves logical broker names to network endpoints for latency fault detection.
///
/// Implementations must be `Sync + 'static`; the `trait_variant::make(Send)` macro
/// additionally supplies `Self: Send` for the generated trait and marks the future
/// returned by [`Resolver::resolve`] as `Send`. The `'static` bound applies to the
/// implementation type only; the returned future may borrow `self` and `name`.
#[trait_variant::make(Send)]
pub trait Resolver: Sync + 'static {
    /// Resolves `name` to a broker endpoint.
    ///
    /// Returns `Some(endpoint)` when an endpoint is available for `name`, and
    /// `None` when resolution yields no endpoint. Resolution is asynchronous and
    /// returns a `Send` future without changing the resolution strategy or result type.
    async fn resolve(&self, name: &CheetahString) -> Option<CheetahString>;
}
