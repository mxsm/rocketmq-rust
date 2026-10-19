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

/// Resolves logical broker names to endpoints for latency fault detection.
///
/// Implementations must be `Send + Sync + 'static`. Asynchronous resolution returns
/// a future that implements `Send`. The `'static` bound applies to the implementation
/// type; the returned future may borrow from `self` and `name`.
#[trait_variant::make(Send)]
pub trait Resolver: Sync + 'static {
    /// Resolves the given logical `name` to an endpoint.
    ///
    /// Returns `Some(endpoint)` with the resolved endpoint address if available,
    /// or `None` if resolution yields no endpoint.
    async fn resolve(&self, name: &CheetahString) -> Option<CheetahString>;
}
