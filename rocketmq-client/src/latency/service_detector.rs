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

/// Detects whether a remote service endpoint is healthy.
///
/// Implementations are used by the latency-aware fault tolerance strategy to
/// probe whether a remote endpoint is reachable before traffic is routed back
/// to it. Implementations must be `Send + Sync + 'static` so detectors can be
/// shared across threads and stored in long-lived client state, and
/// [`detect`](Self::detect) returns a `Send` future so probing never pins the
/// caller to a single thread.
#[trait_variant::make(Send)]
pub trait ServiceDetector: Sync + 'static {
    /// Checks if the remote service at the given endpoint is reachable.
    ///
    /// Returns `true` if the service is back to normal; `false` otherwise.
    async fn detect(&self, endpoint: &str, timeout_millis: u64) -> bool;
}
