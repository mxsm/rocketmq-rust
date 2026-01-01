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

/// `Interceptor` is a trait that provides a common interface for objects that can intercept and
/// modify a sequence of integers. It provides two methods: `inc` and `reset`.
pub trait Interceptor {
    /// The `inc` method accepts a vector of i64 integers, `deltas`, and applies some operation to
    /// them. The specifics of the operation are determined by the implementing type.
    ///
    /// # Arguments
    ///
    /// * `deltas` - A vector of i64 integers to be intercepted and modified.
    fn inc(&self, deltas: Vec<i64>);

    /// The `reset` method resets the state of the implementing object. The specifics of what
    /// "resetting" means are determined by the implementing type.
    fn reset(&self);
}
