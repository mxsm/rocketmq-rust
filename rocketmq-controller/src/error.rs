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

//! Controller error re-exports
//!
//! This module re-exports error types from `rocketmq-error` for convenience.
//! All controller error types are now centralized in the `rocketmq-error` crate.

// Re-export controller error types from rocketmq-error
pub use rocketmq_error::ControllerError;
pub use rocketmq_error::ControllerResult;

// Type alias for backward compatibility
pub type Result<T> = ControllerResult<T>;
