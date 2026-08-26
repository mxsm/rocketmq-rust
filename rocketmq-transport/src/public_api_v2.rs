// Copyright 2026 The RocketMQ Rust Authors
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

//! Explicitly approved source API for the 2.x release line.
//!
//! This surface exposes only immutable request timing, identity, origin, and
//! authentication facts approved for the 2.x request model. These facts are
//! read-only DTOs: they do not expose legacy channels, session handles,
//! operation contexts, or cancellation capabilities.

pub use crate::deadline::RequestDeadline;
pub use crate::dispatch::AuthenticationState;
pub use crate::dispatch::EmbeddedCaller;
pub use crate::dispatch::OriginalRequestIdentity;
pub use crate::dispatch::RequestId;
pub use crate::dispatch::RequestOrigin;
