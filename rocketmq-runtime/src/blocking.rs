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

//! Root-owned blocking admission with lane isolation and one global hard limit.
//!
//! [`BlockingExecutor`] preserves the existing submission interface. Runtime
//! composition creates one global budget and clones only handles into child
//! contexts; queue admission, lane ceilings, reservations, the running permit,
//! absolute deadlines, and diagnostics remain private implementation details.

mod admission;
mod diagnostics;
mod executor;
mod policy;

pub use diagnostics::BlockingExecutorSnapshot;
pub use diagnostics::BlockingTaskSnapshot;
pub use executor::BlockingExecutor;
pub use policy::BlockingKind;
pub use policy::BlockingLane;
pub use policy::BlockingLanePolicies;
pub use policy::BlockingPoolPolicy;
pub use policy::BlockingTaskId;
pub use policy::BlockingTaskState;

pub(crate) use admission::GlobalBlockingBudget;
