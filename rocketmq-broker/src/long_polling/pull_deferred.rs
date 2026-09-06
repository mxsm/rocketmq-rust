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

mod data;
mod deadline;
mod index;
mod service;

pub(crate) use data::PullHookMetadata;
pub(crate) use data::PullMatchCriteria;
pub(crate) use data::PullSessionClientLookup;
pub(crate) use index::PullCandidateReservation;
pub(crate) use index::PullCriteriaLimits;
pub(crate) use service::PullDeferredPrepareOutcome;
pub(crate) use service::PullDeferredRegisterError;
pub(crate) use service::PullDeferredRegisterOutcome;
pub(crate) use service::PullDeferredRegisterRejection;
pub(crate) use service::PullDeferredService;
pub(crate) use service::PullPendingArrivalReservation;
pub(crate) use service::PullPendingOffsetReservation;
pub(crate) use service::PullRetainedEstimate;
pub(crate) use service::PullSuspendTiming;
pub(crate) use service::ResumePull;

#[cfg(test)]
#[path = "../../tests/unit/long_polling/pull_deferred/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "../../tests/unit/long_polling/pull_deferred/acceptance_tests.rs"]
mod acceptance_tests;
