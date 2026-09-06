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

pub(crate) mod deadline;
pub(crate) mod index;
pub(crate) mod service;

#[cfg(test)]
pub(crate) use deadline::LongPollingDeadline;
#[cfg(test)]
pub(crate) use deadline::LongPollingDeadlineErrorKind;
#[cfg(test)]
pub(crate) use deadline::LongPollingDeadlineOutcome;
#[cfg(test)]
pub(crate) use index::PopArrival;
#[cfg(test)]
pub(crate) use index::PopCriteriaIndex;
#[cfg(test)]
pub(crate) use index::PopCriteriaKey;
#[cfg(test)]
pub(crate) use index::PopCriteriaLimits;
#[cfg(test)]
pub(crate) use index::PopIndexRejection;
#[cfg(test)]
pub(crate) use index::PopIndexReserveOutcome;
#[cfg(test)]
pub(crate) use index::PopIndexSnapshot;
#[cfg(test)]
pub(crate) use index::PopMatchCriteria;
#[cfg(test)]
pub(crate) use index::PopSelectionOrder;
#[cfg(test)]
pub(crate) use service::PopDeferredPrepareErrorKind;
#[cfg(test)]
pub(crate) use service::PopDeferredPrepareOutcome;
#[cfg(test)]
pub(crate) use service::PopDeferredPrepareRejectionKind;
#[cfg(test)]
pub(crate) use service::PopDeferredRegisterOutcome;
#[cfg(test)]
pub(crate) use service::PopDeferredService;
#[cfg(test)]
pub(crate) use service::PopRequestData;
#[cfg(test)]
pub(crate) use service::PopRetainedEstimate;

#[cfg(test)]
#[path = "../../tests/unit/long_polling/pop_deferred/acceptance_tests.rs"]
mod acceptance_tests;
#[cfg(test)]
#[path = "../../tests/unit/long_polling/pop_deferred/core.rs"]
mod core_tests;
