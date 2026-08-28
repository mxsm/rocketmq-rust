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
#[allow(
    unused_imports,
    reason = "BRK-04 freezes the broker-private Pull criteria seam before MIG-04 production wiring"
)]
pub(crate) use data::PullMatchCriteria;
#[allow(
    unused_imports,
    reason = "BRK-04 freezes the broker-private retained request seam before MIG-04 production wiring"
)]
pub(crate) use data::PullRequestData;
pub(crate) use data::PullSessionClientLookup;
#[allow(
    unused_imports,
    reason = "BRK-04 freezes the broker-private Pull deadline seam before MIG-04 production wiring"
)]
pub(crate) use deadline::PullWaitDeadline;
#[allow(
    unused_imports,
    reason = "BRK-04 infrastructure is wired to production listeners and composition in BRK-06"
)]
pub(crate) use index::PullArrivalView;
#[allow(
    unused_imports,
    reason = "BRK-04 infrastructure is wired to production listeners and composition in BRK-06"
)]
pub(crate) use index::PullCriteriaLimits;
#[allow(
    unused_imports,
    reason = "BRK-04 infrastructure is wired to production listeners and composition in BRK-06"
)]
pub(crate) use service::PreparedPullRegistration;
#[allow(
    unused_imports,
    reason = "BRK-04 infrastructure is wired to production listeners and composition in BRK-06"
)]
pub(crate) use service::PullDeferredPrepareError;
#[allow(
    unused_imports,
    reason = "BRK-04 infrastructure is wired to production listeners and composition in BRK-06"
)]
pub(crate) use service::PullDeferredRegisterError;
#[allow(
    unused_imports,
    reason = "BRK-04 infrastructure is wired to production listeners and composition in BRK-06"
)]
pub(crate) use service::PullDeferredService;
#[allow(
    unused_imports,
    reason = "BRK-04 infrastructure is wired to production listeners and composition in BRK-06"
)]
pub(crate) use service::PullProducerStats;
#[allow(
    unused_imports,
    reason = "BRK-04 infrastructure is wired to production listeners and composition in BRK-06"
)]
pub(crate) use service::PullRetainedEstimate;
#[allow(
    unused_imports,
    reason = "BRK-04 infrastructure is wired to production listeners and composition in BRK-06"
)]
pub(crate) use service::PullSuspendTiming;
#[allow(
    unused_imports,
    reason = "BRK-04 infrastructure is wired to production listeners and composition in BRK-06"
)]
pub(crate) use service::ResumePull;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod acceptance_tests;
