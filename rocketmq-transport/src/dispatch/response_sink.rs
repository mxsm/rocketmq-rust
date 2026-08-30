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

mod plan;

use std::sync::Arc;

use crate::dispatch::DeferredResponseSeed;
use crate::server::SessionHandle;
use crate::session_view::SessionStateView;
use rocketmq_runtime::TaskGroup;

use plan::LocalPlanSenderState;
pub(crate) use plan::NetworkResponsePlanContext;
pub(crate) use plan::ResponseTransportDropHandle;

#[derive(Clone)]
/// Cloneable single-response capability used by in-process dispatch.
pub(crate) struct LocalResponseSink {
    state: Arc<LocalPlanSenderState>,
}

impl Drop for LocalResponseSink {
    fn drop(&mut self) {
        if Arc::strong_count(&self.state) == 1 {
            self.state.close_last_sender();
        }
    }
}

/// Closed response output variants for network and in-process dispatch.
#[derive(Clone)]
pub(crate) enum ResponseSink {
    /// A bounded canonical session writer.
    Network(Arc<SessionHandle>),
    /// A single-use in-process response channel.
    Local(LocalResponseSink),
}

impl ResponseSink {
    /// Proves that this is the local plan capability whose control observes
    /// the supplied embedded lifecycle owners.
    pub(crate) fn is_local_plan_owner(&self, session: &SessionStateView, task_group: &TaskGroup) -> bool {
        matches!(
            self,
            Self::Local(LocalResponseSink { state })
                if state.control().same_lifecycle_owner(session, task_group)
        )
    }

    /// Builds the deferred responder seed for a canonical embedded local-plan
    /// owner. The lifecycle proof prevents a sink, session view, and task group
    /// from unrelated dispatches being combined.
    pub(crate) fn local_deferred_seed_with_resume(
        &self,
        telemetry: crate::telemetry::TransportTelemetry,
        session: &crate::session_view::SessionView,
        task_group: &TaskGroup,
        ordering: crate::request_ordering::RequestOrdering,
        class: crate::admission::AdmissionClass,
        executor: crate::session_executor::DeferredResumeExecutor,
    ) -> Option<DeferredResponseSeed> {
        if !matches!(session, crate::session_view::SessionView::Embedded { .. })
            || !self.is_local_plan_owner(session.state(), task_group)
        {
            return None;
        }
        Some(
            DeferredResponseSeed::new(
                self.clone(),
                telemetry,
                session.id(),
                self.local_plan_control()?.clone(),
            )
            .with_resume_context(ordering, class, executor),
        )
    }

    fn local_plan_control(&self) -> Option<&crate::dispatch::RequestControlView> {
        match self {
            Self::Local(LocalResponseSink { state }) => Some(state.control()),
            Self::Network(_) => None,
        }
    }

    /// Proves this is the plan-bound view of a canonical network session. A bare
    /// command sink is insufficient because it has no shared completion slot.
    pub(crate) fn is_canonical_network_plan_owner(&self, session: &SessionHandle) -> bool {
        matches!(
            self,
            Self::Network(owner)
                if owner.same_canonical_owner(session)
                    && owner
                        .response_plan_context()
                        .is_some_and(|context| context.same_lifecycle_owner(session))
        )
    }

    pub(crate) fn network_deferred_seed_with_resume(
        &self,
        session: &SessionHandle,
        ordering: crate::request_ordering::RequestOrdering,
        class: crate::admission::AdmissionClass,
        executor: crate::session_executor::DeferredResumeExecutor,
    ) -> Option<DeferredResponseSeed> {
        if !self.is_canonical_network_plan_owner(session) {
            return None;
        }
        let Self::Network(owner) = self else {
            return None;
        };
        let context = owner.response_plan_context()?;
        Some(
            DeferredResponseSeed::new(
                self.clone(),
                session.connection().telemetry(),
                session.session_view().id(),
                context.control().clone(),
            )
            .with_resume_context(ordering, class, executor),
        )
    }

    #[cfg(test)]
    pub(crate) fn network_deferred_seed(&self, session: &SessionHandle) -> Option<DeferredResponseSeed> {
        if !self.is_canonical_network_plan_owner(session) {
            return None;
        }
        let Self::Network(owner) = self else {
            return None;
        };
        let context = owner.response_plan_context()?;
        Some(DeferredResponseSeed::new(
            self.clone(),
            session.connection().telemetry(),
            session.session_view().id(),
            context.control().clone(),
        ))
    }

    #[cfg(test)]
    pub(crate) fn deferred_seed_for_test(
        &self,
        telemetry: crate::telemetry::TransportTelemetry,
        session_id: crate::session_view::SessionId,
        control: crate::dispatch::RequestControlView,
    ) -> DeferredResponseSeed {
        DeferredResponseSeed::new(self.clone(), telemetry, session_id, control)
    }
}
