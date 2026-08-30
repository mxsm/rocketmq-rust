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

//! Channel-free embedded dispatch through the explicit processor contract.

use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use super::admission_response;
use super::deadline_response;
use super::AuthorizedCommandDispatcher;
use super::AuthorizedDispatchSession;
use crate::admission::AdmissionClass;
use crate::admission::FullPolicy;
use crate::base::pending_request_table::materialize_and_estimate_remoting_command_retained_bytes;
use crate::deadline::RequestDeadline;
use crate::dispatch::remoting_request::RemotingRequestBuilder;
use crate::dispatch::remoting_request::RequestLifecycleProvenance;
use crate::dispatch::DeferredSessionCleanupOwner;
use crate::dispatch::EmbeddedCaller;
use crate::dispatch::EmbeddedDispatchError;
use crate::dispatch::EmbeddedDispatchOutcome;
use crate::dispatch::EmbeddedProcessorResolveError;
use crate::dispatch::EmbeddedResolvedOutcome;
use crate::dispatch::ExplicitProcessor;
use crate::dispatch::InternalFailureOrigin;
use crate::dispatch::InternalProcessorCandidate;
use crate::dispatch::InternalProcessorOutcome;
use crate::dispatch::OriginalRequestIdentity;
use crate::dispatch::RequestContext;
use crate::dispatch::RequestControlView;
use crate::dispatch::ResponsePlan;
use crate::dispatch::ResponseSink;
use crate::runtime::processor::RequestProcessor;
use crate::session_executor::SessionDispatchError;
use crate::session_view::EmbeddedSessionRecord;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::TaskGroup;
use rocketmq_security_api::Action;
use rocketmq_security_api::Decision;
use rocketmq_security_api::Principal;
use rocketmq_security_api::Resource;
use rocketmq_security_api::ResourceKind;

#[path = "embedded/terminal.rs"]
mod terminal;
use terminal::terminal;
use terminal::EmbeddedTerminalSender;

type TerminalResult = Result<EmbeddedDispatchOutcome, EmbeddedDispatchError>;

impl<P> AuthorizedCommandDispatcher<P>
where
    P: RequestProcessor + Clone + Sync + 'static,
{
    /// Dispatches one command through the channel-free embedded boundary.
    ///
    /// The supplied principal is authoritative. Command headers cannot replace
    /// it, and embedded requests intentionally have no network peer.
    ///
    /// # Errors
    ///
    /// Returns a typed, redacted error when identity capture, security,
    /// admission, request construction, processor execution, response binding,
    /// in-process handoff, or lifecycle completion fails.
    pub async fn dispatch_embedded(
        &self,
        task_group: &TaskGroup,
        principal: Principal,
        deadline: Option<RequestDeadline>,
        command: RemotingCommand,
    ) -> TerminalResult {
        self.dispatch_embedded_inner(task_group, principal, deadline, command, false)
            .await
    }

    /// Dispatches one embedded command and resolves an accepted deferred
    /// response into its final in-process response plan.
    ///
    /// This terminal form is intended for the Broker-owned local Proxy. It
    /// retains the embedded session and its deferred cleanup owner while the
    /// processor's registration is live, but waits only after the admitted
    /// processor future has returned and released its processor permit.
    ///
    /// # Errors
    ///
    /// Returns a typed, redacted error for the same boundary failures as
    /// [`Self::dispatch_embedded`], plus deferred response cancellation,
    /// deadline, session-close, or local handoff failure.
    pub async fn dispatch_embedded_wait_response(
        &self,
        task_group: &TaskGroup,
        principal: Principal,
        deadline: Option<RequestDeadline>,
        command: RemotingCommand,
    ) -> TerminalResult {
        self.dispatch_embedded_inner(task_group, principal, deadline, command, true)
            .await
    }

    async fn dispatch_embedded_inner(
        &self,
        task_group: &TaskGroup,
        principal: Principal,
        deadline: Option<RequestDeadline>,
        mut command: RemotingCommand,
        wait_for_deferred_response: bool,
    ) -> TerminalResult {
        let (terminal_sender, mut terminal_receiver) = terminal();
        let request_started = Instant::now();
        let (session_id, original) = match capture_identity(&command) {
            Ok(identity) => identity,
            Err(error) => {
                let _ = terminal_sender.complete(Err(error));
                return terminal_receiver.receive().await;
            }
        };
        let session_record = EmbeddedSessionRecord::new(session_id);
        let embedded_session_view = session_record.view();
        let deferred_cleanup =
            wait_for_deferred_response.then(|| DeferredSessionCleanupOwner::new(embedded_session_view.id()));
        let principal_for_security = principal.clone();
        let context =
            match RequestContext::try_embedded_with_caller(EmbeddedCaller::BrokerProxy, Some(principal), deadline) {
                Ok(context) => context,
                Err(_) => {
                    let error = EmbeddedDispatchError::request_construction(
                        crate::dispatch::remoting_request::RemotingRequestBuildError::MissingEmbeddedAuthentication,
                    );
                    let _ = terminal_sender.complete(Err(error));
                    return terminal_receiver.receive().await;
                }
            };
        let retained_bytes = materialize_and_estimate_remoting_command_retained_bytes(&mut command);
        let lifecycle = RequestLifecycleProvenance::from_embedded_session(&session_record, task_group);
        let mut session_record = Some(session_record);
        let mut deferred_cleanup = deferred_cleanup;
        let builder = RemotingRequestBuilder::new(original, request_started, context, lifecycle, command);
        let ordering = self
            .core
            .explicit_processor()
            .embedded_request_ordering(builder.ingress_view());
        let control = builder.control().clone();
        terminal_receiver.attach_control(control, original.is_one_way());
        let mut admitted_session = None;
        let mut deferred_lifecycle = None;
        let mut deferred_receiver = None;

        if builder.deadline().is_some_and(RequestDeadline::is_expired) {
            match ResponsePlan::command(deadline_response(original.original_opaque())) {
                Ok(plan) => {
                    complete_candidate(
                        self.core.explicit_processor(),
                        builder,
                        original,
                        request_started,
                        reply_candidate(plan),
                        terminal_sender,
                    )
                    .await;
                }
                Err(error) => {
                    let _ = terminal_sender.complete(Err(EmbeddedDispatchError::response_construction(error)));
                }
            }
        } else if let Decision::Deny { reason } = self.boundary.security.authorize_embedded_for_dispatch(
            builder.command(),
            &principal_for_security,
            Resource::new(ResourceKind::Other, original.original_code().to_string()),
            Action::Manage,
        ) {
            match ResponsePlan::command(
                RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::NoPermission,
                    reason.to_string(),
                )
                .set_opaque(original.original_opaque()),
            ) {
                Ok(plan) => {
                    complete_candidate(
                        self.core.explicit_processor(),
                        builder,
                        original,
                        request_started,
                        reply_candidate(plan),
                        terminal_sender,
                    )
                    .await;
                }
                Err(error) => {
                    let _ = terminal_sender.complete(Err(EmbeddedDispatchError::response_construction(error)));
                }
            }
        } else {
            match self
                .boundary
                .admission
                .prepare_embedded_scope(EmbeddedCaller::BrokerProxy, session_id)
            {
                Ok(scope) => match self.boundary.session(task_group, scope) {
                    Ok(session) => {
                        let class = AdmissionClass::for_request_code(original.original_code());
                        let builder = if wait_for_deferred_response && !original.is_one_way() {
                            let (response, receiver) = ResponseSink::local_plan(builder.control().clone());
                            deferred_receiver = Some(receiver);
                            response
                                .local_deferred_seed_with_resume(
                                    self.telemetry.clone(),
                                    &embedded_session_view,
                                    task_group,
                                    ordering,
                                    class,
                                    session.executor.deferred_resume_executor(),
                                )
                                .map(|seed| {
                                    let seed = match deferred_cleanup.as_ref() {
                                        Some(cleanup) => seed.with_session_cleanup(cleanup.registration()),
                                        None => seed,
                                    };
                                    builder.with_deferred_response_seed(seed)
                                })
                                .ok_or(crate::dispatch::remoting_request::RemotingRequestBuildError::DeferredResponseOwnerMismatch)
                        } else {
                            Ok(builder)
                        };
                        match builder {
                            Ok(builder) => {
                                if wait_for_deferred_response {
                                    match (session_record.take(), deferred_cleanup.take()) {
                                        (Some(record), Some(cleanup)) => {
                                            deferred_lifecycle =
                                                Some(EmbeddedDeferredLifecycle::new(session, record, cleanup));
                                            if let Some(lifecycle) = deferred_lifecycle.as_ref() {
                                                self.admit_embedded(
                                                    lifecycle.session(),
                                                    builder,
                                                    ordering,
                                                    retained_bytes,
                                                    original,
                                                    request_started,
                                                    terminal_sender,
                                                    true,
                                                )
                                                .await;
                                            } else {
                                                let _ = terminal_sender
                                                    .complete(Err(EmbeddedDispatchError::completion_closed()));
                                            }
                                        }
                                        _ => {
                                            let _ = terminal_sender
                                                .complete(Err(EmbeddedDispatchError::completion_closed()));
                                        }
                                    }
                                } else {
                                    self.admit_embedded(
                                        &session,
                                        builder,
                                        ordering,
                                        retained_bytes,
                                        original,
                                        request_started,
                                        terminal_sender,
                                        false,
                                    )
                                    .await;
                                    admitted_session = Some(session);
                                }
                            }
                            Err(error) => {
                                let _ =
                                    terminal_sender.complete(Err(EmbeddedDispatchError::request_construction(error)));
                            }
                        }
                    }
                    Err(error) => {
                        let _ = terminal_sender.complete(Err(EmbeddedDispatchError::runtime(error)));
                    }
                },
                Err(error) => {
                    let _ = terminal_sender.complete(Err(EmbeddedDispatchError::admission(error)));
                }
            }
        }

        let initial_result = terminal_receiver.receive().await;
        let result = match initial_result {
            Ok(EmbeddedDispatchOutcome::Deferred { .. }) if wait_for_deferred_response => {
                match deferred_receiver.take() {
                    Some(receiver) => receiver
                        .receive()
                        .await
                        .map(EmbeddedDispatchOutcome::Reply)
                        .map_err(EmbeddedDispatchError::response),
                    None => Err(EmbeddedDispatchError::completion_closed()),
                }
            }
            result => result,
        };
        if let Some(lifecycle) = deferred_lifecycle.as_mut() {
            lifecycle.close();
        }
        let session = deferred_lifecycle
            .as_ref()
            .map(EmbeddedDeferredLifecycle::session)
            .or(admitted_session.as_ref());
        if let Some(session) = session {
            session.begin_close();
            let drain_budget = deadline.map_or(Duration::from_secs(3), RequestDeadline::remaining);
            session
                .drain_until(ShutdownDeadline::after(drain_budget))
                .await
                .log_if_unhealthy();
        }
        drop(deferred_lifecycle);
        drop(session_record);
        result
    }

    async fn admit_embedded(
        &self,
        session: &AuthorizedDispatchSession,
        builder: RemotingRequestBuilder,
        ordering: crate::request_ordering::RequestOrdering,
        retained_bytes: usize,
        original: OriginalRequestIdentity,
        request_started: Instant,
        terminal_sender: EmbeddedTerminalSender,
        commit_deferred: bool,
    ) {
        let admitted_core = Arc::clone(&self.core);
        let rejected_core = Arc::clone(&self.core);
        let rejected_control = builder.control().clone();
        let delayed_rejected_control = rejected_control.clone();
        let rejected_original = original;
        let admitted_terminal = terminal_sender.clone();
        let rejected_terminal = terminal_sender.clone();
        match session.executor.try_execute(
            retained_bytes,
            AdmissionClass::for_request_code(original.original_code()),
            None,
            ordering,
            move |_operation| async move {
                let mut processor = admitted_core.clone_explicit_processor();
                let result = execute_admitted(
                    &admitted_core,
                    &mut processor,
                    builder,
                    original,
                    request_started,
                    commit_deferred,
                )
                .await;
                if admitted_terminal.complete(result).is_err() {
                    admitted_core.report_failure_category("completion_closed");
                }
            },
            move |_operation, error| async move {
                let plan = ResponsePlan::command(admission_response(rejected_original.original_opaque(), &error));
                let result = match plan {
                    Ok(plan) => {
                        let lifecycle_result = finish_rejected_candidate(
                            rejected_core.explicit_processor(),
                            delayed_rejected_control,
                            rejected_original,
                            request_started,
                            reply_candidate(plan),
                        )
                        .await;
                        lifecycle_result
                    }
                    Err(error) => Err(EmbeddedDispatchError::response_construction(error)),
                };
                if rejected_terminal.complete(result).is_err() {
                    rejected_core.report_failure_category("completion_closed");
                }
            },
        ) {
            Ok(_) => {}
            Err(SessionDispatchError::Admission {
                error,
                retained_partial,
            }) if error.policy() == FullPolicy::Reject => {
                drop(retained_partial);
                let plan = ResponsePlan::command(admission_response(original.original_opaque(), &error));
                let result = match plan {
                    Ok(plan) => {
                        finish_rejected_candidate(
                            self.core.explicit_processor(),
                            rejected_control,
                            original,
                            request_started,
                            reply_candidate(plan),
                        )
                        .await
                    }
                    Err(error) => Err(EmbeddedDispatchError::response_construction(error)),
                };
                let _ = terminal_sender.complete(result);
            }
            Err(SessionDispatchError::Admission {
                error,
                retained_partial,
            }) => {
                drop(retained_partial);
                let _ = terminal_sender.complete(Err(EmbeddedDispatchError::admission(error)));
            }
            Err(SessionDispatchError::Closing(error)) => {
                let result =
                    current_stop(&rejected_control).map_or_else(|| Err(EmbeddedDispatchError::runtime(error)), Err);
                let _ = terminal_sender.complete(result);
            }
        }
    }
}

struct EmbeddedDeferredLifecycle {
    session: AuthorizedDispatchSession,
    session_record: EmbeddedSessionRecord,
    cleanup: DeferredSessionCleanupOwner,
    active: bool,
}

impl EmbeddedDeferredLifecycle {
    fn new(
        session: AuthorizedDispatchSession,
        session_record: EmbeddedSessionRecord,
        cleanup: DeferredSessionCleanupOwner,
    ) -> Self {
        Self {
            session,
            session_record,
            cleanup,
            active: true,
        }
    }

    const fn session(&self) -> &AuthorizedDispatchSession {
        &self.session
    }

    fn close(&mut self) {
        if !self.active {
            return;
        }
        let _ = self.cleanup.close();
        self.session_record.close();
        self.session.begin_close();
        self.active = false;
    }
}

impl Drop for EmbeddedDeferredLifecycle {
    fn drop(&mut self) {
        self.close();
    }
}

fn capture_identity(command: &RemotingCommand) -> Result<(u64, OriginalRequestIdentity), EmbeddedDispatchError> {
    let session_id = crate::dispatch::reserve_session_owner().ok_or_else(EmbeddedDispatchError::identity_exhausted)?;
    let sequence = AtomicU64::new(1);
    let original = OriginalRequestIdentity::capture(session_id, &sequence, command)
        .ok_or_else(EmbeddedDispatchError::identity_exhausted)?;
    Ok((session_id, original))
}

async fn execute_admitted<P>(
    core: &super::AuthorizedDispatcherCore<ExplicitProcessor<P>>,
    processor: &mut ExplicitProcessor<P>,
    builder: RemotingRequestBuilder,
    original: OriginalRequestIdentity,
    request_started: Instant,
    commit_deferred: bool,
) -> TerminalResult
where
    P: RequestProcessor + Clone + Sync + 'static,
{
    if builder.deadline().is_some_and(RequestDeadline::is_expired) {
        let plan = ResponsePlan::command(deadline_response(original.original_opaque()))
            .map_err(EmbeddedDispatchError::response_construction)?;
        let result = finish_rejected_candidate(
            processor,
            builder.control().clone(),
            original,
            request_started,
            InternalProcessorCandidate::failure(
                InternalProcessorOutcome::Handled(crate::dispatch::HandlerOutcome::Reply(plan)),
                InternalFailureOrigin::Deadline,
            ),
        )
        .await;
        if matches!(&result, Ok(EmbeddedDispatchOutcome::OneWay { .. })) {
            core.report_failure_category(InternalFailureOrigin::Deadline.category());
        }
        return result;
    }
    if let Some(candidate) = processor.embedded_reject_request(original.original_code()) {
        return finish_candidate(processor, builder, original, request_started, candidate).await;
    }
    let mut request = builder.build().map_err(EmbeddedDispatchError::request_construction)?;
    let candidate = processor
        .process_embedded(&mut request)
        .await
        .map_err(map_processor_error)?;
    let InternalProcessorCandidate { outcome, failure } = candidate;
    let outcome = processor
        .resolve_embedded_outcome(&mut request, outcome)
        .map_err(map_resolve_error)?;
    if matches!(&outcome, EmbeddedResolvedOutcome::OneWay) {
        if let Some(failure) = failure {
            core.report_failure_category(failure.category());
        }
    }
    finish_resolved(
        processor,
        request.control().clone(),
        original,
        request_started,
        outcome,
        commit_deferred,
    )
    .await
}

async fn complete_candidate<P>(
    processor: &ExplicitProcessor<P>,
    builder: RemotingRequestBuilder,
    original: OriginalRequestIdentity,
    request_started: Instant,
    candidate: InternalProcessorCandidate,
    sender: EmbeddedTerminalSender,
) where
    P: RequestProcessor + Clone + Sync + 'static,
{
    let result = finish_candidate(processor, builder, original, request_started, candidate).await;
    let _ = sender.complete(result);
}

async fn finish_candidate<P>(
    processor: &ExplicitProcessor<P>,
    builder: RemotingRequestBuilder,
    original: OriginalRequestIdentity,
    request_started: Instant,
    candidate: InternalProcessorCandidate,
) -> TerminalResult
where
    P: RequestProcessor + Clone + Sync + 'static,
{
    let control = builder.control().clone();
    let mut request = builder.build().map_err(EmbeddedDispatchError::request_construction)?;
    let outcome = processor
        .resolve_embedded_outcome(&mut request, candidate.outcome)
        .map_err(map_resolve_error)?;
    finish_resolved(processor, control, original, request_started, outcome, false).await
}

async fn finish_rejected_candidate<P>(
    processor: &ExplicitProcessor<P>,
    control: RequestControlView,
    original: OriginalRequestIdentity,
    request_started: Instant,
    candidate: InternalProcessorCandidate,
) -> TerminalResult
where
    P: RequestProcessor + Clone + Sync + 'static,
{
    let InternalProcessorOutcome::Handled(crate::dispatch::HandlerOutcome::Reply(plan)) = candidate.outcome else {
        return Err(EmbeddedDispatchError::one_way_contract("invalid_rejection"));
    };
    let outcome = if original.is_one_way() {
        drop(plan);
        EmbeddedResolvedOutcome::OneWay
    } else {
        EmbeddedResolvedOutcome::Reply(plan)
    };
    finish_resolved(processor, control, original, request_started, outcome, false).await
}

async fn finish_resolved<P>(
    processor: &ExplicitProcessor<P>,
    control: RequestControlView,
    original: OriginalRequestIdentity,
    request_started: Instant,
    outcome: EmbeddedResolvedOutcome,
    commit_deferred: bool,
) -> TerminalResult
where
    P: RequestProcessor + Clone + Sync + 'static,
{
    match outcome {
        EmbeddedResolvedOutcome::OneWay => Ok(EmbeddedDispatchOutcome::OneWay {
            request_id: original.request_id(),
        }),
        EmbeddedResolvedOutcome::Deferred(registration) => {
            let request_id = registration.request_id();
            if commit_deferred {
                registration.commit().map_err(EmbeddedDispatchError::deferred_commit)?;
            }
            Ok(EmbeddedDispatchOutcome::Deferred { request_id })
        }
        EmbeddedResolvedOutcome::NoReply(marker) => Ok(EmbeddedDispatchOutcome::NoReply {
            request_id: marker.request_id(),
            reason: marker.reason(),
        }),
        EmbeddedResolvedOutcome::Reply(plan) => {
            let response_code = plan.response_code();
            let body_kind = plan.body_kind();
            let bound = plan.bind(original).map_err(EmbeddedDispatchError::response_binding)?;
            let (sink, receiver) = ResponseSink::local_plan(control);
            let write_started = Instant::now();
            let send_result = sink.send_plan(bound).await;
            let failure = send_result
                .as_ref()
                .err()
                .map(|error| (error.kind(), error.write_progress()));
            processor.observe_embedded_response(
                original,
                response_code,
                body_kind,
                write_started.elapsed(),
                request_started.elapsed(),
                send_result,
            );
            if let Some((_kind, _progress)) = failure {
                return receiver
                    .receive()
                    .await
                    .map(EmbeddedDispatchOutcome::Reply)
                    .map_err(EmbeddedDispatchError::response);
            }
            receiver
                .receive()
                .await
                .map(EmbeddedDispatchOutcome::Reply)
                .map_err(EmbeddedDispatchError::response)
        }
    }
}

fn reply_candidate(plan: ResponsePlan) -> InternalProcessorCandidate {
    InternalProcessorCandidate::success(InternalProcessorOutcome::Handled(
        crate::dispatch::HandlerOutcome::Reply(plan),
    ))
}

fn map_processor_error(error: crate::dispatch::DispatchProcessorError) -> EmbeddedDispatchError {
    match error {
        crate::dispatch::DispatchProcessorError::ResponsePlan(error) => {
            EmbeddedDispatchError::response_construction(error)
        }
        crate::dispatch::DispatchProcessorError::HandlerContract(error) => {
            EmbeddedDispatchError::handler_contract(error)
        }
    }
}

fn map_resolve_error(error: EmbeddedProcessorResolveError) -> EmbeddedDispatchError {
    match error {
        EmbeddedProcessorResolveError::HandlerContract(error) => EmbeddedDispatchError::handler_contract(error),
        EmbeddedProcessorResolveError::OneWayContract { outcome } => EmbeddedDispatchError::one_way_contract(outcome),
    }
}

fn current_stop(control: &RequestControlView) -> Option<EmbeddedDispatchError> {
    if control.parent_is_cancelled() {
        Some(EmbeddedDispatchError::cancelled())
    } else if control.session_is_closed() {
        Some(EmbeddedDispatchError::session_closed())
    } else if control.deadline().is_some_and(RequestDeadline::is_expired) {
        Some(EmbeddedDispatchError::deadline_exceeded())
    } else {
        None
    }
}

fn receiver_stop(control: &RequestControlView, original_one_way: bool) -> Option<EmbeddedDispatchError> {
    if control.parent_is_cancelled() {
        Some(EmbeddedDispatchError::cancelled())
    } else if control.session_is_closed() {
        Some(EmbeddedDispatchError::session_closed())
    } else if !original_one_way && control.deadline().is_some_and(RequestDeadline::is_expired) {
        Some(EmbeddedDispatchError::deadline_exceeded())
    } else {
        None
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/dispatch/embedded_dispatcher.rs"]
mod tests;
