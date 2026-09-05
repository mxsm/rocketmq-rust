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

//! Consuming network and embedded response delivery.

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tokio::sync::oneshot;

use super::LocalResponseSink;
use super::ResponseSink;
use crate::admission::AdmissionClass;
use crate::codec::prepare_response;
use crate::dispatch::BoundResponse;
use crate::dispatch::RemotingResponse;
use crate::dispatch::RequestControlView;
use crate::dispatch::ResponseCompletionOutcome;
use crate::dispatch::ResponseDisposition;
use crate::dispatch::ResponseOperationalFailure;
use crate::dispatch::ResponseReceipt;
use crate::dispatch::ResponseSendClaim;
use crate::dispatch::ResponseSendOutcome;
use crate::dispatch::ResponseTerminalState;
use crate::dispatch::WriteProgress;
use crate::server::SessionHandle;

#[derive(Clone)]
pub(crate) struct ResponseDeliveryContext {
    control: RequestControlView,
    slot: Arc<ResponseCompletionSlot>,
    transport_drop: ResponseTransportDropHandle,
    #[cfg(test)]
    enqueue_gate: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
    #[cfg(test)]
    enqueue_complete_signal: Option<Arc<tokio::sync::Notify>>,
}

impl ResponseDeliveryContext {
    fn new(control: RequestControlView) -> Self {
        let slot = Arc::new(ResponseCompletionSlot::new());
        Self {
            control,
            transport_drop: ResponseTransportDropHandle::new(Arc::clone(&slot)),
            slot,
            #[cfg(test)]
            enqueue_gate: None,
            #[cfg(test)]
            enqueue_complete_signal: None,
        }
    }

    pub(crate) fn control(&self) -> &RequestControlView {
        &self.control
    }

    fn slot(&self) -> &Arc<ResponseCompletionSlot> {
        &self.slot
    }

    pub(crate) fn transport_drop_handle(&self) -> ResponseTransportDropHandle {
        self.transport_drop.clone()
    }

    pub(crate) fn same_lifecycle_owner(&self, session: &SessionHandle) -> bool {
        self.control
            .same_lifecycle_owner(session.session_view().state(), session.task_group())
    }

    #[cfg(test)]
    fn with_enqueue_gate(mut self, checked: Arc<tokio::sync::Notify>, resume: Arc<tokio::sync::Notify>) -> Self {
        self.enqueue_gate = Some((checked, resume));
        self
    }

    #[cfg(test)]
    fn with_enqueue_complete_signal(mut self, signal: Arc<tokio::sync::Notify>) -> Self {
        self.enqueue_complete_signal = Some(signal);
        self
    }
}

pub(super) struct LocalResponseSenderState {
    sender: parking_lot::Mutex<Option<oneshot::Sender<Result<RemotingResponse, ResponseCompletionOutcome>>>>,
    sender_taken: Arc<AtomicBool>,
    control: RequestControlView,
    slot: Arc<ResponseCompletionSlot>,
    #[cfg(test)]
    handoff_gate: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
    #[cfg(test)]
    handoff_attempts: Arc<std::sync::atomic::AtomicUsize>,
}

impl LocalResponseSenderState {
    fn new(
        sender: oneshot::Sender<Result<RemotingResponse, ResponseCompletionOutcome>>,
        sender_taken: Arc<AtomicBool>,
        control: RequestControlView,
        slot: Arc<ResponseCompletionSlot>,
    ) -> Self {
        Self {
            sender: parking_lot::Mutex::new(Some(sender)),
            sender_taken,
            control,
            slot,
            #[cfg(test)]
            handoff_gate: None,
            #[cfg(test)]
            handoff_attempts: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    #[cfg(test)]
    fn with_handoff_gate(
        mut self,
        checked: Arc<tokio::sync::Notify>,
        resume: Arc<tokio::sync::Notify>,
        attempts: Arc<std::sync::atomic::AtomicUsize>,
    ) -> Self {
        self.handoff_gate = Some((checked, resume));
        self.handoff_attempts = attempts;
        self
    }

    fn take_sender(&self) -> Option<oneshot::Sender<Result<RemotingResponse, ResponseCompletionOutcome>>> {
        let sender = self.sender.lock().take();
        if sender.is_some() {
            self.sender_taken.store(true, Ordering::Release);
        }
        sender
    }

    pub(super) const fn control(&self) -> &RequestControlView {
        &self.control
    }

    pub(super) fn close_last_sender(&self) {
        let Some(sender) = self.take_sender() else {
            return;
        };
        if self.slot.close_if_open(ResponseTerminalState::Closed) {
            let _ = sender.send(Err(ResponseCompletionOutcome::SessionClosed));
        }
    }
}

/// Single owner of an exact in-process response handoff.
pub(crate) struct LocalResponseReceiver {
    receiver: oneshot::Receiver<Result<RemotingResponse, ResponseCompletionOutcome>>,
    control: RequestControlView,
    slot: Arc<ResponseCompletionSlot>,
    sender_taken: Arc<AtomicBool>,
}

impl LocalResponseReceiver {
    pub(crate) async fn receive(mut self) -> Result<RemotingResponse, ResponseCompletionOutcome> {
        if let Some(stop) = current_stop(&self.control) {
            self.slot.finish_external(stop);
            return Err(stop.into_outcome());
        }

        let result = tokio::select! {
            biased;
            () = self.control.cancelled() => {
                let stop = current_stop(&self.control).unwrap_or(ResponseStop::Cancelled);
                self.slot.finish_external(stop);
                return Err(stop.into_outcome());
            }
            result = &mut self.receiver => result,
        };
        result.map_err(|_| {
            self.slot.finish_external(ResponseStop::SessionClosed);
            ResponseCompletionOutcome::SessionClosed
        })?
    }
}

impl Drop for LocalResponseReceiver {
    fn drop(&mut self) {
        if !self.sender_taken.load(Ordering::Acquire) {
            self.slot.close_from_receiver_drop();
        }
    }
}

impl ResponseSink {
    pub(crate) fn network(
        session: SessionHandle,
        admission_class: AdmissionClass,
        control: RequestControlView,
    ) -> Self {
        let context = ResponseDeliveryContext::new(control);
        Self::Network(Arc::new(
            session
                .with_response_class(admission_class)
                .with_response_context(context),
        ))
    }

    pub(crate) fn local(control: RequestControlView) -> (Self, LocalResponseReceiver) {
        let (sender, receiver) = oneshot::channel();
        let slot = Arc::new(ResponseCompletionSlot::new());
        let sender_taken = Arc::new(AtomicBool::new(false));
        let state = Arc::new(LocalResponseSenderState::new(
            sender,
            Arc::clone(&sender_taken),
            control.clone(),
            Arc::clone(&slot),
        ));
        let sink = Self::Local(LocalResponseSink { state });
        let receiver = LocalResponseReceiver {
            receiver,
            control,
            slot,
            sender_taken,
        };
        (sink, receiver)
    }

    #[cfg(test)]
    pub(in crate::dispatch) fn local_with_handoff_gate(
        control: RequestControlView,
        checked: Arc<tokio::sync::Notify>,
        resume: Arc<tokio::sync::Notify>,
    ) -> (Self, LocalResponseReceiver, Arc<std::sync::atomic::AtomicUsize>) {
        let (sender, receiver) = oneshot::channel();
        let slot = Arc::new(ResponseCompletionSlot::new());
        let sender_taken = Arc::new(AtomicBool::new(false));
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let state = Arc::new(
            LocalResponseSenderState::new(sender, Arc::clone(&sender_taken), control.clone(), Arc::clone(&slot))
                .with_handoff_gate(checked, resume, Arc::clone(&attempts)),
        );
        let sink = Self::Local(LocalResponseSink { state });
        let receiver = LocalResponseReceiver {
            receiver,
            control,
            slot,
            sender_taken,
        };
        (sink, receiver, attempts)
    }

    #[cfg(test)]
    fn network_with_enqueue_gate(
        session: SessionHandle,
        admission_class: AdmissionClass,
        control: RequestControlView,
        checked: Arc<tokio::sync::Notify>,
        resume: Arc<tokio::sync::Notify>,
    ) -> Self {
        let context = ResponseDeliveryContext::new(control).with_enqueue_gate(checked, resume);
        Self::Network(Arc::new(
            session
                .with_response_class(admission_class)
                .with_response_context(context),
        ))
    }

    #[cfg(test)]
    fn network_with_enqueue_observer(
        session: SessionHandle,
        admission_class: AdmissionClass,
        control: RequestControlView,
        signal: Arc<tokio::sync::Notify>,
    ) -> Self {
        let context = ResponseDeliveryContext::new(control).with_enqueue_complete_signal(signal);
        Self::Network(Arc::new(
            session
                .with_response_class(admission_class)
                .with_response_context(context),
        ))
    }

    pub(crate) async fn send_response(
        self,
        bound: BoundResponse,
    ) -> Result<ResponseCompletionOutcome, ResponseOperationalFailure> {
        match self {
            Self::Network(session) => send_network_response(session, bound).await,
            Self::Local(sink) => send_local_response(sink, bound).await,
        }
    }

    pub(crate) async fn send_deferred_response(
        self,
        bound: BoundResponse,
        deferred_claim: &mut ResponseSendClaim,
    ) -> Result<ResponseCompletionOutcome, ResponseOperationalFailure> {
        match self {
            Self::Network(session) => send_deferred_network(session, bound, deferred_claim).await,
            Self::Local(sink) => send_deferred_local(sink, bound).await,
        }
    }
}

async fn send_deferred_network(
    session: Arc<SessionHandle>,
    bound: BoundResponse,
    deferred_claim: &mut ResponseSendClaim,
) -> Result<ResponseCompletionOutcome, ResponseOperationalFailure> {
    let Some(context) = session.response_context() else {
        return Ok(ResponseCompletionOutcome::SessionClosed);
    };
    let mut response_claim = match context.slot().claim().await {
        Ok(claim) => claim,
        Err(outcome) => return Ok(outcome),
    };
    let response_drop = context.transport_drop_handle();
    response_claim.observe_transport_drop(response_drop.clone());
    let deferred_drop = deferred_claim.observe_transport_drop(response_drop.delegation_token());
    let request_id = bound.request_id();
    if let Some(stop) = current_stop(context.control()) {
        response_claim.finish(stop.terminal());
        return Ok(stop.into_outcome());
    }

    let mut connection = session.connection();
    #[cfg(test)]
    if let Some((checked, resume)) = &context.enqueue_gate {
        connection.set_enqueue_gate(Arc::clone(checked), Arc::clone(resume));
    }
    #[cfg(test)]
    if let Some(signal) = &context.enqueue_complete_signal {
        connection.set_enqueue_complete_signal(Arc::clone(signal));
    }
    let prepared = match prepare_response(bound, connection.frame_limits()) {
        Ok(prepared) => prepared,
        Err(error) => {
            response_claim.finish(terminal_for_operational_failure(&error));
            return Err(error);
        }
    };
    let metadata = *prepared.metadata();
    debug_assert_eq!(metadata.request_id(), request_id);
    if let Some(stop) = current_stop(context.control()) {
        response_claim.finish(stop.terminal());
        return Ok(stop.into_outcome());
    }

    let send = connection
        .send_prepared_deferred_response(prepared, context.control(), deferred_drop)
        .await;
    match send {
        ResponseSendOutcome::Written => {
            response_claim.finish(ResponseTerminalState::Completed);
            Ok(ResponseCompletionOutcome::Completed(ResponseReceipt::new(
                metadata.request_id(),
                ResponseDisposition::TransportWritten,
            )))
        }
        ResponseSendOutcome::Rejected(outcome) => {
            response_claim.finish(terminal_for_outcome(outcome));
            Ok(outcome)
        }
        ResponseSendOutcome::OperationalFailure(error) => {
            response_claim.finish(terminal_for_operational_failure(&error));
            Err(error)
        }
    }
}

async fn send_deferred_local(
    sink: LocalResponseSink,
    bound: BoundResponse,
) -> Result<ResponseCompletionOutcome, ResponseOperationalFailure> {
    send_local_response(sink, bound).await
}

async fn send_network_response(
    session: Arc<SessionHandle>,
    bound: BoundResponse,
) -> Result<ResponseCompletionOutcome, ResponseOperationalFailure> {
    let Some(context) = session.response_context() else {
        return Ok(ResponseCompletionOutcome::SessionClosed);
    };
    let mut claim = match context.slot().claim().await {
        Ok(claim) => claim,
        Err(outcome) => return Ok(outcome),
    };
    claim.observe_transport_drop(context.transport_drop_handle());
    let request_id = bound.request_id();
    if let Some(stop) = current_stop(context.control()) {
        claim.finish(stop.terminal());
        return Ok(stop.into_outcome());
    }

    let mut connection = session.connection();
    #[cfg(test)]
    if let Some((checked, resume)) = &context.enqueue_gate {
        connection.set_enqueue_gate(Arc::clone(checked), Arc::clone(resume));
    }
    #[cfg(test)]
    if let Some(signal) = &context.enqueue_complete_signal {
        connection.set_enqueue_complete_signal(Arc::clone(signal));
    }
    let prepared = match prepare_response(bound, connection.frame_limits()) {
        Ok(prepared) => prepared,
        Err(error) => {
            claim.finish(terminal_for_operational_failure(&error));
            return Err(error);
        }
    };
    let metadata = *prepared.metadata();
    debug_assert_eq!(metadata.request_id(), request_id);
    if let Some(stop) = current_stop(context.control()) {
        claim.finish(stop.terminal());
        return Ok(stop.into_outcome());
    }

    match connection.send_prepared_response(prepared, context.control()).await {
        ResponseSendOutcome::Written => {
            claim.finish(ResponseTerminalState::Completed);
            Ok(ResponseCompletionOutcome::Completed(ResponseReceipt::new(
                metadata.request_id(),
                ResponseDisposition::TransportWritten,
            )))
        }
        ResponseSendOutcome::Rejected(outcome) => {
            claim.finish(terminal_for_outcome(outcome));
            Ok(outcome)
        }
        ResponseSendOutcome::OperationalFailure(error) => {
            claim.finish(terminal_for_operational_failure(&error));
            Err(error)
        }
    }
}

async fn send_local_response(
    sink: LocalResponseSink,
    bound: BoundResponse,
) -> Result<ResponseCompletionOutcome, ResponseOperationalFailure> {
    let state = Arc::clone(&sink.state);
    let claim = match state.slot.claim().await {
        Ok(claim) => claim,
        Err(outcome) => return Ok(outcome),
    };
    #[cfg(test)]
    if let Some((checked, resume)) = &state.handoff_gate {
        checked.notify_one();
        resume.notified().await;
    }
    if let Some(stop) = current_stop(&state.control) {
        return Ok(claim.finish_stop(stop));
    }

    let (request_id, head, body) = bound.into_parts();
    let response = RemotingResponse::from_bound_parts(head, body);
    match claim.commit_local_handoff(&state, response) {
        Ok(()) => Ok(ResponseCompletionOutcome::Completed(ResponseReceipt::new(
            request_id,
            ResponseDisposition::InProcessAccepted,
        ))),
        Err(outcome) => Ok(outcome),
    }
}

#[derive(Clone, Copy)]
enum ResponseStop {
    Cancelled,
    SessionClosed,
    DeadlineExceeded,
}

impl ResponseStop {
    fn into_outcome(self) -> ResponseCompletionOutcome {
        match self {
            Self::Cancelled => ResponseCompletionOutcome::Cancelled,
            Self::SessionClosed => ResponseCompletionOutcome::SessionClosed,
            Self::DeadlineExceeded => ResponseCompletionOutcome::DeadlineExpired,
        }
    }

    fn terminal(self) -> ResponseTerminalState {
        match self {
            Self::Cancelled => ResponseTerminalState::Cancelled,
            Self::SessionClosed => ResponseTerminalState::Closed,
            Self::DeadlineExceeded => ResponseTerminalState::Failed {
                progress: WriteProgress::NotStarted,
            },
        }
    }
}

fn current_stop(control: &RequestControlView) -> Option<ResponseStop> {
    if control.parent_is_cancelled() {
        Some(ResponseStop::Cancelled)
    } else if control.session_is_closed() {
        Some(ResponseStop::SessionClosed)
    } else if control
        .deadline()
        .is_some_and(crate::deadline::RequestDeadline::is_expired)
    {
        Some(ResponseStop::DeadlineExceeded)
    } else {
        None
    }
}

fn terminal_for_outcome(outcome: ResponseCompletionOutcome) -> ResponseTerminalState {
    match outcome {
        ResponseCompletionOutcome::Completed(_) => ResponseTerminalState::Completed,
        ResponseCompletionOutcome::AlreadyCompleted(state) => state,
        ResponseCompletionOutcome::Cancelled => ResponseTerminalState::Cancelled,
        ResponseCompletionOutcome::SessionClosed => ResponseTerminalState::Closed,
        ResponseCompletionOutcome::DeadlineExpired | ResponseCompletionOutcome::QueueSaturated => {
            ResponseTerminalState::Failed {
                progress: WriteProgress::NotStarted,
            }
        }
    }
}

fn terminal_for_operational_failure(error: &ResponseOperationalFailure) -> ResponseTerminalState {
    ResponseTerminalState::Failed {
        progress: error.write_progress(),
    }
}

enum ResponseCompletionState {
    Open,
    Claimed,
    Terminal {
        state: ResponseTerminalState,
        primary_session_closed: bool,
        claimant_stop: Option<ResponseStop>,
    },
}

struct ResponseCompletionSlot {
    state: parking_lot::Mutex<ResponseCompletionState>,
    changed: tokio::sync::Notify,
}

#[derive(Clone)]
pub(crate) struct ResponseTransportDropHandle {
    slot: Arc<ResponseCompletionSlot>,
    delegated: Arc<AtomicBool>,
}

impl ResponseTransportDropHandle {
    fn new(slot: Arc<ResponseCompletionSlot>) -> Self {
        Self {
            slot,
            delegated: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn delegate(&self) {
        self.delegated.store(true, Ordering::Release);
    }

    pub(crate) fn resume_outer(&self) {
        self.delegated.store(false, Ordering::Release);
    }

    pub(crate) fn delegation_token(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.delegated)
    }

    fn is_delegated(&self) -> bool {
        self.delegated.load(Ordering::Acquire)
    }

    pub(crate) fn finish_dropped(&self, progress: WriteProgress) {
        self.slot.finish_claim(ResponseTerminalState::Failed { progress });
    }
}

impl ResponseCompletionSlot {
    fn new() -> Self {
        Self {
            state: parking_lot::Mutex::new(ResponseCompletionState::Open),
            changed: tokio::sync::Notify::new(),
        }
    }

    async fn claim(self: &Arc<Self>) -> Result<ResponseClaim, ResponseCompletionOutcome> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut state = self.state.lock();
                match &mut *state {
                    ResponseCompletionState::Open => {
                        *state = ResponseCompletionState::Claimed;
                        return Ok(ResponseClaim {
                            slot: Some(Arc::clone(self)),
                            drop_state: ResponseTerminalState::Failed {
                                progress: WriteProgress::NotStarted,
                            },
                            transport_drop: None,
                        });
                    }
                    ResponseCompletionState::Claimed => {}
                    ResponseCompletionState::Terminal {
                        state,
                        primary_session_closed,
                        claimant_stop,
                    } => {
                        if claimant_stop.is_none() {
                            if *primary_session_closed {
                                *primary_session_closed = false;
                                return Err(ResponseCompletionOutcome::SessionClosed);
                            }
                            return Err(ResponseCompletionOutcome::AlreadyCompleted(*state));
                        }
                    }
                }
            }
            changed.await;
        }
    }

    fn finish_claim(&self, terminal: ResponseTerminalState) {
        let mut state = self.state.lock();
        if matches!(*state, ResponseCompletionState::Claimed) {
            *state = ResponseCompletionState::Terminal {
                state: terminal,
                primary_session_closed: false,
                claimant_stop: None,
            };
            drop(state);
            self.changed.notify_waiters();
        }
    }

    fn finish_external(&self, stop: ResponseStop) {
        let mut state = self.state.lock();
        let claimant_stop = matches!(*state, ResponseCompletionState::Claimed).then_some(stop);
        if !matches!(*state, ResponseCompletionState::Terminal { .. }) {
            *state = ResponseCompletionState::Terminal {
                state: stop.terminal(),
                primary_session_closed: false,
                claimant_stop,
            };
            drop(state);
            self.changed.notify_waiters();
        }
    }

    fn close_from_receiver_drop(&self) {
        let mut state = self.state.lock();
        let claimed = matches!(*state, ResponseCompletionState::Claimed);
        if !matches!(*state, ResponseCompletionState::Terminal { .. }) {
            *state = ResponseCompletionState::Terminal {
                state: ResponseTerminalState::Closed,
                primary_session_closed: !claimed,
                claimant_stop: claimed.then_some(ResponseStop::SessionClosed),
            };
            drop(state);
            self.changed.notify_waiters();
        }
    }

    fn close_if_open(&self, terminal: ResponseTerminalState) -> bool {
        let mut state = self.state.lock();
        if matches!(*state, ResponseCompletionState::Open) {
            *state = ResponseCompletionState::Terminal {
                state: terminal,
                primary_session_closed: false,
                claimant_stop: None,
            };
            drop(state);
            self.changed.notify_waiters();
            true
        } else {
            false
        }
    }

    fn finish_claim_stop(&self, stop: ResponseStop) -> ResponseCompletionOutcome {
        let mut state = self.state.lock();
        let (error, changed) = match &mut *state {
            ResponseCompletionState::Claimed => {
                *state = ResponseCompletionState::Terminal {
                    state: stop.terminal(),
                    primary_session_closed: false,
                    claimant_stop: None,
                };
                (stop.into_outcome(), true)
            }
            ResponseCompletionState::Terminal {
                state,
                primary_session_closed,
                claimant_stop,
            } => {
                if let Some(external_stop) = claimant_stop.take() {
                    (external_stop.into_outcome(), true)
                } else if *primary_session_closed {
                    *primary_session_closed = false;
                    (ResponseCompletionOutcome::SessionClosed, true)
                } else {
                    (ResponseCompletionOutcome::AlreadyCompleted(*state), false)
                }
            }
            ResponseCompletionState::Open => {
                *state = ResponseCompletionState::Terminal {
                    state: stop.terminal(),
                    primary_session_closed: false,
                    claimant_stop: None,
                };
                (stop.into_outcome(), true)
            }
        };
        drop(state);
        if changed {
            self.changed.notify_waiters();
        }
        error
    }

    fn commit_local_handoff(
        &self,
        sender_state: &LocalResponseSenderState,
        response: RemotingResponse,
    ) -> Result<(), ResponseCompletionOutcome> {
        let mut state = self.state.lock();
        let (result, changed) = match &mut *state {
            ResponseCompletionState::Claimed => {
                if let Some(sender) = sender_state.take_sender() {
                    #[cfg(test)]
                    sender_state.handoff_attempts.fetch_add(1, Ordering::SeqCst);
                    let result = if sender.send(Ok(response)).is_ok() {
                        *state = ResponseCompletionState::Terminal {
                            state: ResponseTerminalState::Completed,
                            primary_session_closed: false,
                            claimant_stop: None,
                        };
                        Ok(())
                    } else {
                        *state = ResponseCompletionState::Terminal {
                            state: ResponseTerminalState::Closed,
                            primary_session_closed: false,
                            claimant_stop: None,
                        };
                        Err(ResponseCompletionOutcome::SessionClosed)
                    };
                    (result, true)
                } else {
                    *state = ResponseCompletionState::Terminal {
                        state: ResponseTerminalState::Closed,
                        primary_session_closed: false,
                        claimant_stop: None,
                    };
                    (Err(ResponseCompletionOutcome::SessionClosed), true)
                }
            }
            ResponseCompletionState::Terminal {
                state,
                primary_session_closed,
                claimant_stop,
            } => {
                if let Some(stop) = claimant_stop.take() {
                    (Err(stop.into_outcome()), true)
                } else if *primary_session_closed {
                    *primary_session_closed = false;
                    (Err(ResponseCompletionOutcome::SessionClosed), true)
                } else {
                    (Err(ResponseCompletionOutcome::AlreadyCompleted(*state)), false)
                }
            }
            ResponseCompletionState::Open => {
                *state = ResponseCompletionState::Terminal {
                    state: ResponseTerminalState::Closed,
                    primary_session_closed: false,
                    claimant_stop: None,
                };
                (Err(ResponseCompletionOutcome::SessionClosed), true)
            }
        };
        drop(state);
        if changed {
            self.changed.notify_waiters();
        }
        result
    }

    fn abandon_claim(&self, terminal: ResponseTerminalState) {
        let mut state = self.state.lock();
        let changed = match &mut *state {
            ResponseCompletionState::Claimed => {
                *state = ResponseCompletionState::Terminal {
                    state: terminal,
                    primary_session_closed: false,
                    claimant_stop: None,
                };
                true
            }
            ResponseCompletionState::Terminal { claimant_stop, .. } if claimant_stop.is_some() => {
                *claimant_stop = None;
                true
            }
            ResponseCompletionState::Open | ResponseCompletionState::Terminal { .. } => false,
        };
        drop(state);
        if changed {
            self.changed.notify_waiters();
        }
    }
}

struct ResponseClaim {
    slot: Option<Arc<ResponseCompletionSlot>>,
    drop_state: ResponseTerminalState,
    transport_drop: Option<ResponseTransportDropHandle>,
}

impl ResponseClaim {
    fn observe_transport_drop(&mut self, transport_drop: ResponseTransportDropHandle) {
        self.transport_drop = Some(transport_drop);
    }

    fn finish(mut self, terminal: ResponseTerminalState) {
        if let Some(slot) = self.slot.take() {
            slot.finish_claim(terminal);
        }
    }

    fn finish_stop(mut self, stop: ResponseStop) -> ResponseCompletionOutcome {
        self.slot
            .take()
            .map_or_else(|| stop.into_outcome(), |slot| slot.finish_claim_stop(stop))
    }

    fn commit_local_handoff(
        mut self,
        sender_state: &LocalResponseSenderState,
        response: RemotingResponse,
    ) -> Result<(), ResponseCompletionOutcome> {
        self.slot.take().map_or_else(
            || Err(ResponseCompletionOutcome::SessionClosed),
            |slot| slot.commit_local_handoff(sender_state, response),
        )
    }
}

impl Drop for ResponseClaim {
    fn drop(&mut self) {
        if self
            .transport_drop
            .as_ref()
            .is_some_and(ResponseTransportDropHandle::is_delegated)
        {
            return;
        }
        if let Some(slot) = self.slot.take() {
            slot.abandon_claim(self.drop_state);
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/dispatch/response_sink/delivery.rs"]
mod tests;
