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

use std::sync::Arc;

use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_transport::api::v2::RequestControlView;
use rocketmq_transport::api::v2::ResponsePlan;
use rocketmq_transport::api::v2::ResponsePlanError;
use tokio::sync::OwnedSemaphorePermit;

use crate::latency::broker_fast_failure::BrokerFastFailure;
use crate::latency::broker_fast_failure::FastFailureQueueKind;
use crate::latency::broker_fast_failure::FastFailureTask;

#[derive(Clone, Copy)]
pub(super) struct FastFailureRequestMetadata {
    opaque: i32,
    retained_bytes: usize,
}

impl FastFailureRequestMetadata {
    pub(super) fn from_command(request: &RemotingCommand) -> Self {
        Self {
            opaque: request.opaque(),
            retained_bytes: estimate_retained_bytes(request),
        }
    }
}

pub(super) enum FastFailureControl<'a> {
    Legacy,
    Request(&'a RequestControlView),
    #[cfg(test)]
    CancelOnSecondCheck(&'a std::sync::atomic::AtomicUsize),
}

impl FastFailureControl<'_> {
    pub(super) const fn legacy() -> Self {
        Self::Legacy
    }

    #[cfg(test)]
    const fn cancel_on_second_check(checks: &std::sync::atomic::AtomicUsize) -> FastFailureControl<'_> {
        FastFailureControl::CancelOnSecondCheck(checks)
    }

    fn is_cancelled(&self) -> bool {
        match self {
            Self::Legacy => false,
            Self::Request(control) => control.is_cancelled(),
            #[cfg(test)]
            Self::CancelOnSecondCheck(checks) => checks.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 1,
        }
    }

    async fn cancelled(&self) {
        match self {
            Self::Request(control) => control.cancelled().await,
            Self::Legacy => std::future::pending().await,
            #[cfg(test)]
            Self::CancelOnSecondCheck(_) => std::future::pending().await,
        }
    }
}

impl<'a> From<&'a RequestControlView> for FastFailureControl<'a> {
    fn from(control: &'a RequestControlView) -> Self {
        Self::Request(control)
    }
}

pub(super) struct FastFailureAdmission {
    service: BrokerFastFailure,
    queue_kind: FastFailureQueueKind,
    task: Arc<FastFailureTask>,
    response_rx: Option<tokio::sync::oneshot::Receiver<Option<RemotingCommand>>>,
    opaque: i32,
    armed: bool,
}

impl FastFailureAdmission {
    pub(super) async fn await_run(
        mut self,
        control: FastFailureControl<'_>,
    ) -> Result<FastFailureRunGuard, FastFailureAwaitError> {
        let Some(mut response_rx) = self.response_rx.take() else {
            self.armed = false;
            return Err(FastFailureAwaitError::Rejected(self.missing_response_rejection(
                FastFailureRejectionKind::Internal,
                "fast failure admission lost its response receiver before dispatch",
            )));
        };
        if control.is_cancelled() {
            self.cancel_for_control();
            self.armed = false;
            drop(response_rx);
            return Err(FastFailureAwaitError::LifecycleStopped);
        }

        // A queue cancellation already owns the response. Bias it ahead of a
        // simultaneously available permit so cancelled work never reaches the processor.
        let permit = tokio::select! {
            biased;
            response = &mut response_rx => {
                self.armed = false;
                return Err(FastFailureAwaitError::Rejected(rejection_from_result(
                    response,
                    &self.service.command_factory(),
                    self.opaque,
                    FastFailureRejectionKind::QueueCancelled,
                    "fast failure request was cancelled without a response",
                )));
            }
            () = control.cancelled() => {
                self.cancel_for_control();
                self.armed = false;
                drop(response_rx);
                return Err(FastFailureAwaitError::LifecycleStopped);
            }
            permit = self.service.acquire_permit(self.queue_kind) => permit,
        };

        let Some(permit) = permit else {
            self.cancel_with_response(super::system_error_response(
                &self.service.command_factory(),
                self.opaque,
                "fast failure queue permit acquisition failed",
            ));
            self.armed = false;
            return Err(FastFailureAwaitError::Rejected(rejection_from_result(
                response_rx.await,
                &self.service.command_factory(),
                self.opaque,
                FastFailureRejectionKind::PermitClosed,
                "fast failure queue permit acquisition failed before a response was produced",
            )));
        };
        if !self.service.try_mark_running(self.queue_kind, &self.task) {
            self.armed = false;
            return Err(FastFailureAwaitError::Rejected(rejection_from_result(
                response_rx.await,
                &self.service.command_factory(),
                self.opaque,
                FastFailureRejectionKind::QueueCancelled,
                "fast failure request was cancelled before processor execution",
            )));
        }

        self.armed = false;
        let run = FastFailureRunGuard {
            service: self.service.clone(),
            queue_kind: self.queue_kind,
            task: Arc::clone(&self.task),
            response_rx: Some(response_rx),
            opaque: self.opaque,
            _permit: permit,
            settled: false,
        };
        // Cancellation may race the permit/CAS transition. The affine run
        // owner rolls that transition back before any business future exists.
        if control.is_cancelled() {
            drop(run);
            return Err(FastFailureAwaitError::LifecycleStopped);
        }
        Ok(run)
    }

    fn cancel_for_control(&self) {
        self.cancel_with_response(super::system_error_response(
            &self.service.command_factory(),
            self.opaque,
            "request lifecycle ended before fast-failure dispatch",
        ));
    }

    fn cancel_with_response(&self, response: RemotingCommand) {
        self.service.cancel(self.queue_kind, &self.task, response);
    }

    fn missing_response_rejection(&self, kind: FastFailureRejectionKind, remark: &'static str) -> FastFailureRejection {
        FastFailureRejection::new(
            kind,
            super::system_error_response(&self.service.command_factory(), self.opaque, remark),
        )
    }
}

impl Drop for FastFailureAdmission {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.cancel_with_response(super::system_error_response(
            &self.service.command_factory(),
            self.opaque,
            "fast failure admission owner was dropped before processor execution",
        ));
    }
}

pub(super) struct FastFailureRunGuard {
    service: BrokerFastFailure,
    queue_kind: FastFailureQueueKind,
    task: Arc<FastFailureTask>,
    response_rx: Option<tokio::sync::oneshot::Receiver<Option<RemotingCommand>>>,
    opaque: i32,
    _permit: OwnedSemaphorePermit,
    settled: bool,
}

impl FastFailureRunGuard {
    pub(super) async fn complete(
        mut self,
        response: Option<RemotingCommand>,
    ) -> Result<Option<RemotingCommand>, FastFailureRejection> {
        self.service.complete(self.queue_kind, &self.task, response);
        self.settled = true;
        let Some(response_rx) = self.response_rx.take() else {
            return Err(FastFailureRejection::new(
                FastFailureRejectionKind::Internal,
                super::system_error_response(
                    &self.service.command_factory(),
                    self.opaque,
                    "fast failure run owner lost its response receiver",
                ),
            ));
        };
        response_rx.await.map_err(|_| {
            FastFailureRejection::new(
                FastFailureRejectionKind::Internal,
                super::system_error_response(
                    &self.service.command_factory(),
                    self.opaque,
                    "fast failure response channel closed before request completed",
                ),
            )
        })
    }

    /// Completes a V2 run without transferring response ownership through the
    /// legacy fast-failure response channel.
    ///
    /// The V2 handler outcome remains affine and is delivered only by the
    /// canonical dispatcher. Fast failure owns scheduling/accounting here,
    /// not the response plan.
    pub(super) fn complete_v2(mut self) {
        self.service.complete(self.queue_kind, &self.task, None);
        self.response_rx.take();
        self.settled = true;
    }
}

impl Drop for FastFailureRunGuard {
    fn drop(&mut self) {
        if !self.settled {
            // `complete` performs the running-count transition and wakes the
            // unique receiver; dropping the permit then admits the next waiter.
            self.service.complete(self.queue_kind, &self.task, None);
            self.settled = true;
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FastFailureRejectionKind {
    Budget,
    QueueCancelled,
    PermitClosed,
    Internal,
}

#[derive(Debug)]
pub(super) enum FastFailureAwaitError {
    Rejected(FastFailureRejection),
    LifecycleStopped,
}

pub(super) struct FastFailureRejection {
    kind: FastFailureRejectionKind,
    response: RemotingCommand,
}

impl std::fmt::Debug for FastFailureRejection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FastFailureRejection")
            .field("kind", &self.kind)
            .field("response_code", &self.response.code())
            .field("opaque", &self.response.opaque())
            .finish()
    }
}

impl FastFailureRejection {
    fn new(kind: FastFailureRejectionKind, response: RemotingCommand) -> Self {
        Self { kind, response }
    }

    pub(super) const fn kind(&self) -> FastFailureRejectionKind {
        self.kind
    }

    pub(super) fn into_legacy_command(self) -> RemotingCommand {
        self.response
    }

    pub(super) fn into_response_plan(mut self) -> Result<ResponsePlan, ResponsePlanError> {
        match self.response.take_body() {
            Some(body) => ResponsePlan::bytes(self.response, body),
            None => ResponsePlan::command(self.response),
        }
    }
}

pub(super) fn try_admit(
    service: &BrokerFastFailure,
    queue_kind: FastFailureQueueKind,
    metadata: FastFailureRequestMetadata,
) -> Result<FastFailureAdmission, FastFailureRejection> {
    let (task, response_rx) = service
        .try_enqueue(queue_kind, metadata.opaque, metadata.retained_bytes)
        .map_err(|response| FastFailureRejection::new(FastFailureRejectionKind::Budget, response))?;
    Ok(FastFailureAdmission {
        service: service.clone(),
        queue_kind,
        task,
        response_rx: Some(response_rx),
        opaque: metadata.opaque,
        armed: true,
    })
}

pub(super) fn estimate_retained_bytes(request: &RemotingCommand) -> usize {
    let mut retained_bytes = std::mem::size_of::<RemotingCommand>();
    retained_bytes = retained_bytes.saturating_add(request.body().map_or(0, bytes::Bytes::len));
    retained_bytes = retained_bytes.saturating_add(request.remark().map_or(0, |remark| remark.len()));
    if let Some(ext_fields) = request.ext_fields() {
        retained_bytes = retained_bytes.saturating_add(
            ext_fields
                .iter()
                .map(|(key, value)| {
                    std::mem::size_of_val(key)
                        .saturating_add(std::mem::size_of_val(value))
                        .saturating_add(key.len())
                        .saturating_add(value.len())
                })
                .fold(0usize, usize::saturating_add),
        );
    }
    retained_bytes.max(1)
}

fn rejection_from_result(
    result: Result<Option<RemotingCommand>, tokio::sync::oneshot::error::RecvError>,
    command_factory: &RemotingCommandFactory,
    opaque: i32,
    kind: FastFailureRejectionKind,
    missing_response_remark: &'static str,
) -> FastFailureRejection {
    match result {
        Ok(Some(response)) => FastFailureRejection::new(kind, response),
        Ok(None) | Err(_) => FastFailureRejection::new(
            FastFailureRejectionKind::Internal,
            super::system_error_response(command_factory, opaque, missing_response_remark),
        ),
    }
}

#[cfg(test)]
mod tests;
