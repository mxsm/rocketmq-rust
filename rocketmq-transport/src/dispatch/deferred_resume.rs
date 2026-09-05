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

use std::alloc::Layout;
use std::future::Future;
use std::mem::size_of;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use parking_lot::Mutex;
use rocketmq_error::RocketMQResult;
use rocketmq_runtime::OperationContext;
use tokio::sync::Notify;
use tracing::Instrument;

use super::authorized_dispatcher::admission_response;
use super::deferred_registry::ClaimExecutionParts;
use super::deferred_responder::DeferredResponseAttempt;
use super::ClaimedDeferred;
use super::DeferredResumeOutcome;
use super::DeferredResumeRetainedSize;
use super::DeferredResumeSubmitOutcome;
use super::DeferredWakeReason;
use super::RemotingResponse;
use super::ResponseReceipt;
use crate::admission::AdmissionClass;
use crate::admission::AdmissionRejection;
use crate::admission::FullPolicy;
use crate::contract::TransportContractViolation;
use crate::request_ordering::RequestOrdering;

#[path = "deferred_resume_stop.rs"]
mod stop;

use stop::finish_claimed_stop;
use stop::finish_lifecycle;
use stop::finish_parts_admission;
use stop::finish_parts_stop;
pub(crate) use stop::ResumeStop;
use stop::ResumeStopView;

pub(super) enum ResumeAttempt {
    Completed(ResponseReceipt),
    Cancelled,
    SessionClosed,
    AdmissionRejected,
    Operational(ResumeOperationalFailure),
    TransportFailure(crate::error::TransportError),
}

#[derive(Debug, thiserror::Error)]
pub(super) enum ResumeOperationalFailure {
    #[error("deferred resume executor is closing")]
    ExecutorClosing {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
    #[error("deferred resume task terminated before completion")]
    TaskTerminated,
    #[error(transparent)]
    Contract(#[from] TransportContractViolation),
}

type ResumeResult = ResumeAttempt;
type PublicResumeResult = Result<DeferredResumeOutcome, crate::error::TransportError>;
type WorkFuture = Pin<Box<dyn Future<Output = ResumeResult> + Send + 'static>>;
type ResumeTerminalObserver = Box<dyn FnOnce(&PublicResumeResult) + Send + 'static>;

pub(crate) async fn resume_claimed<R, F, Fut>(
    claimed: ClaimedDeferred<R>,
    handler_retained: DeferredResumeRetainedSize,
    handler: F,
) -> PublicResumeResult
where
    R: Send + 'static,
    F: FnOnce(R, DeferredWakeReason) -> Fut + Send + 'static,
    Fut: Future<Output = RocketMQResult<RemotingResponse>> + Send + 'static,
{
    let id = claimed.deferred_id();
    let request_id = claimed.request_id();
    let stop_view = ResumeStopView::new(claimed.control().clone(), claimed.expiry());
    if let Some(stop) = stop_view.current_before_resume() {
        let result = finish_claimed_stop(claimed, stop, None);
        drop(handler);
        return converge_resume_result(result);
    }
    let Some(context) = claimed.resume_context() else {
        let result = finish_claimed_stop(claimed, ResumeStop::ProcessorUnavailable, None);
        drop(handler);
        return converge_resume_result(result);
    };
    let execution_bytes =
        match execution_retained_bytes::<R, F, Fut>(claimed.retained_bytes(), handler_retained.dynamic_bytes()) {
            Some(bytes) => bytes,
            None => {
                drop(handler);
                drop(claimed);
                return converge_resume_result(ResumeAttempt::Operational(
                    TransportContractViolation::DeferredRetainedSizeOverflow.into(),
                ));
            }
        };
    let parts = claimed.into_execution_parts();
    let stop_view = ResumeStopView::from_execution_parts(&parts);
    let completion = ResumeCompletion::new(id, request_id, None);
    let work = ResumeWorkImpl {
        parts: Some(parts),
        handler: Some(handler),
        stop_view: stop_view.clone(),
    };
    let job = DeferredResumeJob::new(
        execution_bytes,
        context.class,
        context.ordering,
        stop_view,
        Box::new(work),
        Arc::clone(&completion),
    );
    let cell = Arc::new(ResumeJobCell::new(job));

    cell.release_wait_permit();
    let submitted = context.executor.try_execute_resume(Arc::clone(&cell));
    match submitted {
        DeferredResumeEnqueueOutcome::Submitted(_task_id) => drop(cell),
        DeferredResumeEnqueueOutcome::AdmissionRejected { error, cell } => {
            if let Some(job) = cell.take() {
                job.reject(error).await;
            }
        }
        DeferredResumeEnqueueOutcome::ExecutorClosing { cell } => {
            if let Some(job) = cell.take() {
                job.finish_executor_closed();
            }
        }
        DeferredResumeEnqueueOutcome::OperationalFailure { source, cell } => {
            if let Some(job) = cell.take() {
                job.finish_executor_failure(source);
            }
        }
    }
    completion.wait().await
}

pub(crate) fn submit_claimed<R, F, Fut, O>(
    claimed: ClaimedDeferred<R>,
    handler_retained: DeferredResumeRetainedSize,
    handler: F,
    terminal_observer: O,
) -> Result<DeferredResumeSubmitOutcome, crate::error::TransportError>
where
    R: Send + 'static,
    F: FnOnce(R, DeferredWakeReason) -> Fut + Send + 'static,
    Fut: Future<Output = RocketMQResult<RemotingResponse>> + Send + 'static,
    O: FnOnce(&PublicResumeResult) + Send + 'static,
{
    let id = claimed.deferred_id();
    let request_id = claimed.request_id();
    let mut observer = Some(Box::new(terminal_observer) as ResumeTerminalObserver);
    let stop_view = ResumeStopView::new(claimed.control().clone(), claimed.expiry());
    if let Some(stop) = stop_view.current_before_resume() {
        let result = converge_resume_result(finish_claimed_stop(claimed, stop, None));
        drop(handler);
        observe_unsubmitted(observer.take(), &result);
        return submit_outcome(result);
    }
    let Some(context) = claimed.resume_context() else {
        let result = converge_resume_result(finish_claimed_stop(claimed, ResumeStop::ProcessorUnavailable, None));
        drop(handler);
        observe_unsubmitted(observer.take(), &result);
        return submit_outcome(result);
    };
    let execution_bytes =
        match execution_retained_bytes::<R, F, Fut>(claimed.retained_bytes(), handler_retained.dynamic_bytes()) {
            Some(bytes) => bytes,
            None => {
                drop(handler);
                drop(claimed);
                let result = converge_resume_result(ResumeAttempt::Operational(
                    TransportContractViolation::DeferredRetainedSizeOverflow.into(),
                ));
                observe_unsubmitted(observer.take(), &result);
                return submit_outcome(result);
            }
        };
    let parts = claimed.into_execution_parts();
    let stop_view = ResumeStopView::from_execution_parts(&parts);
    let completion = ResumeCompletion::new(id, request_id, observer.take());
    let work = ResumeWorkImpl {
        parts: Some(parts),
        handler: Some(handler),
        stop_view: stop_view.clone(),
    };
    let job = DeferredResumeJob::new(
        execution_bytes,
        context.class,
        context.ordering,
        stop_view,
        Box::new(work),
        Arc::clone(&completion),
    );
    let cell = Arc::new(ResumeJobCell::new(job));

    cell.release_wait_permit();
    match context.executor.try_execute_resume(Arc::clone(&cell)) {
        DeferredResumeEnqueueOutcome::Submitted(_task_id) => {
            drop(cell);
            Ok(DeferredResumeSubmitOutcome::Submitted)
        }
        DeferredResumeEnqueueOutcome::AdmissionRejected { error, cell } => {
            if let Some(job) = cell.take() {
                job.finish_admission_rejected(error);
            }
            submit_outcome(completion.take_finished())
        }
        DeferredResumeEnqueueOutcome::ExecutorClosing { cell } => {
            if let Some(job) = cell.take() {
                job.finish_executor_closed();
            }
            submit_outcome(completion.take_finished())
        }
        DeferredResumeEnqueueOutcome::OperationalFailure { source, cell } => {
            if let Some(job) = cell.take() {
                job.finish_executor_failure(source);
            }
            submit_outcome(completion.take_finished())
        }
    }
}

fn observe_unsubmitted(observer: Option<ResumeTerminalObserver>, result: &PublicResumeResult) {
    if let Some(observer) = observer {
        let _ = std::panic::catch_unwind(AssertUnwindSafe(|| observer(result)));
    }
}

fn converge_resume_result(result: ResumeResult) -> PublicResumeResult {
    match result {
        ResumeAttempt::Completed(receipt) => Ok(DeferredResumeOutcome::Completed(receipt)),
        ResumeAttempt::Cancelled => Ok(DeferredResumeOutcome::Cancelled),
        ResumeAttempt::SessionClosed => Ok(DeferredResumeOutcome::SessionClosed),
        ResumeAttempt::AdmissionRejected => Ok(DeferredResumeOutcome::AdmissionRejected),
        ResumeAttempt::Operational(error) => Err(crate::error::TransportError::resume(error)),
        ResumeAttempt::TransportFailure(error) => Err(error),
    }
}

fn submit_outcome(result: PublicResumeResult) -> Result<DeferredResumeSubmitOutcome, crate::error::TransportError> {
    match result {
        Ok(DeferredResumeOutcome::Completed(_)) => Ok(DeferredResumeSubmitOutcome::Submitted),
        Ok(DeferredResumeOutcome::Cancelled) => Ok(DeferredResumeSubmitOutcome::Cancelled),
        Ok(DeferredResumeOutcome::SessionClosed) => Ok(DeferredResumeSubmitOutcome::SessionClosed),
        Ok(DeferredResumeOutcome::AdmissionRejected) => Ok(DeferredResumeSubmitOutcome::AdmissionRejected),
        Err(error) => Err(error),
    }
}

fn execution_retained_bytes<R, F, Fut>(original: usize, dynamic: usize) -> Option<usize>
where
    R: Send + 'static,
    F: Send + 'static,
{
    checked_execution_charge(
        original,
        size_of::<F>(),
        size_of::<Fut>(),
        dynamic,
        Layout::new::<ResumeWorkImpl<R, F>>().pad_to_align().size(),
    )
}

fn checked_execution_charge(
    original: usize,
    handler: usize,
    future: usize,
    dynamic: usize,
    boxed_work: usize,
) -> Option<usize> {
    original
        .checked_add(handler)?
        .checked_add(future)?
        .checked_add(dynamic)?
        .checked_add(boxed_work)
}

pub(in crate::dispatch) fn deferred_resume_fixed_bytes() -> Option<usize> {
    checked_resume_fixed_sum(
        arc_allocation_bytes::<ResumeCompletion>()?,
        arc_allocation_bytes::<ResumeJobCell>()?,
    )
}

fn checked_resume_fixed_sum(completion: usize, job_cell: usize) -> Option<usize> {
    completion.checked_add(job_cell)
}

fn arc_allocation_bytes<T>() -> Option<usize> {
    let header = Layout::array::<AtomicUsize>(2).ok()?;
    let (allocation, _) = header.extend(Layout::new::<T>()).ok()?;
    Some(allocation.pad_to_align().size())
}

pub(crate) struct ResumeJobCell {
    job: Mutex<Option<DeferredResumeJob>>,
    #[cfg(test)]
    first_poll_gate: Option<(Arc<Notify>, Arc<Notify>)>,
}

impl ResumeJobCell {
    fn new(job: DeferredResumeJob) -> Self {
        Self {
            job: Mutex::new(Some(job)),
            #[cfg(test)]
            first_poll_gate: None,
        }
    }

    #[cfg(test)]
    fn with_first_poll_gate(job: DeferredResumeJob, entered: Arc<Notify>, release: Arc<Notify>) -> Self {
        Self {
            job: Mutex::new(Some(job)),
            first_poll_gate: Some((entered, release)),
        }
    }

    #[cfg(test)]
    pub(crate) async fn wait_first_poll_gate(&self) {
        if let Some((entered, release)) = &self.first_poll_gate {
            let waiting = release.notified();
            tokio::pin!(waiting);
            waiting.as_mut().enable();
            entered.notify_one();
            waiting.await;
        }
    }

    pub(crate) fn take(&self) -> Option<DeferredResumeJob> {
        self.job.lock().take()
    }

    fn release_wait_permit(&self) {
        if let Some(job) = self.job.lock().as_mut() {
            job.release_wait_permit();
        }
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        self.job.lock().as_ref().map_or(0, |job| job.retained_bytes)
    }

    pub(crate) fn class(&self) -> AdmissionClass {
        self.job.lock().as_ref().map_or(AdmissionClass::Data, |job| job.class)
    }

    pub(crate) fn ordering(&self) -> RequestOrdering {
        self.job
            .lock()
            .as_ref()
            .map_or(RequestOrdering::Concurrent, |job| job.ordering)
    }
}

impl Drop for ResumeJobCell {
    fn drop(&mut self) {
        drop(self.job.get_mut().take());
    }
}

pub(crate) enum DeferredResumeEnqueueOutcome {
    Submitted(rocketmq_runtime::TaskId),
    AdmissionRejected {
        error: AdmissionRejection,
        cell: Arc<ResumeJobCell>,
    },
    ExecutorClosing {
        cell: Arc<ResumeJobCell>,
    },
    OperationalFailure {
        source: rocketmq_runtime::RuntimeError,
        cell: Arc<ResumeJobCell>,
    },
}

pub(crate) struct DeferredResumeJob {
    retained_bytes: usize,
    class: AdmissionClass,
    ordering: RequestOrdering,
    stop_view: ResumeStopView,
    work: Option<Box<dyn DeferredResumeWork>>,
    completion: Arc<ResumeCompletion>,
    active: bool,
    #[cfg(test)]
    before_ordering: Option<Arc<Notify>>,
}

impl DeferredResumeJob {
    fn new(
        retained_bytes: usize,
        class: AdmissionClass,
        ordering: RequestOrdering,
        stop_view: ResumeStopView,
        work: Box<dyn DeferredResumeWork>,
        completion: Arc<ResumeCompletion>,
    ) -> Self {
        Self {
            retained_bytes,
            class,
            ordering,
            stop_view,
            work: Some(work),
            completion,
            active: true,
            #[cfg(test)]
            before_ordering: None,
        }
    }

    #[cfg(test)]
    fn with_before_ordering(mut self, signal: Arc<Notify>) -> Self {
        self.before_ordering = Some(signal);
        self
    }

    #[cfg(test)]
    pub(crate) fn notify_before_ordering(&self) {
        if let Some(signal) = &self.before_ordering {
            signal.notify_one();
        }
    }

    fn release_wait_permit(&mut self) {
        if let Some(work) = self.work.as_mut() {
            work.release_wait_permit();
        }
    }

    pub(crate) fn current_before_resume(&self) -> Option<ResumeStop> {
        self.stop_view.current_before_resume()
    }

    pub(crate) fn wait_before_resume(&self) -> impl Future<Output = ResumeStop> + Send + 'static {
        let stop_view = self.stop_view.clone();
        async move { stop_view.wait_before_resume().await }
    }

    pub(crate) fn finish_stopped(mut self, stop: ResumeStop) {
        let work = self.work.take().expect("a stopped resume job owns one work item");
        let result = work.finish_stopped(stop, None);
        self.completion.finish(result);
        self.active = false;
    }

    pub(crate) async fn execute(mut self, _operation: OperationContext) {
        let work = self.work.take().expect("an accepted resume job owns one work item");
        let result = work.execute().await;
        self.completion.finish(result);
        self.active = false;
    }

    pub(crate) async fn reject(mut self, error: AdmissionRejection) {
        let work = self.work.take().expect("a rejected resume job owns one work item");
        let result = work.reject(error).await;
        self.completion.finish(result);
        self.active = false;
    }

    fn finish_admission_rejected(mut self, error: AdmissionRejection) {
        let work = self
            .work
            .take()
            .expect("an admission-rejected resume job owns one work item");
        let result = work.finish_admission_rejected(error);
        self.completion.finish(result);
        self.active = false;
    }

    fn finish_executor_closed(mut self) {
        let stop = self
            .stop_view
            .current_before_resume()
            .unwrap_or(ResumeStop::ServiceStopping);
        let work = self.work.take().expect("a closed resume job owns one work item");
        let result = work.finish_stopped(stop, None);
        self.completion.finish(result);
        self.active = false;
    }

    fn finish_executor_failure(mut self, source: rocketmq_runtime::RuntimeError) {
        let stop = self
            .stop_view
            .current_before_resume()
            .unwrap_or(ResumeStop::ServiceStopping);
        let work = self.work.take().expect("a failed resume job owns one work item");
        // Terminalize affine response ownership with the already-selected
        // lifecycle winner, but do not let that independent race erase the
        // executor's operational failure from this submission attempt.
        let _terminal = work.finish_stopped(stop, None);
        self.completion
            .finish(ResumeAttempt::Operational(ResumeOperationalFailure::ExecutorClosing {
                source: Box::new(source),
            }));
        self.active = false;
    }
}

impl Drop for DeferredResumeJob {
    fn drop(&mut self) {
        if self.active {
            let stop = self
                .stop_view
                .current_before_resume()
                .unwrap_or(ResumeStop::ServiceStopping);
            let result = self.work.take().map_or_else(
                || ResumeAttempt::Operational(ResumeOperationalFailure::TaskTerminated),
                |work| work.finish_stopped(stop, None),
            );
            self.completion.finish(result);
            self.active = false;
        }
    }
}

trait DeferredResumeWork: Send + 'static {
    fn release_wait_permit(&mut self);
    fn execute(self: Box<Self>) -> WorkFuture;
    fn reject(self: Box<Self>, error: AdmissionRejection) -> WorkFuture;
    fn finish_admission_rejected(self: Box<Self>, error: AdmissionRejection) -> ResumeResult;
    fn finish_stopped(
        self: Box<Self>,
        stop: ResumeStop,
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ) -> ResumeResult;
}

struct ResumeWorkImpl<R, F>
where
    R: Send + 'static,
    F: Send + 'static,
{
    parts: Option<ClaimExecutionParts<R>>,
    handler: Option<F>,
    stop_view: ResumeStopView,
}

impl<R, F> Drop for ResumeWorkImpl<R, F>
where
    R: Send + 'static,
    F: Send + 'static,
{
    fn drop(&mut self) {
        drop(self.handler.take());
        drop(self.parts.take());
    }
}

impl<R, F, Fut> DeferredResumeWork for ResumeWorkImpl<R, F>
where
    R: Send + 'static,
    F: FnOnce(R, DeferredWakeReason) -> Fut + Send + 'static,
    Fut: Future<Output = RocketMQResult<RemotingResponse>> + Send + 'static,
{
    fn release_wait_permit(&mut self) {
        if let Some(parts) = self.parts.as_mut() {
            if let Some(permit) = parts.permit.take() {
                permit.release();
            }
        }
    }

    fn execute(mut self: Box<Self>) -> WorkFuture {
        let span = self
            .parts
            .as_ref()
            .map_or_else(tracing::Span::none, |parts| parts.responder.request_span());
        Box::pin(
            async move {
                let parts = self.parts.take().expect("resume work owns claimed parts");
                let handler = self.handler.take().expect("resume work owns its handler");
                execute_work(parts, handler, self.stop_view.clone()).await
            }
            .instrument(span),
        )
    }

    fn reject(mut self: Box<Self>, error: AdmissionRejection) -> WorkFuture {
        Box::pin(async move {
            let parts = self.parts.take().expect("resume work owns claimed parts");
            let handler = self.handler.take();
            let result = reject_work(parts, error, self.stop_view.clone()).await;
            drop(handler);
            result
        })
    }

    fn finish_admission_rejected(mut self: Box<Self>, error: AdmissionRejection) -> ResumeResult {
        let parts = self
            .parts
            .take()
            .expect("admission-rejected resume work owns claimed parts");
        let result = finish_parts_admission(parts, error);
        drop(self.handler.take());
        result
    }

    fn finish_stopped(
        mut self: Box<Self>,
        stop: ResumeStop,
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ) -> ResumeResult {
        let parts = self.parts.take().expect("stopped resume work owns claimed parts");
        let result = finish_parts_stop(parts, stop, source);
        drop(self.handler.take());
        result
    }
}

async fn execute_work<R, F, Fut>(parts: ClaimExecutionParts<R>, handler: F, stop_view: ResumeStopView) -> ResumeResult
where
    R: Send + 'static,
    F: FnOnce(R, DeferredWakeReason) -> Fut + Send + 'static,
    Fut: Future<Output = RocketMQResult<RemotingResponse>> + Send + 'static,
{
    let ClaimExecutionParts {
        id,
        request_id,
        reason,
        resume,
        responder,
        marker,
        ..
    } = parts;
    let mut responder = Some(responder);
    let mut marker = Some(marker);
    if let Some(stop) = stop_view.current_before_resume() {
        let result = finish_lifecycle(
            id,
            request_id,
            responder.take().expect("pre-resume responder"),
            marker.take().expect("pre-resume marker"),
            stop,
            None,
        );
        drop(handler);
        drop(resume);
        return result;
    }
    enum HandlerOutcome<T> {
        Completed(RocketMQResult<T>),
        Stopped(ResumeResult),
    }
    // Keep the completed handler future alive until response delivery reaches
    // its canonical terminal. Broker-private handlers use this ownership seam
    // for affine resources that must span business execution and socket I/O.
    let handler_future = handler(resume, reason);
    tokio::pin!(handler_future);
    let outcome = tokio::select! {
        biased;
        stop = stop_view.wait_before_write() => HandlerOutcome::Stopped(finish_lifecycle(
            id,
            request_id,
            responder.take().expect("stopped handler responder"),
            marker.take().expect("stopped handler marker"),
            stop,
            None,
        )),
        result = &mut handler_future => {
            match stop_view.current_before_write() {
                Some(stop) => HandlerOutcome::Stopped(finish_lifecycle(
                    id,
                    request_id,
                    responder.take().expect("post-handler responder"),
                    marker.take().expect("post-handler marker"),
                    stop,
                    None,
                )),
                None => HandlerOutcome::Completed(result),
            }
        },
    };
    let result = match outcome {
        HandlerOutcome::Stopped(result) => result,
        HandlerOutcome::Completed(Ok(response)) => {
            if let Some(stop) = stop_view.current_before_write() {
                return finish_lifecycle(
                    id,
                    request_id,
                    responder.take().expect("pre-write responder"),
                    marker.take().expect("pre-write marker"),
                    stop,
                    None,
                );
            }
            let result = map_response(
                id,
                request_id,
                responder
                    .take()
                    .expect("response responder")
                    .respond_internal(response)
                    .await,
            );
            drop(marker.take());
            result
        }
        HandlerOutcome::Completed(Err(error)) => {
            let response = match crate::error_response::remoting_response_from_error(&error) {
                Ok(response) => response,
                Err(source) => {
                    drop(responder.take());
                    drop(marker.take());
                    return ResumeAttempt::Operational(ResumeOperationalFailure::Contract(source));
                }
            };
            if let Some(stop) = stop_view.current_before_write() {
                return finish_lifecycle(
                    id,
                    request_id,
                    responder.take().expect("error pre-write responder"),
                    marker.take().expect("error pre-write marker"),
                    stop,
                    None,
                );
            }
            let result = map_response(
                id,
                request_id,
                responder
                    .take()
                    .expect("error response responder")
                    .respond_internal(response)
                    .await,
            );
            drop(marker.take());
            result
        }
    };
    result
}

async fn reject_work<R>(
    parts: ClaimExecutionParts<R>,
    error: AdmissionRejection,
    stop_view: ResumeStopView,
) -> ResumeResult
where
    R: Send + 'static,
{
    let ClaimExecutionParts {
        id,
        request_id,
        resume,
        responder,
        marker,
        ..
    } = parts;
    if let Some(stop) = stop_view.current_before_write() {
        let result = finish_lifecycle(id, request_id, responder, marker, stop, None);
        drop(resume);
        return result;
    }
    drop(resume);
    if error.policy() == FullPolicy::Reject {
        let response = match RemotingResponse::command(admission_response(responder.original_opaque(), &error)) {
            Ok(response) => response,
            Err(source) => {
                drop(responder);
                drop(marker);
                return ResumeAttempt::Operational(ResumeOperationalFailure::Contract(source));
            }
        };
        if let Some(stop) = stop_view.current_before_write() {
            return finish_lifecycle(id, request_id, responder, marker, stop, None);
        }
        let result = map_response(id, request_id, responder.respond_internal(response).await);
        drop(marker);
        result
    } else {
        finish_lifecycle(
            id,
            request_id,
            responder,
            marker,
            ResumeStop::ProcessorUnavailable,
            None,
        )
    }
}

fn map_response(
    id: super::DeferredId,
    request_id: super::RequestId,
    result: Result<DeferredResponseAttempt, crate::error::TransportError>,
) -> ResumeResult {
    let _ = (id, request_id);
    match result {
        Ok(DeferredResponseAttempt::Completed(receipt)) => ResumeAttempt::Completed(receipt),
        Ok(DeferredResponseAttempt::AlreadyCompleted { state, reason }) => reason.map_or_else(
            || match state {
                super::ResponseTerminalState::Closed => ResumeAttempt::SessionClosed,
                super::ResponseTerminalState::Completed
                | super::ResponseTerminalState::Failed { .. }
                | super::ResponseTerminalState::Cancelled => ResumeAttempt::Cancelled,
            },
            stop::resume_attempt_for_reason,
        ),
        Ok(DeferredResponseAttempt::Cancelled | DeferredResponseAttempt::DeadlineExceeded) => ResumeAttempt::Cancelled,
        Ok(DeferredResponseAttempt::SessionClosed) => ResumeAttempt::SessionClosed,
        Ok(DeferredResponseAttempt::QueueSaturated) => ResumeAttempt::AdmissionRejected,
        Err(source) => ResumeAttempt::TransportFailure(source),
    }
}

pub(in crate::dispatch) struct ResumeCompletion {
    completed: AtomicBool,
    result: Mutex<Option<PublicResumeResult>>,
    terminal_observer: Mutex<Option<ResumeTerminalObserver>>,
    changed: Notify,
}

impl ResumeCompletion {
    fn new(
        _id: super::DeferredId,
        _request_id: super::RequestId,
        terminal_observer: Option<ResumeTerminalObserver>,
    ) -> Arc<Self> {
        Arc::new(Self {
            completed: AtomicBool::new(false),
            result: Mutex::new(None),
            terminal_observer: Mutex::new(terminal_observer),
            changed: Notify::new(),
        })
    }

    fn take_finished(&self) -> PublicResumeResult {
        self.result
            .lock()
            .take()
            .expect("a synchronously rejected resume publishes its terminal result")
    }

    async fn wait(&self) -> PublicResumeResult {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(result) = self.result.lock().take() {
                return result;
            }
            notified.await;
        }
    }

    fn finish(&self, result: ResumeResult) {
        let result = converge_resume_result(result);
        if self
            .completed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            observe_unsubmitted(self.terminal_observer.lock().take(), &result);
            *self.result.lock() = Some(result);
            self.changed.notify_waiters();
        }
    }
}

#[cfg(test)]
#[path = "../../tests/unit/dispatch/deferred_resume_terminal_ownership.rs"]
mod terminal_ownership;

#[cfg(test)]
#[path = "../../tests/unit/dispatch/deferred_resume.rs"]
mod tests;
