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
use super::ClaimedDeferred;
use super::DeferredResponseError;
use super::DeferredResponseErrorKind;
use super::DeferredResumeError;
use super::DeferredResumeErrorKind;
use super::DeferredResumeRetainedSize;
use super::DeferredWakeReason;
use super::ResponsePlan;
use super::ResponseReceipt;
use crate::admission::AdmissionClass;
use crate::admission::AdmissionError;
use crate::admission::FullPolicy;
use crate::request_ordering::RequestOrdering;

#[path = "deferred_resume_stop.rs"]
mod stop;

use stop::finish_claimed_stop;
use stop::finish_lifecycle;
use stop::finish_parts_admission;
use stop::finish_parts_stop;
use stop::resume_error_kind_for_reason;
pub(crate) use stop::ResumeStop;
use stop::ResumeStopView;

type ResumeResult = Result<ResponseReceipt, DeferredResumeError>;
type WorkFuture = Pin<Box<dyn Future<Output = ResumeResult> + Send + 'static>>;
type ResumeTerminalObserver = Box<dyn FnOnce(&ResumeResult) + Send + 'static>;

pub(crate) async fn resume_claimed<R, F, Fut>(
    claimed: ClaimedDeferred<R>,
    handler_retained: DeferredResumeRetainedSize,
    handler: F,
) -> ResumeResult
where
    R: Send + 'static,
    F: FnOnce(R, DeferredWakeReason) -> Fut + Send + 'static,
    Fut: Future<Output = RocketMQResult<ResponsePlan>> + Send + 'static,
{
    let id = claimed.deferred_id();
    let request_id = claimed.request_id();
    let stop_view = ResumeStopView::new(claimed.control().clone(), claimed.expiry());
    if let Some(stop) = stop_view.current_before_resume() {
        let result = finish_claimed_stop(claimed, stop, None);
        drop(handler);
        return result;
    }
    let Some(context) = claimed.resume_context() else {
        let result = finish_claimed_stop(claimed, ResumeStop::ProcessorUnavailable, None);
        drop(handler);
        return result;
    };
    let execution_bytes =
        match execution_retained_bytes::<R, F, Fut>(claimed.retained_bytes(), handler_retained.dynamic_bytes()) {
            Some(bytes) => bytes,
            None => {
                drop(handler);
                drop(claimed);
                return Err(DeferredResumeError::new(
                    DeferredResumeErrorKind::RetainedSizeOverflow,
                    id,
                    request_id,
                    None,
                    None,
                    None,
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
        Ok(_task_id) => drop(cell),
        Err(DeferredResumeSubmitError::Admission { error, cell }) => {
            if let Some(job) = cell.take() {
                job.reject(error).await;
            }
        }
        Err(DeferredResumeSubmitError::Closing { source, cell }) => {
            if let Some(job) = cell.take() {
                job.finish_executor_closing(source);
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
) -> Result<(), DeferredResumeError>
where
    R: Send + 'static,
    F: FnOnce(R, DeferredWakeReason) -> Fut + Send + 'static,
    Fut: Future<Output = RocketMQResult<ResponsePlan>> + Send + 'static,
    O: FnOnce(&ResumeResult) + Send + 'static,
{
    let id = claimed.deferred_id();
    let request_id = claimed.request_id();
    let mut observer = Some(Box::new(terminal_observer) as ResumeTerminalObserver);
    let stop_view = ResumeStopView::new(claimed.control().clone(), claimed.expiry());
    if let Some(stop) = stop_view.current_before_resume() {
        let result = finish_claimed_stop(claimed, stop, None);
        drop(handler);
        observe_unsubmitted(observer.take(), &result);
        return result.map(drop);
    }
    let Some(context) = claimed.resume_context() else {
        let result = finish_claimed_stop(claimed, ResumeStop::ProcessorUnavailable, None);
        drop(handler);
        observe_unsubmitted(observer.take(), &result);
        return result.map(drop);
    };
    let execution_bytes =
        match execution_retained_bytes::<R, F, Fut>(claimed.retained_bytes(), handler_retained.dynamic_bytes()) {
            Some(bytes) => bytes,
            None => {
                drop(handler);
                drop(claimed);
                let result = Err(DeferredResumeError::new(
                    DeferredResumeErrorKind::RetainedSizeOverflow,
                    id,
                    request_id,
                    None,
                    None,
                    None,
                ));
                observe_unsubmitted(observer.take(), &result);
                return result.map(drop);
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
        Ok(_task_id) => {
            drop(cell);
            Ok(())
        }
        Err(DeferredResumeSubmitError::Admission { error, cell }) => {
            if let Some(job) = cell.take() {
                job.finish_admission_rejected(error);
            }
            completion.take_finished().map(drop)
        }
        Err(DeferredResumeSubmitError::Closing { source, cell }) => {
            if let Some(job) = cell.take() {
                job.finish_executor_closing(source);
            }
            completion.take_finished().map(drop)
        }
    }
}

fn observe_unsubmitted(observer: Option<ResumeTerminalObserver>, result: &ResumeResult) {
    if let Some(observer) = observer {
        let _ = std::panic::catch_unwind(AssertUnwindSafe(|| observer(result)));
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

pub(crate) enum DeferredResumeSubmitError {
    Admission {
        error: AdmissionError,
        cell: Arc<ResumeJobCell>,
    },
    Closing {
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

    pub(crate) async fn reject(mut self, error: AdmissionError) {
        let work = self.work.take().expect("a rejected resume job owns one work item");
        let result = work.reject(error).await;
        self.completion.finish(result);
        self.active = false;
    }

    fn finish_admission_rejected(mut self, error: AdmissionError) {
        let work = self
            .work
            .take()
            .expect("an admission-rejected resume job owns one work item");
        let result = work.finish_admission_rejected(error);
        self.completion.finish(result);
        self.active = false;
    }

    fn finish_executor_closing(mut self, source: rocketmq_runtime::RuntimeError) {
        let stop = self
            .stop_view
            .current_before_resume()
            .unwrap_or(ResumeStop::ServiceStopping);
        let work = self.work.take().expect("a closing resume job owns one work item");
        let result = work.finish_stopped(stop, Some(Box::new(source)));
        self.completion.finish(result);
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
                || {
                    Err(DeferredResumeError::new(
                        DeferredResumeErrorKind::TaskTerminated,
                        self.completion.id,
                        self.completion.request_id,
                        None,
                        None,
                        None,
                    ))
                },
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
    fn reject(self: Box<Self>, error: AdmissionError) -> WorkFuture;
    fn finish_admission_rejected(self: Box<Self>, error: AdmissionError) -> ResumeResult;
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
    Fut: Future<Output = RocketMQResult<ResponsePlan>> + Send + 'static,
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

    fn reject(mut self: Box<Self>, error: AdmissionError) -> WorkFuture {
        Box::pin(async move {
            let parts = self.parts.take().expect("resume work owns claimed parts");
            let handler = self.handler.take();
            let result = reject_work(parts, error, self.stop_view.clone()).await;
            drop(handler);
            result
        })
    }

    fn finish_admission_rejected(mut self: Box<Self>, error: AdmissionError) -> ResumeResult {
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
    Fut: Future<Output = RocketMQResult<ResponsePlan>> + Send + 'static,
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
        HandlerOutcome::Completed(Ok(plan)) => {
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
                responder.take().expect("response responder").respond(plan).await,
            );
            drop(marker.take());
            result
        }
        HandlerOutcome::Completed(Err(error)) => {
            let plan = match crate::error_response::response_plan_from_error(&error) {
                Ok(plan) => plan,
                Err(source) => {
                    drop(responder.take());
                    drop(marker.take());
                    return Err(DeferredResumeError::new(
                        DeferredResumeErrorKind::ResponsePlan,
                        id,
                        request_id,
                        None,
                        None,
                        Some(Box::new(source)),
                    ));
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
                responder.take().expect("error response responder").respond(plan).await,
            );
            drop(marker.take());
            result
        }
    };
    result
}

async fn reject_work<R>(parts: ClaimExecutionParts<R>, error: AdmissionError, stop_view: ResumeStopView) -> ResumeResult
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
        let plan = match ResponsePlan::command(admission_response(responder.original_opaque(), &error)) {
            Ok(plan) => plan,
            Err(source) => {
                drop(responder);
                drop(marker);
                return Err(DeferredResumeError::new(
                    DeferredResumeErrorKind::ResponsePlan,
                    id,
                    request_id,
                    None,
                    None,
                    Some(Box::new(source)),
                ));
            }
        };
        if let Some(stop) = stop_view.current_before_write() {
            return finish_lifecycle(id, request_id, responder, marker, stop, None);
        }
        let result = map_response(id, request_id, responder.respond(plan).await);
        drop(marker);
        result
    } else {
        finish_lifecycle(
            id,
            request_id,
            responder,
            marker,
            ResumeStop::ProcessorUnavailable,
            Some(Box::new(error)),
        )
    }
}

fn map_response(
    id: super::DeferredId,
    request_id: super::RequestId,
    result: Result<ResponseReceipt, DeferredResponseError>,
) -> ResumeResult {
    result.map_err(|source| {
        let fallback_kind = match source.kind() {
            DeferredResponseErrorKind::Cancelled => DeferredResumeErrorKind::Cancelled,
            DeferredResponseErrorKind::SessionClosed => DeferredResumeErrorKind::SessionClosed,
            DeferredResponseErrorKind::AlreadyCompleted
            | DeferredResponseErrorKind::InvalidTransition
            | DeferredResponseErrorKind::Binding
            | DeferredResponseErrorKind::DeadlineExceeded
            | DeferredResponseErrorKind::QueueSaturated
            | DeferredResponseErrorKind::Encode
            | DeferredResponseErrorKind::Transport => DeferredResumeErrorKind::Response,
        };
        let kind = source
            .prior_terminal_reason()
            .map_or(fallback_kind, resume_error_kind_for_reason);
        DeferredResumeError::new_with_reason(
            kind,
            id,
            request_id,
            source.prior_terminal_state(),
            source.prior_terminal_reason(),
            source.write_progress(),
            Some(Box::new(source)),
        )
    })
}

pub(in crate::dispatch) struct ResumeCompletion {
    id: super::DeferredId,
    request_id: super::RequestId,
    completed: AtomicBool,
    result: Mutex<Option<ResumeResult>>,
    terminal_observer: Mutex<Option<ResumeTerminalObserver>>,
    changed: Notify,
}

impl ResumeCompletion {
    fn new(
        id: super::DeferredId,
        request_id: super::RequestId,
        terminal_observer: Option<ResumeTerminalObserver>,
    ) -> Arc<Self> {
        Arc::new(Self {
            id,
            request_id,
            completed: AtomicBool::new(false),
            result: Mutex::new(None),
            terminal_observer: Mutex::new(terminal_observer),
            changed: Notify::new(),
        })
    }

    fn take_finished(&self) -> ResumeResult {
        self.result
            .lock()
            .take()
            .expect("a synchronously rejected resume publishes its terminal result")
    }

    async fn wait(&self) -> ResumeResult {
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
