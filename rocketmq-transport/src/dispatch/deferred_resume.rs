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
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use parking_lot::Mutex;
use rocketmq_error::RocketMQResult;
use rocketmq_runtime::OperationContext;
use tokio::sync::Notify;

use super::authorized_dispatcher::admission_response;
use super::deferred_registry::ClaimExecutionParts;
use super::ClaimedDeferred;
use super::DeferredResponseError;
use super::DeferredResponseErrorKind;
use super::DeferredResumeError;
use super::DeferredResumeErrorKind;
use super::DeferredResumeRetainedSize;
use super::DeferredWakeReason;
use super::RequestControlView;
use super::ResponsePlan;
use super::ResponseReceipt;
use crate::admission::AdmissionClass;
use crate::admission::AdmissionError;
use crate::admission::FullPolicy;
use crate::request_ordering::RequestOrdering;

type ResumeResult = Result<ResponseReceipt, DeferredResumeError>;
type WorkFuture = Pin<Box<dyn Future<Output = ResumeResult> + Send + 'static>>;

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
    let Some(context) = claimed.resume_context() else {
        drop(handler);
        drop(claimed);
        return Err(DeferredResumeError::new(
            DeferredResumeErrorKind::ExecutorClosing,
            id,
            request_id,
            None,
            None,
            None,
        ));
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
    let completion = ResumeCompletion::new(id, request_id);
    let work = ResumeWorkImpl {
        parts: Some(parts),
        handler: Some(handler),
    };
    let job = DeferredResumeJob::new(
        execution_bytes,
        context.class,
        context.ordering,
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
                job.finish_without_execution(DeferredResumeError::new(
                    DeferredResumeErrorKind::ExecutorClosing,
                    id,
                    request_id,
                    None,
                    None,
                    Some(Box::new(source)),
                ));
            }
        }
    }
    completion.wait().await
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
        work: Box<dyn DeferredResumeWork>,
        completion: Arc<ResumeCompletion>,
    ) -> Self {
        Self {
            retained_bytes,
            class,
            ordering,
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

    fn finish_without_execution(mut self, error: DeferredResumeError) {
        drop(self.work.take());
        self.completion.finish(Err(error));
        self.active = false;
    }
}

impl Drop for DeferredResumeJob {
    fn drop(&mut self) {
        if self.active {
            drop(self.work.take());
            self.completion.finish(Err(DeferredResumeError::new(
                DeferredResumeErrorKind::TaskTerminated,
                self.completion.id,
                self.completion.request_id,
                None,
                None,
                None,
            )));
            self.active = false;
        }
    }
}

trait DeferredResumeWork: Send + 'static {
    fn release_wait_permit(&mut self);
    fn execute(self: Box<Self>) -> WorkFuture;
    fn reject(self: Box<Self>, error: AdmissionError) -> WorkFuture;
}

struct ResumeWorkImpl<R, F>
where
    R: Send + 'static,
    F: Send + 'static,
{
    parts: Option<ClaimExecutionParts<R>>,
    handler: Option<F>,
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
        Box::pin(async move {
            let parts = self.parts.take().expect("resume work owns claimed parts");
            let handler = self.handler.take().expect("resume work owns its handler");
            execute_work(parts, handler).await
        })
    }

    fn reject(mut self: Box<Self>, error: AdmissionError) -> WorkFuture {
        Box::pin(async move {
            let parts = self.parts.take().expect("resume work owns claimed parts");
            drop(self.handler.take());
            reject_work(parts, error).await
        })
    }
}

async fn execute_work<R, F, Fut>(parts: ClaimExecutionParts<R>, handler: F) -> ResumeResult
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
    let control = responder.control().clone();
    match current_stop(&control) {
        Some(ResumeStop::ParentCancelled) => {
            drop(handler);
            drop(resume);
            return finish_lifecycle(id, request_id, responder, marker, ResumeStop::ParentCancelled);
        }
        Some(ResumeStop::SessionClosed) => {
            drop(handler);
            drop(resume);
            return finish_lifecycle(id, request_id, responder, marker, ResumeStop::SessionClosed);
        }
        Some(ResumeStop::DeadlineExpired) => {
            drop(handler);
            drop(resume);
            let result = map_response(id, request_id, responder.respond_deadline().await);
            drop(marker);
            return result;
        }
        None => {}
    }
    enum HandlerOutcome<T> {
        Completed(RocketMQResult<T>),
        Stopped(ResumeStop),
    }
    let outcome = {
        let handler_future = handler(resume, reason);
        tokio::pin!(handler_future);
        match control.deadline() {
            Some(deadline) => {
                tokio::select! {
                    biased;
                    () = control.parent_or_session_cancelled() => {
                        HandlerOutcome::Stopped(current_stop(&control).unwrap_or(ResumeStop::ParentCancelled))
                    }
                    result = deadline.timeout(&mut handler_future) => match result {
                        Ok(result) => HandlerOutcome::Completed(result),
                        Err(_) => HandlerOutcome::Stopped(ResumeStop::DeadlineExpired),
                    },
                }
            }
            None => {
                tokio::select! {
                    biased;
                    () = control.parent_or_session_cancelled() => {
                        HandlerOutcome::Stopped(current_stop(&control).unwrap_or(ResumeStop::ParentCancelled))
                    }
                    result = &mut handler_future => HandlerOutcome::Completed(result),
                }
            }
        }
    };
    let result = match outcome {
        HandlerOutcome::Stopped(ResumeStop::ParentCancelled) => {
            finish_lifecycle(id, request_id, responder, marker, ResumeStop::ParentCancelled)
        }
        HandlerOutcome::Stopped(ResumeStop::SessionClosed) => {
            finish_lifecycle(id, request_id, responder, marker, ResumeStop::SessionClosed)
        }
        HandlerOutcome::Stopped(ResumeStop::DeadlineExpired) => {
            let result = map_response(id, request_id, responder.respond_deadline().await);
            drop(marker);
            result
        }
        HandlerOutcome::Completed(Ok(plan)) => {
            let result = map_response(id, request_id, responder.respond(plan).await);
            drop(marker);
            result
        }
        HandlerOutcome::Completed(Err(error)) => {
            let plan = match crate::error_response::response_plan_from_error(&error) {
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
            let result = map_response(id, request_id, responder.respond(plan).await);
            drop(marker);
            result
        }
    };
    result
}

async fn reject_work<R>(parts: ClaimExecutionParts<R>, error: AdmissionError) -> ResumeResult
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
    drop(resume);
    match current_stop(responder.control()) {
        Some(ResumeStop::ParentCancelled) => {
            return finish_lifecycle(id, request_id, responder, marker, ResumeStop::ParentCancelled);
        }
        Some(ResumeStop::SessionClosed) => {
            return finish_lifecycle(id, request_id, responder, marker, ResumeStop::SessionClosed);
        }
        Some(ResumeStop::DeadlineExpired) => {
            let result = map_response(id, request_id, responder.respond_deadline().await);
            drop(marker);
            return result;
        }
        None => {}
    }
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
        let result = map_response(id, request_id, responder.respond(plan).await);
        drop(marker);
        result
    } else {
        drop(responder);
        drop(marker);
        Err(DeferredResumeError::new(
            DeferredResumeErrorKind::Admission,
            id,
            request_id,
            None,
            None,
            Some(Box::new(error)),
        ))
    }
}

#[derive(Clone, Copy)]
enum ResumeStop {
    ParentCancelled,
    SessionClosed,
    DeadlineExpired,
}

fn current_stop(control: &RequestControlView) -> Option<ResumeStop> {
    if control.parent_is_cancelled() {
        Some(ResumeStop::ParentCancelled)
    } else if control.session_is_closed() {
        Some(ResumeStop::SessionClosed)
    } else if control
        .deadline()
        .is_some_and(crate::deadline::RequestDeadline::is_expired)
    {
        Some(ResumeStop::DeadlineExpired)
    } else {
        None
    }
}

fn finish_lifecycle<R>(
    id: super::DeferredId,
    request_id: super::RequestId,
    responder: super::DeferredResponder,
    marker: Arc<super::deferred_registry::ClaimMarker<R>>,
    stop: ResumeStop,
) -> ResumeResult
where
    R: Send + 'static,
{
    let kind = match stop {
        ResumeStop::ParentCancelled => {
            drop(responder.cancel());
            DeferredResumeErrorKind::Cancelled
        }
        ResumeStop::SessionClosed => {
            drop(responder.close());
            DeferredResumeErrorKind::SessionClosed
        }
        ResumeStop::DeadlineExpired => DeferredResumeErrorKind::Response,
    };
    drop(marker);
    Err(DeferredResumeError::new(kind, id, request_id, None, None, None))
}

fn map_response(
    id: super::DeferredId,
    request_id: super::RequestId,
    result: Result<ResponseReceipt, DeferredResponseError>,
) -> ResumeResult {
    result.map_err(|source| {
        let kind = match source.kind() {
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
        DeferredResumeError::new(
            kind,
            id,
            request_id,
            source.prior_terminal_state(),
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
    changed: Notify,
}

impl ResumeCompletion {
    fn new(id: super::DeferredId, request_id: super::RequestId) -> Arc<Self> {
        Arc::new(Self {
            id,
            request_id,
            completed: AtomicBool::new(false),
            result: Mutex::new(None),
            changed: Notify::new(),
        })
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
            *self.result.lock() = Some(result);
            self.changed.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::alloc::Layout;
    use std::mem::size_of;
    use std::net::IpAddr;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    use rocketmq_runtime::RuntimeConfig;
    use rocketmq_runtime::RuntimeOwner;
    use rocketmq_runtime::ShutdownDeadline;
    use tokio::sync::Notify;

    use super::checked_execution_charge;
    use super::checked_resume_fixed_sum;
    use super::deferred_resume_fixed_bytes;
    use super::execution_retained_bytes;
    use super::ClaimExecutionParts;
    use super::DeferredResumeJob;
    use super::DeferredResumeSubmitError;
    use super::DeferredResumeWork;
    use super::ResumeCompletion;
    use super::ResumeJobCell;
    use super::WorkFuture;
    use crate::admission::AdmissionClass;
    use crate::admission::AdmissionController;
    use crate::admission::AdmissionLimits;
    use crate::admission::AdmissionScope;
    use crate::admission::ResourceLimit;
    use crate::dispatch::DeferredId;
    use crate::dispatch::DeferredResumeError;
    use crate::dispatch::DeferredResumeErrorKind;
    use crate::dispatch::RequestId;
    use crate::request_ordering::RequestOrdering;
    use crate::request_ordering::RequestOrderingKey;
    use crate::session_executor::SessionExecutor;

    #[repr(align(128))]
    struct HighAlignHandler([u8; 33]);

    #[repr(align(256))]
    struct HighAlignFuture([u8; 65]);

    struct WorkOracle<R, F>
    where
        R: Send + 'static,
    {
        _parts: Option<ClaimExecutionParts<R>>,
        _handler: Option<F>,
    }

    fn arc_allocation<T>() -> usize {
        let header = Layout::array::<AtomicUsize>(2).expect("Arc header layout");
        let (allocation, _) = header.extend(Layout::new::<T>()).expect("Arc data layout");
        allocation.pad_to_align().size()
    }

    struct ProbeWork {
        wait_released: Arc<AtomicBool>,
        entered: Arc<Notify>,
        release: Option<Arc<Notify>>,
        executions: Arc<AtomicUsize>,
    }

    impl DeferredResumeWork for ProbeWork {
        fn release_wait_permit(&mut self) {
            self.wait_released.store(true, Ordering::Release);
        }

        fn execute(self: Box<Self>) -> WorkFuture {
            Box::pin(async move {
                assert!(self.wait_released.load(Ordering::Acquire));
                self.executions.fetch_add(1, Ordering::AcqRel);
                self.entered.notify_one();
                if let Some(release) = self.release {
                    release.notified().await;
                }
                Err(test_resume_error(DeferredResumeErrorKind::Response))
            })
        }

        fn reject(self: Box<Self>, _error: crate::admission::AdmissionError) -> WorkFuture {
            Box::pin(async move {
                assert!(self.wait_released.load(Ordering::Acquire));
                self.entered.notify_one();
                Err(test_resume_error(DeferredResumeErrorKind::Admission))
            })
        }
    }

    fn test_resume_error(kind: DeferredResumeErrorKind) -> DeferredResumeError {
        DeferredResumeError::new(
            kind,
            DeferredId::for_test(9814),
            RequestId::real(9814, 1).expect("test request id"),
            None,
            None,
            None,
        )
    }

    fn probe_job(
        retained_bytes: usize,
        ordering: RequestOrdering,
        release: Option<Arc<Notify>>,
    ) -> (
        DeferredResumeJob,
        Arc<ResumeCompletion>,
        Arc<AtomicBool>,
        Arc<Notify>,
        Arc<AtomicUsize>,
    ) {
        let completion = ResumeCompletion::new(
            DeferredId::for_test(9814),
            RequestId::real(9814, 1).expect("test request id"),
        );
        let wait_released = Arc::new(AtomicBool::new(false));
        let entered = Arc::new(Notify::new());
        let executions = Arc::new(AtomicUsize::new(0));
        let work = ProbeWork {
            wait_released: Arc::clone(&wait_released),
            entered: Arc::clone(&entered),
            release,
            executions: Arc::clone(&executions),
        };
        (
            DeferredResumeJob::new(
                retained_bytes,
                AdmissionClass::Data,
                ordering,
                Box::new(work),
                Arc::clone(&completion),
            ),
            completion,
            wait_released,
            entered,
            executions,
        )
    }

    fn executor_with_limits(
        name: &'static str,
        limits: AdmissionLimits,
    ) -> (RuntimeOwner, AdmissionController, SessionExecutor) {
        let runtime = RuntimeOwner::new(RuntimeConfig::server_default(name)).expect("resume test runtime");
        let service = runtime.root_context().component(name);
        let controller = AdmissionController::new(limits);
        let scope = controller
            .prepare_scope(AdmissionScope::new(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)).with_session(9814))
            .expect("resume admission scope");
        let executor = SessionExecutor::try_new(service.task_group(), scope).expect("resume session executor");
        (runtime, controller, executor)
    }

    #[test]
    fn execution_charge_counts_each_handler_component_once_and_checks_every_addition() {
        assert_eq!(checked_execution_charge(11, 13, 17, 19, 23), Some(83));
        assert_eq!(checked_execution_charge(usize::MAX, 1, 0, 0, 0), None);
        assert_eq!(checked_execution_charge(usize::MAX - 1, 1, 1, 0, 0), None);
        assert_eq!(checked_execution_charge(usize::MAX - 2, 1, 1, 1, 0), None);
        assert_eq!(checked_execution_charge(usize::MAX - 3, 1, 1, 1, 1), None);
    }

    #[test]
    fn fixed_resume_metadata_checks_completion_and_job_cell_additions() {
        assert_eq!(checked_resume_fixed_sum(11, 13), Some(24));
        assert_eq!(checked_resume_fixed_sum(usize::MAX, 1), None);
    }

    #[test]
    fn high_alignment_execution_storage_matches_an_independent_layout_oracle() {
        let original = 29usize;
        let dynamic = 31usize;
        let work = Layout::new::<WorkOracle<u128, HighAlignHandler>>()
            .pad_to_align()
            .size();
        let expected = original
            .checked_add(size_of::<HighAlignHandler>())
            .and_then(|bytes| bytes.checked_add(size_of::<HighAlignFuture>()))
            .and_then(|bytes| bytes.checked_add(dynamic))
            .and_then(|bytes| bytes.checked_add(work));
        assert_eq!(
            execution_retained_bytes::<u128, HighAlignHandler, HighAlignFuture>(original, dynamic),
            expected
        );
        assert_eq!(
            deferred_resume_fixed_bytes(),
            arc_allocation::<ResumeCompletion>().checked_add(arc_allocation::<ResumeJobCell>())
        );
        let _ = HighAlignHandler([0; 33]).0;
        let _ = HighAlignFuture([0; 65]).0;
    }

    #[tokio::test]
    async fn real_queued_admission_rejects_the_checked_high_alignment_charge_and_returns_the_exact_job() {
        let charge = execution_retained_bytes::<u128, HighAlignHandler, HighAlignFuture>(29, 31)
            .expect("checked high-alignment charge");
        let defaults = AdmissionLimits::default();
        let control_reserve = ResourceLimit { count: 1, bytes: 64 };
        let limits = AdmissionLimits {
            queued: ResourceLimit {
                bytes: charge + control_reserve.bytes - 1,
                ..defaults.queued
            },
            control_reserve,
            ..defaults
        };
        let (_runtime, controller, executor) = executor_with_limits("deferred-resume-queued-reject", limits);
        let (job, completion, wait_released, _entered, _executions) =
            probe_job(charge, RequestOrdering::Concurrent, None);
        let cell = Arc::new(ResumeJobCell::new(job));
        cell.release_wait_permit();
        assert!(wait_released.load(Ordering::Acquire));
        let returned = match executor
            .deferred_resume_executor()
            .try_execute_resume(Arc::clone(&cell))
        {
            Err(DeferredResumeSubmitError::Admission { cell, .. }) => cell,
            Err(DeferredResumeSubmitError::Closing { .. }) => panic!("executor unexpectedly closed"),
            Ok(_) => panic!("under-capacity queued budget accepted the resume job"),
        };
        assert!(Arc::ptr_eq(&cell, &returned));
        drop(returned);
        drop(cell);
        assert_eq!(
            completion.wait().await.expect_err("unexecuted job terminates").kind(),
            DeferredResumeErrorKind::TaskTerminated
        );
        let snapshot = controller.snapshot();
        assert_eq!(snapshot.queued.current_count, 0);
        assert_eq!(snapshot.inflight.current_count, 0);
        assert_eq!(snapshot.processors.current_count, 0);
    }

    #[tokio::test]
    async fn inflight_admission_rejection_returns_the_exact_job_and_releases_queued_capacity() {
        let charge = 512usize;
        let defaults = AdmissionLimits::default();
        let control_reserve = ResourceLimit { count: 1, bytes: 64 };
        let limits = AdmissionLimits {
            inflight: ResourceLimit {
                bytes: charge + control_reserve.bytes - 1,
                ..defaults.inflight
            },
            control_reserve,
            ..defaults
        };
        let (_runtime, controller, executor) = executor_with_limits("deferred-resume-inflight-reject", limits);
        let (job, completion, _wait_released, _entered, _executions) =
            probe_job(charge, RequestOrdering::Concurrent, None);
        let cell = Arc::new(ResumeJobCell::new(job));
        cell.release_wait_permit();
        let returned = match executor
            .deferred_resume_executor()
            .try_execute_resume(Arc::clone(&cell))
        {
            Err(DeferredResumeSubmitError::Admission { cell, .. }) => cell,
            Err(DeferredResumeSubmitError::Closing { .. }) => panic!("executor unexpectedly closed"),
            Ok(_) => panic!("under-capacity inflight budget accepted the resume job"),
        };
        assert!(Arc::ptr_eq(&cell, &returned));
        drop(returned);
        drop(cell);
        assert_eq!(
            completion.wait().await.expect_err("unexecuted job terminates").kind(),
            DeferredResumeErrorKind::TaskTerminated
        );
        let snapshot = controller.snapshot();
        assert_eq!(snapshot.queued.current_count, 0);
        assert_eq!(snapshot.inflight.current_count, 0);
    }

    #[tokio::test]
    async fn processor_rejection_runs_inside_the_owned_task_and_releases_all_capacity() {
        let charge = 512usize;
        let defaults = AdmissionLimits::default();
        let control_reserve = ResourceLimit { count: 1, bytes: 64 };
        let limits = AdmissionLimits {
            processors: ResourceLimit {
                bytes: charge + control_reserve.bytes - 1,
                ..defaults.processors
            },
            control_reserve,
            ..defaults
        };
        let (_runtime, controller, executor) = executor_with_limits("deferred-resume-processor-reject", limits);
        let (job, completion, _wait_released, rejected, executions) =
            probe_job(charge, RequestOrdering::Concurrent, None);
        let cell = Arc::new(ResumeJobCell::new(job));
        cell.release_wait_permit();
        let submitted = executor
            .deferred_resume_executor()
            .try_execute_resume(Arc::clone(&cell));
        assert!(submitted.is_ok(), "processor rejection happens inside an accepted task");
        drop(cell);
        rejected.notified().await;
        assert_eq!(executions.load(Ordering::Acquire), 0);
        assert_eq!(
            completion.wait().await.expect_err("processor rejection result").kind(),
            DeferredResumeErrorKind::Admission
        );
        let report = executor
            .drain_until(ShutdownDeadline::after(Duration::from_secs(1)))
            .await;
        assert_eq!(report.aborted, 0);
        let snapshot = controller.snapshot();
        assert_eq!(snapshot.queued.current_count, 0);
        assert_eq!(snapshot.inflight.current_count, 0);
        assert_eq!(snapshot.processors.current_count, 0);
    }

    #[tokio::test]
    async fn operation_close_at_spawn_returns_the_exact_job_and_releases_both_permits() {
        let (_runtime, controller, executor) =
            executor_with_limits("deferred-resume-spawn-reject", AdmissionLimits::default());
        executor.close_resume_operation_before_spawn_for_test();
        let (job, completion, _wait_released, _entered, _executions) =
            probe_job(512, RequestOrdering::Concurrent, None);
        let cell = Arc::new(ResumeJobCell::new(job));
        cell.release_wait_permit();
        let returned = match executor
            .deferred_resume_executor()
            .try_execute_resume(Arc::clone(&cell))
        {
            Err(DeferredResumeSubmitError::Closing { cell, .. }) => cell,
            Err(DeferredResumeSubmitError::Admission { .. }) => panic!("capacity unexpectedly rejected"),
            Ok(_) => panic!("closed operation accepted a resume task"),
        };
        assert!(Arc::ptr_eq(&cell, &returned));
        drop(returned);
        drop(cell);
        assert_eq!(
            completion.wait().await.expect_err("unspawned job terminates").kind(),
            DeferredResumeErrorKind::TaskTerminated
        );
        let snapshot = controller.snapshot();
        assert_eq!(snapshot.queued.current_count, 0);
        assert_eq!(snapshot.inflight.current_count, 0);
        assert_eq!(snapshot.processors.current_count, 0);
    }

    #[tokio::test]
    async fn accepted_never_polled_is_recovered_as_task_terminated_without_leaking_admission() {
        let (_runtime, _controller, executor) =
            executor_with_limits("deferred-resume-never-polled", AdmissionLimits::default());
        let (job, completion, wait_released, _entered, _executions) = probe_job(128, RequestOrdering::Concurrent, None);
        let first_poll_entered = Arc::new(Notify::new());
        let never_release = Arc::new(Notify::new());
        let cell = Arc::new(ResumeJobCell::with_first_poll_gate(
            job,
            Arc::clone(&first_poll_entered),
            never_release,
        ));
        cell.release_wait_permit();
        assert!(wait_released.load(Ordering::Acquire));
        let submitted = executor
            .deferred_resume_executor()
            .try_execute_resume(Arc::clone(&cell));
        assert!(submitted.is_ok(), "resume task must be accepted");
        drop(cell);
        first_poll_entered.notified().await;
        let report = executor.drain_until(ShutdownDeadline::after(Duration::ZERO)).await;
        assert_eq!(report.aborted, 1);
        assert_eq!(
            completion
                .wait()
                .await
                .expect_err("aborted owner terminates job")
                .kind(),
            DeferredResumeErrorKind::TaskTerminated
        );
    }

    #[tokio::test]
    async fn dropping_the_caller_completion_after_acceptance_does_not_cancel_the_owned_job() {
        let (_runtime, _controller, executor) =
            executor_with_limits("deferred-resume-caller-drop", AdmissionLimits::default());
        let (job, completion, _wait_released, entered, executions) = probe_job(128, RequestOrdering::Concurrent, None);
        let cell = Arc::new(ResumeJobCell::new(job));
        cell.release_wait_permit();
        let submitted = executor
            .deferred_resume_executor()
            .try_execute_resume(Arc::clone(&cell));
        assert!(submitted.is_ok(), "resume task must be accepted");
        drop(cell);
        drop(completion);
        entered.notified().await;
        assert_eq!(executions.load(Ordering::Acquire), 1);
        let report = executor
            .drain_until(ShutdownDeadline::after(Duration::from_secs(1)))
            .await;
        assert_eq!(report.aborted, 0);
    }

    #[tokio::test]
    async fn same_key_resume_jobs_remain_serialized_while_the_first_job_is_running() {
        let (_runtime, _controller, executor) =
            executor_with_limits("deferred-resume-ordering", AdmissionLimits::default());
        let ordering = RequestOrdering::Ordered(RequestOrderingKey::new(17));
        let release_first = Arc::new(Notify::new());
        let (first_job, first_completion, _first_wait, first_entered, first_executions) =
            probe_job(128, ordering, Some(Arc::clone(&release_first)));
        let second_before_ordering = Arc::new(Notify::new());
        let (second_job, second_completion, _second_wait, second_entered, second_executions) =
            probe_job(128, ordering, None);
        let first = Arc::new(ResumeJobCell::new(first_job));
        let second = Arc::new(ResumeJobCell::new(
            second_job.with_before_ordering(Arc::clone(&second_before_ordering)),
        ));
        first.release_wait_permit();
        second.release_wait_permit();
        let route = executor.deferred_resume_executor();
        let first_submitted = route.try_execute_resume(Arc::clone(&first));
        assert!(first_submitted.is_ok(), "first resume must be accepted");
        drop(first);
        first_entered.notified().await;
        let second_submitted = route.try_execute_resume(Arc::clone(&second));
        assert!(second_submitted.is_ok(), "second resume must be accepted");
        drop(second);
        second_before_ordering.notified().await;
        assert_eq!(first_executions.load(Ordering::Acquire), 1);
        assert_eq!(second_executions.load(Ordering::Acquire), 0);
        release_first.notify_one();
        second_entered.notified().await;
        assert_eq!(second_executions.load(Ordering::Acquire), 1);
        assert_eq!(
            first_completion.wait().await.expect_err("probe response").kind(),
            DeferredResumeErrorKind::Response
        );
        assert_eq!(
            second_completion.wait().await.expect_err("probe response").kind(),
            DeferredResumeErrorKind::Response
        );
        let report = executor
            .drain_until(ShutdownDeadline::after(Duration::from_secs(1)))
            .await;
        assert_eq!(report.aborted, 0);
    }
}
