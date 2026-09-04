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

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use futures_util::FutureExt;
use rocketmq_runtime::TaskGroup;
use schemars::JsonSchema;
use serde::Serialize;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use crate::audit::AuditContext;
use crate::audit::AuditInvocation;
use crate::audit::AuditResult;
use crate::audit::AuditTrail;
use crate::error::ControlError;
use crate::guard::AuthorizedMutation;
use crate::model::MutationArguments;
use crate::session::MutationAdminSession;
use crate::session::MutationSessionFactory;
use crate::session::SessionError;

pub const RESULT_SCHEMA_VERSION: &str = "rocketmq-mcp-control.result.v1";
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MutationOutcome {
    DryRunCompleted,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MutationResult {
    pub schema_version: &'static str,
    pub outcome: MutationOutcome,
    pub dry_run: bool,
}

#[derive(Clone)]
pub struct WorkflowEngine {
    audit: AuditTrail,
    factory: Arc<dyn MutationSessionFactory>,
    operation_timeout: Duration,
    owner: TaskGroup,
}

impl WorkflowEngine {
    pub fn new(
        audit: AuditTrail,
        factory: Arc<dyn MutationSessionFactory>,
        operation_timeout: Duration,
        owner: TaskGroup,
    ) -> Self {
        Self {
            audit,
            factory,
            operation_timeout,
            owner,
        }
    }

    /// Runs one already-authorized synthetic or future registered operation.
    ///
    /// The durable `started` record precedes session creation. Every acquired session is shut down exactly once.
    pub async fn execute(
        &self,
        authorized: &AuthorizedMutation,
        arguments: &MutationArguments,
        cancellation: &CancellationToken,
    ) -> Result<MutationResult, ControlError> {
        arguments.validate()?;
        let audit_context = AuditContext::try_new(authorized.operator(), arguments.reason.as_deref())?;
        let invocation = self
            .audit
            .start(
                &audit_context,
                authorized.operation(),
                authorized.cluster(),
                arguments.dry_run,
            )
            .await?;
        let (sender, receiver) = oneshot::channel();
        let audit = self.audit.clone();
        let factory = self.factory.clone();
        let owner_cancellation = self.owner.cancellation_token();
        let request_cancellation = cancellation.clone();
        let authorized = authorized.clone();
        let arguments = arguments.clone();
        let operation_timeout = self.operation_timeout;
        let task_invocation = invocation.clone();
        let spawn = self.owner.spawn_service("mcp-control-mutation-supervisor", async move {
            let result = supervise_mutation(
                factory,
                authorized,
                arguments,
                operation_timeout,
                request_cancellation,
                owner_cancellation,
            )
            .await;
            let terminal = persist_terminal(&audit, &task_invocation, &result).await;
            let delivered = terminal.map_or_else(Err, |_| result);
            let _ = sender.send(delivered);
        });
        if spawn.is_err() {
            let error = ControlError::execution_failed();
            persist_terminal(&self.audit, &invocation, &Err(error.clone())).await?;
            return Err(error);
        }
        receiver.await.map_err(|_| ControlError::audit_unavailable())?
    }
}

async fn supervise_mutation(
    factory: Arc<dyn MutationSessionFactory>,
    authorized: AuthorizedMutation,
    arguments: MutationArguments,
    operation_timeout: Duration,
    request_cancellation: CancellationToken,
    owner_cancellation: CancellationToken,
) -> Result<MutationResult, ControlError> {
    let deadline = tokio::time::Instant::now() + operation_timeout;
    let opened = tokio::time::timeout_at(
        deadline,
        AssertUnwindSafe(async move { factory.open().await }).catch_unwind(),
    );
    tokio::pin!(opened);
    let opened = tokio::select! {
        biased;
        opened = &mut opened => opened,
        _ = request_cancellation.cancelled() => return Err(ControlError::cancelled()),
        _ = owner_cancellation.cancelled() => return Err(ControlError::cancelled()),
    };
    let mut session = match opened {
        Ok(Ok(Ok(session))) => session,
        Ok(Ok(Err(error))) => return Err(map_session_error(error)),
        Ok(Err(_)) => return Err(ControlError::execution_failed()),
        Err(_) => return Err(ControlError::timeout()),
    };

    let execution = tokio::select! {
        biased;
        _ = request_cancellation.cancelled() => Err(ControlError::cancelled()),
        _ = owner_cancellation.cancelled() => Err(ControlError::cancelled()),
        result = tokio::time::timeout_at(
            deadline,
            AssertUnwindSafe(run_steps(session.as_mut(), &authorized, &arguments)).catch_unwind(),
        ) => match result {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(ControlError::execution_failed()),
            Err(_) => Err(ControlError::timeout()),
        },
    };

    let shutdown = tokio::time::timeout(SHUTDOWN_TIMEOUT, AssertUnwindSafe(session.shutdown()).catch_unwind()).await;
    match shutdown {
        Ok(Ok(Ok(()))) => execution.map(|outcome| MutationResult {
            schema_version: RESULT_SCHEMA_VERSION,
            outcome,
            dry_run: arguments.dry_run,
        }),
        Ok(Ok(Err(_))) | Ok(Err(_)) | Err(_) => Err(ControlError::shutdown_failed()),
    }
}

async fn persist_terminal(
    audit: &AuditTrail,
    invocation: &AuditInvocation,
    result: &Result<MutationResult, ControlError>,
) -> Result<(), ControlError> {
    let (audit_result, error_code) = match result {
        Ok(result) if result.outcome == MutationOutcome::DryRunCompleted => (AuditResult::Planned, None),
        Ok(_) => (AuditResult::Applied, None),
        Err(error) if error.code() == crate::error::ControlErrorCode::PreconditionConflict => {
            (AuditResult::Conflict, Some(error.code()))
        }
        Err(error) => (AuditResult::Failed, Some(error.code())),
    };
    match AssertUnwindSafe(audit.terminal(invocation, audit_result, error_code))
        .catch_unwind()
        .await
    {
        Ok(result) => result,
        Err(_) => Err(ControlError::audit_unavailable()),
    }
}

async fn run_steps(
    session: &mut dyn MutationAdminSession,
    authorized: &AuthorizedMutation,
    arguments: &MutationArguments,
) -> Result<MutationOutcome, ControlError> {
    session
        .preflight(authorized, arguments)
        .await
        .map_err(map_session_error)?;
    session
        .dry_run(authorized, arguments)
        .await
        .map_err(map_session_error)?;
    if arguments.dry_run {
        return Ok(MutationOutcome::DryRunCompleted);
    }
    session
        .execute(authorized, arguments)
        .await
        .map_err(map_session_error)?;
    session.verify(authorized, arguments).await.map_err(map_session_error)?;
    Ok(MutationOutcome::Completed)
}

const fn map_session_error(error: SessionError) -> ControlError {
    match error {
        SessionError::Conflict => ControlError::precondition_conflict(),
        SessionError::Failed => ControlError::execution_failed(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use tokio::sync::Notify;

    use super::*;
    use crate::audit::AuditEvent;
    use crate::audit::AuditFuture;
    use crate::audit::AuditRecord;
    use crate::audit::MemoryAuditSink;
    use crate::audit::ReliableAuditSink;
    use crate::error::ControlErrorCode;
    use crate::model::ClusterName;
    use crate::model::ControlOperation;
    use crate::model::MUTATION_ARGUMENTS_SCHEMA_VERSION;
    use crate::session::SessionFuture;

    #[derive(Default)]
    struct Counters {
        opens: AtomicUsize,
        acquired: AtomicUsize,
        preflight: AtomicUsize,
        dry_run: AtomicUsize,
        execute: AtomicUsize,
        verify: AtomicUsize,
        shutdown: AtomicUsize,
    }

    struct SyntheticFactory {
        counters: Arc<Counters>,
        behavior: Behavior,
        gate: Arc<Notify>,
    }

    #[derive(Clone, Copy)]
    enum Behavior {
        Success,
        Conflict,
        Failure,
        Block,
        BlockOpen,
        PanicOpen,
        PanicPreflight,
        PanicDryRun,
        PanicExecute,
        PanicVerify,
        ShutdownFail,
        ShutdownHang,
        PanicShutdown,
    }

    impl MutationSessionFactory for SyntheticFactory {
        fn open(&self) -> SessionFuture<'_, Result<Box<dyn MutationAdminSession>, SessionError>> {
            Box::pin(async move {
                self.counters.opens.fetch_add(1, Ordering::SeqCst);
                if matches!(self.behavior, Behavior::PanicOpen) {
                    panic!("synthetic open panic");
                }
                if matches!(self.behavior, Behavior::BlockOpen) {
                    self.gate.notified().await;
                }
                self.counters.acquired.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(SyntheticSession {
                    counters: self.counters.clone(),
                    behavior: self.behavior,
                    gate: self.gate.clone(),
                }) as Box<dyn MutationAdminSession>)
            })
        }
    }

    struct SyntheticSession {
        counters: Arc<Counters>,
        behavior: Behavior,
        gate: Arc<Notify>,
    }

    struct HangingTerminalSink {
        records: tokio::sync::Mutex<Vec<AuditRecord>>,
        appends: AtomicUsize,
    }

    struct HostileAppendSink {
        records: tokio::sync::Mutex<Vec<AuditRecord>>,
        appends: AtomicUsize,
        fail_at: usize,
    }

    impl HostileAppendSink {
        fn new(fail_at: usize) -> Self {
            Self {
                records: tokio::sync::Mutex::new(Vec::new()),
                appends: AtomicUsize::new(0),
                fail_at,
            }
        }
    }

    impl ReliableAuditSink for HostileAppendSink {
        fn append<'a>(&'a self, record: &'a AuditRecord) -> AuditFuture<'a, Result<(), ControlError>> {
            Box::pin(async move {
                let call = self.appends.fetch_add(1, Ordering::SeqCst);
                if call == self.fail_at {
                    Err(ControlError::execution_failed())
                } else {
                    self.records.lock().await.push(record.clone());
                    Ok(())
                }
            })
        }

        fn records(&self) -> AuditFuture<'_, Result<Vec<AuditRecord>, ControlError>> {
            Box::pin(async move { Ok(self.records.lock().await.clone()) })
        }
    }

    impl HangingTerminalSink {
        fn new() -> Self {
            Self {
                records: tokio::sync::Mutex::new(Vec::new()),
                appends: AtomicUsize::new(0),
            }
        }
    }

    impl ReliableAuditSink for HangingTerminalSink {
        fn append<'a>(&'a self, record: &'a AuditRecord) -> AuditFuture<'a, Result<(), ControlError>> {
            Box::pin(async move {
                let call = self.appends.fetch_add(1, Ordering::SeqCst);
                if call == 0 {
                    self.records.lock().await.push(record.clone());
                    Ok(())
                } else {
                    std::future::pending().await
                }
            })
        }

        fn records(&self) -> AuditFuture<'_, Result<Vec<AuditRecord>, ControlError>> {
            Box::pin(async move { Ok(self.records.lock().await.clone()) })
        }
    }

    impl MutationAdminSession for SyntheticSession {
        fn preflight<'a>(
            &'a mut self,
            _authorized: &'a AuthorizedMutation,
            _arguments: &'a MutationArguments,
        ) -> SessionFuture<'a, Result<(), SessionError>> {
            Box::pin(async move {
                self.counters.preflight.fetch_add(1, Ordering::SeqCst);
                if matches!(self.behavior, Behavior::PanicPreflight) {
                    panic!("synthetic preflight panic");
                } else if matches!(self.behavior, Behavior::Conflict) {
                    Err(SessionError::Conflict)
                } else {
                    Ok(())
                }
            })
        }

        fn dry_run<'a>(
            &'a mut self,
            _authorized: &'a AuthorizedMutation,
            _arguments: &'a MutationArguments,
        ) -> SessionFuture<'a, Result<(), SessionError>> {
            Box::pin(async move {
                self.counters.dry_run.fetch_add(1, Ordering::SeqCst);
                if matches!(self.behavior, Behavior::PanicDryRun) {
                    panic!("synthetic dry-run panic");
                } else if matches!(self.behavior, Behavior::Failure) {
                    Err(SessionError::Failed)
                } else {
                    Ok(())
                }
            })
        }

        fn execute<'a>(
            &'a mut self,
            _authorized: &'a AuthorizedMutation,
            _arguments: &'a MutationArguments,
        ) -> SessionFuture<'a, Result<(), SessionError>> {
            Box::pin(async move {
                self.counters.execute.fetch_add(1, Ordering::SeqCst);
                if matches!(self.behavior, Behavior::PanicExecute) {
                    panic!("synthetic execute panic");
                }
                if matches!(self.behavior, Behavior::Block) {
                    self.gate.notified().await;
                }
                Ok(())
            })
        }

        fn verify<'a>(
            &'a mut self,
            _authorized: &'a AuthorizedMutation,
            _arguments: &'a MutationArguments,
        ) -> SessionFuture<'a, Result<(), SessionError>> {
            Box::pin(async move {
                self.counters.verify.fetch_add(1, Ordering::SeqCst);
                if matches!(self.behavior, Behavior::PanicVerify) {
                    panic!("synthetic verify panic");
                }
                Ok(())
            })
        }

        fn shutdown(&mut self) -> SessionFuture<'_, Result<(), SessionError>> {
            Box::pin(async move {
                self.counters.shutdown.fetch_add(1, Ordering::SeqCst);
                match self.behavior {
                    Behavior::ShutdownFail => Err(SessionError::Failed),
                    Behavior::ShutdownHang => {
                        self.gate.notified().await;
                        Ok(())
                    }
                    Behavior::PanicShutdown => panic!("synthetic shutdown panic"),
                    _ => Ok(()),
                }
            })
        }
    }

    fn arguments(dry_run: bool) -> MutationArguments {
        MutationArguments {
            schema_version: MUTATION_ARGUMENTS_SCHEMA_VERSION.to_string(),
            dry_run,
            confirm: !dry_run,
            reason: (!dry_run).then(|| "bounded maintenance reason".to_string()),
            request_key: Some("request-1234".to_string()),
        }
    }

    fn authorized() -> AuthorizedMutation {
        AuthorizedMutation::synthetic(
            ControlOperation::TopicUpsert,
            ClusterName::try_new("cluster-a").unwrap(),
        )
    }

    fn engine(
        behavior: Behavior,
        timeout: Duration,
    ) -> (WorkflowEngine, Arc<Counters>, Arc<MemoryAuditSink>, Arc<Notify>) {
        let counters = Arc::new(Counters::default());
        let sink = Arc::new(MemoryAuditSink::new(16, 4096));
        let gate = Arc::new(Notify::new());
        let factory = Arc::new(SyntheticFactory {
            counters: counters.clone(),
            behavior,
            gate: gate.clone(),
        });
        let runtime = rocketmq_runtime::RuntimeContext::from_current("mcp-control-workflow-test");
        let owner = runtime
            .service_context("mcp-control-workflow-test")
            .task_group()
            .clone();
        (
            WorkflowEngine::new(AuditTrail::new(sink.clone()), factory, timeout, owner),
            counters,
            sink,
            gate,
        )
    }

    async fn wait_for(counter: &AtomicUsize, expected: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while counter.load(Ordering::SeqCst) < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn started_audit_failure_creates_no_session() {
        let counters = Arc::new(Counters::default());
        let sink = Arc::new(MemoryAuditSink::failing(16, 4096));
        let factory = Arc::new(SyntheticFactory {
            counters: counters.clone(),
            behavior: Behavior::Success,
            gate: Arc::new(Notify::new()),
        });
        let runtime = rocketmq_runtime::RuntimeContext::from_current("mcp-control-audit-failure-test");
        let owner = runtime
            .service_context("mcp-control-audit-failure-test")
            .task_group()
            .clone();
        let engine = WorkflowEngine::new(AuditTrail::new(sink), factory, Duration::from_secs(1), owner);
        let error = engine
            .execute(&authorized(), &arguments(false), &CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(error, ControlError::audit_unavailable());
        assert_eq!(counters.opens.load(Ordering::SeqCst), 0);
        assert_eq!(counters.acquired.load(Ordering::SeqCst), 0);
        assert_eq!(counters.preflight.load(Ordering::SeqCst), 0);
        assert_eq!(counters.dry_run.load(Ordering::SeqCst), 0);
        assert_eq!(counters.execute.load(Ordering::SeqCst), 0);
        assert_eq!(counters.verify.load(Ordering::SeqCst), 0);
        assert_eq!(counters.shutdown.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn synthetic_success_uses_one_session_and_ordered_audit() {
        let (engine, counters, sink, _) = engine(Behavior::Success, Duration::from_secs(1));
        let result = engine
            .execute(&authorized(), &arguments(false), &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(result.outcome, MutationOutcome::Completed);
        assert_eq!(counters.opens.load(Ordering::SeqCst), 1);
        assert_eq!(counters.acquired.load(Ordering::SeqCst), 1);
        assert_eq!(counters.preflight.load(Ordering::SeqCst), 1);
        assert_eq!(counters.dry_run.load(Ordering::SeqCst), 1);
        assert_eq!(counters.execute.load(Ordering::SeqCst), 1);
        assert_eq!(counters.verify.load(Ordering::SeqCst), 1);
        assert_eq!(counters.shutdown.load(Ordering::SeqCst), 1);
        let records = sink.records().await.unwrap();
        assert_eq!(
            records.iter().map(|record| record.event).collect::<Vec<_>>(),
            vec![AuditEvent::Started, AuditEvent::Completed]
        );
    }

    #[tokio::test]
    async fn dry_run_never_executes_or_verifies() {
        let (engine, counters, sink, _) = engine(Behavior::Success, Duration::from_secs(1));
        let result = engine
            .execute(&authorized(), &arguments(true), &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(result.outcome, MutationOutcome::DryRunCompleted);
        assert_eq!(counters.preflight.load(Ordering::SeqCst), 1);
        assert_eq!(counters.dry_run.load(Ordering::SeqCst), 1);
        assert_eq!(counters.execute.load(Ordering::SeqCst), 0);
        assert_eq!(counters.verify.load(Ordering::SeqCst), 0);
        assert_eq!(counters.shutdown.load(Ordering::SeqCst), 1);
        assert_eq!(sink.records().await.unwrap()[1].event, AuditEvent::Completed);
    }

    #[tokio::test]
    async fn conflict_and_failure_shutdown_exactly_once() {
        for (behavior, expected) in [
            (Behavior::Conflict, ControlErrorCode::PreconditionConflict),
            (Behavior::Failure, ControlErrorCode::ExecutionFailed),
        ] {
            let (engine, counters, sink, _) = engine(behavior, Duration::from_secs(1));
            let error = engine
                .execute(&authorized(), &arguments(false), &CancellationToken::new())
                .await
                .unwrap_err();
            assert_eq!(error.code(), expected);
            assert_eq!(counters.shutdown.load(Ordering::SeqCst), 1);
            let records = sink.records().await.unwrap();
            assert_eq!(records[0].event, AuditEvent::Started);
            assert_eq!(records[1].event, AuditEvent::Failed);
        }
    }

    #[tokio::test]
    async fn timeout_and_cancellation_shutdown_exactly_once() {
        let (timeout_engine, timeout_counters, _, _) = engine(Behavior::Block, Duration::from_millis(10));
        let timeout_error = timeout_engine
            .execute(&authorized(), &arguments(false), &CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(timeout_error.code(), ControlErrorCode::Timeout);
        assert_eq!(timeout_counters.shutdown.load(Ordering::SeqCst), 1);

        let (cancel_engine, cancel_counters, _, _) = engine(Behavior::Block, Duration::from_secs(1));
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let task = tokio::spawn(async move {
            cancel_engine
                .execute(&authorized(), &arguments(false), &task_cancellation)
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while cancel_counters.execute.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        cancellation.cancel();
        let cancelled = task.await.unwrap().unwrap_err();
        assert_eq!(cancelled.code(), ControlErrorCode::Cancelled);
        assert_eq!(cancel_counters.opens.load(Ordering::SeqCst), 1);
        assert_eq!(cancel_counters.shutdown.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn request_and_owner_cancellation_during_open_leave_no_acquired_session_or_task() {
        let cluster = authorized();
        for owner_cancel in [false, true] {
            let counters = Arc::new(Counters::default());
            let sink = Arc::new(MemoryAuditSink::new(16, 4096));
            let gate = Arc::new(Notify::new());
            let factory = Arc::new(SyntheticFactory {
                counters: counters.clone(),
                behavior: Behavior::BlockOpen,
                gate,
            });
            let runtime = rocketmq_runtime::RuntimeContext::from_current("mcp-control-block-open-test");
            let owner = runtime
                .service_context("mcp-control-block-open-test")
                .task_group()
                .clone();
            let engine = WorkflowEngine::new(
                AuditTrail::new(sink.clone()),
                factory,
                Duration::from_secs(10),
                owner.clone(),
            );
            let cancellation = CancellationToken::new();
            let task_cancellation = cancellation.clone();
            let task = tokio::spawn({
                let cluster = cluster.clone();
                async move { engine.execute(&cluster, &arguments(false), &task_cancellation).await }
            });
            wait_for(&counters.opens, 1).await;
            if owner_cancel {
                let report = owner.shutdown(Duration::from_secs(1)).await;
                assert!(report.is_healthy());
            } else {
                cancellation.cancel();
            }
            let error = task.await.unwrap().unwrap_err();
            assert_eq!(error.code(), ControlErrorCode::Cancelled);
            assert_eq!(counters.acquired.load(Ordering::SeqCst), 0);
            assert_eq!(counters.shutdown.load(Ordering::SeqCst), 0);
            let records = sink.records().await.unwrap();
            assert_eq!(records.len(), 2);
            assert_eq!(records[0].event, AuditEvent::Started);
            assert_eq!(records[1].event, AuditEvent::Failed);
            assert_eq!(records[1].error_code, Some(ControlErrorCode::Cancelled));
        }
    }

    #[tokio::test]
    async fn adapter_panics_are_contained_and_terminally_audited() {
        for behavior in [
            Behavior::PanicOpen,
            Behavior::PanicPreflight,
            Behavior::PanicDryRun,
            Behavior::PanicExecute,
            Behavior::PanicVerify,
        ] {
            let (engine, counters, sink, _) = engine(behavior, Duration::from_secs(1));
            let error = engine
                .execute(&authorized(), &arguments(false), &CancellationToken::new())
                .await
                .unwrap_err();
            assert_eq!(error.code(), ControlErrorCode::ExecutionFailed);
            assert_eq!(
                counters.shutdown.load(Ordering::SeqCst),
                usize::from(!matches!(behavior, Behavior::PanicOpen))
            );
            let records = sink.records().await.unwrap();
            assert_eq!(records.last().unwrap().event, AuditEvent::Failed);
            assert_eq!(
                records.last().unwrap().error_code,
                Some(ControlErrorCode::ExecutionFailed)
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn hanging_failed_and_panicking_shutdown_have_stable_failure() {
        for behavior in [Behavior::ShutdownHang, Behavior::ShutdownFail, Behavior::PanicShutdown] {
            let (engine, counters, sink, _) = engine(behavior, Duration::from_secs(10));
            let error = engine
                .execute(&authorized(), &arguments(false), &CancellationToken::new())
                .await
                .unwrap_err();
            assert_eq!(error.code(), ControlErrorCode::ShutdownFailed);
            assert_eq!(counters.shutdown.load(Ordering::SeqCst), 1);
            let records = sink.records().await.unwrap();
            assert_eq!(
                records.last().unwrap().error_code,
                Some(ControlErrorCode::ShutdownFailed)
            );
        }
    }

    #[tokio::test]
    async fn dropping_caller_after_acquisition_does_not_drop_cleanup() {
        let (engine, counters, sink, gate) = engine(Behavior::Block, Duration::from_secs(1));
        let task = tokio::spawn({
            let engine = engine.clone();
            async move {
                engine
                    .execute(&authorized(), &arguments(false), &CancellationToken::new())
                    .await
            }
        });
        wait_for(&counters.execute, 1).await;
        task.abort();
        gate.notify_waiters();
        wait_for(&counters.shutdown, 1).await;
        tokio::time::timeout(Duration::from_secs(1), async {
            while sink.records().await.unwrap().len() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(counters.shutdown.load(Ordering::SeqCst), 1);
        assert_eq!(sink.records().await.unwrap()[1].event, AuditEvent::Completed);
    }

    #[tokio::test]
    async fn outer_timeout_drop_cannot_bypass_supervisor_cleanup() {
        let (engine, counters, sink, _) = engine(Behavior::Block, Duration::from_millis(25));
        let outer = tokio::time::timeout(
            Duration::from_millis(5),
            engine.execute(&authorized(), &arguments(false), &CancellationToken::new()),
        )
        .await;
        assert!(outer.is_err());
        wait_for(&counters.shutdown, 1).await;
        tokio::time::timeout(Duration::from_secs(1), async {
            while sink.records().await.unwrap().len() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            sink.records().await.unwrap()[1].error_code,
            Some(ControlErrorCode::Timeout)
        );
    }

    #[tokio::test]
    async fn terminal_audit_failure_is_returned_after_shutdown() {
        let counters = Arc::new(Counters::default());
        let sink = Arc::new(HostileAppendSink::new(1));
        let factory = Arc::new(SyntheticFactory {
            counters: counters.clone(),
            behavior: Behavior::Success,
            gate: Arc::new(Notify::new()),
        });
        let runtime = rocketmq_runtime::RuntimeContext::from_current("mcp-control-terminal-failure-test");
        let owner = runtime
            .service_context("mcp-control-terminal-failure-test")
            .task_group()
            .clone();
        let engine = WorkflowEngine::new(AuditTrail::new(sink.clone()), factory, Duration::from_secs(1), owner);
        let error = engine
            .execute(&authorized(), &arguments(false), &CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(error, ControlError::audit_unavailable());
        assert_eq!(counters.shutdown.load(Ordering::SeqCst), 1);
        assert_eq!(sink.records().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn hostile_started_audit_failure_opens_no_session_or_rpc() {
        let counters = Arc::new(Counters::default());
        let sink = Arc::new(HostileAppendSink::new(0));
        let factory = Arc::new(SyntheticFactory {
            counters: counters.clone(),
            behavior: Behavior::Success,
            gate: Arc::new(Notify::new()),
        });
        let runtime = rocketmq_runtime::RuntimeContext::from_current("mcp-control-started-failure-test");
        let owner = runtime
            .service_context("mcp-control-started-failure-test")
            .task_group()
            .clone();
        let engine = WorkflowEngine::new(AuditTrail::new(sink.clone()), factory, Duration::from_secs(1), owner);
        let error = engine
            .execute(&authorized(), &arguments(false), &CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(error, ControlError::audit_unavailable());
        assert_eq!(counters.opens.load(Ordering::SeqCst), 0);
        assert_eq!(counters.acquired.load(Ordering::SeqCst), 0);
        assert_eq!(counters.preflight.load(Ordering::SeqCst), 0);
        assert_eq!(counters.dry_run.load(Ordering::SeqCst), 0);
        assert_eq!(counters.execute.load(Ordering::SeqCst), 0);
        assert_eq!(counters.verify.load(Ordering::SeqCst), 0);
        assert_eq!(counters.shutdown.load(Ordering::SeqCst), 0);
        assert!(sink.records().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn owner_abort_during_terminal_audit_poisoned_the_trail_and_opened_no_second_session() {
        let counters = Arc::new(Counters::default());
        let sink = Arc::new(HangingTerminalSink::new());
        let audit = AuditTrail::new(sink.clone());
        let factory = Arc::new(SyntheticFactory {
            counters: counters.clone(),
            behavior: Behavior::Success,
            gate: Arc::new(Notify::new()),
        });
        let runtime = rocketmq_runtime::RuntimeContext::from_current("mcp-control-terminal-abort-test");
        let owner = runtime
            .service_context("mcp-control-terminal-abort-test")
            .task_group()
            .clone();
        let engine = WorkflowEngine::new(audit.clone(), factory, Duration::from_secs(10), owner.clone());
        let task = tokio::spawn({
            let engine = engine.clone();
            async move {
                engine
                    .execute(&authorized(), &arguments(false), &CancellationToken::new())
                    .await
            }
        });
        wait_for(&sink.appends, 2).await;
        let report = owner.shutdown(Duration::from_millis(10)).await;
        assert!(!report.is_healthy());
        let error = task.await.unwrap().unwrap_err();
        assert_eq!(error.code(), ControlErrorCode::AuditUnavailable);
        assert_eq!(counters.opens.load(Ordering::SeqCst), 1);
        assert_eq!(counters.shutdown.load(Ordering::SeqCst), 1);
        assert!(audit.records().await.is_err());
        assert_eq!(
            engine
                .execute(&authorized(), &arguments(false), &CancellationToken::new())
                .await
                .unwrap_err()
                .code(),
            ControlErrorCode::AuditUnavailable
        );
        assert_eq!(counters.opens.load(Ordering::SeqCst), 1);
    }
}
