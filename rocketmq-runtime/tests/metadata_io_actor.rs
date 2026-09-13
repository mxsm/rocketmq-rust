// Copyright 2023 The RocketMQ Rust Authors
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

use std::error::Error;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::time::Duration;

use rocketmq_runtime::BlockingPoolPolicy;
use rocketmq_runtime::LocalMetadataFileSystem;
use rocketmq_runtime::MetadataDeadline;
use rocketmq_runtime::MetadataFileSystem;
use rocketmq_runtime::MetadataGeneration;
use rocketmq_runtime::MetadataIoActor;
use rocketmq_runtime::MetadataIoAdmissionOutcome;
use rocketmq_runtime::MetadataIoCommitAdmissionOutcome;
use rocketmq_runtime::MetadataIoCommitOutcome;
use rocketmq_runtime::MetadataIoConfig;
use rocketmq_runtime::MetadataIoOperation;
use rocketmq_runtime::MetadataWriteRequest;
use rocketmq_runtime::RuntimeContext;
use rocketmq_runtime::RuntimeError;
use rocketmq_runtime::RuntimeResult;
use tempfile::TempDir;
use tokio::sync::Notify;

#[derive(Debug, Default)]
struct Gate {
    released: Mutex<bool>,
    condition: Condvar,
    started: Notify,
}

impl Gate {
    fn wait(&self) {
        self.started.notify_waiters();
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.condition.wait(released).unwrap();
        }
    }

    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.condition.notify_all();
    }
}

#[derive(Debug, Default)]
struct GateRecordingFileSystem {
    gate: Arc<Gate>,
    writes: Mutex<Vec<Vec<u8>>>,
}

impl MetadataFileSystem for GateRecordingFileSystem {
    fn persist_atomic(&self, _target: &Path, bytes: &[u8]) -> RuntimeResult<()> {
        self.gate.wait();
        self.writes.lock().unwrap().push(bytes.to_vec());
        Ok(())
    }
}

#[derive(Debug, Default)]
struct RecordingFileSystem {
    writes: Mutex<Vec<Vec<u8>>>,
}

impl MetadataFileSystem for RecordingFileSystem {
    fn persist_atomic(&self, _target: &Path, bytes: &[u8]) -> RuntimeResult<()> {
        self.writes.lock().unwrap().push(bytes.to_vec());
        Ok(())
    }
}

#[derive(Debug)]
struct FailingFileSystem {
    operation: MetadataIoOperation,
    error_kind: io::ErrorKind,
}

impl MetadataFileSystem for FailingFileSystem {
    fn persist_atomic(&self, _target: &Path, _bytes: &[u8]) -> RuntimeResult<()> {
        Err(RuntimeError::io(
            self.operation.runtime_operation(),
            io::Error::new(self.error_kind, "injected metadata I/O failure"),
        ))
    }
}

#[derive(Debug)]
struct PanickingFileSystem;

impl MetadataFileSystem for PanickingFileSystem {
    fn persist_atomic(&self, _target: &Path, _bytes: &[u8]) -> RuntimeResult<()> {
        panic!("injected metadata worker panic");
    }
}

fn config(max_pending_operations: usize, max_pending_bytes: usize) -> MetadataIoConfig {
    MetadataIoConfig {
        max_pending_operations,
        max_pending_bytes,
        blocking_queue_timeout: Duration::from_secs(5),
        blocking_task_timeout: Duration::from_secs(30),
        blocking_warn_after: Duration::from_secs(30),
    }
}

fn start_actor(
    file_system: Arc<dyn MetadataFileSystem>,
    config: MetadataIoConfig,
) -> (RuntimeContext, MetadataIoActor) {
    let context = RuntimeContext::try_from_current("metadata-io-test").unwrap();
    let actor = start_actor_in(&context, "test-service", file_system, config);
    (context, actor)
}

fn start_actor_in(
    context: &RuntimeContext,
    scope: &'static str,
    file_system: Arc<dyn MetadataFileSystem>,
    config: MetadataIoConfig,
) -> MetadataIoActor {
    config
        .into_plan()
        .unwrap()
        .start_with_file_system(&context.service_context(scope), file_system)
        .unwrap()
}

fn request(resource: &str, generation: u64, bytes: &[u8]) -> MetadataWriteRequest {
    MetadataWriteRequest::new(
        resource,
        generation,
        PathBuf::from(format!("{resource}.json")),
        bytes.to_vec(),
    )
}

fn accepted(outcome: MetadataIoAdmissionOutcome) -> rocketmq_runtime::MetadataIoReceipt {
    match outcome {
        MetadataIoAdmissionOutcome::Accepted(receipt) => receipt,
        MetadataIoAdmissionOutcome::TargetConflict(_) => {
            panic!("test request unexpectedly conflicted with another target")
        }
    }
}

#[tokio::test]
async fn queue_and_retained_bytes_are_bounded_independently() {
    let file_system = Arc::new(GateRecordingFileSystem::default());
    let started = file_system.gate.started.notified();
    let (_context, actor) = start_actor(file_system.clone(), config(1, 4));
    let deadline = MetadataDeadline::after(Duration::from_secs(5));
    let first = accepted(actor.submit(request("first", 1, b"1234"), deadline).unwrap());
    started.await;

    let queue_error = actor.submit(request("second", 1, b"1"), deadline).unwrap_err();
    assert_eq!(
        queue_error.condition(),
        rocketmq_error::CanonicalCondition::ResourceExhausted
    );

    file_system.gate.release();
    assert_eq!(first.wait_until(deadline).await.unwrap(), MetadataGeneration::new(1));
    let report = actor.shutdown_until(deadline).await;
    assert!(!report.timed_out);

    let (_context, actor) = start_actor(Arc::new(LocalMetadataFileSystem), config(2, 3));
    let byte_error = actor.submit(request("too-large", 1, b"1234"), deadline).unwrap_err();
    assert_eq!(
        byte_error.condition(),
        rocketmq_error::CanonicalCondition::ResourceExhausted
    );
    assert_eq!(actor.snapshot().pending_operations, 0);
    assert!(!actor.shutdown_until(deadline).await.timed_out);
}

#[tokio::test]
async fn queued_generations_coalesce_without_losing_waiters() {
    let file_system = Arc::new(GateRecordingFileSystem::default());
    let started = file_system.gate.started.notified();
    let (_context, actor) = start_actor(file_system.clone(), config(3, 64));
    let deadline = MetadataDeadline::after(Duration::from_secs(5));
    let first = accepted(actor.submit(request("routes", 1, b"one"), deadline).unwrap());
    started.await;
    let second = accepted(actor.submit(request("routes", 2, b"two"), deadline).unwrap());
    let third = accepted(actor.submit(request("routes", 3, b"three"), deadline).unwrap());

    let snapshot = actor.snapshot();
    assert_eq!(snapshot.pending_operations, 2);
    assert_eq!(snapshot.pending_bytes, b"one".len() + b"three".len());
    assert_eq!(
        snapshot.resources[0].queued_generation,
        Some(MetadataGeneration::new(3))
    );

    file_system.gate.release();
    assert_eq!(first.wait_until(deadline).await.unwrap(), MetadataGeneration::new(1));
    assert_eq!(second.wait_until(deadline).await.unwrap(), MetadataGeneration::new(3));
    assert_eq!(third.wait_until(deadline).await.unwrap(), MetadataGeneration::new(3));
    assert_eq!(
        *file_system.writes.lock().unwrap(),
        vec![b"one".to_vec(), b"three".to_vec()]
    );

    let stale = accepted(actor.submit(request("routes", 2, b"stale"), deadline).unwrap());
    assert_eq!(stale.wait_until(deadline).await.unwrap(), MetadataGeneration::new(3));
    assert!(!actor.shutdown_until(deadline).await.timed_out);
}

#[tokio::test]
async fn waiter_admission_is_bounded_per_actor() {
    let file_system = Arc::new(GateRecordingFileSystem::default());
    let started = file_system.gate.started.notified();
    tokio::pin!(started);
    started.as_mut().enable();
    let (_context, actor) = start_actor(file_system.clone(), config(1, 64));
    let deadline = MetadataDeadline::after(Duration::from_secs(5));

    let first = accepted(actor.submit(request("waiters", 1, b"one"), deadline).unwrap());
    started.await;
    let mut observers = Vec::new();
    for _ in 0..3 {
        observers.push(accepted(
            actor.submit(request("waiters", 1, b"duplicate"), deadline).unwrap(),
        ));
    }
    let error = actor
        .submit(request("waiters", 1, b"overflow"), deadline)
        .expect_err("waiter admission must be bounded");
    assert_eq!(error.condition(), rocketmq_error::CanonicalCondition::ResourceExhausted);
    assert_eq!(actor.snapshot().resources[0].waiter_count, 4);

    file_system.gate.release();
    assert_eq!(first.wait_until(deadline).await.unwrap(), MetadataGeneration::new(1));
    for observer in observers {
        assert_eq!(observer.wait_until(deadline).await.unwrap(), MetadataGeneration::new(1));
    }
    assert_eq!(actor.snapshot().resources[0].waiter_count, 0);
    assert!(!actor.shutdown_until(deadline).await.timed_out);
}

#[tokio::test]
async fn pending_resource_target_conflict_returns_the_original_request() {
    let file_system = Arc::new(GateRecordingFileSystem::default());
    let started = file_system.gate.started.notified();
    let (_context, actor) = start_actor(file_system.clone(), config(2, 64));
    let deadline = MetadataDeadline::after(Duration::from_secs(5));
    let accepted_request = accepted(actor.submit(request("routes", 1, b"one"), deadline).unwrap());
    started.await;

    let conflicting_request =
        MetadataWriteRequest::new("routes", 2, PathBuf::from("alternate-routes.json"), b"two".to_vec());
    let rejected_request = match actor.submit(conflicting_request, deadline).unwrap() {
        MetadataIoAdmissionOutcome::Accepted(_) => panic!("different pending target must not be accepted"),
        MetadataIoAdmissionOutcome::TargetConflict(request) => request,
    };
    assert_eq!(rejected_request.resource(), "routes");
    assert_eq!(rejected_request.generation(), MetadataGeneration::new(2));
    assert_eq!(rejected_request.target(), Path::new("alternate-routes.json"));
    assert_eq!(rejected_request.len(), b"two".len());

    let snapshot = actor.snapshot();
    assert_eq!(snapshot.pending_operations, 1);
    assert_eq!(snapshot.pending_bytes, b"one".len());
    assert_eq!(snapshot.resources[0].target.as_deref(), Some(Path::new("routes.json")));
    assert_eq!(snapshot.resources[0].queued_generation, None);

    file_system.gate.release();
    assert_eq!(
        accepted_request.wait_until(deadline).await.unwrap(),
        MetadataGeneration::new(1)
    );
    assert!(!actor.shutdown_until(deadline).await.timed_out);
}

#[tokio::test]
async fn older_generation_cannot_overwrite_a_newer_in_flight_snapshot() {
    let file_system = Arc::new(GateRecordingFileSystem::default());
    let started = file_system.gate.started.notified();
    let (_context, actor) = start_actor(file_system.clone(), config(2, 64));
    let deadline = MetadataDeadline::after(Duration::from_secs(5));
    let newer = accepted(actor.submit(request("routes", 3, b"three"), deadline).unwrap());
    started.await;
    let stale = accepted(actor.submit(request("routes", 2, b"two"), deadline).unwrap());

    let snapshot = actor.snapshot();
    assert_eq!(snapshot.pending_operations, 1);
    assert_eq!(snapshot.pending_bytes, b"three".len());
    assert_eq!(snapshot.resources[0].queued_generation, None);

    file_system.gate.release();
    assert_eq!(newer.wait_until(deadline).await.unwrap(), MetadataGeneration::new(3));
    assert_eq!(stale.wait_until(deadline).await.unwrap(), MetadataGeneration::new(3));
    assert_eq!(*file_system.writes.lock().unwrap(), vec![b"three".to_vec()]);
    assert!(!actor.shutdown_until(deadline).await.timed_out);
}

#[tokio::test]
async fn hot_resource_yields_to_other_pending_resources() {
    let file_system = Arc::new(GateRecordingFileSystem::default());
    let started = file_system.gate.started.notified();
    let (_context, actor) = start_actor(file_system.clone(), config(3, 64));
    let deadline = MetadataDeadline::after(Duration::from_secs(5));
    let first = accepted(actor.submit(request("hot", 1, b"hot-1"), deadline).unwrap());
    started.await;
    let other = accepted(actor.submit(request("other", 1, b"other-1"), deadline).unwrap());
    let second = accepted(actor.submit(request("hot", 2, b"hot-2"), deadline).unwrap());

    file_system.gate.release();
    first.wait_until(deadline).await.unwrap();
    other.wait_until(deadline).await.unwrap();
    second.wait_until(deadline).await.unwrap();
    assert_eq!(
        *file_system.writes.lock().unwrap(),
        vec![b"hot-1".to_vec(), b"other-1".to_vec(), b"hot-2".to_vec()]
    );
    assert!(!actor.shutdown_until(deadline).await.timed_out);
}

#[tokio::test]
async fn write_rename_and_disk_failures_do_not_publish_generation() {
    for (operation, error_kind) in [
        (MetadataIoOperation::WriteTemporary, io::ErrorKind::WriteZero),
        (MetadataIoOperation::ReplaceTarget, io::ErrorKind::PermissionDenied),
        (MetadataIoOperation::WriteTemporary, io::ErrorKind::StorageFull),
    ] {
        let file_system = Arc::new(FailingFileSystem { operation, error_kind });
        let (_context, actor) = start_actor(file_system, config(2, 64));
        let deadline = MetadataDeadline::after(Duration::from_secs(5));
        let error = actor
            .submit_durable(request("acl", 7, b"snapshot"), deadline)
            .await
            .unwrap_err();
        assert_eq!(error.condition(), rocketmq_error::CanonicalCondition::Internal);
        assert_eq!(error.operation(), operation.runtime_operation());
        assert!(error.source().is_some());
        let resource = &actor.snapshot().resources[0];
        assert_eq!(resource.durable_generation, None);
        assert_eq!(resource.in_flight_generation, None);
        assert_eq!(resource.queued_generation, None);
        assert!(!actor.shutdown_until(deadline).await.timed_out);
    }
}

#[tokio::test]
async fn worker_panic_is_typed_and_does_not_leak_capacity() {
    let (_context, actor) = start_actor(Arc::new(PanickingFileSystem), config(1, 64));
    let deadline = MetadataDeadline::after(Duration::from_secs(5));
    let error = actor
        .submit_durable(request("topics", 1, b"snapshot"), deadline)
        .await
        .unwrap_err();
    assert_eq!(error.condition(), rocketmq_error::CanonicalCondition::Internal);
    let snapshot = actor.snapshot();
    assert_eq!(snapshot.pending_operations, 0);
    assert_eq!(snapshot.pending_bytes, 0);
    assert_eq!(snapshot.resources[0].durable_generation, None);
    assert!(!actor.shutdown_until(deadline).await.timed_out);
}

#[tokio::test(start_paused = true)]
async fn shutdown_stops_admission_and_reports_unfinished_generations() {
    let file_system = Arc::new(GateRecordingFileSystem::default());
    let started = file_system.gate.started.notified();
    let (_context, actor) = start_actor(file_system.clone(), config(2, 64));
    let initial_deadline = MetadataDeadline::after(Duration::from_secs(30));
    let receipt = accepted(
        actor
            .submit(request("offsets", 9, b"snapshot"), initial_deadline)
            .unwrap(),
    );
    started.await;

    actor.stop_admission();
    assert_eq!(
        actor
            .submit(request("late", 1, b"x"), initial_deadline)
            .expect_err("stopped actor must reject admission")
            .condition(),
        rocketmq_error::CanonicalCondition::Unavailable
    );

    let shutdown_deadline = MetadataDeadline::after(Duration::from_secs(1));
    let shutdown = actor.shutdown_until(shutdown_deadline);
    tokio::pin!(shutdown);
    tokio::time::advance(Duration::from_secs(1)).await;
    let report = shutdown.await;
    assert!(report.timed_out);
    assert_eq!(report.pending_operations, 1);
    assert_eq!(
        report.unfinished[0].in_flight_generation,
        Some(MetadataGeneration::new(9))
    );

    file_system.gate.release();
    assert_eq!(
        receipt.wait_until(initial_deadline).await.unwrap(),
        MetadataGeneration::new(9)
    );
    let drained = actor.shutdown_until(initial_deadline).await;
    assert!(!drained.timed_out);
    assert_eq!(drained.pending_operations, 0);
}

#[tokio::test(start_paused = true)]
async fn expired_absolute_deadline_rejects_admission_without_side_effects() {
    let (_context, actor) = start_actor(Arc::new(LocalMetadataFileSystem), config(1, 64));
    let deadline = MetadataDeadline::after(Duration::from_secs(1));
    tokio::time::advance(Duration::from_secs(1)).await;
    let error = actor.submit(request("expired", 1, b"snapshot"), deadline).unwrap_err();
    assert_eq!(error.condition(), rocketmq_error::CanonicalCondition::DeadlineExceeded);
    assert_eq!(
        error.operation(),
        rocketmq_runtime::RuntimeOperation::AdmitMetadataSnapshot
    );
    assert_eq!(actor.snapshot().pending_operations, 0);
    actor.stop_admission();
    tokio::task::yield_now().await;
    assert!(
        !actor
            .shutdown_until(MetadataDeadline::after(Duration::from_secs(1)))
            .await
            .timed_out
    );
}

#[tokio::test]
async fn target_binding_is_process_local_and_rejects_a_second_resource() {
    let context = RuntimeContext::try_from_current("metadata-target-binding").unwrap();
    let file_system = Arc::new(RecordingFileSystem::default());
    let actor = start_actor_in(&context, "binding", file_system.clone(), config(2, 64));
    let deadline = MetadataDeadline::after(Duration::from_secs(5));

    let first = accepted(actor.submit(request("first", 1, b"one"), deadline).unwrap());
    assert_eq!(first.wait_until(deadline).await.unwrap(), MetadataGeneration::new(1));

    let conflicting = MetadataWriteRequest::new("second", 1, PathBuf::from("first.json"), b"two");
    match actor.submit(conflicting, deadline).unwrap() {
        MetadataIoAdmissionOutcome::TargetConflict(request) => {
            assert_eq!(request.resource(), "second");
        }
        MetadataIoAdmissionOutcome::Accepted(_) => {
            panic!("a second resource must not bind the same target")
        }
    }
    assert_eq!(*file_system.writes.lock().unwrap(), vec![b"one".to_vec()]);
    let _ = context.shutdown_tasks(Duration::from_secs(1)).await;
}

#[tokio::test]
async fn cancelled_actor_retains_target_until_the_real_closure_exits() {
    let context = RuntimeContext::try_from_current("metadata-target-cancel").unwrap();
    let gated_file_system = Arc::new(GateRecordingFileSystem::default());
    let started = gated_file_system.gate.started.notified();
    let cancelled_scope = context.service_context("cancelled");
    let cancelled_actor = config(2, 64)
        .into_plan()
        .unwrap()
        .start_with_file_system(&cancelled_scope, gated_file_system.clone())
        .unwrap();
    let deadline = MetadataDeadline::after(Duration::from_secs(5));
    let receipt = accepted(cancelled_actor.submit(request("shared", 1, b"one"), deadline).unwrap());
    started.await;

    cancelled_scope.task_group().shutdown_now();
    drop(cancelled_actor);
    drop(receipt);
    let replacement_file_system = Arc::new(RecordingFileSystem::default());
    let replacement_actor = start_actor_in(&context, "replacement", replacement_file_system.clone(), config(2, 64));
    assert!(matches!(
        replacement_actor
            .submit(request("shared", 1, b"two"), deadline)
            .unwrap(),
        MetadataIoAdmissionOutcome::TargetConflict(_)
    ));

    gated_file_system.gate.release();
    let replacement = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match replacement_actor
                .submit(request("shared", 1, b"two"), deadline)
                .unwrap()
            {
                MetadataIoAdmissionOutcome::Accepted(receipt) => break receipt,
                MetadataIoAdmissionOutcome::TargetConflict(_) => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(
        replacement.wait_until(deadline).await.unwrap(),
        MetadataGeneration::new(1)
    );
    assert!(replacement_file_system.writes.lock().unwrap().is_empty());

    let newer = replacement_actor
        .submit_commit(
            MetadataWriteRequest::new("shared", 2, PathBuf::from("shared.json"), b"newer".to_vec()),
            deadline,
        )
        .await
        .unwrap();
    match newer {
        MetadataIoCommitAdmissionOutcome::Completed(MetadataIoCommitOutcome::Durable(generation)) => {
            assert_eq!(generation, MetadataGeneration::new(2));
        }
        outcome => panic!("replacement write must become durable: {outcome:?}"),
    }
    assert_eq!(*replacement_file_system.writes.lock().unwrap(), vec![b"newer".to_vec()]);
    let _ = context.shutdown_tasks(Duration::from_secs(1)).await;
}

#[tokio::test]
async fn commit_outcome_distinguishes_unknown_durability() {
    let context = RuntimeContext::try_from_current("metadata-commit-outcome").unwrap();
    let unknown_actor = start_actor_in(
        &context,
        "unknown",
        Arc::new(FailingFileSystem {
            operation: MetadataIoOperation::SyncParent,
            error_kind: io::ErrorKind::Other,
        }),
        config(1, 64),
    );
    let failed_actor = start_actor_in(
        &context,
        "failed",
        Arc::new(FailingFileSystem {
            operation: MetadataIoOperation::WriteTemporary,
            error_kind: io::ErrorKind::Other,
        }),
        config(1, 64),
    );
    let deadline = MetadataDeadline::after(Duration::from_secs(5));

    let unknown = unknown_actor
        .submit_next_commit("unknown-resource", PathBuf::from("unknown.json"), b"unknown", deadline)
        .await
        .unwrap();
    assert!(matches!(
        unknown,
        MetadataIoCommitAdmissionOutcome::Completed(MetadataIoCommitOutcome::CommitOutcomeUnknown(_))
    ));
    let blocked = unknown_actor
        .submit_next_commit("unknown-resource", PathBuf::from("unknown.json"), b"retry", deadline)
        .await
        .unwrap_err();
    assert_eq!(blocked.condition(), rocketmq_error::CanonicalCondition::Unavailable);

    let failed = failed_actor
        .submit_next_commit("failed-resource", PathBuf::from("failed.json"), b"failed", deadline)
        .await
        .unwrap();
    assert!(matches!(
        failed,
        MetadataIoCommitAdmissionOutcome::Completed(MetadataIoCommitOutcome::FailedBeforeCommit(_))
    ));
    assert!(context.shutdown_tasks(Duration::from_secs(1)).await.is_healthy());
}

#[test]
fn local_filesystem_atomically_replaces_target_and_cleans_temporary_file() {
    let directory = TempDir::new().unwrap();
    let target = directory.path().join("metadata.json");
    std::fs::write(&target, b"old").unwrap();

    LocalMetadataFileSystem
        .persist_atomic(&target, b"new durable value")
        .unwrap();

    assert_eq!(std::fs::read(&target).unwrap(), b"new durable value");
    let entries = std::fs::read_dir(directory.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    assert_eq!(entries, vec![target.file_name().unwrap()]);
}

#[derive(Debug, Clone, Copy)]
enum FirstWriteOutcome {
    Success,
    Failure,
    Panic,
}

#[derive(Debug)]
struct LateCompletionFileSystem {
    gate: Arc<Gate>,
    outcome: FirstWriteOutcome,
    writes: Mutex<Vec<Vec<u8>>>,
}

impl MetadataFileSystem for LateCompletionFileSystem {
    fn persist_atomic(&self, _target: &Path, bytes: &[u8]) -> RuntimeResult<()> {
        if bytes == b"one" {
            self.gate.wait();
            match self.outcome {
                FirstWriteOutcome::Success => {}
                FirstWriteOutcome::Failure => {
                    return Err(RuntimeError::io(
                        MetadataIoOperation::WriteTemporary.runtime_operation(),
                        io::Error::other("late write failure"),
                    ));
                }
                FirstWriteOutcome::Panic => panic!("late write panic"),
            }
        }
        self.writes.lock().unwrap().push(bytes.to_vec());
        Ok(())
    }
}

struct ReleaseGateOnDrop(Arc<Gate>);

impl Drop for ReleaseGateOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

async fn verify_late_write_order(outcome: FirstWriteOutcome) {
    let file_system = Arc::new(LateCompletionFileSystem {
        gate: Arc::new(Gate::default()),
        outcome,
        writes: Mutex::new(Vec::new()),
    });
    let _release_on_failure = ReleaseGateOnDrop(file_system.gate.clone());
    let context = RuntimeContext::try_from_current_with_blocking_policy(
        "metadata-late-completion",
        BlockingPoolPolicy {
            max_concurrency: 2,
            task_timeout: Duration::from_secs(1),
            ..BlockingPoolPolicy::default()
        },
    )
    .unwrap();
    let actor = config(2, 6)
        .into_plan()
        .unwrap()
        .start_with_file_system(&context.service_context("metadata"), file_system.clone())
        .unwrap();
    let deadline = MetadataDeadline::after(Duration::from_secs(60));
    let started = file_system.gate.started.notified();
    let first = accepted(actor.submit(request("routes", 1, b"one"), deadline).unwrap());
    started.await;
    let late_observer = accepted(actor.submit(request("routes", 1, b"one"), deadline).unwrap());

    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        first
            .wait_until(MetadataDeadline::after(Duration::ZERO))
            .await
            .unwrap_err()
            .condition(),
        rocketmq_error::CanonicalCondition::DeadlineExceeded
    );
    let second = accepted(actor.submit(request("routes", 2, b"two"), deadline).unwrap());
    let late_result = late_observer.wait_until(deadline);
    tokio::pin!(late_result);
    assert!(futures::poll!(&mut late_result).is_pending());

    let snapshot = actor.snapshot();
    assert_eq!(snapshot.pending_operations, 2);
    assert_eq!(snapshot.pending_bytes, 6);
    assert_eq!(snapshot.resources[0].in_flight_generation, Some(1.into()));
    assert_eq!(snapshot.resources[0].queued_generation, Some(2.into()));
    assert_eq!(snapshot.resources[0].durable_generation, None);
    assert!(file_system.writes.lock().unwrap().is_empty());

    // Stopping the actor must retain the real in-flight write and its charge.
    let stopped = actor.shutdown_until(MetadataDeadline::after(Duration::ZERO)).await;
    assert!(stopped.timed_out);
    assert_eq!(stopped.pending_bytes, 6);
    assert_eq!(stopped.pending_operations, 2);

    file_system.gate.release();
    match outcome {
        FirstWriteOutcome::Success => assert_eq!(late_result.await.unwrap(), 1.into()),
        FirstWriteOutcome::Failure => assert_eq!(
            late_result.await.unwrap_err().operation(),
            MetadataIoOperation::WriteTemporary.runtime_operation()
        ),
        FirstWriteOutcome::Panic => assert_eq!(
            late_result.await.unwrap_err().operation(),
            rocketmq_runtime::RuntimeOperation::RunBlockingTask
        ),
    }
    assert_eq!(second.wait_until(deadline).await.unwrap(), 2.into());
    let drained = actor.shutdown_until(deadline).await;
    assert!(!drained.timed_out);
    assert_eq!(drained.pending_operations, 0);
    assert_eq!(drained.pending_bytes, 0);
    assert_eq!(actor.snapshot().resources[0].durable_generation, Some(2.into()));
    let expected = match outcome {
        FirstWriteOutcome::Success => vec![b"one".to_vec(), b"two".to_vec()],
        FirstWriteOutcome::Failure | FirstWriteOutcome::Panic => vec![b"two".to_vec()],
    };
    assert_eq!(*file_system.writes.lock().unwrap(), expected);
}

#[tokio::test(start_paused = true)]
async fn late_success_preserves_write_order_after_observer_and_lane_deadlines() {
    verify_late_write_order(FirstWriteOutcome::Success).await;
}

#[tokio::test(start_paused = true)]
async fn late_failure_preserves_write_order_after_observer_and_lane_deadlines() {
    verify_late_write_order(FirstWriteOutcome::Failure).await;
}

#[tokio::test(start_paused = true)]
async fn late_panic_preserves_write_order_after_observer_and_lane_deadlines() {
    verify_late_write_order(FirstWriteOutcome::Panic).await;
}

/// Waits until one generation occupies the actor's single in-flight slot.
///
/// Polling the public snapshot keeps the assertions independent of blocking
/// thread startup and of notification registration order.
async fn wait_for_in_flight(actor: &MetadataIoActor, generation: u64) {
    for _ in 0..200_000 {
        let observed = actor
            .snapshot()
            .resources
            .iter()
            .any(|resource| resource.in_flight_generation == Some(generation.into()));
        if observed {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("generation {generation} never occupied the in-flight slot");
}

/// Waits until the actor owns no queued or in-flight generation.
async fn wait_for_idle_worker(actor: &MetadataIoActor) {
    for _ in 0..200_000 {
        if actor.snapshot().pending_operations == 0 {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("the metadata actor never drained its accepted generations");
}

#[tokio::test(start_paused = true)]
async fn observation_timeout_reports_the_unobserved_generation_and_late_success_stays_authoritative() {
    let file_system = Arc::new(GateRecordingFileSystem::default());
    // Release the gate on every exit path: a failed assertion must not leave a
    // real blocking closure parked, because dropping the runtime then waits.
    let _release_on_failure = ReleaseGateOnDrop(file_system.gate.clone());
    let (_context, actor) = start_actor(file_system.clone(), config(3, 64));
    let deadline = MetadataDeadline::after(Duration::from_secs(60));

    let first = accepted(actor.submit(request("routes", 1, b"routes-1"), deadline).unwrap());
    wait_for_in_flight(&actor, 1).await;

    // The caller stops observing while the admitted closure is still gated.
    let observation = first.observe_until(MetadataDeadline::after(Duration::from_secs(1)));
    tokio::pin!(observation);
    assert!(futures::poll!(&mut observation).is_pending());
    tokio::time::advance(Duration::from_secs(2)).await;
    let observation = observation.await;
    assert_eq!(observation.unobserved_generation(), Some(MetadataGeneration::new(1)));
    assert!(
        observation.requires_reconciliation(),
        "an unobserved generation is not a confirmed failure"
    );
    assert!(observation.settled().is_none());
    assert!(actor.confirmed_durable_generation("routes").is_none());
    assert_eq!(actor.snapshot().resources[0].durable_generation, None);

    // The accepted generation keeps its ordering and commits after the caller
    // gave up; it is never retroactively reported to the departed observer.
    file_system.gate.release();
    let second = accepted(actor.submit(request("routes", 2, b"routes-2"), deadline).unwrap());
    assert_eq!(second.wait_until(deadline).await.unwrap(), MetadataGeneration::new(2));
    assert_eq!(
        actor.confirmed_durable_generation("routes"),
        Some(MetadataGeneration::new(2))
    );
    assert_eq!(
        *file_system.writes.lock().unwrap(),
        vec![b"routes-1".to_vec(), b"routes-2".to_vec()]
    );

    let drained = actor.shutdown_until(deadline).await;
    assert!(!drained.timed_out);
}

#[tokio::test(start_paused = true)]
async fn observation_timeout_does_not_publish_a_generation_for_a_late_failure() {
    let file_system = Arc::new(LateCompletionFileSystem {
        gate: Arc::new(Gate::default()),
        outcome: FirstWriteOutcome::Failure,
        writes: Mutex::new(Vec::new()),
    });
    let _release_on_failure = ReleaseGateOnDrop(file_system.gate.clone());
    let (_context, actor) = start_actor(file_system.clone(), config(3, 64));
    let deadline = MetadataDeadline::after(Duration::from_secs(60));

    let first = accepted(actor.submit(request("routes", 1, b"one"), deadline).unwrap());
    wait_for_in_flight(&actor, 1).await;

    let observation = first.observe_until(MetadataDeadline::after(Duration::from_secs(1)));
    tokio::pin!(observation);
    assert!(futures::poll!(&mut observation).is_pending());
    tokio::time::advance(Duration::from_secs(2)).await;
    let observation = observation.await;
    assert_eq!(observation.unobserved_generation(), Some(MetadataGeneration::new(1)));

    // Releasing the gate turns the unobserved generation into a real
    // pre-commit failure. Nothing about it may be published as durable, and
    // the resource must remain writable afterwards.
    file_system.gate.release();
    wait_for_idle_worker(&actor).await;
    assert!(actor.confirmed_durable_generation("routes").is_none());
    assert_eq!(actor.snapshot().resources[0].durable_generation, None);
    assert!(file_system.writes.lock().unwrap().is_empty());

    let retried = accepted(actor.submit(request("routes", 2, b"two"), deadline).unwrap());
    assert_eq!(retried.wait_until(deadline).await.unwrap(), MetadataGeneration::new(2));

    let drained = actor.shutdown_until(deadline).await;
    assert!(!drained.timed_out);
}

#[tokio::test]
async fn observed_submission_classifies_every_settled_conclusion() {
    let context = RuntimeContext::try_from_current("metadata-observation-classification").unwrap();
    let deadline = MetadataDeadline::after(Duration::from_secs(5));

    let durable = start_actor_in(
        &context,
        "durable",
        Arc::new(RecordingFileSystem::default()),
        config(1, 64),
    );
    let observed = durable
        .submit_next_observed("durable-resource", PathBuf::from("durable.json"), b"durable", deadline)
        .await
        .unwrap();
    assert!(!observed.requires_reconciliation());
    assert!(matches!(
        observed.settled(),
        Some(MetadataIoCommitOutcome::Durable(generation)) if generation == MetadataGeneration::new(1)
    ));

    // A second resource cannot bind the same target; the conflict is reported
    // as an admission outcome rather than as a durability conclusion.
    let conflicted = durable
        .submit_next_observed("other-resource", PathBuf::from("durable.json"), b"other", deadline)
        .await
        .unwrap();
    assert!(!conflicted.requires_reconciliation());
    assert_eq!(conflicted.unobserved_generation(), None);
    assert!(conflicted.settled().is_none());

    let failed = start_actor_in(
        &context,
        "failed",
        Arc::new(FailingFileSystem {
            operation: MetadataIoOperation::WriteTemporary,
            error_kind: io::ErrorKind::Other,
        }),
        config(1, 64),
    );
    let observed = failed
        .submit_next_observed("failed-resource", PathBuf::from("failed.json"), b"failed", deadline)
        .await
        .unwrap();
    assert!(
        !observed.requires_reconciliation(),
        "a failure before target replacement is a definite conclusion"
    );
    assert!(matches!(
        observed.settled(),
        Some(MetadataIoCommitOutcome::FailedBeforeCommit(_))
    ));
    assert!(failed.confirmed_durable_generation("failed-resource").is_none());

    let unknown = start_actor_in(
        &context,
        "unknown",
        Arc::new(FailingFileSystem {
            operation: MetadataIoOperation::SyncParent,
            error_kind: io::ErrorKind::Other,
        }),
        config(1, 64),
    );
    let observed = unknown
        .submit_next_observed("unknown-resource", PathBuf::from("unknown.json"), b"unknown", deadline)
        .await
        .unwrap();
    assert!(
        observed.requires_reconciliation(),
        "an unconfirmed replacement must reach the business owner"
    );
    assert_eq!(observed.unobserved_generation(), None);
    assert!(matches!(
        observed.settled(),
        Some(MetadataIoCommitOutcome::CommitOutcomeUnknown(_))
    ));
}

#[tokio::test]
async fn generations_are_unique_across_actors_sharing_one_owner() {
    let context = RuntimeContext::try_from_current("metadata-generation-scope").unwrap();
    let deadline = MetadataDeadline::after(Duration::from_secs(5));
    let first_actor = start_actor_in(
        &context,
        "first",
        Arc::new(RecordingFileSystem::default()),
        config(1, 64),
    );
    let second_actor = start_actor_in(
        &context,
        "second",
        Arc::new(RecordingFileSystem::default()),
        config(1, 64),
    );

    let first = accepted(
        first_actor
            .submit_next("first-resource", PathBuf::from("first.json"), b"first", deadline)
            .unwrap(),
    );
    let second = accepted(
        second_actor
            .submit_next("second-resource", PathBuf::from("second.json"), b"second", deadline)
            .unwrap(),
    );
    let first_generation = first.generation();
    let second_generation = second.generation();
    assert_eq!(first.wait_until(deadline).await.unwrap(), first_generation);
    assert_eq!(second.wait_until(deadline).await.unwrap(), second_generation);
    assert!(
        second_generation > first_generation,
        "an actor replacement must not reuse a generation from the same owner"
    );
}

/// A file system whose first write is gated and then fails after the target
/// was replaced, which is the actor's unconfirmed-replacement case. Later
/// writes are gated by a second, independent gate.
#[derive(Debug, Default)]
struct TwoGateFileSystem {
    first_gate: Arc<Gate>,
    second_gate: Arc<Gate>,
    writes: Mutex<Vec<Vec<u8>>>,
    attempts: AtomicUsize,
}

impl MetadataFileSystem for TwoGateFileSystem {
    fn persist_atomic(&self, _target: &Path, bytes: &[u8]) -> RuntimeResult<()> {
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            self.first_gate.wait();
            self.writes.lock().unwrap().push(bytes.to_vec());
            return Err(RuntimeError::io(
                MetadataIoOperation::SyncParent.runtime_operation(),
                io::Error::other("injected parent-directory sync failure"),
            ));
        }
        self.second_gate.wait();
        self.writes.lock().unwrap().push(bytes.to_vec());
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn unconfirmed_commit_fences_a_target_that_still_has_queued_work() {
    let file_system = Arc::new(TwoGateFileSystem::default());
    // Both gates must be released on every exit path, including a failed
    // fence assertion, or the parked closures block runtime shutdown.
    let _release_on_failure = (
        ReleaseGateOnDrop(file_system.first_gate.clone()),
        ReleaseGateOnDrop(file_system.second_gate.clone()),
    );
    let (_context, actor) = start_actor(file_system.clone(), config(4, 64));
    let deadline = MetadataDeadline::after(Duration::from_secs(60));

    // Generation 1 owns the in-flight slot and will fail after replacement.
    let first = accepted(actor.submit(request("fenced", 1, b"one"), deadline).unwrap());
    wait_for_in_flight(&actor, 1).await;
    // Generation 2 is admitted and queued, so the target registration stays
    // cached in the actor while generation 1 becomes unconfirmed.
    let second = accepted(actor.submit(request("fenced", 2, b"two"), deadline).unwrap());

    file_system.first_gate.release();
    wait_for_in_flight(&actor, 2).await;

    assert!(
        matches!(
            first.wait_until_outcome(deadline).await.unwrap(),
            MetadataIoCommitOutcome::CommitOutcomeUnknown(_)
        ),
        "generation 1 must be reported as an unconfirmed replacement"
    );

    // The already-admitted generation continues and repairs durability, but a
    // new generation must not reuse the cached registration for a target whose
    // replacement was never confirmed.
    let blocked = actor.submit(request("fenced", 3, b"three"), deadline).unwrap_err();
    assert_eq!(blocked.condition(), rocketmq_error::CanonicalCondition::Unavailable);

    file_system.second_gate.release();
    assert_eq!(second.wait_until(deadline).await.unwrap(), MetadataGeneration::new(2));
    assert_eq!(
        actor.confirmed_durable_generation("fenced"),
        Some(MetadataGeneration::new(2))
    );
    assert_eq!(
        *file_system.writes.lock().unwrap(),
        vec![b"one".to_vec(), b"two".to_vec()]
    );

    let drained = actor.shutdown_until(deadline).await;
    assert!(!drained.timed_out);
}
