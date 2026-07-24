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

use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::time::Duration;

use rocketmq_runtime::LocalMetadataFileSystem;
use rocketmq_runtime::MetadataDeadline;
use rocketmq_runtime::MetadataFileSystem;
use rocketmq_runtime::MetadataGeneration;
use rocketmq_runtime::MetadataIoActor;
use rocketmq_runtime::MetadataIoConfig;
use rocketmq_runtime::MetadataIoError;
use rocketmq_runtime::MetadataIoOperation;
use rocketmq_runtime::MetadataWriteRequest;
use rocketmq_runtime::RuntimeContext;
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
    fn persist_atomic(&self, _target: &Path, bytes: &[u8]) -> Result<(), MetadataIoError> {
        self.gate.wait();
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
    fn persist_atomic(&self, target: &Path, _bytes: &[u8]) -> Result<(), MetadataIoError> {
        Err(MetadataIoError::Io {
            operation: self.operation,
            path: Arc::from(target),
            source: Arc::new(io::Error::new(self.error_kind, "injected metadata I/O failure")),
        })
    }
}

#[derive(Debug)]
struct PanickingFileSystem;

impl MetadataFileSystem for PanickingFileSystem {
    fn persist_atomic(&self, _target: &Path, _bytes: &[u8]) -> Result<(), MetadataIoError> {
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
    let actor =
        MetadataIoActor::start_with_file_system(&context.service_context("test-service"), config, file_system).unwrap();
    (context, actor)
}

fn request(resource: &str, generation: u64, bytes: &[u8]) -> MetadataWriteRequest {
    MetadataWriteRequest::new(
        resource,
        generation,
        PathBuf::from(format!("{resource}.json")),
        bytes.to_vec(),
    )
}

#[tokio::test]
async fn queue_and_retained_bytes_are_bounded_independently() {
    let file_system = Arc::new(GateRecordingFileSystem::default());
    let started = file_system.gate.started.notified();
    let (_context, actor) = start_actor(file_system.clone(), config(1, 4));
    let deadline = MetadataDeadline::after(Duration::from_secs(5));
    let first = actor.submit_accepted(request("first", 1, b"1234"), deadline).unwrap();
    started.await;

    let queue_error = actor.submit_accepted(request("second", 1, b"1"), deadline).unwrap_err();
    assert!(matches!(queue_error, MetadataIoError::QueueFull { limit: 1 }));

    file_system.gate.release();
    assert_eq!(first.wait_until(deadline).await.unwrap(), MetadataGeneration::new(1));
    let report = actor.shutdown_until(deadline).await;
    assert!(!report.timed_out);

    let (_context, actor) = start_actor(Arc::new(LocalMetadataFileSystem), config(2, 3));
    let byte_error = actor
        .submit_accepted(request("too-large", 1, b"1234"), deadline)
        .unwrap_err();
    assert!(matches!(
        byte_error,
        MetadataIoError::ByteLimitExceeded {
            requested: 4,
            limit: 3,
            ..
        }
    ));
    assert_eq!(actor.snapshot().pending_operations, 0);
    assert!(!actor.shutdown_until(deadline).await.timed_out);
}

#[tokio::test]
async fn queued_generations_coalesce_without_losing_waiters() {
    let file_system = Arc::new(GateRecordingFileSystem::default());
    let started = file_system.gate.started.notified();
    let (_context, actor) = start_actor(file_system.clone(), config(3, 64));
    let deadline = MetadataDeadline::after(Duration::from_secs(5));
    let first = actor.submit_accepted(request("routes", 1, b"one"), deadline).unwrap();
    started.await;
    let second = actor.submit_accepted(request("routes", 2, b"two"), deadline).unwrap();
    let third = actor.submit_accepted(request("routes", 3, b"three"), deadline).unwrap();

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

    let stale = actor.submit_accepted(request("routes", 2, b"stale"), deadline).unwrap();
    assert_eq!(stale.wait_until(deadline).await.unwrap(), MetadataGeneration::new(3));
    assert!(!actor.shutdown_until(deadline).await.timed_out);
}

#[tokio::test]
async fn older_generation_cannot_overwrite_a_newer_in_flight_snapshot() {
    let file_system = Arc::new(GateRecordingFileSystem::default());
    let started = file_system.gate.started.notified();
    let (_context, actor) = start_actor(file_system.clone(), config(2, 64));
    let deadline = MetadataDeadline::after(Duration::from_secs(5));
    let newer = actor.submit_accepted(request("routes", 3, b"three"), deadline).unwrap();
    started.await;
    let stale = actor.submit_accepted(request("routes", 2, b"two"), deadline).unwrap();

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
    let first = actor.submit_accepted(request("hot", 1, b"hot-1"), deadline).unwrap();
    started.await;
    let other = actor
        .submit_accepted(request("other", 1, b"other-1"), deadline)
        .unwrap();
    let second = actor.submit_accepted(request("hot", 2, b"hot-2"), deadline).unwrap();

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
        assert!(matches!(
            error,
            MetadataIoError::Io {
                operation: actual,
                ..
            } if actual == operation
        ));
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
    assert!(matches!(error, MetadataIoError::WorkerFailed { .. }));
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
    let receipt = actor
        .submit_accepted(request("offsets", 9, b"snapshot"), initial_deadline)
        .unwrap();
    started.await;

    actor.stop_admission();
    assert!(matches!(
        actor.submit_accepted(request("late", 1, b"x"), initial_deadline),
        Err(MetadataIoError::Closed)
    ));

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
    let error = actor
        .submit_accepted(request("expired", 1, b"snapshot"), deadline)
        .unwrap_err();
    assert!(matches!(
        error,
        MetadataIoError::DeadlineExceeded {
            operation: "admit metadata snapshot"
        }
    ));
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
