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

//! Durable-before-publish persistence for NameServer KV mutations.
//!
//! The worker is the only KV writer. It drains already-admitted commands into
//! one candidate snapshot, persists that snapshot through `MetadataIoActor`,
//! and only then replaces the affected namespace maps. Readers therefore see
//! the previous durable namespace or the next durable namespace, never a
//! partially applied batch.

use std::collections::HashMap;
use std::collections::HashSet;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_observability::metrics::namesrv::NameServerKvEvent;
use rocketmq_observability::metrics::namesrv::NameServerMetrics;
use rocketmq_protocol::protocol::RemotingSerializable;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::MetadataDeadline;
use rocketmq_runtime::MetadataIoActor;
use rocketmq_runtime::MetadataIoDurabilityOutcome;
use rocketmq_runtime::RuntimeError;
use rocketmq_runtime::RuntimeOperation;
use rocketmq_runtime::RuntimeResult;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::kvconfig_mananger::ConfigMap;
use super::kvconfig_mananger::ConfigTable;
use super::kvconfig_mananger::Key;
use super::kvconfig_mananger::Namespace;
use super::kvconfig_mananger::Value;
use super::KVConfigSerializeWrapper;

const KV_RESOURCE: &str = "namesrv.kv-config";
pub(crate) const DEFAULT_KV_MUTATION_MAX_PENDING_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug)]
pub(crate) enum KvMutation {
    Put {
        namespace: Namespace,
        key: Key,
        value: Value,
    },
    Delete {
        namespace: Namespace,
        key: Key,
    },
    BatchPut {
        namespace: Namespace,
        values: HashMap<Key, Value>,
    },
    BatchDelete {
        namespace: Namespace,
        keys: Vec<Key>,
    },
    DeleteNamespace {
        namespace: Namespace,
    },
    Persist,
}

impl KvMutation {
    fn estimated_bytes(&self) -> usize {
        const COMMAND_OVERHEAD: usize = 64;
        match self {
            Self::Put { namespace, key, value } => COMMAND_OVERHEAD
                .saturating_add(namespace.len())
                .saturating_add(key.len())
                .saturating_add(value.len()),
            Self::Delete { namespace, key } => COMMAND_OVERHEAD
                .saturating_add(namespace.len())
                .saturating_add(key.len()),
            Self::BatchPut { namespace, values } => values.iter().fold(
                COMMAND_OVERHEAD.saturating_add(namespace.len()),
                |bytes, (key, value)| bytes.saturating_add(key.len()).saturating_add(value.len()),
            ),
            Self::BatchDelete { namespace, keys } => keys
                .iter()
                .fold(COMMAND_OVERHEAD.saturating_add(namespace.len()), |bytes, key| {
                    bytes.saturating_add(key.len())
                }),
            Self::DeleteNamespace { namespace } => COMMAND_OVERHEAD.saturating_add(namespace.len()),
            Self::Persist => COMMAND_OVERHEAD,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct KvCommitReceipt {
    pub(crate) requested_generation: u64,
    pub(crate) durable_generation: u64,
    pub(crate) applied_generation: u64,
    pub(crate) changed_entries: usize,
}

#[derive(Clone, Debug)]
enum KvCommitError {
    Metadata(RuntimeError),
    MetadataTargetConflict,
    Serialization(Arc<str>),
    WorkerStopped,
}

impl KvCommitError {
    fn into_rocketmq_error(self) -> RocketMQError {
        match self {
            Self::Metadata(error) => crate::runtime_to_rocketmq_error(error),
            Self::MetadataTargetConflict => {
                RocketMQError::storage_write_failed(KV_RESOURCE, "metadata resource target conflict")
            }
            Self::Serialization(message) => {
                RocketMQError::IO(io::Error::new(io::ErrorKind::InvalidData, message.to_string()))
            }
            Self::WorkerStopped => RocketMQError::IO(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "NameServer KV mutation worker stopped before commit",
            )),
        }
    }
}

#[derive(Debug)]
pub(crate) struct KvMutationReceipt {
    #[cfg(test)]
    requested_generation: u64,
    completion: oneshot::Receiver<Result<KvCommitReceipt, KvCommitError>>,
}

impl KvMutationReceipt {
    pub(crate) async fn wait_until(self, deadline: MetadataDeadline) -> RocketMQResult<KvCommitReceipt> {
        if deadline.is_expired() {
            return Err(crate::runtime_to_rocketmq_error(RuntimeError::timed_out(
                RuntimeOperation::AdmitKvMutation,
            )));
        }
        match tokio::time::timeout_at(deadline.instant(), self.completion).await {
            Ok(Ok(Ok(receipt))) => Ok(receipt),
            Ok(Ok(Err(error))) => Err(error.into_rocketmq_error()),
            Ok(Err(_)) => Err(KvCommitError::WorkerStopped.into_rocketmq_error()),
            Err(_) => Err(crate::runtime_to_rocketmq_error(RuntimeError::timed_out(
                RuntimeOperation::AdmitKvMutation,
            ))),
        }
    }

    #[cfg(test)]
    const fn requested_generation(&self) -> u64 {
        self.requested_generation
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct KvMutationSnapshot {
    pub(crate) accepting: bool,
    pub(crate) pending_commands: usize,
    pub(crate) pending_bytes: usize,
    pub(crate) desired_generation: u64,
    pub(crate) durable_generation: u64,
    pub(crate) applied_generation: u64,
    pub(crate) persist_count: u64,
    pub(crate) worker_finished: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct KvMutationShutdownReport {
    pub(crate) timed_out: bool,
    pub(crate) snapshot: KvMutationSnapshot,
}

#[derive(Debug)]
struct MutationEnvelope {
    mutation: KvMutation,
    requested_generation: u64,
    estimated_bytes: usize,
    deadline: MetadataDeadline,
    completion: oneshot::Sender<Result<KvCommitReceipt, KvCommitError>>,
}

#[derive(Debug)]
struct MutationServiceInner {
    accepting: AtomicBool,
    pending_commands: AtomicUsize,
    pending_bytes: AtomicUsize,
    desired_generation: AtomicU64,
    durable_generation: AtomicU64,
    applied_generation: AtomicU64,
    persist_count: AtomicU64,
    worker_finished: AtomicBool,
    max_pending_bytes: usize,
    shutdown: CancellationToken,
    finished: Notify,
}

#[derive(Clone)]
pub(crate) struct KvMutationService {
    sender: mpsc::Sender<MutationEnvelope>,
    inner: Arc<MutationServiceInner>,
    metrics: NameServerMetrics,
}

impl KvMutationService {
    pub(crate) fn start(
        service_context: &ChildServiceContext,
        config_table: Arc<ConfigTable>,
        metadata_io: MetadataIoActor,
        target: PathBuf,
        queue_capacity: usize,
        batch_size: usize,
        max_pending_bytes: usize,
        metrics: NameServerMetrics,
    ) -> RuntimeResult<Self> {
        let (sender, receiver) = mpsc::channel(queue_capacity);
        let inner = Arc::new(MutationServiceInner {
            accepting: AtomicBool::new(true),
            pending_commands: AtomicUsize::new(0),
            pending_bytes: AtomicUsize::new(0),
            desired_generation: AtomicU64::new(0),
            durable_generation: AtomicU64::new(0),
            applied_generation: AtomicU64::new(0),
            persist_count: AtomicU64::new(0),
            worker_finished: AtomicBool::new(false),
            max_pending_bytes,
            shutdown: CancellationToken::new(),
            finished: Notify::new(),
        });
        let service = Self {
            sender,
            inner: Arc::clone(&inner),
            metrics: metrics.clone(),
        };
        let task_group = service_context.task_group().clone();
        let owner_cancellation = task_group.cancellation_token();
        task_group.spawn_service(
            "namesrv.kv-mutation.worker",
            run_worker(
                receiver,
                inner,
                config_table,
                metadata_io,
                target,
                batch_size,
                owner_cancellation,
                metrics,
            ),
        )?;
        Ok(service)
    }

    pub(crate) fn submit(&self, mutation: KvMutation, deadline: MetadataDeadline) -> RocketMQResult<KvMutationReceipt> {
        if deadline.is_expired() {
            return Err(crate::runtime_to_rocketmq_error(RuntimeError::timed_out(
                RuntimeOperation::AdmitKvMutation,
            )));
        }
        if !self.inner.accepting.load(Ordering::Acquire) {
            self.metrics.record_kv_event(NameServerKvEvent::Closed);
            return Err(crate::runtime_to_rocketmq_error(RuntimeError::context_unavailable(
                RuntimeOperation::KvMutationWorker,
            )));
        }

        let estimated_bytes = mutation.estimated_bytes();
        if let Err(error) = reserve_pending_bytes(&self.inner, estimated_bytes) {
            self.metrics.record_kv_event(NameServerKvEvent::ByteLimit);
            return Err(error);
        }
        let permit = match self.sender.try_reserve() {
            Ok(permit) => permit,
            Err(error) => {
                self.inner.pending_bytes.fetch_sub(estimated_bytes, Ordering::AcqRel);
                let metadata_error = match error {
                    mpsc::error::TrySendError::Closed(_) => {
                        self.metrics.record_kv_event(NameServerKvEvent::Closed);
                        RuntimeError::context_unavailable(RuntimeOperation::KvMutationWorker)
                    }
                    mpsc::error::TrySendError::Full(_) => {
                        self.metrics.record_kv_event(NameServerKvEvent::QueueFull);
                        RuntimeError::capacity(RuntimeOperation::AdmitKvMutation)
                    }
                };
                return Err(crate::runtime_to_rocketmq_error(metadata_error));
            }
        };

        let requested_generation = next_generation(&self.inner.desired_generation)?;
        let (completion, receiver) = oneshot::channel();
        self.inner.pending_commands.fetch_add(1, Ordering::AcqRel);
        permit.send(MutationEnvelope {
            mutation,
            requested_generation,
            estimated_bytes,
            deadline,
            completion,
        });
        self.metrics.record_kv_event(NameServerKvEvent::Queued);
        record_kv_snapshot(&self.metrics, &self.inner);
        Ok(KvMutationReceipt {
            #[cfg(test)]
            requested_generation,
            completion: receiver,
        })
    }

    pub(crate) fn stop_admission(&self) {
        self.inner.accepting.store(false, Ordering::Release);
        self.inner.shutdown.cancel();
    }

    pub(crate) async fn shutdown_until(&self, deadline: MetadataDeadline) -> KvMutationShutdownReport {
        self.stop_admission();
        loop {
            let notification = self.inner.finished.notified();
            tokio::pin!(notification);
            notification.as_mut().enable();
            if self.inner.worker_finished.load(Ordering::Acquire) {
                return KvMutationShutdownReport {
                    timed_out: false,
                    snapshot: self.snapshot(),
                };
            }
            if deadline.is_expired() || tokio::time::timeout_at(deadline.instant(), notification).await.is_err() {
                return KvMutationShutdownReport {
                    timed_out: true,
                    snapshot: self.snapshot(),
                };
            }
        }
    }

    pub(crate) fn snapshot(&self) -> KvMutationSnapshot {
        KvMutationSnapshot {
            accepting: self.inner.accepting.load(Ordering::Acquire),
            pending_commands: self.inner.pending_commands.load(Ordering::Acquire),
            pending_bytes: self.inner.pending_bytes.load(Ordering::Acquire),
            desired_generation: self.inner.desired_generation.load(Ordering::Acquire),
            durable_generation: self.inner.durable_generation.load(Ordering::Acquire),
            applied_generation: self.inner.applied_generation.load(Ordering::Acquire),
            persist_count: self.inner.persist_count.load(Ordering::Relaxed),
            worker_finished: self.inner.worker_finished.load(Ordering::Acquire),
        }
    }
}

fn reserve_pending_bytes(inner: &MutationServiceInner, requested: usize) -> RocketMQResult<()> {
    let mut retained = inner.pending_bytes.load(Ordering::Acquire);
    loop {
        let next = retained.checked_add(requested).ok_or_else(|| {
            crate::runtime_to_rocketmq_error(RuntimeError::capacity(RuntimeOperation::AdmitKvMutationBytes))
        })?;
        if next > inner.max_pending_bytes {
            return Err(crate::runtime_to_rocketmq_error(RuntimeError::capacity(
                RuntimeOperation::AdmitKvMutationBytes,
            )));
        }
        match inner
            .pending_bytes
            .compare_exchange_weak(retained, next, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => return Ok(()),
            Err(observed) => retained = observed,
        }
    }
}

fn next_generation(generation: &AtomicU64) -> RocketMQResult<u64> {
    let mut current = generation.load(Ordering::Acquire);
    loop {
        let next = current
            .checked_add(1)
            .ok_or_else(|| RocketMQError::IO(io::Error::other("NameServer KV mutation generation exhausted")))?;
        match generation.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return Ok(next),
            Err(observed) => current = observed,
        }
    }
}

async fn run_worker(
    mut receiver: mpsc::Receiver<MutationEnvelope>,
    inner: Arc<MutationServiceInner>,
    config_table: Arc<ConfigTable>,
    metadata_io: MetadataIoActor,
    target: PathBuf,
    batch_size: usize,
    owner_cancellation: CancellationToken,
    metrics: NameServerMetrics,
) {
    let mut draining = false;
    loop {
        let first = if draining {
            receiver.recv().await
        } else {
            tokio::select! {
                biased;
                _ = inner.shutdown.cancelled() => {
                    inner.accepting.store(false, Ordering::Release);
                    receiver.close();
                    draining = true;
                    continue;
                }
                _ = owner_cancellation.cancelled() => {
                    inner.accepting.store(false, Ordering::Release);
                    receiver.close();
                    draining = true;
                    continue;
                }
                command = receiver.recv() => command,
            }
        };
        let Some(first) = first else {
            break;
        };
        let mut batch = Vec::with_capacity(batch_size);
        batch.push(first);
        while batch.len() < batch_size {
            match receiver.try_recv() {
                Ok(command) => batch.push(command),
                Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => break,
            }
        }
        process_batch(&inner, &config_table, &metadata_io, &target, batch, &metrics).await;
        tokio::task::yield_now().await;
    }

    inner.accepting.store(false, Ordering::Release);
    inner.worker_finished.store(true, Ordering::Release);
    metrics.record_kv_event(NameServerKvEvent::Drained);
    record_kv_snapshot(&metrics, &inner);
    inner.finished.notify_waiters();
}

struct MutationOutcome {
    changed_entries: usize,
    namespace: Option<Namespace>,
}

async fn process_batch(
    inner: &MutationServiceInner,
    config_table: &ConfigTable,
    metadata_io: &MetadataIoActor,
    target: &PathBuf,
    batch: Vec<MutationEnvelope>,
    metrics: &NameServerMetrics,
) {
    let batch_size = batch.len();
    let mut candidate = snapshot_table(config_table);
    let mut touched_namespaces = HashSet::new();
    let mut force_persist = false;
    let mut outcomes = Vec::with_capacity(batch.len());
    let mut max_generation = 0;
    let mut deadline = batch[0].deadline;

    for command in &batch {
        max_generation = max_generation.max(command.requested_generation);
        if command.deadline.instant() < deadline.instant() {
            deadline = command.deadline;
        }
        force_persist |= matches!(command.mutation, KvMutation::Persist);
        let outcome = apply_mutation(&mut candidate, &command.mutation);
        if outcome.changed_entries > 0 {
            if let Some(namespace) = &outcome.namespace {
                touched_namespaces.insert(namespace.clone());
            }
        }
        outcomes.push(outcome);
    }

    let changed = outcomes.iter().any(|outcome| outcome.changed_entries > 0);
    if changed || force_persist {
        let persist_started = std::time::Instant::now();
        let bytes = match serialize_candidate(&candidate) {
            Ok(bytes) => bytes,
            Err(error) => {
                metrics.record_kv_persist(persist_started.elapsed(), false, batch_size);
                finish_batch_with_error(inner, batch, KvCommitError::Serialization(error.to_string().into()));
                record_kv_snapshot(metrics, inner);
                return;
            }
        };
        match metadata_io
            .submit_next_durable(KV_RESOURCE, target, bytes, deadline)
            .await
        {
            Ok(MetadataIoDurabilityOutcome::Durable(_)) => {}
            Ok(MetadataIoDurabilityOutcome::TargetConflict(_request)) => {
                metrics.record_kv_persist(persist_started.elapsed(), false, batch_size);
                finish_batch_with_error(inner, batch, KvCommitError::MetadataTargetConflict);
                record_kv_snapshot(metrics, inner);
                return;
            }
            Err(error) => {
                metrics.record_kv_persist(persist_started.elapsed(), false, batch_size);
                finish_batch_with_error(inner, batch, KvCommitError::Metadata(error));
                record_kv_snapshot(metrics, inner);
                return;
            }
        }
        metrics.record_kv_persist(persist_started.elapsed(), true, batch_size);
        inner.persist_count.fetch_add(1, Ordering::Relaxed);
    }

    for namespace in touched_namespaces {
        if let Some(values) = candidate.remove(&namespace) {
            config_table.insert(namespace, values);
        } else {
            config_table.remove(&namespace);
        }
    }
    inner.durable_generation.store(max_generation, Ordering::Release);
    inner.applied_generation.store(max_generation, Ordering::Release);

    for (command, outcome) in batch.into_iter().zip(outcomes) {
        let receipt = KvCommitReceipt {
            requested_generation: command.requested_generation,
            durable_generation: max_generation,
            applied_generation: max_generation,
            changed_entries: outcome.changed_entries,
        };
        let _ = command.completion.send(Ok(receipt));
        finish_command(inner, command.estimated_bytes);
    }
    record_kv_snapshot(metrics, inner);
}

fn record_kv_snapshot(metrics: &NameServerMetrics, inner: &MutationServiceInner) {
    if !metrics.is_enabled() {
        return;
    }
    metrics.record_kv_snapshot(
        inner.desired_generation.load(Ordering::Acquire),
        inner.durable_generation.load(Ordering::Acquire),
        inner.applied_generation.load(Ordering::Acquire),
        inner.pending_commands.load(Ordering::Acquire),
        inner.pending_bytes.load(Ordering::Acquire),
    );
}

fn finish_batch_with_error(inner: &MutationServiceInner, batch: Vec<MutationEnvelope>, error: KvCommitError) {
    for command in batch {
        let _ = command.completion.send(Err(error.clone()));
        finish_command(inner, command.estimated_bytes);
    }
}

fn finish_command(inner: &MutationServiceInner, estimated_bytes: usize) {
    inner.pending_commands.fetch_sub(1, Ordering::AcqRel);
    inner.pending_bytes.fetch_sub(estimated_bytes, Ordering::AcqRel);
}

fn snapshot_table(config_table: &ConfigTable) -> HashMap<Namespace, ConfigMap> {
    config_table
        .iter()
        .map(|entry| (entry.key().clone(), entry.value().clone()))
        .collect()
}

fn serialize_candidate(candidate: &HashMap<Namespace, ConfigMap>) -> RocketMQResult<Vec<u8>> {
    let snapshot = candidate
        .iter()
        .map(|(namespace, values)| (namespace.clone(), values.clone()))
        .collect();
    Ok(KVConfigSerializeWrapper::new_with_config_table(snapshot)
        .serialize_json_pretty()?
        .into_bytes())
}

fn apply_mutation(candidate: &mut HashMap<Namespace, ConfigMap>, mutation: &KvMutation) -> MutationOutcome {
    match mutation {
        KvMutation::Put { namespace, key, value } => {
            let values = candidate.entry(namespace.clone()).or_default();
            let changed = usize::from(values.get(key) != Some(value));
            if changed > 0 {
                values.insert(key.clone(), value.clone());
            }
            MutationOutcome {
                changed_entries: changed,
                namespace: Some(namespace.clone()),
            }
        }
        KvMutation::Delete { namespace, key } => {
            let changed = candidate
                .get_mut(namespace)
                .and_then(|values| values.remove(key))
                .is_some() as usize;
            MutationOutcome {
                changed_entries: changed,
                namespace: Some(namespace.clone()),
            }
        }
        KvMutation::BatchPut { namespace, values } => {
            let current = candidate.entry(namespace.clone()).or_default();
            let mut changed = 0;
            for (key, value) in values {
                if current.get(key) != Some(value) {
                    current.insert(key.clone(), value.clone());
                    changed += 1;
                }
            }
            MutationOutcome {
                changed_entries: changed,
                namespace: Some(namespace.clone()),
            }
        }
        KvMutation::BatchDelete { namespace, keys } => {
            let mut changed = 0;
            if let Some(current) = candidate.get_mut(namespace) {
                for key in keys {
                    changed += usize::from(current.remove(key).is_some());
                }
            }
            MutationOutcome {
                changed_entries: changed,
                namespace: Some(namespace.clone()),
            }
        }
        KvMutation::DeleteNamespace { namespace } => MutationOutcome {
            changed_entries: candidate.remove(namespace).map_or(0, |values| values.len()),
            namespace: Some(namespace.clone()),
        },
        KvMutation::Persist => MutationOutcome {
            changed_entries: 0,
            namespace: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    use cheetah_string::CheetahString;
    use parking_lot::Mutex;
    use rocketmq_runtime::MetadataFileSystem;
    use rocketmq_runtime::MetadataIoConfig;
    use rocketmq_runtime::RuntimeContext;

    use super::*;

    #[derive(Debug, Default)]
    struct RecordingFileSystem {
        fail: AtomicBool,
        writes: AtomicUsize,
        last_bytes: Mutex<Vec<u8>>,
    }

    impl MetadataFileSystem for RecordingFileSystem {
        fn persist_atomic(&self, _target: &std::path::Path, bytes: &[u8]) -> rocketmq_runtime::RuntimeResult<()> {
            self.writes.fetch_add(1, Ordering::Relaxed);
            if self.fail.load(Ordering::Acquire) {
                return Err(RuntimeError::internal_failure(RuntimeOperation::KvPersistenceFault));
            }
            *self.last_bytes.lock() = bytes.to_vec();
            Ok(())
        }
    }

    fn put(namespace: &'static str, key: &'static str, value: &'static str) -> KvMutation {
        KvMutation::Put {
            namespace: CheetahString::from_static_str(namespace),
            key: CheetahString::from_static_str(key),
            value: CheetahString::from_static_str(value),
        }
    }

    fn start_service(
        name: &'static str,
        file_system: Arc<RecordingFileSystem>,
        table: Arc<ConfigTable>,
        queue_capacity: usize,
        batch_size: usize,
    ) -> (RuntimeContext, MetadataIoActor, KvMutationService, tempfile::TempDir) {
        let context = RuntimeContext::try_from_current(name).expect("test should use the current Tokio runtime");
        let service_context = context.service_context("namesrv-kv-test");
        let actor = MetadataIoConfig::default()
            .into_plan()
            .expect("default metadata I/O config is valid")
            .start_with_file_system(&service_context.component("metadata"), file_system)
            .expect("metadata actor should start");
        let root = tempfile::tempdir().expect("KV test directory should be created");
        let service = KvMutationService::start(
            &service_context.component("mutation"),
            table,
            actor.clone(),
            root.path().join("kv.json"),
            queue_capacity,
            batch_size,
            1024 * 1024,
            NameServerMetrics::noop(),
        )
        .expect("KV mutation service should start");
        (context, actor, service, root)
    }

    #[tokio::test]
    async fn persistence_failure_leaves_memory_unchanged() {
        let table = Arc::new(ConfigTable::new());
        table.insert(
            CheetahString::from_static_str("ns"),
            HashMap::from([(
                CheetahString::from_static_str("key"),
                CheetahString::from_static_str("old"),
            )]),
        );
        let file_system = Arc::new(RecordingFileSystem::default());
        file_system.fail.store(true, Ordering::Release);
        let (_context, actor, service, _root) = start_service("kv-failure-test", file_system, Arc::clone(&table), 8, 8);
        let deadline = MetadataDeadline::after(Duration::from_secs(5));

        let error = service
            .submit(put("ns", "key", "new"), deadline)
            .expect("mutation should be admitted")
            .wait_until(deadline)
            .await
            .expect_err("injected persistence failure should reach the caller");

        assert!(error.to_string().contains("injected KV persistence failure"));
        assert_eq!(
            table.get("ns").and_then(|values| values.get("key").cloned()).as_deref(),
            Some("old")
        );
        let snapshot = service.snapshot();
        assert_eq!(snapshot.desired_generation, 1);
        assert_eq!(snapshot.durable_generation, 0);
        assert_eq!(snapshot.applied_generation, 0);
        let _ = service.shutdown_until(deadline).await;
        let _ = actor.shutdown_until(deadline).await;
    }

    #[tokio::test]
    async fn queued_mutations_share_one_candidate_snapshot_and_preserve_order() {
        let table = Arc::new(ConfigTable::new());
        let file_system = Arc::new(RecordingFileSystem::default());
        let (_context, actor, service, _root) =
            start_service("kv-batch-test", Arc::clone(&file_system), Arc::clone(&table), 8, 8);
        let deadline = MetadataDeadline::after(Duration::from_secs(5));
        let first = service.submit(put("ns", "key", "one"), deadline).unwrap();
        let second = service.submit(put("ns", "key", "two"), deadline).unwrap();
        let third = service.submit(put("ns", "key", "three"), deadline).unwrap();

        let first_generation = first.requested_generation();
        let first_receipt = first.wait_until(deadline).await.unwrap();
        let second_receipt = second.wait_until(deadline).await.unwrap();
        let third_receipt = third.wait_until(deadline).await.unwrap();

        assert_eq!(first_generation, 1);
        assert_eq!(first_receipt.durable_generation, 3);
        assert_eq!(second_receipt.applied_generation, 3);
        assert_eq!(third_receipt.requested_generation, 3);
        assert_eq!(
            table.get("ns").and_then(|values| values.get("key").cloned()).as_deref(),
            Some("three")
        );
        assert_eq!(file_system.writes.load(Ordering::Relaxed), 1);
        assert_eq!(service.snapshot().persist_count, 1);
        let _ = service.shutdown_until(deadline).await;
        let _ = actor.shutdown_until(deadline).await;
    }

    #[tokio::test]
    async fn deleting_a_missing_key_does_not_persist() {
        let table = Arc::new(ConfigTable::new());
        let file_system = Arc::new(RecordingFileSystem::default());
        let (_context, actor, service, _root) =
            start_service("kv-delete-missing-test", Arc::clone(&file_system), table, 8, 8);
        let deadline = MetadataDeadline::after(Duration::from_secs(5));
        let receipt = service
            .submit(
                KvMutation::Delete {
                    namespace: CheetahString::from_static_str("missing"),
                    key: CheetahString::from_static_str("key"),
                },
                deadline,
            )
            .unwrap()
            .wait_until(deadline)
            .await
            .unwrap();

        assert_eq!(receipt.changed_entries, 0);
        assert_eq!(file_system.writes.load(Ordering::Relaxed), 0);
        assert_eq!(receipt.durable_generation, receipt.requested_generation);
        let _ = service.shutdown_until(deadline).await;
        let _ = actor.shutdown_until(deadline).await;
    }

    #[tokio::test]
    async fn queue_capacity_and_bytes_fail_fast() {
        let table = Arc::new(ConfigTable::new());
        let file_system = Arc::new(RecordingFileSystem::default());
        let (_context, actor, service, _root) = start_service("kv-capacity-test", file_system, table, 1, 1);
        let deadline = MetadataDeadline::after(Duration::from_secs(5));
        let accepted = service.submit(put("ns", "one", "value"), deadline).unwrap();
        let full = service
            .submit(put("ns", "two", "value"), deadline)
            .expect_err("a full command queue should fail immediately");
        assert!(full.to_string().contains("queue is full"));

        accepted.wait_until(deadline).await.unwrap();
        let _ = service.shutdown_until(deadline).await;
        let _ = actor.shutdown_until(deadline).await;
    }

    #[tokio::test]
    async fn shutdown_stops_admission_and_drains_accepted_mutations() {
        let table = Arc::new(ConfigTable::new());
        let file_system = Arc::new(RecordingFileSystem::default());
        let (_context, actor, service, _root) =
            start_service("kv-shutdown-test", file_system, Arc::clone(&table), 8, 8);
        let deadline = MetadataDeadline::after(Duration::from_secs(5));
        let first = service.submit(put("ns", "one", "1"), deadline).unwrap();
        let second = service.submit(put("ns", "two", "2"), deadline).unwrap();

        let report = service.shutdown_until(deadline).await;
        assert!(!report.timed_out);
        assert!(!report.snapshot.accepting);
        assert!(report.snapshot.worker_finished);
        assert_eq!(report.snapshot.pending_commands, 0);
        assert_eq!(report.snapshot.pending_bytes, 0);
        assert_eq!(report.snapshot.desired_generation, 2);
        assert_eq!(report.snapshot.durable_generation, 2);
        assert_eq!(report.snapshot.applied_generation, 2);
        first.wait_until(deadline).await.unwrap();
        second.wait_until(deadline).await.unwrap();
        assert_eq!(table.get("ns").map(|values| values.len()), Some(2));
        assert!(service.submit(put("ns", "three", "3"), deadline).is_err());
        let _ = actor.shutdown_until(deadline).await;
    }
}
