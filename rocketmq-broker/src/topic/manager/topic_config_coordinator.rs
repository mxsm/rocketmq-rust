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

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_common::FileUtils;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_runtime::schedule::simple_scheduler::ScheduledTaskManager;
use rocketmq_runtime::BlockingExecutor;
use rocketmq_runtime::BlockingPoolPolicy;
use rocketmq_runtime::MetadataDeadline;
use rocketmq_runtime::MetadataIoActor;
use rocketmq_runtime::ServiceContext;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskGroupChildLease;
use rocketmq_runtime::TaskId;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tracing::warn;

use crate::topic::manager::topic_config_manager::TopicConfigManager;

const COMMAND_CAPACITY: usize = 256;

pub(crate) type TopicRegistrationFuture = Pin<Box<dyn Future<Output = RocketMQResult<()>> + Send + 'static>>;
pub(crate) type TopicRegistrationAction = Box<dyn FnOnce() -> TopicRegistrationFuture + Send + 'static>;

enum TopicConfigCommand {
    Persist {
        registration: Option<TopicRegistrationAction>,
        completion: Option<oneshot::Sender<RocketMQResult<()>>>,
        _pending: TopicConfigPendingGuard,
    },
    Finalize {
        completion: oneshot::Sender<RocketMQResult<()>>,
    },
}

struct TopicConfigPendingGuard {
    pending: Arc<AtomicU64>,
}

impl Drop for TopicConfigPendingGuard {
    fn drop(&mut self) {
        self.pending.fetch_sub(1, Ordering::AcqRel);
    }
}

struct TopicConfigRuntimeCapabilities {
    task_group: TaskGroup,
    blocking: BlockingExecutor,
}

struct TopicConfigRun {
    _lease: TaskGroupChildLease,
    task_group: TaskGroup,
    worker: TaskId,
}

struct TopicConfigLifecycle {
    next_generation: u64,
    run: Option<TopicConfigRun>,
    finalized: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct TopicConfigCoordinatorShutdownReport {
    pub(crate) admission_closed: bool,
    pub(crate) pending_at_end: u64,
    pub(crate) final_persist_succeeded: bool,
    pub(crate) worker_exited: bool,
    pub(crate) registration_quiesced: bool,
    pub(crate) unregister_succeeded: bool,
    pub(crate) blocking_still_running: usize,
    pub(crate) timed_out: bool,
    pub(crate) elapsed: Duration,
    pub(crate) detail: Option<String>,
}

impl TopicConfigCoordinatorShutdownReport {
    pub(crate) fn is_healthy(&self) -> bool {
        self.can_unregister() && self.unregister_succeeded
    }

    pub(crate) fn can_unregister(&self) -> bool {
        self.admission_closed
            && self.pending_at_end == 0
            && self.final_persist_succeeded
            && self.worker_exited
            && self.registration_quiesced
            && self.blocking_still_running == 0
            && !self.timed_out
            && self.detail.is_none()
    }

    pub(crate) fn can_detach(&self) -> bool {
        self.is_healthy()
    }
}

pub(crate) struct TopicConfigCoordinator {
    manager: Arc<TopicConfigManager>,
    service_context: Option<ServiceContext>,
    compatibility_scheduler: ScheduledTaskManager,
    runtime_capabilities: std::sync::OnceLock<TopicConfigRuntimeCapabilities>,
    lifecycle: Mutex<TopicConfigLifecycle>,
    admission: Mutex<Option<mpsc::Sender<TopicConfigCommand>>>,
    pending: Arc<AtomicU64>,
    rejected_after_close: AtomicU64,
    persist_failures: Arc<AtomicU64>,
    registration_failures: Arc<AtomicU64>,
    metadata_io: Option<MetadataIoActor>,
}

impl TopicConfigCoordinator {
    pub(crate) fn new(
        manager: Arc<TopicConfigManager>,
        service_context: Option<ServiceContext>,
        compatibility_scheduler: ScheduledTaskManager,
    ) -> Self {
        Self::new_with_metadata_io(manager, service_context, compatibility_scheduler, None)
    }

    pub(crate) fn new_with_metadata_io(
        manager: Arc<TopicConfigManager>,
        service_context: Option<ServiceContext>,
        compatibility_scheduler: ScheduledTaskManager,
        metadata_io: Option<MetadataIoActor>,
    ) -> Self {
        Self {
            manager,
            service_context,
            compatibility_scheduler,
            runtime_capabilities: std::sync::OnceLock::new(),
            lifecycle: Mutex::new(TopicConfigLifecycle {
                next_generation: 1,
                run: None,
                finalized: false,
            }),
            admission: Mutex::new(None),
            pending: Arc::new(AtomicU64::new(0)),
            rejected_after_close: AtomicU64::new(0),
            persist_failures: Arc::new(AtomicU64::new(0)),
            registration_failures: Arc::new(AtomicU64::new(0)),
            metadata_io,
        }
    }

    pub(crate) fn manager(&self) -> &Arc<TopicConfigManager> {
        &self.manager
    }

    pub(crate) fn pending_count(&self) -> u64 {
        self.pending.load(Ordering::Acquire)
    }

    pub(crate) fn rejected_after_close_count(&self) -> u64 {
        self.rejected_after_close.load(Ordering::Acquire)
    }

    pub(crate) fn persist_failure_count(&self) -> u64 {
        self.persist_failures.load(Ordering::Acquire)
    }

    pub(crate) fn registration_failure_count(&self) -> u64 {
        self.registration_failures.load(Ordering::Acquire)
    }

    fn runtime_capabilities(&self) -> RocketMQResult<&TopicConfigRuntimeCapabilities> {
        if let Some(capabilities) = self.runtime_capabilities.get() {
            return Ok(capabilities);
        }

        let capabilities = if let Some(service_context) = &self.service_context {
            TopicConfigRuntimeCapabilities {
                task_group: service_context.task_group().clone(),
                blocking: service_context.blocking().clone(),
            }
        } else {
            let task_group = self
                .compatibility_scheduler
                .compatibility_task_group()
                .map_err(topic_coordinator_error)?;
            let reaper_group = task_group
                .try_child("topic-config.blocking-reaper")
                .map_err(topic_coordinator_error)?;
            let blocking =
                BlockingExecutor::new(BlockingPoolPolicy::default(), reaper_group).map_err(topic_coordinator_error)?;
            TopicConfigRuntimeCapabilities { task_group, blocking }
        };
        let _ = self.runtime_capabilities.set(capabilities);
        self.runtime_capabilities
            .get()
            .ok_or_else(|| topic_coordinator_error("failed to install runtime capabilities"))
    }

    async fn ensure_started(&self) -> RocketMQResult<()> {
        let mut lifecycle = self.lifecycle.lock().await;
        if lifecycle.finalized {
            return Err(topic_coordinator_error("coordinator is already finalized"));
        }
        if lifecycle.run.is_some() {
            return Ok(());
        }

        let capabilities = self.runtime_capabilities()?;
        let generation = lifecycle.next_generation;
        lifecycle.next_generation = generation.checked_add(1).unwrap_or(1);
        let lease = capabilities
            .task_group
            .try_child_lease(format!("rocketmq-broker.topic-config.generation-{generation}"))
            .map_err(topic_coordinator_error)?;
        let task_group = lease.group().clone();
        let (sender, receiver) = mpsc::channel(COMMAND_CAPACITY);
        let manager = Arc::clone(&self.manager);
        let blocking = capabilities.blocking.clone();
        let persist_failures = Arc::clone(&self.persist_failures);
        let registration_failures = Arc::clone(&self.registration_failures);
        let metadata_io = self.metadata_io.clone();
        let worker = task_group
            .spawn_service(
                "broker.topic-config.coordinator",
                run_topic_config_worker(
                    manager,
                    blocking,
                    metadata_io,
                    receiver,
                    persist_failures,
                    registration_failures,
                ),
            )
            .map_err(topic_coordinator_error)?;
        *self.admission.lock().await = Some(sender);
        lifecycle.run = Some(TopicConfigRun {
            _lease: lease,
            task_group,
            worker,
        });
        Ok(())
    }

    pub(crate) async fn load(&self) -> RocketMQResult<bool> {
        self.ensure_started().await?;
        let manager = Arc::clone(&self.manager);
        self.runtime_capabilities()?
            .blocking
            .spawn_io("broker.topic-config.load", move || manager.load())
            .await
            .map_err(topic_coordinator_error)
    }

    #[cfg(feature = "rocksdb_store")]
    pub(crate) async fn export_to_json(&self) -> RocketMQResult<()> {
        self.ensure_started().await?;
        let manager = Arc::clone(&self.manager);
        self.runtime_capabilities()?
            .blocking
            .spawn_io("broker.topic-config.export-json", move || manager.export_to_json())
            .await
            .map_err(topic_coordinator_error)?
    }

    pub(crate) async fn persist_and_wait(&self) -> RocketMQResult<()> {
        self.submit(None, true).await
    }

    pub(crate) async fn persist_accepted(&self) -> RocketMQResult<()> {
        self.submit(None, false).await
    }

    pub(crate) async fn persist_and_register_wait(&self, registration: TopicRegistrationAction) -> RocketMQResult<()> {
        self.submit(Some(registration), true).await
    }

    pub(crate) async fn persist_and_register_accepted(
        &self,
        registration: TopicRegistrationAction,
    ) -> RocketMQResult<()> {
        self.submit(Some(registration), false).await
    }

    async fn submit(&self, registration: Option<TopicRegistrationAction>, wait: bool) -> RocketMQResult<()> {
        self.ensure_started().await?;
        let (completion, receiver) = if wait {
            let (sender, receiver) = oneshot::channel();
            (Some(sender), Some(receiver))
        } else {
            (None, None)
        };
        self.pending.fetch_add(1, Ordering::AcqRel);
        let command = TopicConfigCommand::Persist {
            registration,
            completion,
            _pending: TopicConfigPendingGuard {
                pending: Arc::clone(&self.pending),
            },
        };

        let admission = self.admission.lock().await;
        let Some(sender) = admission.as_ref() else {
            self.rejected_after_close.fetch_add(1, Ordering::AcqRel);
            return Err(topic_coordinator_error("topic config admission is closed"));
        };
        sender
            .send(command)
            .await
            .map_err(|_| topic_coordinator_error("topic config worker is unavailable"))?;
        drop(admission);

        match receiver {
            Some(receiver) => receiver
                .await
                .map_err(|_| topic_coordinator_error("topic config command result channel closed"))?,
            None => Ok(()),
        }
    }

    pub(crate) async fn shutdown_until(&self, deadline: ShutdownDeadline) -> TopicConfigCoordinatorShutdownReport {
        let started = Instant::now();
        if let Err(error) = self.ensure_started().await {
            return TopicConfigCoordinatorShutdownReport {
                admission_closed: false,
                pending_at_end: self.pending_count(),
                final_persist_succeeded: false,
                worker_exited: false,
                registration_quiesced: false,
                unregister_succeeded: false,
                blocking_still_running: 0,
                timed_out: false,
                elapsed: started.elapsed(),
                detail: Some(error.to_string()),
            };
        }
        let mut lifecycle = self.lifecycle.lock().await;
        let Some(run) = lifecycle.run.take() else {
            lifecycle.finalized = true;
            return TopicConfigCoordinatorShutdownReport {
                admission_closed: true,
                pending_at_end: self.pending_count(),
                final_persist_succeeded: true,
                worker_exited: true,
                registration_quiesced: true,
                unregister_succeeded: false,
                blocking_still_running: self.runtime_capabilities.get().map_or(0, |capabilities| {
                    capabilities.blocking.snapshot().blocking_still_running
                }),
                timed_out: false,
                elapsed: started.elapsed(),
                detail: None,
            };
        };

        let (completion, receiver) = oneshot::channel();
        let mut admission = self.admission.lock().await;
        let sender = admission.take();
        let admission_closed = sender.is_some();
        let send_result = match sender {
            Some(sender) => sender.send(TopicConfigCommand::Finalize { completion }).await,
            None => Err(mpsc::error::SendError(TopicConfigCommand::Finalize { completion })),
        };
        drop(admission);

        let mut detail = send_result
            .err()
            .map(|_| "failed to enqueue final persistence".to_string());
        let final_persist_succeeded = if detail.is_none() {
            match tokio::time::timeout(deadline.remaining(), receiver).await {
                Ok(Ok(Ok(()))) => true,
                Ok(Ok(Err(error))) => {
                    detail = Some(error.to_string());
                    false
                }
                Ok(Err(_)) => {
                    detail = Some("final persistence result channel closed".to_string());
                    false
                }
                Err(_) => false,
            }
        } else {
            false
        };
        let timed_out = deadline.is_expired() || (!final_persist_succeeded && detail.is_none());
        let worker_exited = run.task_group.wait_task(run.worker, deadline.remaining()).await;
        let blocking_still_running = self.runtime_capabilities.get().map_or(0, |capabilities| {
            capabilities.blocking.snapshot().blocking_still_running
        });
        let report = TopicConfigCoordinatorShutdownReport {
            admission_closed,
            pending_at_end: self.pending_count(),
            final_persist_succeeded,
            worker_exited,
            registration_quiesced: worker_exited,
            unregister_succeeded: false,
            blocking_still_running,
            timed_out: timed_out || !worker_exited,
            elapsed: started.elapsed(),
            detail,
        };
        if report.can_detach() {
            lifecycle.finalized = true;
        } else {
            lifecycle.run = Some(run);
        }
        report
    }
}

async fn run_topic_config_worker(
    manager: Arc<TopicConfigManager>,
    blocking: BlockingExecutor,
    metadata_io: Option<MetadataIoActor>,
    mut receiver: mpsc::Receiver<TopicConfigCommand>,
    persist_failures: Arc<AtomicU64>,
    registration_failures: Arc<AtomicU64>,
) {
    while let Some(command) = receiver.recv().await {
        match command {
            TopicConfigCommand::Persist {
                registration,
                completion,
                _pending,
            } => {
                let result = persist_stable(&manager, &blocking, metadata_io.as_ref()).await;
                let result = match (result, registration) {
                    (Ok(()), Some(registration)) => match registration().await {
                        Ok(()) => persist_stable(&manager, &blocking, metadata_io.as_ref()).await,
                        Err(error) => {
                            registration_failures.fetch_add(1, Ordering::AcqRel);
                            Err(error)
                        }
                    },
                    (result, _) => result,
                };
                if result.is_err() {
                    persist_failures.fetch_add(1, Ordering::AcqRel);
                }
                if let Some(completion) = completion {
                    let _ = completion.send(result);
                } else if let Err(error) = result {
                    warn!(?error, "asynchronous topic config command failed");
                }
            }
            TopicConfigCommand::Finalize { completion } => {
                let result = persist_stable(&manager, &blocking, metadata_io.as_ref()).await;
                if result.is_err() {
                    persist_failures.fetch_add(1, Ordering::AcqRel);
                }
                let _ = completion.send(result);
                break;
            }
        }
    }
}

async fn persist_stable(
    manager: &Arc<TopicConfigManager>,
    blocking: &BlockingExecutor,
    metadata_io: Option<&MetadataIoActor>,
) -> RocketMQResult<()> {
    loop {
        let persisted_version = if manager.supports_metadata_io_actor() {
            if let Some(metadata_io) = metadata_io {
                let (version, path, content) = manager.encoded_persistence_snapshot()?;
                metadata_io
                    .submit_next_durable(
                        "broker.topic-config",
                        path,
                        content,
                        MetadataDeadline::after(Duration::from_secs(30)),
                    )
                    .await
                    .map_err(FileUtils::metadata_io_error)?;
                version
            } else {
                let manager_for_write = Arc::clone(manager);
                blocking
                    .spawn_io("broker.topic-config.persist", move || {
                        manager_for_write.persist_latest_snapshot()
                    })
                    .await
                    .map_err(topic_coordinator_error)??
            }
        } else {
            let manager_for_write = Arc::clone(manager);
            blocking
                .spawn_io("broker.topic-config.persist", move || {
                    manager_for_write.persist_latest_snapshot()
                })
                .await
                .map_err(topic_coordinator_error)??
        };
        if manager.data_version() == persisted_version {
            return Ok(());
        }
    }
}

fn topic_coordinator_error(error: impl ToString) -> RocketMQError {
    RocketMQError::not_initialized(format!("topic config coordinator: {}", error.to_string()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_common::common::config::TopicConfig;
    use rocketmq_runtime::schedule::simple_scheduler::ScheduledTaskManager;
    use rocketmq_runtime::ShutdownDeadline;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;
    use tempfile::TempDir;
    use tokio::sync::oneshot;

    use super::TopicConfigCoordinator;
    use super::TopicRegistrationAction;
    use crate::topic::manager::topic_config_manager::TopicConfigManager;

    fn test_coordinator(temp_dir: &TempDir) -> Arc<TopicConfigCoordinator> {
        let root = temp_dir.path().to_string_lossy().to_string();
        let broker_config = BrokerConfig {
            store_path_root_dir: root.clone().into(),
            ..BrokerConfig::default()
        };
        let message_store_config = MessageStoreConfig {
            store_path_root_dir: root.into(),
            ..MessageStoreConfig::default()
        };
        let manager = Arc::new(TopicConfigManager::new(&broker_config, &message_store_config, false));
        Arc::new(TopicConfigCoordinator::new(
            manager,
            None,
            ScheduledTaskManager::new_legacy_compatibility(),
        ))
    }

    #[tokio::test]
    async fn shutdown_drains_accepted_registration_before_closing_admission() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let coordinator = test_coordinator(&temp_dir);
        coordinator
            .manager()
            .update_topic_config(TopicConfig::with_queues("BarrierTopic", 1, 1), 0);
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let registration: TopicRegistrationAction = Box::new(move || {
            Box::pin(async move {
                let _ = entered_tx.send(());
                let _ = release_rx.await;
                Ok(())
            })
        });
        coordinator
            .persist_and_register_accepted(registration)
            .await
            .expect("command should be admitted");
        entered_rx.await.expect("registration should enter");
        assert_eq!(coordinator.pending_count(), 1);

        let coordinator_for_shutdown = Arc::clone(&coordinator);
        let shutdown = tokio::spawn(async move {
            coordinator_for_shutdown
                .shutdown_until(ShutdownDeadline::after(Duration::from_secs(5)))
                .await
        });
        tokio::task::yield_now().await;
        assert_eq!(coordinator.pending_count(), 1);
        release_tx.send(()).expect("registration should still be waiting");

        let mut report = shutdown.await.expect("shutdown task should join");
        assert!(report.can_unregister(), "{report:?}");
        assert_eq!(report.pending_at_end, 0);
        report.unregister_succeeded = true;
        assert!(report.can_detach(), "{report:?}");
        assert!(coordinator.persist_and_wait().await.is_err());
        assert_eq!(coordinator.rejected_after_close_count(), 1);
    }

    #[tokio::test]
    async fn persistence_runs_through_single_worker_and_records_no_failure() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let coordinator = test_coordinator(&temp_dir);
        coordinator
            .manager()
            .update_topic_config(TopicConfig::with_queues("PersistedTopic", 3, 4), 0);

        coordinator.persist_and_wait().await.expect("snapshot should persist");
        assert_eq!(coordinator.pending_count(), 0);
        assert_eq!(coordinator.persist_failure_count(), 0);
        assert_eq!(coordinator.registration_failure_count(), 0);

        let report = coordinator
            .shutdown_until(ShutdownDeadline::after(Duration::from_secs(5)))
            .await;
        assert!(report.can_unregister(), "{report:?}");
    }
}
