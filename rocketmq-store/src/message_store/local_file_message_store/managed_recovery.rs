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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use rocketmq_model::common::attribute::cq_type::CQType;
use rocketmq_store_local::mapped_file::prepare_managed_lifecycle_activation;
use rocketmq_store_local::mapped_file::DefaultMappedFile;
use rocketmq_store_local::mapped_file::LockedManagedLifecycleInspection;
use rocketmq_store_local::mapped_file::ManagedLifecycleRuntime;
use rocketmq_store_local::mapped_file::ManagedMappedFileQueueGeneration;
use rocketmq_store_local::mapped_file::ManagedQueueDescriptor;
use rocketmq_store_local::mapped_file::ManagedReconciliationDisposition;
use rocketmq_store_local::mapped_file::ManagedReconciliationLimits;
use rocketmq_store_local::mapped_file::ManagedRecoverySession;
use rocketmq_store_local::mapped_file::PreparedManagedLifecycleActivation;

use super::root_lock::StoreRootLease;
use super::LocalFileMessageStore;
use super::MappedFileRetirementService;
use crate::config::message_store_config::MessageStoreConfig;
use crate::store_error::StoreComponent;
use crate::store_error::StoreError;
use crate::store_error::StoreOperation;

/// Result of the complete read-only Wave-A proof chain.
#[derive(Debug)]
pub(super) enum ManagedReadOnlyDisposition {
    Ready(PreparedManagedLifecycleActivation),
    RecoveryRequired(ManagedRecoverySession),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ManagedQueueKind {
    CommitLog,
    SimpleConsumeQueue,
    ConsumeQueueExtension,
    BatchConsumeQueue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ManagedQueueRoute {
    pub directory: Box<str>,
    pub kind: ManagedQueueKind,
    pub topic: Option<Box<str>>,
    pub queue_id: Option<i32>,
    pub expected_file_length: u64,
}

pub(super) struct StagedManagedQueue {
    pub route: ManagedQueueRoute,
    pub generation: ManagedMappedFileQueueGeneration<DefaultMappedFile>,
}

pub(super) struct ActivatedManagedQueues {
    pub runtime: ManagedLifecycleRuntime,
    pub queues: Vec<StagedManagedQueue>,
}

pub(super) fn validate_wave_b_configuration(config: &MessageStoreConfig) -> Result<(), StoreError> {
    if !cfg!(any(target_os = "linux", windows)) {
        return Err(
            StoreError::new(&rocketmq_error::STORAGE_OPERATION_UNSUPPORTED, StoreOperation::Load)
                .in_component(StoreComponent::MappedFile)
                .with_detail(
                    "managed mapped-file lifecycle Wave-B writes require a qualified Linux or Windows backend",
                ),
        );
    }
    let expected_commit_log = PathBuf::from(config.store_path_root_dir.as_str()).join("commitlog");
    if Path::new(&config.get_store_path_commit_log()) != expected_commit_log {
        return Err(
            StoreError::new(&rocketmq_error::STORAGE_OPERATION_UNSUPPORTED, StoreOperation::Load)
                .in_component(StoreComponent::MappedFile)
                .with_detail("managed mapped-file lifecycle requires CommitLog under <storeRoot>/commitlog"),
        );
    }
    if config.is_enable_rocksdb_store() {
        return Err(
            StoreError::new(&rocketmq_error::STORAGE_OPERATION_UNSUPPORTED, StoreOperation::Load)
                .in_component(StoreComponent::MappedFile)
                .with_detail("managed mapped-file lifecycle does not yet cover RocksDB consume-queue storage"),
        );
    }
    if config.enable_compaction {
        return Err(
            StoreError::new(&rocketmq_error::STORAGE_OPERATION_UNSUPPORTED, StoreOperation::Load)
                .in_component(StoreComponent::MappedFile)
                .with_detail("managed mapped-file lifecycle does not yet cover compaction queues"),
        );
    }
    if config.is_timer_wheel_enable() {
        return Err(
            StoreError::new(&rocketmq_error::STORAGE_OPERATION_UNSUPPORTED, StoreOperation::Load)
                .in_component(StoreComponent::MappedFile)
                .with_detail("managed mapped-file lifecycle does not yet cover timer-wheel mapped files"),
        );
    }
    if config.message_index_enable {
        return Err(
            StoreError::new(&rocketmq_error::STORAGE_OPERATION_UNSUPPORTED, StoreOperation::Load)
                .in_component(StoreComponent::MappedFile)
                .with_detail("managed mapped-file lifecycle does not yet cover index-file retirement"),
        );
    }
    Ok(())
}

/// Validates the complete replay-authorized queue inventory before any retained handle is claimed.
pub(super) fn plan_managed_queue_routes(
    descriptors: &[ManagedQueueDescriptor],
    config: &MessageStoreConfig,
) -> Result<Vec<ManagedQueueRoute>, StoreError> {
    plan_queue_routes(
        descriptors
            .iter()
            .map(|descriptor| (descriptor.directory(), descriptor.expected_file_length())),
        config.mapped_file_size_commit_log as u64,
        config.get_mapped_file_size_consume_queue() as u64,
        config.mapped_file_size_consume_queue_ext as u64,
        config.mapper_file_size_batch_consume_queue as u64,
        config.enable_consume_queue_ext,
    )
    .map_err(|detail| {
        StoreError::new(&rocketmq_error::STORAGE_STATE_CORRUPTED, StoreOperation::Load)
            .in_component(StoreComponent::MappedFile)
            .with_detail(detail)
    })
}

/// Claims every preflighted queue generation, then opens managed write capabilities exactly once.
pub(super) fn stage_and_activate_managed_queues(
    mut activation: PreparedManagedLifecycleActivation,
    store_root: &Path,
    routes: Vec<ManagedQueueRoute>,
) -> Result<Option<ActivatedManagedQueues>, StoreError> {
    let mut queues = Vec::new();
    queues.try_reserve_exact(routes.len()).map_err(|_| {
        StoreError::new(&rocketmq_error::STORAGE_BACKEND_UNAVAILABLE, StoreOperation::Load)
            .in_component(StoreComponent::MappedFile)
            .with_detail("failed to reserve managed queue staging inventory")
    })?;
    for route in routes {
        let Some(generation) = activation.stage_queue(store_root, &route.directory, route.expected_file_length)? else {
            return Ok(None);
        };
        queues.push(StagedManagedQueue { route, generation });
    }
    let Some(runtime) = activation.activate()? else {
        return Ok(None);
    };
    Ok(Some(ActivatedManagedQueues { runtime, queues }))
}

fn plan_queue_routes<'a>(
    descriptors: impl IntoIterator<Item = (&'a str, u64)>,
    commit_log_file_size: u64,
    consume_queue_file_size: u64,
    consume_queue_ext_file_size: u64,
    batch_consume_queue_file_size: u64,
    consume_queue_ext_enabled: bool,
) -> Result<Vec<ManagedQueueRoute>, String> {
    let mut routes = Vec::new();
    let mut logical_queues = BTreeMap::<(Box<str>, i32), ManagedQueueKind>::new();
    let mut extensions = BTreeSet::<(Box<str>, i32)>::new();
    let mut has_commit_log = false;

    for (directory, expected_file_length) in descriptors {
        let components = directory.split('/').collect::<Vec<_>>();
        let (kind, topic, queue_id, configured_file_length) = match components.as_slice() {
            ["commitlog"] => {
                has_commit_log = true;
                (ManagedQueueKind::CommitLog, None, None, commit_log_file_size)
            }
            ["consumequeue", topic, queue_id] => (
                ManagedQueueKind::SimpleConsumeQueue,
                Some(*topic),
                Some(parse_queue_id(directory, queue_id)?),
                consume_queue_file_size,
            ),
            ["consumequeue_ext", topic, queue_id] => {
                if !consume_queue_ext_enabled {
                    return Err(format!(
                        "managed consume-queue extension {directory:?} exists while consume-queue extensions are disabled"
                    ));
                }
                (
                    ManagedQueueKind::ConsumeQueueExtension,
                    Some(*topic),
                    Some(parse_queue_id(directory, queue_id)?),
                    consume_queue_ext_file_size,
                )
            }
            ["batchconsumequeue", topic, queue_id] => (
                ManagedQueueKind::BatchConsumeQueue,
                Some(*topic),
                Some(parse_queue_id(directory, queue_id)?),
                batch_consume_queue_file_size,
            ),
            _ => {
                return Err(format!(
                    "replay-authorized mapped-file directory {directory:?} is not a supported queue layout"
                ));
            }
        };
        if topic.is_some_and(str::is_empty) {
            return Err(format!("managed queue directory {directory:?} has an empty topic"));
        }
        if expected_file_length != configured_file_length {
            return Err(format!(
                "managed queue {directory:?} has replay length {expected_file_length}, but configuration requires {configured_file_length}"
            ));
        }

        let topic = topic.map(Box::<str>::from);
        if let (Some(topic), Some(queue_id)) = (topic.as_ref(), queue_id) {
            let identity = (topic.clone(), queue_id);
            match kind {
                ManagedQueueKind::SimpleConsumeQueue | ManagedQueueKind::BatchConsumeQueue => {
                    if let Some(previous) = logical_queues.insert(identity, kind) {
                        return Err(format!(
                            "managed queue {topic}/{queue_id} is present as both {previous:?} and {kind:?}"
                        ));
                    }
                }
                ManagedQueueKind::ConsumeQueueExtension => {
                    extensions.insert(identity);
                }
                ManagedQueueKind::CommitLog => {}
            }
        }
        routes.push(ManagedQueueRoute {
            directory: directory.into(),
            kind,
            topic,
            queue_id,
            expected_file_length,
        });
    }

    if !has_commit_log {
        routes.insert(
            0,
            ManagedQueueRoute {
                directory: "commitlog".into(),
                kind: ManagedQueueKind::CommitLog,
                topic: None,
                queue_id: None,
                expected_file_length: commit_log_file_size,
            },
        );
    }

    for identity in &extensions {
        if logical_queues.get(identity) != Some(&ManagedQueueKind::SimpleConsumeQueue) {
            return Err(format!(
                "managed consume-queue extension {}/{} has no matching simple consume queue",
                identity.0, identity.1
            ));
        }
    }
    if consume_queue_ext_enabled {
        for ((topic, queue_id), kind) in &logical_queues {
            if *kind == ManagedQueueKind::SimpleConsumeQueue && !extensions.contains(&(topic.clone(), *queue_id)) {
                routes.push(ManagedQueueRoute {
                    directory: format!("consumequeue_ext/{topic}/{queue_id}").into(),
                    kind: ManagedQueueKind::ConsumeQueueExtension,
                    topic: Some(topic.clone()),
                    queue_id: Some(*queue_id),
                    expected_file_length: consume_queue_ext_file_size,
                });
            }
        }
    }
    Ok(routes)
}

fn parse_queue_id(directory: &str, value: &str) -> Result<i32, String> {
    let queue_id = value
        .parse::<i32>()
        .map_err(|_| format!("managed queue directory {directory:?} has an invalid queue id"))?;
    if queue_id < 0 || queue_id.to_string() != value {
        return Err(format!(
            "managed queue directory {directory:?} does not use the canonical non-negative queue id"
        ));
    }
    Ok(queue_id)
}

impl LocalFileMessageStore {
    pub(super) fn activate_managed_queue_runtime(&mut self) -> Result<bool, StoreError> {
        let Some(activation) = self.managed_lifecycle_activation.as_ref() else {
            return Err(managed_queue_install_error(
                "managed Store has no reconciled lifecycle activation candidate",
            ));
        };
        let Some(descriptors) = activation.queue_descriptors()? else {
            return Ok(false);
        };
        let routes = plan_managed_queue_routes(&descriptors, &self.message_store_config)?;

        for route in &routes {
            let Some(topic) = route.topic.as_deref() else {
                continue;
            };
            let expected_type = match route.kind {
                ManagedQueueKind::SimpleConsumeQueue | ManagedQueueKind::ConsumeQueueExtension => CQType::SimpleCQ,
                ManagedQueueKind::BatchConsumeQueue => CQType::BatchCQ,
                ManagedQueueKind::CommitLog => continue,
            };
            if !self
                .consume_queue_store
                .accepts_reconciled_queue_type(topic, expected_type)
            {
                return Err(managed_queue_install_error(format!(
                    "managed queue {} does not match the configured topic queue type",
                    route.directory
                )));
            }
        }

        let activation = self
            .managed_lifecycle_activation
            .take()
            .ok_or_else(|| managed_queue_install_error("managed lifecycle activation candidate disappeared"))?;
        let store_root = Path::new(self.message_store_config.store_path_root_dir.as_str());
        let Some(activated) = stage_and_activate_managed_queues(activation, store_root, routes)? else {
            return Ok(false);
        };

        let mut commit_log = None;
        let mut logical_queues = Vec::new();
        let mut extensions = BTreeMap::new();
        for staged in activated.queues {
            let topic = staged.route.topic.clone();
            let queue_id = staged.route.queue_id;
            match staged.route.kind {
                ManagedQueueKind::CommitLog => commit_log = Some(staged.generation),
                ManagedQueueKind::SimpleConsumeQueue | ManagedQueueKind::BatchConsumeQueue => {
                    logical_queues.push((staged.route, staged.generation));
                }
                ManagedQueueKind::ConsumeQueueExtension => {
                    let (Some(topic), Some(queue_id)) = (topic, queue_id) else {
                        return Err(managed_queue_install_error(
                            "managed consume-queue extension lost its queue identity",
                        ));
                    };
                    if extensions.insert((topic, queue_id), staged.generation).is_some() {
                        return Err(managed_queue_install_error(
                            "managed consume-queue extension was staged more than once",
                        ));
                    }
                }
            }
        }

        let Some(commit_log) = commit_log else {
            return Err(managed_queue_install_error(
                "managed activation did not stage the required CommitLog generation",
            ));
        };
        let runtime = activated.runtime;
        if !self
            .commit_log
            .install_reconciled_generation(commit_log, runtime.clone())
        {
            return Err(managed_runtime_install_error(
                &runtime,
                "failed to publish the reconciled CommitLog generation before worker start",
            ));
        }

        for (route, generation) in logical_queues {
            let (Some(topic), Some(queue_id)) = (route.topic, route.queue_id) else {
                return Err(managed_runtime_install_error(
                    &runtime,
                    "managed consume queue lost its queue identity",
                ));
            };
            let installed = match route.kind {
                ManagedQueueKind::SimpleConsumeQueue => {
                    let extension = extensions.remove(&(topic.clone(), queue_id));
                    self.consume_queue_store.install_reconciled_simple_queue(
                        &topic,
                        queue_id,
                        generation,
                        extension,
                        runtime.clone(),
                    )
                }
                ManagedQueueKind::BatchConsumeQueue => self.consume_queue_store.install_reconciled_batch_queue(
                    &topic,
                    queue_id,
                    generation,
                    runtime.clone(),
                ),
                ManagedQueueKind::CommitLog | ManagedQueueKind::ConsumeQueueExtension => false,
            };
            if !installed {
                return Err(managed_runtime_install_error(
                    &runtime,
                    format!("failed to publish reconciled managed queue {}", route.directory),
                ));
            }
        }
        if !extensions.is_empty() {
            return Err(managed_runtime_install_error(
                &runtime,
                "one or more managed consume-queue extensions were not attached to their queue",
            ));
        }

        if !self.allocate_mapped_file_service.install_managed_lifecycle(
            runtime.clone(),
            PathBuf::from(self.message_store_config.store_path_root_dir.as_str()),
        ) {
            return Err(managed_runtime_install_error(
                &runtime,
                "failed to bind the reconciled lifecycle runtime to the allocation worker before start",
            ));
        }
        if !self.consume_queue_store.bind_managed_lifecycle_runtime(runtime.clone()) {
            return Err(managed_runtime_install_error(
                &runtime,
                "failed to bind the reconciled lifecycle runtime to dynamic consume-queue creation",
            ));
        }
        self.mapped_file_retirement_service = Some(MappedFileRetirementService::new(
            runtime.clone(),
            self.runtime_scope.clone(),
            Arc::clone(&self.running_flags),
        ));
        self.managed_lifecycle_runtime = Some(runtime);
        Ok(true)
    }
}

fn managed_queue_install_error(detail: impl Into<String>) -> StoreError {
    StoreError::new(&rocketmq_error::STORAGE_STATE_CORRUPTED, StoreOperation::Load)
        .in_component(StoreComponent::MappedFile)
        .with_detail(detail)
}

fn managed_runtime_install_error(runtime: &ManagedLifecycleRuntime, detail: impl Into<String>) -> StoreError {
    runtime.begin_shutdown();
    managed_queue_install_error(detail)
}

/// Replays and reconciles managed lifecycle evidence before any persistent Store component exists.
///
/// This function deliberately returns data rather than a publication capability. Wave-B must bind
/// the reconciled session to staged queue generations, the retirement registry, and the owned
/// reaper before the constructor may proceed beyond this boundary.
pub(super) fn inspect_and_reconcile_managed_root(
    store_root_lease: &StoreRootLease,
) -> Result<ManagedReadOnlyDisposition, StoreError> {
    let inspection = store_root_lease.inspect_managed_lifecycle(StoreOperation::Load)?;
    let LockedManagedLifecycleInspection::Managed(session) = inspection else {
        return Err(
            StoreError::new(&rocketmq_error::STORAGE_STATE_CORRUPTED, StoreOperation::Load)
                .in_component(StoreComponent::MappedFile)
                .with_detail("managed lifecycle evidence disappeared between classification and reconciliation"),
        );
    };

    match session.reconcile(ManagedReconciliationLimits::default())? {
        ManagedReconciliationDisposition::Ready(reconciled) => {
            prepare_managed_lifecycle_activation(reconciled).map(ManagedReadOnlyDisposition::Ready)
        }
        ManagedReconciliationDisposition::RecoveryRequired(recovery) => {
            Ok(ManagedReadOnlyDisposition::RecoveryRequired(recovery))
        }
    }
}

/// Converts a successful read-only proof into the deliberate Wave-B activation fence.
pub(super) fn wave_b_activation_fence(disposition: ManagedReadOnlyDisposition) -> StoreError {
    let detail = match disposition {
        ManagedReadOnlyDisposition::Ready(prepared) => format!(
            "managed lifecycle reconciled {} active segments and rebuilt {} pending retirements, but Wave-B queue, registry, and reaper activation is not yet enabled",
            prepared.unclaimed_active_count(),
            prepared.recovered_retirement_count(),
        ),
        ManagedReadOnlyDisposition::RecoveryRequired(recovery) => format!(
            "managed lifecycle requires {} durable recovery actions, but Wave-B lifecycle writes are not yet enabled",
            recovery.required_action_count(),
        ),
    };
    StoreError::new(&rocketmq_error::STORAGE_OPERATION_UNSUPPORTED, StoreOperation::Load)
        .in_component(StoreComponent::MappedFile)
        .with_detail(detail)
}

/// Accepts only a completely reconciled root for the explicit local Wave-B mode.
pub(super) fn require_wave_b_ready(
    disposition: ManagedReadOnlyDisposition,
) -> Result<PreparedManagedLifecycleActivation, StoreError> {
    match disposition {
        ManagedReadOnlyDisposition::Ready(prepared) => Ok(prepared),
        ManagedReadOnlyDisposition::RecoveryRequired(recovery) => Err(StoreError::new(
            &rocketmq_error::STORAGE_BACKEND_UNAVAILABLE,
            StoreOperation::Load,
        )
        .in_component(StoreComponent::MappedFile)
        .with_detail(format!(
            "managed lifecycle requires {} durable recovery actions before Wave-B activation",
            recovery.required_action_count(),
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const COMMIT_LOG_SIZE: u64 = 1_073_741_824;
    const CONSUME_QUEUE_SIZE: u64 = 6_000_000;
    const EXT_SIZE: u64 = 50_000_000;
    const BATCH_SIZE: u64 = 13_800_000;

    fn plan<const N: usize>(bindings: [(&str, u64); N]) -> Result<Vec<ManagedQueueRoute>, String> {
        plan_queue_routes(
            bindings,
            COMMIT_LOG_SIZE,
            CONSUME_QUEUE_SIZE,
            EXT_SIZE,
            BATCH_SIZE,
            true,
        )
    }

    #[test]
    fn managed_queue_routes_accept_only_complete_supported_layouts() {
        let routes = plan([
            ("commitlog", COMMIT_LOG_SIZE),
            ("consumequeue/topic-a/0", CONSUME_QUEUE_SIZE),
            ("consumequeue_ext/topic-a/0", EXT_SIZE),
            ("batchconsumequeue/topic-b/7", BATCH_SIZE),
        ])
        .expect("supported queue inventory");

        assert_eq!(routes.len(), 4);
        assert_eq!(routes[0].kind, ManagedQueueKind::CommitLog);
        assert_eq!(routes[1].topic.as_deref(), Some("topic-a"));
        assert_eq!(routes[1].queue_id, Some(0));
        assert_eq!(routes[3].kind, ManagedQueueKind::BatchConsumeQueue);
    }

    #[test]
    fn managed_queue_routes_reject_unknown_or_noncanonical_directories() {
        for directory in [
            "timerlog",
            "consumequeue/topic-a",
            "consumequeue//0",
            "consumequeue/topic-a/-1",
            "consumequeue/topic-a/01",
        ] {
            assert!(
                plan([(directory, CONSUME_QUEUE_SIZE)]).is_err(),
                "{directory} must fail before handle claiming"
            );
        }
    }

    #[test]
    fn managed_queue_routes_reject_size_and_queue_type_mismatches() {
        assert!(plan([("commitlog", 512)]).is_err());
        assert!(plan([
            ("consumequeue/topic-a/0", CONSUME_QUEUE_SIZE),
            ("batchconsumequeue/topic-a/0", BATCH_SIZE),
        ])
        .is_err());
        assert!(plan([("consumequeue_ext/topic-a/0", EXT_SIZE)]).is_err());
    }

    #[test]
    fn empty_managed_inventory_stages_an_empty_commit_log_queue() {
        let routes = plan([]).expect("an empty Store still needs a managed CommitLog queue");

        assert_eq!(
            routes,
            vec![ManagedQueueRoute {
                directory: "commitlog".into(),
                kind: ManagedQueueKind::CommitLog,
                topic: None,
                queue_id: None,
                expected_file_length: COMMIT_LOG_SIZE,
            }]
        );
    }

    #[test]
    fn enabled_extensions_stage_an_empty_managed_generation_for_each_simple_queue() {
        let routes = plan([("consumequeue/topic-a/0", CONSUME_QUEUE_SIZE)])
            .expect("simple queue gets an empty managed extension generation");

        assert!(routes.iter().any(|route| {
            route.directory.as_ref() == "consumequeue_ext/topic-a/0"
                && route.kind == ManagedQueueKind::ConsumeQueueExtension
        }));
    }

    #[test]
    fn wave_b_configuration_rejects_external_commitlog_and_unmigrated_queue_backends() {
        let root = tempfile::tempdir().expect("temporary Store root");
        let mut config = MessageStoreConfig {
            store_path_root_dir: root.path().to_string_lossy().into_owned().into(),
            timer_wheel_enable: false,
            message_index_enable: false,
            ..MessageStoreConfig::default()
        };
        if !cfg!(any(target_os = "linux", windows)) {
            assert!(validate_wave_b_configuration(&config).is_err());
            return;
        }
        assert!(validate_wave_b_configuration(&config).is_ok());

        config.store_path_commit_log = Some(root.path().join("external").to_string_lossy().into_owned().into());
        assert!(validate_wave_b_configuration(&config).is_err());

        config.store_path_commit_log = None;
        config.enable_compaction = true;
        assert!(validate_wave_b_configuration(&config).is_err());

        config.enable_compaction = false;
        config.timer_wheel_enable = true;
        assert!(validate_wave_b_configuration(&config).is_err());

        config.timer_wheel_enable = false;
        config.message_index_enable = true;
        assert!(validate_wave_b_configuration(&config).is_err());
    }
}
