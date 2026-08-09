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

use super::*;

#[cfg(feature = "extended_timeline")]
use crate::timer::timeline::TimelinePromotionObservation;

impl LocalFileMessageStore {
    pub(super) fn is_dledger_commit_log_enabled_config(message_store_config: &MessageStoreConfig) -> bool {
        message_store_config.enable_dledger_commit_log || message_store_config.enable_dleger_commit_log
    }

    pub fn new(
        message_store_config: Arc<MessageStoreConfig>,
        broker_config: Arc<StoreRuntimeConfig>,
        topic_config_table: Arc<DashMap<CheetahString, Arc<TopicConfig>>>,
        broker_stats_manager: Option<Arc<BrokerStatsManager>>,
        notify_message_arrive_in_batch: bool,
        service_context: ChildServiceContext,
    ) -> Self {
        Self::try_new(
            message_store_config,
            broker_config,
            topic_config_table,
            broker_stats_manager,
            notify_message_arrive_in_batch,
            service_context,
        )
        .unwrap_or_else(|error| panic!("failed to create local file message store: {error}"))
    }

    pub fn try_new(
        message_store_config: Arc<MessageStoreConfig>,
        broker_config: Arc<StoreRuntimeConfig>,
        topic_config_table: Arc<DashMap<CheetahString, Arc<TopicConfig>>>,
        broker_stats_manager: Option<Arc<BrokerStatsManager>>,
        notify_message_arrive_in_batch: bool,
        service_context: ChildServiceContext,
    ) -> Result<Self, StoreError> {
        Self::try_new_with_telemetry(
            message_store_config,
            broker_config,
            topic_config_table,
            broker_stats_manager,
            notify_message_arrive_in_batch,
            service_context,
            crate::telemetry::StoreTelemetry::noop(),
        )
    }

    pub fn try_new_with_telemetry(
        message_store_config: Arc<MessageStoreConfig>,
        broker_config: Arc<StoreRuntimeConfig>,
        topic_config_table: Arc<DashMap<CheetahString, Arc<TopicConfig>>>,
        broker_stats_manager: Option<Arc<BrokerStatsManager>>,
        notify_message_arrive_in_batch: bool,
        service_context: ChildServiceContext,
        telemetry: crate::telemetry::StoreTelemetry,
    ) -> Result<Self, StoreError> {
        let runtime_scope = StoreRuntimeScope::new(service_context.clone());
        let local_backend_config = message_store_config.normalized_local_backend_config();
        let cleanup_policy = LocalCleanupPolicy::new(local_backend_config.cleanup);
        let (delay_level_table, max_delay_level) = parse_delay_level(message_store_config.message_delay_level.as_str());
        let delay_level_table = Arc::new(delay_level_table);
        let running_flags = Arc::new(RunningFlags::new());
        let store_health_recorder = StoreHealthRecorder::new(running_flags.clone());
        let alive_replica_num_in_group = Arc::new(AtomicI32::new(1));
        let store_stats_service = Arc::new(StoreStatsService::new(
            Some(broker_config.broker_identity.clone()),
            service_context.component("store-stats"),
        ));
        let store_checkpoint = Arc::new(
            StoreCheckpoint::new(get_store_checkpoint(message_store_config.store_path_root_dir.as_str())).map_err(
                |error| {
                    StoreError::storage(
                        StoreOperation::Load,
                        format!(
                            "failed to create store checkpoint under {}",
                            message_store_config.store_path_root_dir
                        ),
                    )
                    .with_source(error)
                },
            )?,
        );
        let index_service = IndexService::new(
            runtime_scope.clone(),
            message_store_config.clone(),
            store_checkpoint.clone(),
            running_flags.clone(),
        );
        let build_index: Arc<dyn CommitLogDispatcher> = Arc::new(CommitLogDispatcherBuildIndex::new(
            index_service.clone(),
            message_store_config.clone(),
        ));
        let consume_queue_store = ConsumeQueueStore::new(
            runtime_scope.clone(),
            message_store_config.clone(),
            broker_config.clone(),
        );
        let build_consume_queue: Arc<dyn CommitLogDispatcher> =
            Arc::new(CommitLogDispatcherBuildConsumeQueue::new(consume_queue_store.clone()));

        let keep_local_derived_dispatchers =
            !message_store_config.is_enable_rocksdb_store() || message_store_config.rocksdb_cq_double_write_enable;
        let mut dispatcher_vec = if keep_local_derived_dispatchers {
            vec![build_consume_queue, build_index]
        } else {
            Vec::new()
        };
        #[cfg(feature = "extended_timeline")]
        let extended_timeline_completion_wake = Arc::new(TimelineCompletionWake::default());
        #[cfg(feature = "extended_timeline")]
        if message_store_config.timer_store_mode == TimerStoreMode::ExtendedTimeline {
            dispatcher_vec.push(extended_timeline_completion_wake.clone());
        }
        let mut dispatcher = CommitLogDispatcherDefault::with_dispatchers(dispatcher_vec);

        let memory_lock_budget_bytes = Self::effective_linux_memory_lock_budget_bytes(message_store_config.as_ref());
        #[cfg(feature = "observability")]
        let transient_store_pool = TransientStorePool::new_with_memory_lock_budget_and_store_metrics(
            message_store_config.transient_store_pool_size,
            message_store_config.mapped_file_size_commit_log,
            memory_lock_budget_bytes,
            telemetry.store().clone(),
        );
        #[cfg(not(feature = "observability"))]
        let transient_store_pool = TransientStorePool::new_with_memory_lock_budget(
            message_store_config.transient_store_pool_size,
            message_store_config.mapped_file_size_commit_log,
            memory_lock_budget_bytes,
        );
        let transient_store_pool_enable = message_store_config.transient_store_pool_enable
            && (broker_config.enable_controller_mode || message_store_config.broker_role != BrokerRole::Slave);
        let allocate_transient_store_pool = transient_store_pool_enable.then(|| Arc::new(transient_store_pool.clone()));
        let allocate_mapped_file_service = AllocateMappedFileService::new_with_message_store_config_and_storage_io(
            allocate_transient_store_pool,
            transient_store_pool_enable,
            message_store_config.fast_fail_if_no_buffer_in_store_pool,
            message_store_config.as_ref(),
            runtime_scope.mapped_file_allocation_budget(),
            runtime_scope.storage_io(),
        );
        #[cfg(feature = "observability")]
        let allocate_mapped_file_service = allocate_mapped_file_service.with_store_metrics(telemetry.store().clone());
        let allocate_mapped_file_service = Arc::new(allocate_mapped_file_service);

        let store_context = CommitLogStoreContext::new(
            running_flags.clone(),
            alive_replica_num_in_group.clone(),
            store_stats_service.clone(),
            max_delay_level,
            delay_level_table.clone(),
        );
        let store_runtime_state = Arc::new(StoreRuntimeState::new(message_store_config.as_ref()));
        let mut commit_log = CommitLog::try_new(
            runtime_scope.clone(),
            message_store_config.clone(),
            Arc::clone(&store_runtime_state),
            broker_config.clone(),
            store_context,
            dispatcher.handle(),
            store_checkpoint.clone(),
            topic_config_table.clone(),
            consume_queue_store.clone(),
            (*allocate_mapped_file_service).clone(),
            telemetry.handle().clone(),
            telemetry.store().clone(),
        )?;
        commit_log.set_store_health_recorder(store_health_recorder.clone());
        let commit_log_read = commit_log.read_handle();
        let commit_log_cleanup = commit_log.cleanup_handle();
        let compaction_store = Arc::new(CompactionStore::with_root(
            PathBuf::from(message_store_config.store_path_root_dir.as_str()).join("compaction"),
            runtime_scope.clone(),
        ));
        if message_store_config.enable_compaction {
            dispatcher.add_dispatcher(Arc::new(CommitLogDispatcherCompaction::new(
                compaction_store.clone(),
                message_store_config.clone(),
                topic_config_table.clone(),
            )));
        }
        let compaction_commit_log = commit_log_read.clone();
        compaction_store.set_payload_resolver(move |physical_offset, size| {
            compaction_commit_log
                .get_message(physical_offset, size)
                .and_then(|result| result.get_bytes())
        });
        #[cfg(feature = "tieredstore")]
        let tiered_store = Self::build_tiered_store(
            message_store_config.clone(),
            commit_log_read,
            &mut dispatcher,
            telemetry.tiered_store().clone(),
            runtime_scope.clone(),
        )?;
        #[cfg(feature = "tieredstore")]
        let minimum_pinned_wal_segment = tiered_store.as_ref().map(|tiered_store| {
            let tiered_store = tiered_store.clone();
            Arc::new(move || tiered_store.minimum_pinned_wal_segment()) as Arc<CommitLogWalPin>
        });
        #[cfg(not(feature = "tieredstore"))]
        let minimum_pinned_wal_segment: Option<Arc<CommitLogWalPin>> = None;
        #[cfg(feature = "extended_timeline")]
        let extended_timeline_enabled = message_store_config.timer_extended_shadow_enable
            || message_store_config.timer_store_mode == TimerStoreMode::ExtendedTimeline;
        #[cfg(feature = "extended_timeline")]
        let extended_timeline_cleanup_pin = Arc::new(AtomicI64::new(if extended_timeline_enabled { 0 } else { -1 }));
        #[cfg(feature = "extended_timeline")]
        let minimum_pinned_wal_segment = if extended_timeline_enabled {
            let existing_pin = minimum_pinned_wal_segment.clone();
            let timeline_pin = Arc::clone(&extended_timeline_cleanup_pin);
            Some(Arc::new(move || {
                let timeline_offset = timeline_pin.load(Ordering::Acquire);
                let timeline_offset = u64::try_from(timeline_offset).ok();
                match (existing_pin.as_ref().and_then(|pin| pin()), timeline_offset) {
                    (Some(existing), Some(timeline)) => Some(existing.min(timeline)),
                    (existing, timeline) => existing.or(timeline),
                }
            }) as Arc<CommitLogWalPin>)
        } else {
            minimum_pinned_wal_segment
        };

        ensure_dir_ok(message_store_config.store_path_root_dir.as_str());
        ensure_dir_ok(Self::get_store_path_physic(&message_store_config).as_str());
        ensure_dir_ok(Self::get_store_path_logic(&message_store_config).as_str());

        let compaction_service = message_store_config.enable_compaction.then(|| {
            CompactionService::new(
                runtime_scope.clone(),
                compaction_store.clone(),
                message_store_config.compaction_schedule_internal,
            )
        });
        Ok(Self {
            runtime_scope: runtime_scope.clone(),
            telemetry,
            message_store_config: message_store_config.clone(),
            store_runtime_state,
            composition: LocalStoreComposition::new(local_backend_config),
            broker_config,
            put_message_hook_list: HookRegistry::new(),
            topic_config_table,
            // message_store_runtime: Some(RocketMQRuntime::new_multi(10, "message-store-thread")),
            commit_log,
            compaction_service,
            store_checkpoint: Some(store_checkpoint.clone()),
            master_flushed_offset: Arc::new(AtomicI64::new(-1)),
            alive_replica_num_in_group,
            index_service: index_service.clone(),
            allocate_mapped_file_service,
            consume_queue_store: consume_queue_store.clone(),
            dispatcher,
            #[cfg(feature = "tieredstore")]
            tiered_store,
            broker_init_max_offset: Arc::new(AtomicI64::new(-1)),
            state_machine_version: Arc::new(AtomicI64::new(0)),
            controller_epoch_start_offset: Arc::new(AtomicI64::new(-1)),
            shutdown: Arc::new(AtomicBool::new(false)),
            background_index_query_degradation_total: Arc::new(AtomicU64::new(0)),
            store_lock_guard: None,
            running_flags: running_flags.clone(),
            store_health_recorder,
            reput_message_service: ReputMessageService {
                shutdown_token: CancellationToken::new(),
                new_message_notify: Arc::new(Notify::new()),
                dispatch_progress_notify: Arc::new(Notify::new()),
                pending_messages: Arc::new(AtomicI64::new(0)),
                inflight_dispatch_batches: Arc::new(AtomicU64::new(0)),
                reput_from_offset: None,
                dispatch_tx: None,
                inner: None,
                task_group: None,
            },
            background_index_rebuild_service: BackgroundIndexRebuildService::new(),
            clean_commit_log_service: Arc::new(CleanCommitLogService::new(
                message_store_config.clone(),
                commit_log_cleanup.clone(),
                running_flags.clone(),
                cleanup_policy,
                minimum_pinned_wal_segment,
            )),
            correct_logic_offset_service: Arc::new(CorrectLogicOffsetService::new(
                commit_log_cleanup.clone(),
                consume_queue_store.clone(),
            )),
            clean_consume_queue_service: Arc::new(CleanConsumeQueueService::new(
                commit_log_cleanup,
                consume_queue_store.clone(),
                index_service.clone(),
            )),
            broker_stats_manager,
            message_arrival: MessageArrivalCapability::default(),
            notify_message_arrive_in_batch,
            store_stats_service,
            compaction_store,
            timer_message_store: None,
            #[cfg(feature = "extended_timeline")]
            extended_timeline_cleanup_pin,
            #[cfg(feature = "extended_timeline")]
            extended_timeline_materializer: None,
            #[cfg(feature = "extended_timeline")]
            extended_timeline_due_scanner: None,
            #[cfg(feature = "extended_timeline")]
            extended_timeline_recall_service: None,
            #[cfg(feature = "extended_timeline")]
            extended_timeline_completion_wake,
            #[cfg(feature = "extended_timeline")]
            extended_timeline_completion_reconciler: None,
            #[cfg(feature = "extended_timeline")]
            extended_timeline_delivery: None,
            #[cfg(feature = "extended_timeline")]
            extended_timeline_snapshot: None,
            #[cfg(feature = "extended_timeline")]
            extended_timeline_promotion_gate: None,
            #[cfg(feature = "extended_timeline")]
            extended_timeline_admission: None,
            #[cfg(feature = "extended_timeline")]
            extended_timeline_gc: None,
            #[cfg(feature = "extended_timeline")]
            extended_timeline_role: Arc::new(TimerRoleState::new(message_store_config.store_path_root_dir.as_str())),
            #[cfg(feature = "extended_timeline")]
            extended_timeline_clock: Arc::new(TimerClockSafety::new(
                Arc::new(SystemTimerClock::default()),
                message_store_config.timer_store_config.clock_backward_tolerance_ms,
            )),
            transient_store_pool,
            root_dependencies_wired: false,
            master_store_in_process: StdRwLock::new(None),
            send_message_back_hook: StdRwLock::new(None),
            pending_ha_service: None,
            ha_service: None,
            flush_consume_queue_service: FlushConsumeQueueService::new(
                runtime_scope.clone(),
                message_store_config.clone(),
                consume_queue_store.clone(),
                store_checkpoint,
            ),
            scheduled_task_shutdown: CancellationToken::new(),
            scheduled_task_group: None,
            scheduled_tasks: None,
            ha_update_master_group: Arc::new(StdMutex::new(None)),
            delay_level_table,
            max_delay_level,
            last_recovery_report: None,
        })
    }

    pub(super) fn effective_linux_memory_lock_budget_bytes(message_store_config: &MessageStoreConfig) -> u64 {
        message_store_config.effective_linux_memory_lock_budget_bytes(
            crate::platform::current_store_platform_capability().memory_lock_limit_bytes,
        )
    }

    #[cfg(feature = "tieredstore")]
    pub(super) fn build_tiered_store(
        message_store_config: Arc<MessageStoreConfig>,
        commit_log: CommitLogReadHandle,
        dispatcher: &mut CommitLogDispatcherDefault,
        metrics: rocketmq_observability::metrics::tiered_store::TieredStoreMetricsRecorder,
        runtime_scope: StoreRuntimeScope,
    ) -> Result<Option<Arc<TieredStoreDecorator>>, StoreError> {
        let Some(tiered_store_config) = message_store_config.tiered_store_config.clone() else {
            return Ok(None);
        };
        if !tiered_store_config.storage_level.enabled() {
            return Ok(None);
        }

        let tiered_store = Arc::new(TieredStoreDecorator::new_with_metrics(
            tiered_store_config,
            metrics,
            runtime_scope.task_group("rocketmq-store.tiered"),
        )?);
        let commit_log_for_dispatch = commit_log;
        let body_resolver = Arc::new(move |request: &DispatchRequest| -> Option<Bytes> {
            resolve_tiered_dispatch_body_with_reader(&commit_log_for_dispatch, request)
        });
        dispatcher.add_dispatcher(tiered_store.commit_log_dispatcher(body_resolver));

        Ok(Some(tiered_store))
    }

    #[cfg(feature = "tieredstore")]
    pub(super) fn should_try_tiered_offset_by_time(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        timestamp: i64,
    ) -> bool {
        let Some(logic_queue) = self.get_consume_queue(topic, queue_id) else {
            return true;
        };
        let logic_queue = logic_queue.read();
        if logic_queue.get_message_total_in_queue() <= 0 {
            return true;
        }
        logic_queue
            .get_earliest_unit_and_store_time()
            .map(|(_, earliest_store_time)| timestamp < earliest_store_time)
            .unwrap_or(true)
    }

    pub fn get_store_path_physic(message_store_config: &Arc<MessageStoreConfig>) -> String {
        message_store_config.get_store_path_commit_log()
    }

    pub fn get_store_path_logic(message_store_config: &Arc<MessageStoreConfig>) -> String {
        get_store_path_consume_queue(message_store_config.store_path_root_dir.as_str())
    }

    pub fn message_store_config(&self) -> Arc<MessageStoreConfig> {
        self.message_store_config.clone()
    }

    pub(crate) fn ha_replica_store_handle(&self) -> HAReplicaStoreHandle {
        HAReplicaStoreHandle {
            message_store_config: self.message_store_config.clone(),
            shutdown: self.shutdown.clone(),
            running_flags: self.running_flags.clone(),
            master_flushed_offset: self.master_flushed_offset.clone(),
            alive_replica_num_in_group: self.alive_replica_num_in_group.clone(),
            state_machine_version: self.state_machine_version.clone(),
            controller_epoch_start_offset: self.controller_epoch_start_offset.clone(),
            commit_log: self.commit_log.replica_handle(),
        }
    }

    pub(crate) fn consume_queue_context(&self) -> ConsumeQueueStoreContext {
        ConsumeQueueStoreContext::new(
            Arc::clone(&self.message_store_config),
            Arc::clone(&self.topic_config_table),
            self.commit_log.read_handle(),
            Arc::clone(&self.running_flags),
            self.store_checkpoint
                .clone()
                .expect("store checkpoint is initialized with LocalFileMessageStore"),
        )
    }

    pub(crate) fn consume_queue_lookup_handle(&self) -> ConsumeQueueLookupHandle {
        self.consume_queue_store.lookup_handle()
    }

    pub(super) fn timer_message_write_handle(&self) -> TimerMessageWriteHandle {
        TimerMessageWriteHandle {
            message_store_config: Arc::clone(&self.message_store_config),
            lifecycle: self.composition.lifecycle_handle(),
            shutdown: Arc::clone(&self.shutdown),
            running_flags: Arc::clone(&self.running_flags),
            put_message_hooks: self.put_message_hook_list.clone(),
            topic_config_table: Arc::clone(&self.topic_config_table),
            commit_log: self.commit_log.internal_message_write_handle(),
            consume_queue_store: self.consume_queue_store.clone(),
            store_stats_service: Arc::clone(&self.store_stats_service),
            reput_notify: self.reput_message_service.notify_handle(),
        }
    }

    pub(super) fn timer_store_context(&self) -> TimerStoreContext {
        TimerStoreContext::new(
            self.consume_queue_lookup_handle(),
            self.commit_log.read_handle(),
            self.timer_message_write_handle(),
        )
    }

    #[cfg(feature = "tieredstore")]
    pub fn tiered_store_metrics(
        &self,
    ) -> Option<Arc<rocketmq_observability::metrics::tiered_store::TieredStoreMetrics>> {
        self.tiered_store.as_ref().map(|tiered_store| tiered_store.metrics())
    }

    pub fn message_store_config_ref(&self) -> &MessageStoreConfig {
        self.message_store_config.as_ref()
    }

    pub fn is_transient_store_pool_enable(&self) -> bool {
        self.message_store_config.transient_store_pool_enable
            && (self.broker_config.enable_controller_mode
                || self.store_runtime_state.broker_role() != BrokerRole::Slave)
    }

    /// Completes root wiring when this store has exclusive ownership.
    ///
    /// ConsumeQueue, Timer, and HA use independently owned capability handles, so they do not
    /// require the full Store root to be shared.
    ///
    /// # Errors
    ///
    /// Returns an error when an enabled derived store cannot be opened or its durable owner epoch
    /// cannot be recovered.
    pub fn wire_owned_root_dependencies(&mut self) -> Result<(), StoreError> {
        self.consume_queue_store.set_context(self.consume_queue_context());
        #[cfg(feature = "extended_timeline")]
        if (self.message_store_config.timer_extended_shadow_enable
            || self.message_store_config.timer_store_mode == TimerStoreMode::ExtendedTimeline)
            && self.extended_timeline_materializer.is_none()
        {
            let formal = self.message_store_config.timer_store_mode == TimerStoreMode::ExtendedTimeline;
            self.extended_timeline_role.load().map_err(|error| {
                StoreError::storage(StoreOperation::Load, "failed to recover Extended Timeline owner epoch")
                    .with_source(error)
            })?;
            let admission_epoch = TimerEngineEpoch::new(self.message_store_config.timer_extended_activation_epoch);
            let open_result = if formal {
                ShadowTimelineMaterializer::open_formal(
                    self.message_store_config.store_path_root_dir.as_str(),
                    self.message_store_config.timer_store_config.clone(),
                    self.consume_queue_store.lookup_handle(),
                    self.commit_log.read_handle(),
                    Arc::clone(&self.extended_timeline_cleanup_pin),
                    admission_epoch,
                )
            } else {
                ShadowTimelineMaterializer::open(
                    self.message_store_config.store_path_root_dir.as_str(),
                    self.message_store_config.timer_store_config.clone(),
                    self.consume_queue_store.lookup_handle(),
                    self.commit_log.read_handle(),
                    Arc::clone(&self.extended_timeline_cleanup_pin),
                )
            };
            let materializer = Arc::new(open_result.map_err(|error| {
                StoreError::storage(
                    StoreOperation::Load,
                    format!("failed to open Extended Timeline storage: {error}"),
                )
            })?);
            let timeline = materializer.timeline();
            self.extended_timeline_due_scanner = Some(Arc::new(if formal {
                TimelineDueScanner::new_with_clock(
                    self.message_store_config.timer_store_config.clone(),
                    Arc::clone(&timeline),
                    Arc::clone(&self.extended_timeline_clock),
                )
            } else {
                TimelineDueScanner::new_shadow(
                    self.message_store_config.timer_store_config.clone(),
                    Arc::clone(&timeline),
                    materializer.reconciler(),
                )
            }));
            self.extended_timeline_recall_service = Some(Arc::new(TimelineRecallService::new(Arc::clone(&timeline))));
            if formal {
                let completion = Arc::new(TimelineCompletionReconciler::new(
                    Arc::clone(&timeline),
                    self.commit_log.read_handle(),
                    Arc::clone(&self.extended_timeline_completion_wake),
                ));
                let timer_config = &self.message_store_config.timer_store_config;
                let delivery = TimelineDeliveryCoordinator::new(
                    timeline,
                    materializer.payload_store(),
                    self.timer_message_write_handle(),
                    Arc::clone(&self.extended_timeline_role),
                    Arc::clone(&self.extended_timeline_clock),
                    Arc::clone(&completion),
                    admission_epoch,
                    timer_config.lane_count,
                    timer_config.due_scan_messages,
                    timer_config.due_scan_bytes,
                    timer_config.delivery_lease_ms,
                )
                .map_err(|error| {
                    StoreError::storage(
                        StoreOperation::Load,
                        format!("failed to construct Extended Timeline delivery: {error}"),
                    )
                })?;
                let snapshot = Arc::new(TimelineSnapshotManager::new(
                    self.message_store_config.store_path_root_dir.as_str(),
                    materializer.timeline(),
                    materializer.payload_store(),
                    Arc::clone(&materializer),
                    Arc::clone(&completion),
                    Arc::clone(&self.extended_timeline_role),
                    Arc::clone(&self.extended_timeline_clock),
                    admission_epoch,
                ));
                let (_, _, format_fingerprint) = materializer.snapshot_source_cursors().map_err(|error| {
                    StoreError::storage(
                        StoreOperation::Load,
                        format!("failed to read Extended Timeline promotion identity: {error}"),
                    )
                })?;
                let promotion_gate = Arc::new(TimelinePromotionGate::new(
                    admission_epoch.get(),
                    format_fingerprint,
                    1,
                    Arc::clone(&self.extended_timeline_clock),
                ));
                if let Some(manifest) = snapshot.latest_published().map_err(|error| {
                    StoreError::storage(
                        StoreOperation::Load,
                        format!("failed to restore Extended Timeline snapshot identity: {error}"),
                    )
                })? {
                    promotion_gate.mark_snapshot_installed(manifest).map_err(|error| {
                        StoreError::config(
                            StoreOperation::Load,
                            format!("latest Extended Timeline snapshot is not promotable: {error}"),
                        )
                    })?;
                }
                let admission = Arc::new(TimelineAdmissionController::new(
                    self.message_store_config.timer_store_config.clone(),
                    self.message_store_config.timer_extended_admission_horizon_days,
                    PathBuf::from(self.message_store_config.store_path_root_dir.as_str()),
                    materializer.timeline(),
                    Arc::clone(&materializer),
                    Arc::clone(&self.extended_timeline_role),
                ));
                let gc = Arc::new(TimelineGcService::new(
                    materializer.timeline(),
                    materializer.payload_store(),
                    timer_config.gc_retention_grace_ms,
                ));
                self.extended_timeline_completion_reconciler = Some(completion);
                self.extended_timeline_delivery = Some(Arc::new(delivery));
                self.extended_timeline_snapshot = Some(snapshot);
                self.extended_timeline_promotion_gate = Some(promotion_gate);
                self.extended_timeline_admission = Some(admission);
                self.extended_timeline_gc = Some(gc);
            }
            self.extended_timeline_materializer = Some(materializer);
        }
        if self.message_store_config.is_timer_wheel_enable() && self.timer_message_store.is_none() {
            let timer_message_store = Arc::new(TimerMessageStore::new_with_store_context(
                self.timer_store_context(),
                Arc::clone(&self.message_store_config),
                self.runtime_scope.clone(),
                self.telemetry.timer().clone(),
                self.telemetry.store().clone(),
            ));
            self.set_timer_message_store(timer_message_store);
        }
        if !Self::is_dledger_commit_log_enabled_config(self.message_store_config.as_ref())
            && !self.message_store_config.duplication_enable
        {
            let replica_store = self.ha_replica_store_handle();
            self.pending_ha_service = Some(if self.message_store_config.enable_controller_mode {
                PendingHAService::AutoSwitch(
                    crate::ha::auto_switch::auto_switch_ha_service::AutoSwitchHAService::new(
                        crate::ha::default_ha_service::DefaultHAService::new_with_store_metrics(
                            replica_store,
                            self.runtime_scope.clone(),
                            self.telemetry.store().clone(),
                        ),
                    ),
                )
            } else {
                PendingHAService::Default(Box::new(
                    crate::ha::default_ha_service::DefaultHAService::new_with_store_metrics(
                        replica_store,
                        self.runtime_scope.clone(),
                        self.telemetry.store().clone(),
                    ),
                ))
            });
        }
        self.root_dependencies_wired = true;
        Ok(())
    }

    #[inline]
    pub fn delay_level_table(&self) -> &Arc<BTreeMap<i32, i64>> {
        &self.delay_level_table
    }

    #[inline]
    pub fn delay_level_table_ref(&self) -> &BTreeMap<i32, i64> {
        self.delay_level_table.as_ref()
    }

    #[inline]
    pub fn max_delay_level(&self) -> i32 {
        self.max_delay_level
    }

    pub(super) fn enabled_rocksdb_specific_options(message_store_config: &MessageStoreConfig) -> Vec<&'static str> {
        let mut enabled = Vec::new();
        if message_store_config.clean_rocksdb_dirty_cq_interval_min > 0 {
            enabled.push("clean_rocksdb_dirty_cq_interval_min");
        }
        if message_store_config.stat_rocksdb_cq_interval_sec > 0 {
            enabled.push("stat_rocksdb_cq_interval_sec");
        }
        if message_store_config.real_time_persist_rocksdb_config {
            enabled.push("real_time_persist_rocksdb_config");
        }
        if message_store_config.enable_rocksdb_log {
            enabled.push("enable_rocksdb_log");
        }
        if message_store_config.rocksdb_cq_double_write_enable {
            enabled.push("rocksdb_cq_double_write_enable");
        }
        if message_store_config.trans_rocksdb_enable {
            enabled.push("trans_rocksdb_enable");
        }
        enabled
    }

    pub(super) fn validate_supported_configuration(&self) -> Result<(), StoreError> {
        if Self::is_dledger_commit_log_enabled_config(self.message_store_config.as_ref()) {
            return Err(StoreError::dledger(
                StoreOperation::Load,
                "DLedger commit log is Java-specific and is intentionally unsupported in rocketmq-rust".to_string(),
            ));
        }
        if self.message_store_config.timer_rocksdb_enable && !self.message_store_config.is_enable_rocksdb_store() {
            return Err(StoreError::unsupported(
                StoreOperation::Load,
                "Timer RocksDB backend is not implemented in rocketmq-rust; keep timer_rocksdb_enable=false"
                    .to_string(),
            ));
        }
        if self.message_store_config.timer_store_mode == TimerStoreMode::ExtendedTimeline {
            if !cfg!(feature = "extended_timeline") {
                return Err(StoreError::unsupported(
                    StoreOperation::Load,
                    "timer_store_mode=extended_timeline requires the rocketmq-store/extended_timeline feature"
                        .to_string(),
                ));
            }
            if self.message_store_config.timer_extended_shadow_enable
                || !self.message_store_config.timer_extended_admission_enable
                || self.message_store_config.timer_extended_activation_epoch == 0
                || !(3..=self.message_store_config.timer_store_config.horizon_days)
                    .contains(&self.message_store_config.timer_extended_admission_horizon_days)
                || self.message_store_config.timer_extended_admission_horizon_days > 400
            {
                return Err(StoreError::config(
                    StoreOperation::Load,
                    "Extended mode requires formal admission, a non-zero activation epoch, a 3..=400 day admission horizon, and shadow=false"
                        .to_string(),
                ));
            }
        } else if self.message_store_config.timer_extended_admission_enable {
            return Err(StoreError::config(
                StoreOperation::Load,
                "timer_extended_admission_enable requires timer_store_mode=extended_timeline".to_string(),
            ));
        }
        if self.message_store_config.timer_extended_shadow_enable && !cfg!(feature = "extended_timeline") {
            return Err(StoreError::unsupported(
                StoreOperation::Load,
                "timer_extended_shadow_enable requires the rocketmq-store/extended_timeline feature".to_string(),
            ));
        }
        if self.message_store_config.timer_extended_shadow_enable
            || self.message_store_config.timer_store_mode == TimerStoreMode::ExtendedTimeline
        {
            self.message_store_config
                .timer_store_config
                .validate()
                .map_err(|error| StoreError::config(StoreOperation::Load, error.to_string()))?;
        }

        let enabled_rocksdb_options = Self::enabled_rocksdb_specific_options(self.message_store_config.as_ref());
        if !self.message_store_config.is_enable_rocksdb_store() && !enabled_rocksdb_options.is_empty() {
            return Err(StoreError::config(
                StoreOperation::Load,
                format!(
                    "RocksDB-specific configuration requires store_type=RocksDB: {}",
                    enabled_rocksdb_options.join(", ")
                ),
            ));
        }
        if self.message_store_config.effective_linux_memory_lock_mode() == LinuxMemoryLockMode::ActiveFile
            && self.message_store_config.linux_memory_lock_budget_bytes == 0
            && !self.message_store_config.linux_memory_lock_warn_only
        {
            return Err(StoreError::config(
                StoreOperation::Load,
                "linux_memory_lock_mode=active_file requires explicit linux_memory_lock_budget_bytes when \
                 linux_memory_lock_warn_only=false; set linux_memory_lock_budget_bytes or \
                 linux_memory_lock_warn_only=true"
                    .to_string(),
            ));
        }
        Ok(())
    }

    #[inline]
    pub(super) fn lifecycle_state(&self) -> LocalStoreState {
        self.composition.lifecycle().state()
    }

    #[inline]
    pub(super) fn set_lifecycle_state(&self, state: LocalStoreState) {
        self.composition.lifecycle().transition_to(state);
    }

    #[inline]
    pub(super) fn is_store_available_for_io(&self) -> bool {
        self.composition
            .lifecycle()
            .is_available_for_io(self.shutdown.load(Ordering::Acquire))
    }

    pub(super) fn ensure_root_dependencies_wired(&self, operation: &str) -> Result<(), StoreError> {
        if self.root_dependencies_wired {
            return Ok(());
        }
        Err(StoreError::invalid_state(
            StoreOperation::Start,
            format!(
                "message store root dependencies are not wired; call wire_owned_root_dependencies before {operation}"
            ),
        ))
    }

    pub(super) fn acquire_store_lock(&mut self) -> Result<(), StoreError> {
        if self.store_lock_guard.is_some() {
            return Ok(());
        }

        let lock_path = PathBuf::from(get_lock_file(self.message_store_config.store_path_root_dir.as_str()));
        if let Some(parent) = lock_path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                StoreError::storage(
                    StoreOperation::Start,
                    format!("failed to create store lock parent directory {}", parent.display()),
                )
                .with_source(error)
            })?;
        }

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|error| {
                StoreError::storage(
                    StoreOperation::Start,
                    format!("failed to open store lock file {}", lock_path.display()),
                )
                .with_source(error)
            })?;
        file.try_lock_exclusive().map_err(|error| {
            StoreError::storage(
                StoreOperation::Start,
                format!(
                    "message store lock file is held by another instance: {}",
                    lock_path.display()
                ),
            )
            .with_source(error)
        })?;
        file.set_len(0).map_err(|error| {
            StoreError::storage(
                StoreOperation::Start,
                format!("failed to truncate store lock file {}", lock_path.display()),
            )
            .with_source(error)
        })?;
        writeln!(file, "pid={}", std::process::id()).map_err(|error| {
            StoreError::storage(
                StoreOperation::Start,
                format!("failed to write store lock file {}", lock_path.display()),
            )
            .with_source(error)
        })?;

        self.store_lock_guard = Some(StoreLockGuard { file });
        Ok(())
    }

    pub(super) fn release_store_lock(&mut self) {
        self.store_lock_guard.take();
    }

    pub(super) fn should_run_timer_dequeue(&self) -> bool {
        self.message_store_config.is_timer_wheel_enable()
            && self.message_store_config.timer_store_mode == TimerStoreMode::JavaCompat
            && self.store_runtime_state.broker_role() != BrokerRole::Slave
    }

    pub(super) fn sync_timer_message_store_role(&self) {
        if let Some(timer_message_store) = self.timer_message_store.as_ref() {
            timer_message_store.set_should_running_dequeue(self.should_run_timer_dequeue());
        }
    }

    #[cfg(feature = "extended_timeline")]
    pub(super) fn sync_extended_timeline_role(&self) {
        let active = self.message_store_config.timer_store_mode == TimerStoreMode::ExtendedTimeline
            && self.store_runtime_state.broker_role() != BrokerRole::Slave;
        if let Err(error) = self.extended_timeline_role.transition(active) {
            error!(
                "Extended Timeline role transition failed closed; delivery remains fenced: {}",
                error
            );
        }
    }

    #[cfg(feature = "extended_timeline")]
    pub(super) fn sync_extended_timeline_controller_role(
        &self,
        previous_role: BrokerRole,
        target_role: BrokerRole,
        external_term: u64,
    ) -> Result<(), StoreError> {
        if self.message_store_config.timer_store_mode != TimerStoreMode::ExtendedTimeline {
            return Ok(());
        }
        if target_role == BrokerRole::Slave {
            self.extended_timeline_role
                .transition_with_term(false, external_term)
                .map_err(|error| {
                    StoreError::storage(StoreOperation::Admin, "failed to persist Extended demotion fence")
                        .with_source(error)
                })?;
            return Ok(());
        }
        if previous_role == BrokerRole::Slave {
            self.validate_extended_timeline_promotion(external_term)?;
        }
        self.extended_timeline_role
            .transition_with_term(true, external_term)
            .map_err(|error| {
                StoreError::storage(StoreOperation::Admin, "failed to persist Extended promotion epoch")
                    .with_source(error)
            })?;
        Ok(())
    }

    #[cfg(feature = "extended_timeline")]
    fn validate_extended_timeline_promotion(&self, external_term: u64) -> Result<(), StoreError> {
        let gate = self
            .extended_timeline_promotion_gate
            .as_ref()
            .ok_or_else(|| StoreError::invalid_state(StoreOperation::Admin, "Extended promotion gate is not wired"))?;
        let materializer = self
            .extended_timeline_materializer
            .as_ref()
            .ok_or_else(|| StoreError::invalid_state(StoreOperation::Admin, "Extended materializer is not wired"))?;
        let completion = self.extended_timeline_completion_reconciler.as_ref().ok_or_else(|| {
            StoreError::invalid_state(StoreOperation::Admin, "Extended completion replay is not wired")
        })?;
        let (source_cq_cursor, source_physical_cursor, format_fingerprint) = materializer
            .snapshot_source_cursors()
            .map_err(|error| StoreError::storage(StoreOperation::Read, error.to_string()))?;
        let metrics = materializer.metrics();
        let completion_cursor = completion
            .completion_physical_cursor()
            .map_err(|error| StoreError::storage(StoreOperation::Read, error.to_string()))?;
        let replicated_final_end = self
            .commit_log
            .get_flushed_where()
            .max(self.commit_log.get_confirm_offset())
            .min(self.commit_log.get_max_offset())
            .max(0);
        let prospective_epoch = self.extended_timeline_role.epoch().saturating_add(1).max(external_term);
        gate.evaluate(TimelinePromotionObservation {
            source_retention_start: self.commit_log.get_min_offset().max(0),
            source_replay_cursor: source_physical_cursor,
            replicated_source_end: source_physical_cursor,
            final_retention_start: self.commit_log.get_min_offset().max(0),
            completion_replay_cursor: completion_cursor,
            replicated_final_end,
            materialization_backlog: metrics.materialization_lag,
            due_backlog: 0,
            completion_backlog: u64::try_from(replicated_final_end.saturating_sub(completion_cursor))
                .unwrap_or(u64::MAX),
            role_epoch: prospective_epoch,
            activation_epoch: self.message_store_config.timer_extended_activation_epoch,
            format_fingerprint,
            capability_version: 1,
        })
        .map_err(|error| {
            StoreError::invalid_state(
                StoreOperation::Admin,
                format!("Extended promotion rejected at source CQ cursor {source_cq_cursor}: {error}"),
            )
        })
    }

    /// Creates one consistent Extended Timeline snapshot and publishes it to the promotion gate.
    #[cfg(feature = "extended_timeline")]
    pub fn create_extended_timer_snapshot(&self) -> Result<rocketmq_store_api::TimerSnapshotManifest, StoreError> {
        let snapshot = self.extended_timeline_snapshot.as_ref().ok_or_else(|| {
            StoreError::unsupported(StoreOperation::Admin, "Extended Timeline snapshot is not enabled")
        })?;
        let manifest = snapshot
            .create()
            .map_err(|error| StoreError::storage(StoreOperation::Admin, error.to_string()))?;
        self.extended_timeline_promotion_gate
            .as_ref()
            .ok_or_else(|| StoreError::invalid_state(StoreOperation::Admin, "Extended promotion gate is not wired"))?
            .mark_snapshot_installed(manifest.clone())
            .map_err(|error| StoreError::invalid_state(StoreOperation::Admin, error.to_string()))?;
        Ok(manifest)
    }

    /// Releases snapshot GC pins after the artifact has been durably installed elsewhere.
    #[cfg(feature = "extended_timeline")]
    pub fn release_extended_timer_snapshot(
        &self,
        manifest: &rocketmq_store_api::TimerSnapshotManifest,
    ) -> Result<(), StoreError> {
        self.extended_timeline_snapshot
            .as_ref()
            .ok_or_else(|| StoreError::unsupported(StoreOperation::Admin, "Extended Timeline snapshot is not enabled"))?
            .release(manifest)
            .map_err(|error| StoreError::storage(StoreOperation::Admin, error.to_string()))
    }

    pub(super) fn refresh_controller_confirm_offset_after_role_change(&self) {
        if !self.broker_config.enable_controller_mode {
            return;
        }

        let min_phy_offset = self.get_min_phy_offset();
        let max_phy_offset = self.get_max_phy_offset().max(min_phy_offset);
        let next_confirm_offset = match self.store_runtime_state.broker_role() {
            BrokerRole::Slave => self.commit_log.get_confirm_offset_directly(),
            _ => self.commit_log.get_confirm_offset(),
        };
        self.publish_confirm_offset(next_confirm_offset.clamp(min_phy_offset, max_phy_offset));
    }
}

pub fn parse_delay_level(level_string: &str) -> (BTreeMap<i32, i64>, i32) {
    let mut delay_level_table = BTreeMap::new();

    let level_array: Vec<&str> = level_string.split(' ').collect();
    let mut max_delay_level = 0;

    for (i, value) in level_array.iter().enumerate() {
        let Some(ch) = value.chars().last() else {
            continue;
        };
        let tu = match ch {
            's' => 1000,
            'm' => 1000 * 60,
            'h' => 1000 * 60 * 60,
            'd' => 1000 * 60 * 60 * 24,
            _ => continue,
        };

        let level = i as i32 + 1;
        if level > max_delay_level {
            max_delay_level = level;
        }

        let num_str = &value[0..value.len() - 1];
        let Ok(num) = num_str.parse::<i64>() else {
            continue;
        };
        let delay_time_millis = tu * num;
        delay_level_table.insert(level, delay_time_millis);
    }

    (delay_level_table, max_delay_level)
}
