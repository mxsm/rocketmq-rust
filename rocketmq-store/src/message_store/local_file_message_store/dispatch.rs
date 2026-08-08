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

pub(super) type MessageArrivingListenerHandle = Arc<Box<dyn MessageArrivingListener + Sync + Send + 'static>>;

#[derive(Clone, Default)]
pub(super) struct MessageArrivalCapability {
    listener: Arc<parking_lot::RwLock<Option<MessageArrivingListenerHandle>>>,
}

impl MessageArrivalCapability {
    pub(super) fn replace(&self, listener: Option<MessageArrivingListenerHandle>) {
        *self.listener.write() = listener;
    }

    pub(super) fn snapshot(&self) -> Option<MessageArrivingListenerHandle> {
        self.listener.read().clone()
    }
}

#[derive(Clone)]
pub(super) struct ReputRuntimeContext {
    message_store_config: Arc<MessageStoreConfig>,
    store_runtime_state: Arc<StoreRuntimeState>,
    max_delay_level: i32,
    delay_level_table: Arc<BTreeMap<i32, i64>>,
    store_stats_service: Arc<StoreStatsService>,
    message_arrival: MessageArrivalCapability,
    long_polling_enable: bool,
}

impl ReputRuntimeContext {
    pub(super) fn notify_message_arrive_for_multi_queue(&self, dispatch_request: &mut DispatchRequest) {
        let Some(message_arriving_listener) = self.message_arrival.snapshot() else {
            return;
        };
        notify_message_arrive_for_multi_dispatch(
            self.message_store_config.as_ref(),
            message_arriving_listener.as_ref().as_ref(),
            dispatch_request,
        );
    }

    fn notify_message_arrive_if_necessary(&self, dispatch_request: &mut DispatchRequest) {
        if !self.long_polling_enable {
            return;
        }
        let Some(message_arriving_listener) = self.message_arrival.snapshot() else {
            return;
        };
        message_arriving_listener.arriving(
            dispatch_request.topic.as_ref(),
            dispatch_request.queue_id,
            dispatch_request.consume_queue_offset + 1,
            Some(dispatch_request.tags_code),
            dispatch_request.store_timestamp,
            dispatch_request.bit_map.clone(),
            dispatch_request.properties_map.as_ref(),
        );
        notify_message_arrive_for_multi_dispatch(
            self.message_store_config.as_ref(),
            message_arriving_listener.as_ref().as_ref(),
            dispatch_request,
        );
    }
}

impl LocalFileMessageStore {
    #[inline]
    pub fn get_topic_config(&self, topic: &CheetahString) -> Option<Arc<TopicConfig>> {
        if self.topic_config_table.is_empty() {
            return None;
        }
        self.topic_config_table.get(topic).as_deref().cloned()
    }

    pub(super) fn delete_topics_inner(&self, delete_topics: &[CheetahString]) -> i32 {
        if delete_topics.is_empty() {
            return 0;
        }

        let mut consume_queue_store = self.consume_queue_store.clone();
        let mut delete_count = 0;
        for topic in delete_topics {
            let removed = consume_queue_store.with_topic_closing(topic, |consume_queue_store| {
                let Some(queue_table) = consume_queue_store.find_consume_queue_map(topic) else {
                    return false;
                };
                let failures = consume_queue_store.retire_topic_queue_snapshot(topic, &queue_table);
                for queue_id in &failures {
                    warn!(
                        topic = %topic,
                        queue_id,
                        "DeleteTopic cleanup deferred or queue generation changed; retaining identity for retry"
                    );
                }
                if !failures.is_empty() {
                    return false;
                }
                if !consume_queue_store.remove_topic_if_empty(topic) {
                    warn!(
                        topic = %topic,
                        "DeleteTopic observed additional consume queues; retaining topic for retry"
                    );
                    return false;
                }

                if self.broker_config.auto_delete_unused_stats {
                    if let Some(broker_stats_manager) = self.broker_stats_manager.as_ref() {
                        broker_stats_manager.on_topic_deleted(topic);
                    }
                }

                let root_dir = self.message_store_config.store_path_root_dir.as_str();
                let consume_queue_dir = PathBuf::from(get_store_path_consume_queue(root_dir)).join(topic.as_str());
                let consume_queue_ext_dir =
                    PathBuf::from(get_store_path_consume_queue_ext(root_dir)).join(topic.as_str());
                let batch_consume_queue_dir =
                    PathBuf::from(get_store_path_batch_consume_queue(root_dir)).join(topic.as_str());

                util_all::delete_empty_directory(consume_queue_dir);
                util_all::delete_empty_directory(consume_queue_ext_dir);
                util_all::delete_empty_directory(batch_consume_queue_dir);
                true
            });
            if !removed {
                continue;
            }
            info!("DeleteTopic: Topic has been destroyed, topic={}", topic);
            delete_count += 1;
        }

        delete_count
    }

    pub(super) fn prepare_lmq_dispatch(&self, msg: &mut MessageExtBrokerInner) -> Vec<String> {
        if !self.message_store_config.enable_multi_dispatch {
            return Vec::new();
        }
        let Some(multi_dispatch_queue) = msg.property(MessageConst::PROPERTY_INNER_MULTI_DISPATCH) else {
            return Vec::new();
        };
        if multi_dispatch_queue.is_empty() {
            return Vec::new();
        }
        let (queue_keys, is_all_lmq_dispatch) =
            self.collect_lmq_dispatch_queue_keys_from_value(multi_dispatch_queue.as_str());
        if msg
            .property(MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET)
            .is_some_and(|queue_offset| !queue_offset.is_empty())
        {
            return queue_keys;
        }
        if !is_all_lmq_dispatch {
            return Vec::new();
        }

        let mut queue_offsets = String::new();
        for (index, queue_key) in queue_keys.iter().enumerate() {
            if index > 0 {
                queue_offsets.push_str(MULTI_DISPATCH_QUEUE_SPLITTER);
            }
            let _ = write!(
                &mut queue_offsets,
                "{}",
                self.consume_queue_store.get_lmq_queue_offset(queue_key.as_str())
            );
        }
        msg.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET),
            CheetahString::from_string(queue_offsets),
        );
        queue_keys
    }

    pub(super) fn collect_lmq_dispatch_queue_keys_from_value(&self, multi_dispatch_queue: &str) -> (Vec<String>, bool) {
        if !self.message_store_config.enable_lmq {
            return (Vec::new(), false);
        }

        let mut queue_keys = Vec::new();
        let mut saw_queue = false;
        let mut is_all_lmq_dispatch = true;
        for queue_name in multi_dispatch_queue.split(MULTI_DISPATCH_QUEUE_SPLITTER) {
            if queue_name.is_empty() {
                is_all_lmq_dispatch = false;
                continue;
            }
            saw_queue = true;
            if is_lmq(Some(queue_name)) {
                queue_keys.push(format!("{queue_name}-{LMQ_QUEUE_ID}"));
            } else {
                is_all_lmq_dispatch = false;
            }
        }
        (queue_keys, saw_queue && is_all_lmq_dispatch)
    }

    pub(super) fn update_lmq_offsets(&self, queue_keys: &[String], message_num: i16) {
        for queue_key in queue_keys {
            self.consume_queue_store
                .increase_lmq_offset(queue_key.as_str(), message_num);
        }
    }

    pub(super) fn get_lmq_dispatch_message_num(&self, msg: &MessageExtBrokerInner) -> i16 {
        msg.property(MessageConst::PROPERTY_INNER_NUM)
            .and_then(|message_num| message_num.parse::<i16>().ok())
            .unwrap_or(1)
    }

    pub fn on_commit_log_dispatch(
        &mut self,
        dispatch_request: &mut DispatchRequest,
        do_dispatch: bool,
        is_recover: bool,
        _is_file_end: bool,
    ) {
        if do_dispatch && !is_recover {
            self.do_dispatch(dispatch_request);
        }
    }

    pub fn do_dispatch(&mut self, dispatch_request: &mut DispatchRequest) {
        self.dispatcher.dispatch(dispatch_request)
    }

    /*    pub fn truncate_dirty_logic_files(&mut self, phy_offset: i64) {
        self.consume_queue_store.truncate_dirty(phy_offset);
    }*/

    pub fn consume_queue_store_mut(&mut self) -> &mut ConsumeQueueStore {
        &mut self.consume_queue_store
    }

    pub(crate) fn replace_topic_queue_table(&self, topic_queue_table: HashMap<CheetahString, i64>) {
        self.consume_queue_store.replace_topic_queue_table(topic_queue_table);
    }

    pub(super) fn delete_file(&mut self, file_name: String) {
        let _ = self.delete_file_with_outcome(file_name);
    }

    pub(super) fn delete_file_with_outcome(&mut self, file_name: String) -> bool {
        match fs::remove_file(PathBuf::from(file_name.as_str())) {
            Ok(_) => {
                info!("delete OK, file:{}", file_name);
                true
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                info!("delete skipped because file is already absent: {}", file_name);
                true
            }
            Err(err) => {
                error!("delete error, file:{}, {:?}", file_name, err);
                false
            }
        }
    }

    pub fn set_message_arriving_listener(
        &mut self,
        message_arriving_listener: Option<Arc<Box<dyn MessageArrivingListener + Sync + Send + 'static>>>,
    ) {
        self.message_arrival.replace(message_arriving_listener);
    }

    pub(super) fn reput_runtime_context(&self) -> ReputRuntimeContext {
        ReputRuntimeContext {
            message_store_config: self.message_store_config.clone(),
            store_runtime_state: Arc::clone(&self.store_runtime_state),
            max_delay_level: self.max_delay_level,
            delay_level_table: Arc::new(self.delay_level_table_ref().clone()),
            store_stats_service: self.store_stats_service.clone(),
            message_arrival: self.message_arrival.clone(),
            long_polling_enable: self.broker_config.long_polling_enable,
        }
    }

    pub(super) fn do_recheck_reput_offset_from_dispatchers(&self) {
        let Some(reput_from_offset) = self.reput_message_service.reput_from_offset.as_ref() else {
            return;
        };

        let commit_log_confirm_offset = self.commit_log.get_confirm_offset();
        let dispatch_recovery_offset = self.get_dispatch_recovery_offset();
        let target_reput_from_offset = dispatch_recovery_offset
            .min(commit_log_confirm_offset)
            .max(self.get_controller_epoch_start_offset().max(0));
        let previous_reput_from_offset = reput_from_offset.swap(target_reput_from_offset, Ordering::SeqCst);

        if previous_reput_from_offset != target_reput_from_offset {
            info!(
                "rechecked reputFromOffset from {} to {} using dispatch recovery offset {}",
                previous_reput_from_offset, target_reput_from_offset, dispatch_recovery_offset
            );
        }
    }

    pub async fn reput_once(&mut self) {
        if self.reput_message_service.reput_from_offset.is_none() {
            let start_offset = self.get_dispatch_recovery_offset().max(0);
            self.reput_message_service.set_reput_from_offset(start_offset);
        }
        if !self.root_dependencies_wired {
            return;
        }
        let runtime_context = self.reput_runtime_context();
        self.reput_message_service
            .run_once(
                self.commit_log.read_handle(),
                self.composition.reput(),
                self.dispatcher.handle(),
                self.notify_message_arrive_in_batch,
                runtime_context,
            )
            .await;
    }
}

pub struct CommitLogDispatcherDefault {
    pub(super) dispatcher_vec: Vec<Arc<dyn CommitLogDispatcher>>,
    published: Arc<ArcSwap<Vec<Arc<dyn CommitLogDispatcher>>>>,
}

impl Default for CommitLogDispatcherDefault {
    fn default() -> Self {
        Self::with_dispatchers(Vec::new())
    }
}

#[derive(Clone)]
pub(crate) struct CommitLogDispatchHandle {
    published: Arc<ArcSwap<Vec<Arc<dyn CommitLogDispatcher>>>>,
}

impl CommitLogDispatcherDefault {
    pub fn new() -> Self {
        Self::default()
    }

    pub(super) fn with_dispatchers(dispatcher_vec: Vec<Arc<dyn CommitLogDispatcher>>) -> Self {
        let published = Arc::new(ArcSwap::from_pointee(dispatcher_vec.clone()));
        Self {
            dispatcher_vec,
            published,
        }
    }

    #[inline]
    pub(crate) fn handle(&self) -> CommitLogDispatchHandle {
        CommitLogDispatchHandle {
            published: Arc::clone(&self.published),
        }
    }

    #[inline]
    pub(super) fn publish(&self) {
        self.published.store(Arc::new(self.dispatcher_vec.clone()));
    }

    #[inline]
    pub fn add_dispatcher(&mut self, dispatcher: Arc<dyn CommitLogDispatcher>) {
        self.dispatcher_vec.push(dispatcher);
        self.publish();
    }

    #[inline]
    pub fn add_first_dispatcher(&mut self, dispatcher: Arc<dyn CommitLogDispatcher>) {
        self.dispatcher_vec.insert(0, dispatcher);
        self.publish();
    }

    #[inline]
    pub fn min_dispatch_progress_offset(&self, commit_log_min_offset: i64) -> Option<i64> {
        self.dispatcher_vec
            .iter()
            .filter_map(|dispatcher| dispatcher.dispatch_progress_offset(commit_log_min_offset))
            .min()
    }
}

impl CommitLogDispatcher for CommitLogDispatchHandle {
    fn dispatch(&self, dispatch_request: &mut DispatchRequest) {
        let dispatchers = self.published.load();
        for dispatcher in dispatchers.iter() {
            dispatcher.dispatch(dispatch_request);
        }
    }

    fn dispatch_batch(&self, dispatch_requests: &mut [DispatchRequest]) {
        let dispatchers = self.published.load();
        for dispatcher in dispatchers.iter() {
            dispatcher.dispatch_batch(dispatch_requests);
        }
    }

    fn dispatch_async<'a>(
        &'a self,
        dispatch_request: &'a mut DispatchRequest,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        let dispatchers = self.published.load_full();
        Box::pin(async move {
            for dispatcher in dispatchers.iter() {
                dispatcher.dispatch_async(dispatch_request).await;
            }
        })
    }

    fn dispatch_batch_async<'a>(
        &'a self,
        dispatch_requests: &'a mut [DispatchRequest],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        let dispatchers = self.published.load_full();
        Box::pin(async move {
            for dispatcher in dispatchers.iter() {
                dispatcher.dispatch_batch_async(dispatch_requests).await;
            }
        })
    }
}

impl CommitLogDispatcher for CommitLogDispatcherDefault {
    fn dispatch(&self, dispatch_request: &mut DispatchRequest) {
        for dispatcher in self.dispatcher_vec.iter() {
            dispatcher.dispatch(dispatch_request);
        }
    }

    fn dispatch_batch(&self, dispatch_requests: &mut [DispatchRequest]) {
        for dispatcher in self.dispatcher_vec.iter() {
            dispatcher.dispatch_batch(dispatch_requests);
        }
    }

    fn dispatch_async<'a>(
        &'a self,
        dispatch_request: &'a mut DispatchRequest,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            for dispatcher in &self.dispatcher_vec {
                dispatcher.dispatch_async(dispatch_request).await;
            }
        })
    }

    fn dispatch_batch_async<'a>(
        &'a self,
        dispatch_requests: &'a mut [DispatchRequest],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            for dispatcher in &self.dispatcher_vec {
                dispatcher.dispatch_batch_async(dispatch_requests).await;
            }
        })
    }
}

#[derive(Clone)]
pub(super) struct ReputNotifyHandle {
    new_message_notify: Arc<Notify>,
    pending_messages: Arc<AtomicI64>,
}

impl ReputNotifyHandle {
    pub(super) fn notify_new_message(&self) {
        self.pending_messages.fetch_add(1, Ordering::Relaxed);
        self.new_message_notify.notify_one();
    }
}

pub(super) struct ReputMessageService {
    pub(super) shutdown_token: CancellationToken,
    pub(super) new_message_notify: Arc<Notify>,
    pub(super) dispatch_progress_notify: Arc<Notify>,
    pub(super) pending_messages: Arc<AtomicI64>,
    pub(super) inflight_dispatch_batches: Arc<AtomicU64>,
    pub(super) reput_from_offset: Option<Arc<AtomicI64>>,
    pub(super) dispatch_tx: Option<tokio::sync::mpsc::Sender<Vec<DispatchRequest>>>,
    pub(super) inner: Option<ReputMessageServiceInner>,
    pub(super) task_group: Option<rocketmq_runtime::TaskGroup>,
}

impl ReputMessageService {
    pub(super) fn notify_handle(&self) -> ReputNotifyHandle {
        ReputNotifyHandle {
            new_message_notify: Arc::clone(&self.new_message_notify),
            pending_messages: Arc::clone(&self.pending_messages),
        }
    }

    pub(super) fn notify_message_arrive4multi_queue(&self, dispatch_request: &mut DispatchRequest) {
        if let Some(inner) = self.inner.as_ref() {
            inner
                .runtime_context
                .notify_message_arrive_for_multi_queue(dispatch_request);
        }
    }

    pub fn set_reput_from_offset(&mut self, reput_from_offset: i64) {
        self.reput_from_offset = Some(Arc::new(AtomicI64::new(reput_from_offset)));
    }

    /// Notify that new messages have arrived and need to be reput
    pub fn notify_new_message(&self) {
        self.notify_handle().notify_new_message();
    }

    pub fn start(
        &mut self,
        runtime_scope: &StoreRuntimeScope,
        commit_log: CommitLogReadHandle,
        policy: ReputPolicy,
        dispatcher: CommitLogDispatchHandle,
        notify_message_arrive_in_batch: bool,
        runtime_context: ReputRuntimeContext,
    ) {
        if self.task_group.is_some() {
            return;
        }

        let task_group = crate::runtime::task_group(runtime_scope, "rocketmq-store.local-file.reput");

        // Create channel for decoupling read and dispatch
        let (dispatch_tx, mut dispatch_rx) = tokio::sync::mpsc::channel::<Vec<DispatchRequest>>(128);
        self.dispatch_tx = Some(dispatch_tx.clone());
        self.shutdown_token = CancellationToken::new();
        let reput_from_offset = self
            .reput_from_offset
            .get_or_insert_with(|| Arc::new(AtomicI64::new(0)))
            .clone();

        let mut inner = ReputMessageServiceInner {
            reput_from_offset,
            commit_log,
            policy,
            dispatcher: dispatcher.clone(),
            notify_message_arrive_in_batch,
            runtime_context: runtime_context.clone(),
        };
        self.inner = Some(inner.clone());

        let shutdown = self.shutdown_token.clone();
        let new_message_notify = self.new_message_notify.clone();
        let dispatch_progress_notify = self.dispatch_progress_notify.clone();
        let pending_messages = self.pending_messages.clone();
        let inflight_dispatch_batches = self.inflight_dispatch_batches.clone();

        // Task 1: Read messages from CommitLog and send to channel
        let shutdown_reader = shutdown.clone();
        if let Err(error) = task_group.spawn_service("reput-reader", async move {
            loop {
                tokio::select! {
                    _ = new_message_notify.notified() => {
                        // Process all available messages when notified
                        loop {
                            // Check if there are messages to process
                            if !inner.is_commit_log_available() {
                                if inner.has_unconfirmed_commit_log() {
                                    dispatch_progress_notify.notify_waiters();
                                    tokio::select! {
                                        _ = shutdown_reader.cancelled() => return,
                                        _ = tokio::time::sleep(Duration::from_millis(1)) => {}
                                    }
                                    continue;
                                }
                                break;
                            }

                            // Read and parse messages, send to dispatch channel
                            inflight_dispatch_batches.fetch_add(1, Ordering::AcqRel);
                            match inner.read_and_parse_batch().await {
                                Some(batch) => {
                                    // Successfully read a batch, try to send
                                    if dispatch_tx.send(batch).await.is_err() {
                                        inflight_dispatch_batches.fetch_sub(1, Ordering::AcqRel);
                                        error!("Failed to send dispatch batch to channel, channel closed");
                                        break;
                                    }

                                    // Decrement pending counter after successful send
                                    // Use saturating_sub to prevent underflow
                                    pending_messages
                                        .try_update(Ordering::Relaxed, Ordering::Relaxed, |x| {
                                            if x > 0 { Some(x - 1) } else { Some(0) }
                                        })
                                        .ok();
                                    dispatch_progress_notify.notify_waiters();
                                }
                                None => {
                                    inflight_dispatch_batches.fetch_sub(1, Ordering::AcqRel);
                                    // No more messages available at this offset
                                    dispatch_progress_notify.notify_waiters();
                                    if inner.has_unconfirmed_commit_log() {
                                        tokio::select! {
                                            _ = shutdown_reader.cancelled() => return,
                                            _ = tokio::time::sleep(Duration::from_millis(1)) => {}
                                        }
                                        continue;
                                    }
                                    break;
                                }
                            }

                            // Check if there are still pending messages
                            // If no pending and no available, exit loop
                            if pending_messages.load(Ordering::Relaxed) == 0
                                && !inner.is_commit_log_available() {
                                break;
                            }
                        }
                    }
                    _ = shutdown_reader.cancelled() => {
                        break;
                    }
                }
            }
        }) {
            self.shutdown_token.cancel();
            self.dispatch_tx.take();
            self.inner.take();
            error!("failed to spawn ReputMessageService reader: {error}");
            return;
        }

        // Task 2: Receive from channel and dispatch
        let shutdown_dispatcher = shutdown;
        let dispatcher_progress_notify = self.dispatch_progress_notify.clone();
        let dispatcher_inflight_batches = self.inflight_dispatch_batches.clone();
        if let Err(error) = task_group.spawn_service("reput-dispatcher", async move {
            loop {
                tokio::select! {
                    Some(mut batch) = dispatch_rx.recv() => {
                        dispatch_reput_batch(
                            &dispatcher,
                            &runtime_context,
                            notify_message_arrive_in_batch,
                            &mut batch,
                        )
                        .await;
                        dispatcher_inflight_batches.fetch_sub(1, Ordering::AcqRel);
                        dispatcher_progress_notify.notify_waiters();
                    }
                    _ = shutdown_dispatcher.cancelled() => {
                        // Process remaining messages in channel before shutdown
                        while let Ok(mut batch) = dispatch_rx.try_recv() {
                            dispatch_reput_batch(
                                &dispatcher,
                                &runtime_context,
                                notify_message_arrive_in_batch,
                                &mut batch,
                            )
                            .await;
                            dispatcher_inflight_batches.fetch_sub(1, Ordering::AcqRel);
                            dispatcher_progress_notify.notify_waiters();
                        }
                        break;
                    }
                }
            }
        }) {
            self.shutdown_token.cancel();
            self.dispatch_tx.take();
            task_group.cancel();
            error!("failed to spawn ReputMessageService dispatcher: {error}");
            self.task_group = Some(task_group);
            return;
        }

        self.task_group = Some(task_group);
        self.new_message_notify.notify_one();
    }

    pub(super) async fn wait_until_commit_log_dispatched(
        &self,
        inner: &ReputMessageServiceInner,
        timeout: Duration,
    ) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let progress = self.dispatch_progress_notify.notified();
            if !inner.is_commit_log_available() {
                return true;
            }
            if tokio::time::timeout_at(deadline, progress).await.is_err() {
                return !inner.is_commit_log_available();
            }
        }
    }

    pub(super) async fn wait_until_release_checkpoint_drained(&self, deadline: ShutdownDeadline) -> bool {
        let Some(inner) = self.inner.as_ref() else {
            return self.inflight_dispatch_batches.load(Ordering::Acquire) == 0;
        };
        let deadline = tokio::time::Instant::from_std(deadline.instant());
        loop {
            let progress = self.dispatch_progress_notify.notified();
            tokio::pin!(progress);
            progress.as_mut().enable();
            if !inner.is_commit_log_available() && self.inflight_dispatch_batches.load(Ordering::Acquire) == 0 {
                return true;
            }
            if tokio::time::timeout_at(deadline, progress).await.is_err() {
                return !inner.is_commit_log_available() && self.inflight_dispatch_batches.load(Ordering::Acquire) == 0;
            }
        }
    }

    pub async fn run_once(
        &mut self,
        commit_log: CommitLogReadHandle,
        policy: ReputPolicy,
        dispatcher: CommitLogDispatchHandle,
        notify_message_arrive_in_batch: bool,
        runtime_context: ReputRuntimeContext,
    ) {
        if self.task_group.is_some() {
            self.new_message_notify.notify_one();
            let deadline = ShutdownDeadline::after(Duration::from_secs(5));
            if !self.wait_until_release_checkpoint_drained(deadline).await {
                warn!("manual reput did not drain every background dispatch within five seconds");
            }
            return;
        }
        if self.reput_from_offset.is_none() {
            self.reput_from_offset = Some(Arc::new(AtomicI64::new(0)));
        }
        if self.inner.is_none() {
            let reput_from_offset = self
                .reput_from_offset
                .get_or_insert_with(|| Arc::new(AtomicI64::new(0)))
                .clone();
            self.inner = Some(ReputMessageServiceInner {
                reput_from_offset,
                commit_log,
                policy,
                dispatcher,
                notify_message_arrive_in_batch,
                runtime_context,
            });
        }
        if let Some(inner) = self.inner.as_mut() {
            inner.do_reput().await;
        }
        let deadline = ShutdownDeadline::after(Duration::from_secs(5));
        if !self.wait_until_release_checkpoint_drained(deadline).await {
            warn!("manual reput did not drain every derived-state dispatch within five seconds");
        }
    }

    pub async fn shutdown(&mut self) {
        // Step 1: Wait for pending messages to be dispatched (max 5 seconds)
        if let Some(inner) = self.inner.as_ref() {
            self.wait_until_commit_log_dispatched(inner, Duration::from_secs(5))
                .await;

            // Warn if there are still undispatched messages
            if inner.is_commit_log_available() {
                warn!(
                    "shutdown ReputMessageService, but CommitLog have not finish to be dispatched, CommitLog max \
                     offset={}, reputFromOffset={}",
                    inner.commit_log.get_max_offset(),
                    inner.reput_from_offset.load(Ordering::Relaxed)
                );
            }
        }

        // Step 2: Notify tasks to shutdown
        self.shutdown_token.cancel();
        self.dispatch_tx.take();

        // Step 3: Wait for tasks to complete with timeout (3 seconds)
        if let Some(task_group) = self.task_group.take() {
            let report = task_group.shutdown(Duration::from_secs(3)).await;
            match crate::runtime::shutdown_report_result("ReputMessageService", report) {
                Ok(()) => info!("ReputMessageService tasks shut down successfully"),
                Err(error) => warn!("ReputMessageService task shutdown reported an error: {error}"),
            }
        }
        self.inner.take();

        info!("ReputMessageService shutdown complete");
    }

    #[cfg(test)]
    pub(super) fn has_task_group(&self) -> bool {
        self.task_group.is_some()
    }

    #[inline]
    pub fn behind(&self) -> i64 {
        let Some(inner) = self.inner.as_ref() else {
            return 0;
        };
        inner.commit_log.get_confirm_offset() - inner.reput_from_offset.load(Ordering::Relaxed)
    }
}

//Construct a consumer queue and index file.
#[derive(Clone)]
pub(super) struct ReputMessageServiceInner {
    pub(super) reput_from_offset: Arc<AtomicI64>,
    pub(super) commit_log: CommitLogReadHandle,
    pub(super) policy: ReputPolicy,
    pub(super) dispatcher: CommitLogDispatchHandle,
    pub(super) notify_message_arrive_in_batch: bool,
    pub(super) runtime_context: ReputRuntimeContext,
}

async fn dispatch_reput_batch(
    dispatcher: &CommitLogDispatchHandle,
    runtime_context: &ReputRuntimeContext,
    notify_message_arrive_in_batch: bool,
    dispatch_batch: &mut [DispatchRequest],
) {
    let batch_size = dispatch_batch.len();
    let started = Instant::now();
    dispatcher.dispatch_batch_async(dispatch_batch).await;
    runtime_context
        .store_stats_service
        .record_reput_dispatch_batch(batch_size, started.elapsed());

    if !notify_message_arrive_in_batch {
        for req in dispatch_batch.iter_mut() {
            runtime_context.notify_message_arrive_if_necessary(req);
        }
    }
}

impl ReputMessageServiceInner {
    pub async fn do_reput(&mut self) {
        let reput_from_offset = self.reput_from_offset.load(Ordering::Relaxed);
        if reput_from_offset < self.commit_log.get_min_offset() {
            warn!(
                "The reputFromOffset={} is smaller than minPyOffset={}, this usually indicate that the dispatch \
                 behind too much and the commitlog has expired.",
                reput_from_offset,
                self.commit_log.get_min_offset()
            );
            self.reput_from_offset
                .store(self.commit_log.get_min_offset(), Ordering::Release);
        }
        let mut do_next = true;
        let mut dispatch_batch: Vec<DispatchRequest> = Vec::with_capacity(64);

        while do_next && self.is_commit_log_available() {
            let Some(mut result) = self.commit_log.get_data(self.reput_from_offset.load(Ordering::Acquire)) else {
                break;
            };
            self.reput_from_offset
                .store(result.start_offset as i64, Ordering::Release);
            let mut read_size = 0i32;
            while read_size < result.size
                && self.reput_from_offset.load(Ordering::Acquire) < self.get_reput_end_offset()
                && do_next
            {
                let Some(bytes) = result.bytes.as_mut() else {
                    warn!("commitlog data is missing bytes during reput dispatch");
                    break;
                };
                let dispatch_request = commit_log::check_message_and_return_size(
                    bytes,
                    false,
                    false,
                    false,
                    &self.runtime_context.message_store_config,
                    self.runtime_context.max_delay_level,
                    self.runtime_context.delay_level_table.as_ref(),
                );
                let size = if dispatch_request.buffer_size == -1 {
                    dispatch_request.msg_size
                } else {
                    dispatch_request.buffer_size
                };
                if self.reput_from_offset.load(Ordering::Acquire) + size as i64 > self.get_reput_end_offset() {
                    do_next = false;
                    break;
                }
                if dispatch_request.success {
                    match dispatch_request.msg_size.cmp(&0) {
                        std::cmp::Ordering::Greater => {
                            // Update stats before moving dispatch_request
                            if !self.runtime_context.message_store_config.duplication_enable
                                && self.runtime_context.store_runtime_state.broker_role() == BrokerRole::Slave
                            {
                                self.runtime_context
                                    .store_stats_service
                                    .add_single_put_message_topic_times_total(
                                        dispatch_request.topic.as_str(),
                                        dispatch_request.batch_size as usize,
                                    );
                                self.runtime_context
                                    .store_stats_service
                                    .add_single_put_message_topic_size_total(
                                        dispatch_request.topic.as_str(),
                                        dispatch_request.msg_size as usize,
                                    );
                            }

                            // Batch dispatch: accumulate requests (no clone needed)
                            dispatch_batch.push(dispatch_request);

                            // Dispatch batch when reaching threshold or at end
                            if dispatch_batch.len() >= 32 {
                                dispatch_reput_batch(
                                    &self.dispatcher,
                                    &self.runtime_context,
                                    self.notify_message_arrive_in_batch,
                                    &mut dispatch_batch,
                                )
                                .await;
                                dispatch_batch.clear();
                            }

                            self.reput_from_offset.fetch_add(size as i64, Ordering::AcqRel);
                            read_size += size;
                        }
                        std::cmp::Ordering::Equal => {
                            self.reput_from_offset.store(
                                self.commit_log
                                    .roll_next_file(self.reput_from_offset.load(Ordering::Relaxed)),
                                Ordering::SeqCst,
                            );
                            read_size = result.size;
                        }
                        std::cmp::Ordering::Less => {}
                    }
                } else if size > 0 {
                    error!(
                        "[BUG]read total count not equals msg total size. reputFromOffset={}",
                        self.reput_from_offset.load(Ordering::Relaxed)
                    );
                    self.reput_from_offset.fetch_add(size as i64, Ordering::SeqCst);
                } else {
                    do_next = false;
                    if LocalFileMessageStore::is_dledger_commit_log_enabled_config(
                        self.runtime_context.message_store_config.as_ref(),
                    ) {
                        warn!("reput reached an unsupported DLedger branch; stopping batch dispatch for this tick");
                    }
                }
            }
        }

        // Dispatch remaining messages in batch
        if !dispatch_batch.is_empty() {
            dispatch_reput_batch(
                &self.dispatcher,
                &self.runtime_context,
                self.notify_message_arrive_in_batch,
                &mut dispatch_batch,
            )
            .await;
        }
        self.record_dispatch_behind_bytes();
    }

    pub(super) fn is_commit_log_available(&self) -> bool {
        self.policy.is_available(
            self.reput_from_offset.load(Ordering::Relaxed),
            self.commit_log.get_confirm_offset(),
            self.commit_log.get_max_offset(),
        )
    }

    pub(super) fn has_unconfirmed_commit_log(&self) -> bool {
        self.policy.has_unconfirmed(
            self.reput_from_offset.load(Ordering::Relaxed),
            self.commit_log.get_confirm_offset(),
            self.commit_log.get_max_offset(),
        )
    }

    pub(super) fn get_reput_end_offset(&self) -> i64 {
        self.policy
            .end_offset(self.commit_log.get_confirm_offset(), self.commit_log.get_max_offset())
    }

    pub(super) fn record_dispatch_behind_bytes(&self) {
        let behind = self.policy.behind_bytes(
            self.reput_from_offset.load(Ordering::Relaxed),
            self.commit_log.get_confirm_offset(),
            self.commit_log.get_max_offset(),
        );
        self.runtime_context
            .store_stats_service
            .set_reput_dispatch_behind_bytes(behind);
    }

    pub fn reput_from_offset(&self) -> i64 {
        self.reput_from_offset.load(Ordering::Relaxed)
    }

    pub fn set_reput_from_offset(&mut self, reput_from_offset: i64) {
        self.reput_from_offset.store(reput_from_offset, Ordering::SeqCst);
    }

    /// Read and parse a batch of messages from CommitLog (for channel-based dispatch)
    pub async fn read_and_parse_batch(&mut self) -> Option<Vec<DispatchRequest>> {
        let reput_from_offset = self.reput_from_offset.load(Ordering::Relaxed);
        if reput_from_offset < self.commit_log.get_min_offset() {
            warn!(
                "The reputFromOffset={} is smaller than minPyOffset={}, this usually indicate that the dispatch \
                 behind too much and the commitlog has expired.",
                reput_from_offset,
                self.commit_log.get_min_offset()
            );
            self.reput_from_offset
                .store(self.commit_log.get_min_offset(), Ordering::Release);
        }

        if !self.is_commit_log_available() {
            self.record_dispatch_behind_bytes();
            return None;
        }

        let mut dispatch_batch: Vec<DispatchRequest> = Vec::with_capacity(64);

        let mut result = self
            .commit_log
            .get_data(self.reput_from_offset.load(Ordering::Acquire))?;
        self.reput_from_offset
            .store(result.start_offset as i64, Ordering::Release);
        let mut read_size = 0i32;

        while read_size < result.size
            && self.reput_from_offset.load(Ordering::Acquire) < self.get_reput_end_offset()
            && dispatch_batch.len() < 64
        {
            let Some(bytes) = result.bytes.as_mut() else {
                warn!("commitlog data is missing bytes during batch reput dispatch");
                break;
            };
            let dispatch_request = commit_log::check_message_and_return_size(
                bytes,
                false,
                false,
                false,
                &self.runtime_context.message_store_config,
                self.runtime_context.max_delay_level,
                self.runtime_context.delay_level_table.as_ref(),
            );
            let size = if dispatch_request.buffer_size == -1 {
                dispatch_request.msg_size
            } else {
                dispatch_request.buffer_size
            };

            if self.reput_from_offset.load(Ordering::Acquire) + size as i64 > self.get_reput_end_offset() {
                break;
            }

            if dispatch_request.success {
                match dispatch_request.msg_size.cmp(&0) {
                    std::cmp::Ordering::Greater => {
                        // Update stats before moving dispatch_request
                        if !self.runtime_context.message_store_config.duplication_enable
                            && self.runtime_context.store_runtime_state.broker_role() == BrokerRole::Slave
                        {
                            self.runtime_context
                                .store_stats_service
                                .add_single_put_message_topic_times_total(
                                    dispatch_request.topic.as_str(),
                                    dispatch_request.batch_size as usize,
                                );
                            self.runtime_context
                                .store_stats_service
                                .add_single_put_message_topic_size_total(
                                    dispatch_request.topic.as_str(),
                                    dispatch_request.msg_size as usize,
                                );
                        }

                        // Move dispatch_request into batch (no clone needed)
                        dispatch_batch.push(dispatch_request);
                        self.reput_from_offset.fetch_add(size as i64, Ordering::AcqRel);
                        read_size += size;
                    }
                    std::cmp::Ordering::Equal => {
                        self.reput_from_offset.store(
                            self.commit_log
                                .roll_next_file(self.reput_from_offset.load(Ordering::Relaxed)),
                            Ordering::SeqCst,
                        );
                        read_size = result.size;
                    }
                    std::cmp::Ordering::Less => {}
                }
            } else if size > 0 {
                error!(
                    "[BUG]read total count not equals msg total size. reputFromOffset={}",
                    self.reput_from_offset.load(Ordering::Relaxed)
                );
                self.reput_from_offset.fetch_add(size as i64, Ordering::SeqCst);
            } else {
                if LocalFileMessageStore::is_dledger_commit_log_enabled_config(
                    self.runtime_context.message_store_config.as_ref(),
                ) {
                    warn!(
                        "read_and_parse_batch reached an unsupported DLedger branch; stopping batch dispatch for this \
                         tick"
                    );
                }
                break;
            }
        }

        self.record_dispatch_behind_bytes();

        if dispatch_batch.is_empty() {
            None
        } else {
            Some(dispatch_batch)
        }
    }
}
