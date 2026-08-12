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

use super::send::DefaultResolver;
use super::send::DefaultServiceDetector;
use super::*;

#[allow(unused_must_use)]
#[allow(unused_assignments)]
impl DefaultMQProducerImpl {
    pub fn new(
        client_runtime: Arc<ClientRuntime>,
        client_config: ClientConfig,
        producer_config: ProducerConfig,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> Self {
        Self::new_with_options(
            client_runtime,
            ClientOptions::legacy(client_config),
            producer_config,
            rpc_hook,
        )
    }

    pub(crate) fn new_with_options(
        client_runtime: Arc<ClientRuntime>,
        options: ClientOptions,
        producer_config: ProducerConfig,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> Self {
        let service_context = client_runtime.component(format!("producer-{}", producer_config.producer_group()));
        let client_pool = client_runtime.pool().clone();
        let client_config = options.client_config().clone();
        let nameserver_discovery = options.nameserver_discovery().cloned();
        Self::new_with_runtime(
            Some(client_runtime),
            Some(client_pool),
            service_context,
            client_config,
            nameserver_discovery,
            producer_config,
            rpc_hook,
        )
    }

    pub(crate) fn new_internal(
        service_context: ChildServiceContext,
        client_config: ClientConfig,
        producer_config: ProducerConfig,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> Self {
        Self::new_with_runtime(
            None,
            None,
            service_context,
            client_config,
            None,
            producer_config,
            rpc_hook,
        )
    }

    pub(super) fn new_with_runtime(
        client_runtime: Option<Arc<ClientRuntime>>,
        client_pool: Option<ClientPool>,
        service_context: ChildServiceContext,
        client_config: ClientConfig,
        nameserver_discovery: Option<NameServerDiscoveryConfig>,
        producer_config: ProducerConfig,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> Self {
        let semaphore_async_send_num = Semaphore::new(
            producer_config
                .back_pressure_for_async_send_num()
                .max(MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM) as usize,
        );
        let semaphore_async_send_size = Semaphore::new(
            producer_config
                .back_pressure_for_async_send_size()
                .max(MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE) as usize,
        );
        let topic_publish_info_table = Arc::new(DashMap::new());
        let (state_changes, _) = watch::channel(ProducerState::Created);
        let runtime = ProducerRuntimeSnapshot::new(client_config.clone(), producer_config);
        let mut mq_fault_strategy = MQFaultStrategy::new(service_context.component("fault-detector"), &client_config);
        mq_fault_strategy.set_latency_max(runtime.producer_config.latency_max().to_vec());
        mq_fault_strategy.set_not_available_duration(runtime.producer_config.not_available_duration().to_vec());
        let request_future_holder = client_pool
            .as_ref()
            .map(ClientPool::request_future_holder)
            .unwrap_or_else(|| Arc::new(RequestFutureHolder::new(service_context.component("request-futures"))));
        DefaultMQProducerImpl {
            client_runtime,
            service_context,
            client_pool,
            client_pool_token: ParkingLotMutex::new(None),
            nameserver_discovery,
            request_future_holder,
            runtime: ArcSwap::from_pointee(runtime),
            config_update: ParkingLotMutex::new(()),
            state: AtomicU8::new(ProducerState::Created as u8),
            state_changes,
            lifecycle_transition: TokioMutex::new(()),
            service_state: ParkingLotRwLock::new(ServiceState::CreateJust),
            topic_publish_info_table,
            send_message_hook_list: ParkingLotRwLock::new(Arc::new([])),
            end_transaction_hook_list: ParkingLotRwLock::new(Arc::new([])),
            check_forbidden_hook_list: ParkingLotRwLock::new(Arc::new([])),
            pending_send_hooks: ParkingLotMutex::new(Some(Vec::new())),
            pending_end_transaction_hooks: ParkingLotMutex::new(Some(Vec::new())),
            pending_forbidden_hooks: ParkingLotMutex::new(Some(Vec::new())),
            rpc_hook: ParkingLotRwLock::new(rpc_hook),
            client_instance: OnceLock::new(),
            mq_fault_strategy: ParkingLotRwLock::new(mq_fault_strategy),
            semaphore_async_send_num: Arc::new(semaphore_async_send_num),
            semaphore_async_send_size: Arc::new(semaphore_async_send_size),
            default_mqproducer_impl_inner: OnceLock::new(),
            transaction_runtime: ParkingLotRwLock::new(TransactionRuntime::default()),
            producer_task_tracker: TaskTracker::new(),
            producer_task_shutdown: CancellationToken::new(),
            task_admission: ParkingLotMutex::new(()),
            oneway_egress: OnceLock::new(),
            compressor_missing_logged: AtomicBool::new(false),
        }
    }

    pub(crate) fn client_runtime(&self) -> Option<Arc<ClientRuntime>> {
        self.client_runtime.clone()
    }

    pub(crate) fn client_pool(&self) -> &ClientPool {
        self.client_pool
            .as_ref()
            .expect("public producer builders always bind a ClientRuntime")
    }

    pub(crate) fn bind_client_instance(
        &self,
        client_instance: &Arc<MQClientInstance>,
    ) -> rocketmq_error::RocketMQResult<()> {
        let candidate = Arc::downgrade(client_instance);
        if let Some(current) = self.client_instance.get() {
            if current.ptr_eq(&candidate) {
                return Ok(());
            }
            return Err(mq_client_err!("producer is already bound to another MQClientInstance"));
        }
        self.client_instance
            .set(candidate)
            .map_err(|_| mq_client_err!("MQClientInstance initialization raced"))
    }

    pub(super) async fn release_client_pool_lease(&self) {
        let token = self.client_pool_token.lock().take();
        if let (Some(pool), Some(token)) = (self.client_pool.as_ref(), token) {
            pool.release(token).await;
        }
    }

    #[inline]
    pub(crate) fn client_config_snapshot(&self) -> ClientConfig {
        self.runtime.load().client_config.clone()
    }

    #[inline]
    pub(crate) fn producer_config_snapshot(&self) -> Arc<ProducerConfig> {
        Arc::clone(&self.runtime.load().producer_config)
    }

    #[inline]
    pub(super) fn runtime_snapshot(&self) -> Arc<ProducerRuntimeSnapshot> {
        self.runtime.load_full()
    }

    #[inline]
    pub(super) fn store_runtime_config(&self, client_config: ClientConfig, producer_config: ProducerConfig) {
        self.runtime
            .store(Arc::new(ProducerRuntimeSnapshot::new(client_config, producer_config)));
    }

    pub(super) fn prepare_start_runtime(&self) -> Arc<ProducerRuntimeSnapshot> {
        let _update = self.config_update.lock();
        let current = self.runtime_snapshot();
        if current.producer_config.producer_group() == CLIENT_INNER_PRODUCER_GROUP {
            return current;
        }

        let mut client_config = current.client_config.clone();
        client_config.change_instance_name_to_pid();
        let next = Arc::new(ProducerRuntimeSnapshot::new(
            client_config,
            current.producer_config.as_ref().clone(),
        ));
        self.runtime.store(Arc::clone(&next));
        next
    }

    #[inline]
    pub(super) fn service_state(&self) -> ServiceState {
        *self.service_state.read()
    }

    #[inline]
    pub(super) fn set_service_state(&self, state: ServiceState) {
        *self.service_state.write() = state;
    }

    pub(crate) fn initialize_self_reference(
        &self,
        producer: &Arc<DefaultMQProducerImpl>,
    ) -> rocketmq_error::RocketMQResult<()> {
        if !std::ptr::eq(self, Arc::as_ref(producer)) {
            return Err(mq_client_err!(
                "DefaultMQProducerImpl self reference must use its owning root"
            ));
        }
        let candidate = Arc::downgrade(producer);
        if let Some(current) = self.default_mqproducer_impl_inner.get() {
            if current.ptr_eq(&candidate) {
                return Ok(());
            }
            return Err(mq_client_err!(
                "DefaultMQProducerImpl self reference is already initialized for another root"
            ));
        }
        self.default_mqproducer_impl_inner
            .set(candidate)
            .map_err(|_| mq_client_err!("DefaultMQProducerImpl self reference initialization raced"))
    }

    #[inline]
    pub(super) fn self_reference(&self) -> rocketmq_error::RocketMQResult<Arc<DefaultMQProducerImpl>> {
        self.default_mqproducer_impl_inner
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(|| {
                mq_client_err!(
                    "Failed to upgrade default_mqproducer_impl_inner: producer implementation is not available"
                )
            })
    }

    #[inline]
    pub(super) fn registry_owner(&self) -> rocketmq_error::RocketMQResult<MQProducerInnerImpl> {
        Ok(MQProducerInnerImpl::new(Arc::downgrade(&self.self_reference()?)))
    }

    pub(super) fn spawn_tracked_task<F>(&self, thread_name: &'static str, task: F) -> std::io::Result<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let _admission = self.task_admission.lock();
        if self.load_state(Ordering::Acquire) != ProducerState::Running {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "producer is not accepting background work",
            ));
        }
        spawn_producer_task(
            &self.service_context,
            thread_name,
            &self.producer_task_tracker,
            &self.producer_task_shutdown,
            task,
        )
    }

    pub(super) fn initialize_oneway_egress(
        &self,
        runtime: &ProducerRuntimeSnapshot,
    ) -> rocketmq_error::RocketMQResult<()> {
        if self.oneway_egress.get().is_some() {
            return Ok(());
        }
        let count_limit = runtime
            .producer_config
            .back_pressure_for_async_send_num()
            .max(MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM) as usize;
        let byte_limit = runtime
            .producer_config
            .back_pressure_for_async_send_size()
            .max(MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE) as usize;
        let egress = BoundedEgress::new(
            &self.service_context,
            runtime.producer_config.producer_group(),
            count_limit,
            byte_limit,
            &self.producer_task_tracker,
            &self.producer_task_shutdown,
            self.client_runtime
                .as_ref()
                .map(|runtime| runtime.client_metrics().clone())
                .unwrap_or_default(),
        )?;
        self.oneway_egress
            .set(egress)
            .map_err(|_| mq_client_err!("producer one-way egress initialization raced"))
    }

    pub(super) fn oneway_egress(&self) -> rocketmq_error::RocketMQResult<&BoundedEgress> {
        self.oneway_egress
            .get()
            .ok_or_else(|| mq_client_err!("producer one-way egress is not initialized"))
    }

    pub(crate) fn oneway_egress_snapshot(&self) -> OnewayEgressSnapshot {
        self.oneway_egress
            .get()
            .map(BoundedEgress::snapshot)
            .unwrap_or_default()
    }

    #[inline]
    pub(super) fn load_state(&self, ordering: Ordering) -> ProducerState {
        ProducerState::from_u8(self.state.load(ordering))
    }

    #[inline]
    pub(super) fn store_state(&self, state: ProducerState, ordering: Ordering) {
        self.state.store(state as u8, ordering);
        let _ = self.state_changes.send(state);
    }

    #[inline]
    pub(super) fn compare_exchange_state(
        &self,
        current: ProducerState,
        next: ProducerState,
        success: Ordering,
        failure: Ordering,
    ) -> Result<ProducerState, ProducerState> {
        match self.state.compare_exchange(current as u8, next as u8, success, failure) {
            Ok(previous) => {
                let _ = self.state_changes.send(next);
                Ok(ProducerState::from_u8(previous))
            }
            Err(previous) => Err(ProducerState::from_u8(previous)),
        }
    }

    pub(super) async fn wait_until_state_changes_from(&self, pending: ProducerState) {
        let mut state_changes = self.state_changes.subscribe();
        while self.load_state(Ordering::SeqCst) == pending {
            if state_changes.changed().await.is_err() {
                break;
            }
        }
    }

    #[inline]
    pub(crate) fn is_use_tls(&self) -> bool {
        self.runtime.load().client_config.is_use_tls()
    }

    #[inline]
    pub(crate) fn set_use_tls(&self, use_tls: bool) {
        let _update = self.config_update.lock();
        let current = self.runtime_snapshot();
        let mut client_config = current.client_config.clone();
        client_config.set_use_tls(use_tls);
        self.runtime.store(Arc::new(ProducerRuntimeSnapshot::new(
            client_config,
            current.producer_config.as_ref().clone(),
        )));
    }

    #[inline]
    pub(super) fn message_body_len_for_backpressure<T: MessageTrait>(msg: &T) -> usize {
        msg.get_body().map_or(1, |body| body.len())
    }

    pub(super) fn select_message_queue_with_user_message<M, S, T>(
        client_config: &ClientConfig,
        message_queue_list: &[MessageQueue],
        msg: &mut M,
        selector: &S,
        arg: &T,
    ) -> Option<MessageQueue>
    where
        M: MessageTrait,
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue>,
    {
        let original_topic = msg.topic().clone();
        let user_topic = NamespaceUtil::without_namespace_with_namespace(
            original_topic.as_str(),
            client_config.resolved_namespace().unwrap_or_default().as_str(),
        );
        msg.set_topic(CheetahString::from_string(user_topic));
        let selected = selector(message_queue_list, msg, arg).map(|mq| client_config.queue_with_resolved_namespace(mq));
        msg.set_topic(original_topic);
        selected
    }

    #[inline]
    pub(super) fn notify_callback_exception(send_callback: &Option<ArcSendCallback>, error: &RocketMQError) {
        if let Some(send_callback) = send_callback.as_ref() {
            send_callback.on_exception(error);
        } else {
            tracing::error!("Async send failed without callback: {}", error);
        }
    }

    #[inline]
    pub(super) fn async_send_rejected_error(message: impl Into<String>) -> RocketMQError {
        RocketMQError::illegal_argument(message)
    }

    #[inline]
    pub(super) fn request_cause_from_error(error: &RocketMQError) -> RocketMQError {
        RocketMQError::response_process_failed("request_response_callback", error.to_string())
    }

    #[inline]
    pub(super) fn remaining_async_timeout(timeout: u64, elapsed: u64) -> Option<u64> {
        timeout.checked_sub(elapsed).filter(|remaining| *remaining > 0)
    }

    #[inline]
    pub(super) fn remaining_request_timeout(timeout: u64, elapsed: u64) -> rocketmq_error::RocketMQResult<u64> {
        Self::remaining_async_timeout(timeout, elapsed).ok_or(rocketmq_error::RocketMQError::Timeout {
            operation: "send request message",
            timeout_ms: timeout,
        })
    }

    #[inline]
    pub(super) fn client_instance(&self) -> rocketmq_error::RocketMQResult<Arc<MQClientInstance>> {
        self.client_instance
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(|| mq_client_err!("MQClientInstance is not available; producer has not been started"))
    }

    pub fn get_mq_client_factory(&self) -> rocketmq_error::RocketMQResult<Arc<MQClientInstance>> {
        self.client_instance()
    }

    #[inline]
    pub(crate) fn client_id(&self) -> Option<CheetahString> {
        self.client_instance()
            .ok()
            .map(|client_instance| client_instance.client_id.clone())
    }

    #[inline]
    pub(crate) fn producer_config(&self) -> Arc<ProducerConfig> {
        self.producer_config_snapshot()
    }

    #[inline]
    pub(crate) fn fault_strategy_snapshot(&self) -> MQFaultStrategy {
        self.mq_fault_strategy.read().clone()
    }

    #[inline]
    pub(crate) fn enable_backpressure_for_async_mode(&self) -> bool {
        self.runtime.load().producer_config.enable_backpressure_for_async_mode()
    }

    #[inline]
    pub(crate) fn back_pressure_for_async_send_num(&self) -> u32 {
        self.runtime.load().producer_config.back_pressure_for_async_send_num()
    }

    #[inline]
    pub(crate) fn back_pressure_for_async_send_size(&self) -> u32 {
        self.runtime.load().producer_config.back_pressure_for_async_send_size()
    }

    pub fn set_enable_backpressure_for_async_mode(&self, enable_backpressure_for_async_mode: bool) {
        let _update = self.config_update.lock();
        let current = self.runtime_snapshot();
        let mut producer_config = current.producer_config.as_ref().clone();
        producer_config.set_enable_backpressure_for_async_mode(enable_backpressure_for_async_mode);
        self.store_runtime_config(current.client_config.clone(), producer_config);
    }

    pub fn replace_producer_config(&self, producer_config: ProducerConfig) {
        let _update = self.config_update.lock();
        let current = self.runtime_snapshot();
        let old_num_total = current
            .producer_config
            .back_pressure_for_async_send_num()
            .max(MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM) as usize;
        let old_size_total = current
            .producer_config
            .back_pressure_for_async_send_size()
            .max(MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE) as usize;
        let new_num_total = producer_config
            .back_pressure_for_async_send_num()
            .max(MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM) as usize;
        let new_size_total = producer_config
            .back_pressure_for_async_send_size()
            .max(MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE) as usize;

        {
            let mut strategy = self.mq_fault_strategy.write();
            strategy.set_latency_max(producer_config.latency_max().to_vec());
            strategy.set_not_available_duration(producer_config.not_available_duration().to_vec());
        }
        self.compressor_missing_logged.store(false, Ordering::Relaxed);
        self.store_runtime_config(current.client_config.clone(), producer_config);
        Self::resize_available_permits(&self.semaphore_async_send_num, old_num_total, new_num_total);
        Self::resize_available_permits(&self.semaphore_async_send_size, old_size_total, new_size_total);
    }

    pub fn set_back_pressure_for_async_send_num(&self, back_pressure_for_async_send_num: u32) {
        let _update = self.config_update.lock();
        let current = self.runtime_snapshot();
        let old_total = current
            .producer_config
            .back_pressure_for_async_send_num()
            .max(MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM) as usize;
        let new_total = back_pressure_for_async_send_num.max(MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM) as usize;
        let mut producer_config = current.producer_config.as_ref().clone();
        producer_config.set_back_pressure_for_async_send_num(back_pressure_for_async_send_num);
        self.store_runtime_config(current.client_config.clone(), producer_config);
        Self::resize_available_permits(&self.semaphore_async_send_num, old_total, new_total);
    }

    pub fn set_back_pressure_for_async_send_size(&self, back_pressure_for_async_send_size: u32) {
        let _update = self.config_update.lock();
        let current = self.runtime_snapshot();
        let old_total = current
            .producer_config
            .back_pressure_for_async_send_size()
            .max(MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE) as usize;
        let new_total = back_pressure_for_async_send_size.max(MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE) as usize;
        let mut producer_config = current.producer_config.as_ref().clone();
        producer_config.set_back_pressure_for_async_send_size(back_pressure_for_async_send_size);
        self.store_runtime_config(current.client_config.clone(), producer_config);
        Self::resize_available_permits(&self.semaphore_async_send_size, old_total, new_total);
    }

    pub fn semaphore_processor(&self) {}

    pub fn semaphore_async_adjust(
        &self,
        semaphore_async_num: i32,
        semaphore_async_size: i32,
    ) -> rocketmq_error::RocketMQResult<()> {
        let _update = self.config_update.lock();
        let current = self.runtime_snapshot();
        let current_num = current.producer_config.back_pressure_for_async_send_num() as i64;
        let current_size = current.producer_config.back_pressure_for_async_send_size() as i64;
        let new_num = current_num + semaphore_async_num as i64;
        let new_size = current_size + semaphore_async_size as i64;

        if new_num <= 0 || new_num > u32::MAX as i64 {
            return Err(rocketmq_error::RocketMQError::IllegalArgument(format!(
                "semaphoreAsyncNum adjustment out of range: current={}, delta={}",
                current_num, semaphore_async_num
            )));
        }
        if new_size <= 0 || new_size > u32::MAX as i64 {
            return Err(rocketmq_error::RocketMQError::IllegalArgument(format!(
                "semaphoreAsyncSize adjustment out of range: current={}, delta={}",
                current_size, semaphore_async_size
            )));
        }

        let mut producer_config = current.producer_config.as_ref().clone();
        producer_config.set_back_pressure_for_async_send_num(new_num as u32);
        producer_config.set_back_pressure_for_async_send_size(new_size as u32);
        self.store_runtime_config(current.client_config.clone(), producer_config);
        Self::resize_available_permits(
            &self.semaphore_async_send_num,
            current_num.max(MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM as i64) as usize,
            (new_num as u32).max(MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM) as usize,
        );
        Self::resize_available_permits(
            &self.semaphore_async_send_size,
            current_size.max(MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE as i64) as usize,
            (new_size as u32).max(MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE) as usize,
        );
        Ok(())
    }

    pub(super) fn resize_available_permits(semaphore: &Semaphore, old_total: usize, new_total: usize) {
        let available = semaphore.available_permits();
        let in_flight = old_total.saturating_sub(available);
        let target_available = new_total.saturating_sub(in_flight);

        match target_available.cmp(&available) {
            std::cmp::Ordering::Greater => semaphore.add_permits(target_available - available),
            std::cmp::Ordering::Less => {
                let _ = semaphore.forget_permits(available - target_available);
            }
            std::cmp::Ordering::Equal => {}
        }
    }
}

impl MQProducerInner for DefaultMQProducerImpl {
    fn get_publish_topic_list(&self) -> HashSet<CheetahString> {
        self.topic_publish_info_table
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    fn is_publish_topic_need_update(&self, topic: &CheetahString) -> bool {
        if let Some(topic_publish_info) = self.topic_publish_info_table.get(topic) {
            return !topic_publish_info.ok();
        }
        true
    }

    fn get_check_listener(&self) -> Option<ArcTransactionListener> {
        self.transaction_runtime.read().listener.clone()
    }

    fn check_transaction_state(
        &self,
        broker_addr: &CheetahString,
        msg: MessageExt,
        check_request_header: CheckTransactionStateRequestHeader,
    ) {
        let transaction_runtime = self.transaction_runtime.read().clone();
        let Some(transaction_listener) = transaction_runtime.listener else {
            warn!("TransactionListener is null, cannot check transaction state");
            return;
        };
        let Ok(producer_impl_inner) = self.self_reference() else {
            warn!("Failed to upgrade default_mqproducer_impl_inner: producer implementation is not available");
            return;
        };
        let broker_addr = broker_addr.clone();
        let group = self.runtime_snapshot().producer_config.producer_group().clone();

        let Some(transaction_check_env) = transaction_runtime.check_env else {
            warn!("Transaction check env is not initialized, cannot check transaction state");
            return;
        };
        let Ok(request_slot) = transaction_check_env.request_slots.clone().try_acquire_owned() else {
            warn!(
                "Transaction check request rejected: hold queue is full for producer group {}",
                group
            );
            return;
        };
        let worker_slots = transaction_check_env.worker_slots.clone();
        let service_context = self.service_context.clone();
        let group_for_spawn_error = group.clone();

        let task = async move {
            let _request_slot = request_slot;
            let Ok(_worker_slot) = worker_slots.acquire_owned().await else {
                tracing::warn!(
                    "Transaction check worker limiter was closed for producer group {}",
                    group
                );
                return;
            };

            // Route synchronous listener work through the client runtime's blocking lane.
            let check_group = group.clone();
            let check_task = move || {
                let unique_key = msg
                    .property(&CheetahString::from_static_str(
                        MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
                    ))
                    .unwrap_or_else(|| msg.msg_id.clone());

                // Check local transaction state with exception handling (synchronous execution)
                let transaction_state = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    transaction_listener.check_local_transaction(&msg)
                })) {
                    Ok(state) => state,
                    Err(e) => {
                        tracing::error!(
                            "Broker call checkTransactionState, but checkLocalTransaction panic: {:?}, group: {}",
                            e,
                            check_group
                        );
                        LocalTransactionState::Unknown
                    }
                };

                (msg, unique_key, transaction_state)
            };
            let check_result =
                spawn_client_blocking_io_with_context(&service_context, "client.transaction.check", check_task)
                    .await
                    .map_err(|error| error.to_string());

            let Ok((msg, unique_key, transaction_state)) = check_result else {
                tracing::error!("Transaction check task join failed for producer group {}", group);
                return;
            };

            let request_header = Self::build_end_transaction_header_for_check(
                producer_impl_inner
                    .runtime_snapshot()
                    .producer_config
                    .producer_group()
                    .clone(),
                &check_request_header,
                unique_key.clone(),
                transaction_state,
            );
            // Execute end transaction hook
            producer_impl_inner.do_execute_end_transaction_hook(
                &msg.message,
                &unique_key,
                &broker_addr,
                transaction_state,
                true,
            );

            // Send end transaction request with error handling
            let Ok(client_instance) = producer_impl_inner.client_instance() else {
                tracing::warn!("endTransactionOneway skipped: client instance is not available");
                return;
            };
            let Some(mq_client_api_impl) = client_instance.mq_client_api_impl.load_full() else {
                tracing::warn!("endTransactionOneway skipped: MQClientAPIImpl is not available");
                return;
            };
            if let Err(e) = mq_client_api_impl
                .end_transaction_oneway(&broker_addr, request_header, CheetahString::from_static_str(""), 3000)
                .await
            {
                tracing::error!("endTransactionOneway exception: {:?}", e);
            }
        };
        if let Err(error) = self.spawn_tracked_task("rocketmq-client-producer-transaction-check", task) {
            warn!(
                "Failed to spawn transaction check task for producer group {}: {}",
                group_for_spawn_error, error
            );
        }
    }

    fn update_topic_publish_info(&self, topic: impl Into<CheetahString>, info: Option<TopicPublishInfo>) {
        let topic = topic.into();
        if topic.is_empty() {
            return;
        }
        let Some(info) = info else {
            return;
        };
        self.topic_publish_info_table.insert(topic, Arc::new(info));
    }

    fn is_unit_mode(&self) -> bool {
        self.runtime_snapshot().send_config.unit_mode
    }
}

#[allow(unused_must_use)]
#[allow(unused_assignments)]
impl DefaultMQProducerImpl {
    pub async fn start(&self) -> rocketmq_error::RocketMQResult<()> {
        self.start_with_factory(true).await
    }

    #[inline]
    pub async fn start_with_factory(&self, start_factory: bool) -> rocketmq_error::RocketMQResult<()> {
        let _transition = self.lifecycle_transition.lock().await;
        if self.load_state(Ordering::SeqCst) == ProducerState::Starting {
            // A cancelled start future releases the transition mutex but leaves the
            // atomic state at Starting. Roll back its partial registration before retrying.
            self.cleanup_partial_start(false).await?;
        }

        match self.load_state(Ordering::SeqCst) {
            ProducerState::Running => return Ok(()),
            ProducerState::Stopped => {
                return Err(mq_client_err!("The producer service state is ShutdownAlready"));
            }
            ProducerState::StartFailed => {
                return Err(mq_client_err!(format!(
                    "The producer service state not OK, maybe started once, {:?} {}",
                    ProducerState::StartFailed,
                    FAQUrl::suggest_todo(FAQUrl::CLIENT_SERVICE_NOT_OK)
                )));
            }
            ProducerState::Stopping => {
                return Err(mq_client_err!("Cannot start producer while it is stopping"));
            }
            ProducerState::Created => {}
            ProducerState::Starting => unreachable!("partial start was cleaned under lifecycle lock"),
        }

        self.store_state(ProducerState::Starting, Ordering::SeqCst);
        self.set_service_state(ServiceState::StartFailed);
        self.freeze_hook_lists();
        let runtime = self.prepare_start_runtime();
        if let Err(error) = self.check_config(&runtime) {
            self.store_state(ProducerState::StartFailed, Ordering::SeqCst);
            return Err(error);
        }
        if let Err(error) = self.initialize_oneway_egress(&runtime) {
            self.store_state(ProducerState::StartFailed, Ordering::SeqCst);
            return Err(error);
        }

        let producer_config = Arc::clone(&runtime.producer_config);
        let client_instance = if let Ok(instance) = self.client_instance() {
            instance
        } else {
            let client_pool = self
                .client_pool
                .as_ref()
                .ok_or_else(|| mq_client_err!("internal producer must be pre-bound to its MQClientInstance"))?;
            let options = ClientOptions::from_parts(runtime.client_config.clone(), self.nameserver_discovery.clone());
            let pooled = client_pool.get_or_create_with_options(options, self.rpc_hook.read().clone())?;
            let (instance, token) = pooled.into_parts();
            *self.client_pool_token.lock() = Some(token);
            instance
        };
        let weak_client = Arc::downgrade(&client_instance);
        if let Err(error) = self.bind_client_instance(&client_instance) {
            self.store_state(ProducerState::StartFailed, Ordering::SeqCst);
            self.release_client_pool_lease().await;
            return Err(error);
        }

        let service_detector = DefaultServiceDetector {
            client_instance: weak_client.clone(),
            topic_publish_info_table: Arc::clone(&self.topic_publish_info_table),
        };
        let resolver = DefaultResolver {
            client_instance: weak_client,
        };
        {
            let strategy = self.mq_fault_strategy.read();
            strategy.set_resolve(resolver);
            strategy.set_service_detector(service_detector);
        }
        let producer = match self.self_reference() {
            Ok(producer) => producer,
            Err(error) => {
                self.store_state(ProducerState::StartFailed, Ordering::SeqCst);
                return Err(error);
            }
        };
        let registry_owner = MQProducerInnerImpl::new(Arc::downgrade(&producer));
        let register_ok = client_instance
            .register_producer(producer_config.producer_group(), registry_owner.clone())
            .await;
        if !register_ok {
            self.release_client_pool_lease().await;
            self.set_service_state(ServiceState::CreateJust);
            self.store_state(ProducerState::Created, Ordering::SeqCst);
            return Err(mq_client_err!(format!(
                "The producer group[{}] has been created before, specify another name please. {}",
                producer_config.producer_group(),
                FAQUrl::suggest_todo(FAQUrl::GROUP_NAME_DUPLICATE_URL)
            )));
        }
        if start_factory {
            if let Err(error) = Box::pin(client_instance.start()).await {
                client_instance
                    .unregister_producer_if_owner(producer_config.producer_group(), &registry_owner)
                    .await;
                self.release_client_pool_lease().await;
                self.store_state(ProducerState::StartFailed, Ordering::SeqCst);
                return Err(error);
            }
        }

        self.init_topic_route(&runtime).await;
        self.mq_fault_strategy.read().start_detector();
        self.complete_start_after(
            self.request_future_holder
                .start_scheduled_task(producer_config.producer_group().to_string()),
        )
        .await;
        tracing::info!("Producer [{}] started successfully", producer_config.producer_group());
        Ok(())
    }

    pub(super) async fn complete_start_after<F>(&self, initialization: F)
    where
        F: Future<Output = ()>,
    {
        initialization.await;
        self.set_service_state(ServiceState::Running);
        self.store_state(ProducerState::Running, Ordering::SeqCst);
    }

    /// Shutdown the producer gracefully
    pub async fn shutdown(&self) -> rocketmq_error::RocketMQResult<()> {
        self.shutdown_with_factory(true).await
    }

    pub(super) async fn shutdown_producer_tasks(&self) {
        if tokio::time::timeout(PRODUCER_TASK_SHUTDOWN_TIMEOUT, self.producer_task_tracker.wait())
            .await
            .is_ok()
        {
            return;
        }

        tracing::warn!(
            timeout_ms = PRODUCER_TASK_SHUTDOWN_TIMEOUT.as_millis(),
            "producer background send tasks did not stop before graceful timeout; cancelling"
        );
        self.producer_task_shutdown.cancel();

        if tokio::time::timeout(PRODUCER_TASK_FORCE_SHUTDOWN_TIMEOUT, self.producer_task_tracker.wait())
            .await
            .is_err()
        {
            tracing::warn!(
                timeout_ms = PRODUCER_TASK_FORCE_SHUTDOWN_TIMEOUT.as_millis(),
                "producer background send tasks did not stop after cancellation"
            );
        }
    }

    /// Shutdown the producer with option to shutdown factory
    pub async fn shutdown_with_factory(&self, shutdown_factory: bool) -> rocketmq_error::RocketMQResult<()> {
        let _transition = self.lifecycle_transition.lock().await;
        match self.load_state(Ordering::SeqCst) {
            ProducerState::Stopped => Ok(()),
            ProducerState::Created | ProducerState::StartFailed => {
                if let Some(egress) = self.oneway_egress.get() {
                    egress.close();
                }
                self.producer_task_tracker.close();
                self.shutdown_producer_tasks().await;
                Ok(())
            }
            ProducerState::Starting => {
                self.begin_task_shutdown(ProducerState::Starting)?;
                self.shutdown_producer_tasks().await;
                self.do_shutdown_internal(shutdown_factory).await?;
                self.finish_shutdown();
                Ok(())
            }
            ProducerState::Running => self.shutdown_running_locked(shutdown_factory).await,
            ProducerState::Stopping => {
                // A cancelled shutdown future releases the transition lock. Resume
                // the idempotent cleanup instead of waiting forever in Stopping.
                self.producer_task_tracker.close();
                self.shutdown_producer_tasks().await;
                self.do_shutdown_internal(shutdown_factory).await?;
                self.finish_shutdown();
                Ok(())
            }
        }
    }

    pub(super) fn begin_task_shutdown(&self, expected: ProducerState) -> rocketmq_error::RocketMQResult<()> {
        let _admission = self.task_admission.lock();
        self.compare_exchange_state(expected, ProducerState::Stopping, Ordering::SeqCst, Ordering::SeqCst)
            .map_err(|actual| mq_client_err!(format!("Cannot shutdown producer in state {:?}", actual)))?;
        if let Some(egress) = self.oneway_egress.get() {
            egress.close();
        }
        self.producer_task_tracker.close();
        Ok(())
    }

    pub(super) async fn shutdown_running_locked(&self, shutdown_factory: bool) -> rocketmq_error::RocketMQResult<()> {
        self.begin_task_shutdown(ProducerState::Running)?;
        self.shutdown_producer_tasks().await;
        self.do_shutdown_internal(shutdown_factory).await?;
        self.finish_shutdown();
        Ok(())
    }

    pub(super) fn finish_shutdown(&self) {
        self.set_service_state(ServiceState::ShutdownAlready);
        self.store_state(ProducerState::Stopped, Ordering::SeqCst);
        tracing::info!(
            "Producer [{}] shutdown OK",
            self.runtime_snapshot().producer_config.producer_group()
        );
    }

    /// Rolls back a producer whose owned start future failed or was cancelled.
    ///
    /// The caller must own the start future and ensure it is no longer being
    /// polled before invoking this method.
    pub(crate) async fn shutdown_after_partial_start_with_factory(
        &self,
        shutdown_factory: bool,
    ) -> rocketmq_error::RocketMQResult<()> {
        let _transition = self.lifecycle_transition.lock().await;
        match self.load_state(Ordering::SeqCst) {
            ProducerState::Created | ProducerState::Stopped => Ok(()),
            ProducerState::Running => self.shutdown_running_locked(shutdown_factory).await,
            state @ (ProducerState::Starting | ProducerState::StartFailed) => {
                self.begin_task_shutdown(state)?;
                self.shutdown_producer_tasks().await;
                if let Err(error) = self.do_shutdown_internal(shutdown_factory).await {
                    self.store_state(ProducerState::StartFailed, Ordering::SeqCst);
                    return Err(error);
                }
                self.finish_shutdown();
                Ok(())
            }
            ProducerState::Stopping => {
                self.producer_task_tracker.close();
                self.shutdown_producer_tasks().await;
                self.do_shutdown_internal(shutdown_factory).await?;
                self.finish_shutdown();
                Ok(())
            }
        }
    }

    pub(super) async fn cleanup_partial_start(&self, shutdown_factory: bool) -> rocketmq_error::RocketMQResult<()> {
        let runtime = self.runtime_snapshot();
        if let Ok(client_instance) = self.client_instance() {
            if let Ok(owner) = self.registry_owner() {
                client_instance
                    .unregister_producer_if_owner(runtime.producer_config.producer_group(), &owner)
                    .await;
            }
            if shutdown_factory {
                self.release_client_pool_lease().await;
            }
        }
        self.request_future_holder
            .shutdown(runtime.producer_config.producer_group())
            .await;
        let strategy = self.mq_fault_strategy.read().clone();
        let _ = strategy.shutdown_async().await;
        self.set_service_state(ServiceState::CreateJust);
        self.store_state(ProducerState::Created, Ordering::SeqCst);
        Ok(())
    }

    /// Internal shutdown logic
    pub(super) async fn do_shutdown_internal(&self, shutdown_factory: bool) -> rocketmq_error::RocketMQResult<()> {
        let runtime = self.runtime_snapshot();
        let producer_group = runtime.producer_config.producer_group().to_string();

        // 1. Unregister producer from client instance
        if let Ok(client_instance) = self.client_instance() {
            if let Ok(owner) = self.registry_owner() {
                client_instance
                    .unregister_producer_if_owner(runtime.producer_config.producer_group(), &owner)
                    .await;
            }
        }

        self.request_future_holder.shutdown(producer_group.as_str()).await;

        // 2. Stop fault strategy detector
        let strategy = self.mq_fault_strategy.read().clone();
        if !strategy.shutdown_async().await {
            tracing::warn!(
                "producer [{}] fault detector task did not stop before timeout; aborted",
                producer_group
            );
        }

        // 3. Shutdown client factory if requested
        if shutdown_factory {
            self.release_client_pool_lease().await;
        }

        Ok(())
    }

    pub fn register_end_transaction_hook(&self, hook: Arc<dyn EndTransactionHook>) {
        let mut pending = self.pending_end_transaction_hooks.lock();
        let current_state = ProducerState::from_u8(self.state.load(Ordering::Relaxed));
        if current_state != ProducerState::Created {
            tracing::warn!(
                "Cannot register hook after producer started (state: {:?})",
                current_state
            );
            return;
        }

        if let Some(pending) = pending.as_mut() {
            pending.push(hook);
            tracing::info!("Registered endTransaction Hook, pending hooks: {}", pending.len());
        }
    }

    pub fn register_check_forbidden_hook(&self, hook: Arc<dyn CheckForbiddenHook>) {
        let mut pending = self.pending_forbidden_hooks.lock();
        let current_state = ProducerState::from_u8(self.state.load(Ordering::Relaxed));
        if current_state != ProducerState::Created {
            tracing::warn!(
                "Cannot register hook after producer started (state: {:?})",
                current_state
            );
            return;
        }

        if let Some(pending) = pending.as_mut() {
            pending.push(hook);
            tracing::info!("Registered checkForbidden Hook, pending hooks: {}", pending.len());
        }
    }

    pub fn register_send_message_hook(&self, hook: Arc<dyn SendMessageHook>) {
        let mut pending = self.pending_send_hooks.lock();
        let current_state = ProducerState::from_u8(self.state.load(Ordering::Relaxed));
        if current_state != ProducerState::Created {
            tracing::warn!(
                "Cannot register hook after producer started (state: {:?})",
                current_state
            );
            return;
        }

        if let Some(pending) = pending.as_mut() {
            pending.push(hook);
            tracing::info!("Registered sendMessage Hook, pending hooks: {}", pending.len());
        }
    }

    pub fn set_rpc_hook(&self, rpc_hook: Arc<dyn RPCHook>) {
        let mut current_hook = self.rpc_hook.write();
        let current_state = ProducerState::from_u8(self.state.load(Ordering::Relaxed));
        if current_state != ProducerState::Created {
            tracing::warn!(
                "Cannot update RPC hook after producer started (state: {:?})",
                current_state
            );
            return;
        }

        *current_hook = Some(rpc_hook);
    }

    #[inline]
    pub(super) fn check_config(&self, runtime: &ProducerRuntimeSnapshot) -> rocketmq_error::RocketMQResult<()> {
        Validators::check_group(runtime.producer_config.producer_group())?;
        if runtime.producer_config.producer_group() == DEFAULT_PRODUCER_GROUP {
            return Err(mq_client_err!(format!(
                "The specified group name[{}] is equal to default group, please specify another one.",
                DEFAULT_PRODUCER_GROUP
            )));
        }
        Ok(())
    }

    pub(super) async fn init_topic_route(&self, runtime: &ProducerRuntimeSnapshot) {
        for topic in runtime.producer_config.topics() {
            let new_topic = NamespaceUtil::wrap_namespace(
                runtime.client_config.resolved_namespace().unwrap_or_default().as_str(),
                topic,
            );
            let topic_publish_info = self.try_to_find_topic_publish_info(&new_topic).await;
            if !topic_publish_info.as_ref().is_some_and(|info| info.ok()) {
                warn!(
                    "No route info of this topic: {} {}",
                    new_topic,
                    FAQUrl::suggest_todo(FAQUrl::NO_TOPIC_ROUTE_INFO)
                );
            }
        }
    }

    #[inline]
    pub fn set_send_latency_fault_enable(&self, send_latency_fault_enable: bool) {
        let _update = self.config_update.lock();
        let runtime = self.runtime_snapshot();
        let mut client_config = runtime.client_config.clone();
        client_config.set_send_latency_enable(send_latency_fault_enable);
        self.store_runtime_config(client_config, runtime.producer_config.as_ref().clone());
        self.mq_fault_strategy
            .read()
            .set_send_latency_fault_enable(send_latency_fault_enable);
    }

    #[inline]
    pub fn is_send_latency_fault_enable(&self) -> bool {
        self.mq_fault_strategy.read().is_send_latency_fault_enable()
    }

    #[inline]
    pub fn set_start_detector_enable(&self, start_detector_enable: bool) {
        let _update = self.config_update.lock();
        let runtime = self.runtime_snapshot();
        let mut client_config = runtime.client_config.clone();
        client_config.set_start_detector_enable(start_detector_enable);
        self.store_runtime_config(client_config, runtime.producer_config.as_ref().clone());
        self.mq_fault_strategy
            .read()
            .set_start_detector_enable(start_detector_enable);
    }

    #[inline]
    pub fn is_start_detector_enable(&self) -> bool {
        self.mq_fault_strategy.read().is_start_detector_enable()
    }

    #[inline]
    pub fn is_send_message_with_vip_channel(&self) -> bool {
        self.runtime_snapshot().client_config.is_vip_channel_enabled()
    }

    #[inline]
    pub fn set_send_message_with_vip_channel(&self, send_message_with_vip_channel: bool) {
        let _update = self.config_update.lock();
        let runtime = self.runtime_snapshot();
        let mut client_config = runtime.client_config.clone();
        client_config.set_vip_channel_enabled(send_message_with_vip_channel);
        self.store_runtime_config(client_config, runtime.producer_config.as_ref().clone());
    }

    #[inline]
    pub fn latency_max(&self) -> Vec<u64> {
        self.runtime_snapshot().producer_config.latency_max().to_vec()
    }

    #[inline]
    pub fn set_latency_max(&self, latency_max: impl Into<Vec<u64>>) {
        let latency_max = latency_max.into();
        let _update = self.config_update.lock();
        let runtime = self.runtime_snapshot();
        let mut producer_config = runtime.producer_config.as_ref().clone();
        producer_config.set_latency_max(latency_max.clone());
        self.mq_fault_strategy.write().set_latency_max(latency_max);
        self.store_runtime_config(runtime.client_config.clone(), producer_config);
    }

    #[inline]
    pub fn not_available_duration(&self) -> Vec<u64> {
        self.runtime_snapshot()
            .producer_config
            .not_available_duration()
            .to_vec()
    }

    #[inline]
    pub fn set_not_available_duration(&self, not_available_duration: impl Into<Vec<u64>>) {
        let not_available_duration = not_available_duration.into();
        let _update = self.config_update.lock();
        let runtime = self.runtime_snapshot();
        let mut producer_config = runtime.producer_config.as_ref().clone();
        producer_config.set_not_available_duration(not_available_duration.clone());
        self.mq_fault_strategy
            .write()
            .set_not_available_duration(not_available_duration);
        self.store_runtime_config(runtime.client_config.clone(), producer_config);
    }
}
