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
#[allow(unused_must_use)]
#[allow(unused_assignments)]
impl DefaultMQProducerImpl {
    pub async fn send_message_in_transaction<M>(
        &self,
        mut msg: M,
        arg: Option<Box<dyn Any + Send + Sync>>,
    ) -> rocketmq_error::RocketMQResult<TransactionSendResult>
    where
        M: MessageTrait + Send + Sync,
    {
        let runtime = self.runtime_snapshot();
        let transaction_listener = self
            .transaction_runtime
            .read()
            .listener
            .clone()
            .ok_or_else(|| mq_client_err!("tranExecutor is null"))?;

        // Ensure transactional messages do not support delayed delivery
        self.ensure_not_delayed_for_transactional(&msg)?;

        // ignore DelayTimeLevel parameter
        if msg.delay_time_level() != 0 {
            MessageAccessor::clear_property(&mut msg, MessageConst::PROPERTY_DELAY_TIME_LEVEL);
        }
        Validators::check_message(Some(&msg), runtime.producer_config.as_ref())?;
        MessageAccessor::put_property(
            &mut msg,
            CheetahString::from_static_str(MessageConst::PROPERTY_TRANSACTION_PREPARED),
            CheetahString::from_static_str("true"),
        );
        MessageAccessor::put_property(
            &mut msg,
            CheetahString::from_static_str(MessageConst::PROPERTY_PRODUCER_GROUP),
            runtime.producer_config.producer_group().to_owned(),
        );
        let send_result = self
            .send_default_impl_with_runtime(
                &mut msg,
                CommunicationMode::Sync,
                None,
                runtime.producer_config.send_msg_timeout() as u64,
                &runtime,
            )
            .await
            .map_err(|e| mq_client_err!(format!("send message in transaction error, {}", e)))?
            .ok_or_else(|| mq_client_err!("send result is none"))?;
        let (local_transaction_state, local_exception) = match send_result.send_status {
            SendStatus::SendOk => {
                if let Some(ref transaction_id) = send_result.transaction_id {
                    msg.put_user_property(
                        CheetahString::from_static_str(MessageConst::PROPERTY_TRANSACTION_ID),
                        CheetahString::from_string(transaction_id.to_owned()),
                    )
                    .map_err(|e| mq_client_err!(e.to_string()))?;
                }
                let transaction_id = msg.property(&CheetahString::from_static_str(
                    MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
                ));
                if let Some(transaction_id) = transaction_id {
                    msg.set_transaction_id(transaction_id);
                }
                Self::execute_local_transaction_branch(&transaction_listener, &msg, arg.as_deref())
            }
            SendStatus::FlushDiskTimeout | SendStatus::FlushSlaveTimeout | SendStatus::SlaveNotAvailable => {
                (LocalTransactionState::RollbackMessage, None)
            }
        };
        let transaction_topic = msg.topic().clone();
        let transaction_message = msg.as_any().downcast_ref::<Message>().cloned();
        if let Err(e) = self
            .end_transaction_owned(
                transaction_topic,
                transaction_message,
                &send_result,
                local_transaction_state,
                local_exception,
            )
            .await
        {
            warn!(
                "local transaction execute {}, but end broker transaction failed,{}",
                local_transaction_state,
                e.to_string()
            );
        }
        let transaction_send_result = TransactionSendResult {
            local_transaction_state: Some(local_transaction_state),
            send_result: Some(send_result),
        };
        Ok(transaction_send_result)
    }

    pub(super) fn execute_local_transaction_branch(
        transaction_listener: &ArcTransactionListener,
        msg: &dyn MessageTrait,
        arg: Option<&(dyn Any + Send + Sync)>,
    ) -> (LocalTransactionState, Option<CheetahString>) {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            transaction_listener.execute_local_transaction(msg, arg)
        })) {
            Ok(state) => (state, None),
            Err(error) => {
                let error_message = Self::panic_payload_to_string(error.as_ref());
                tracing::error!(
                    "executeLocalTransactionBranch panic, messageTopic: {} transactionId: {:?}: {}",
                    msg.topic(),
                    msg.transaction_id(),
                    error_message
                );
                (
                    LocalTransactionState::Unknown,
                    Some(CheetahString::from_string(format!(
                        "executeLocalTransactionBranch exception: {}",
                        error_message
                    ))),
                )
            }
        }
    }

    pub(super) fn panic_payload_to_string(error: &(dyn Any + Send)) -> String {
        if let Some(message) = error.downcast_ref::<&'static str>() {
            (*message).to_string()
        } else if let Some(message) = error.downcast_ref::<String>() {
            message.clone()
        } else {
            "non-string panic payload".to_string()
        }
    }

    pub(super) fn u64_to_java_long_field(
        operation: &'static str,
        field: &'static str,
        value: u64,
    ) -> rocketmq_error::RocketMQResult<i64> {
        i64::try_from(value).map_err(|_| {
            rocketmq_error::RocketMQError::IllegalArgument(format!("{operation} {field} exceeds Java long range"))
        })
    }

    pub async fn end_transaction(
        &self,
        msg: &dyn MessageTrait,
        send_result: &SendResult,
        local_transaction_state: LocalTransactionState,
        local_exception: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<()> {
        let topic = msg.topic().clone();
        let message = msg.as_any().downcast_ref::<Message>().cloned();
        self.end_transaction_owned(topic, message, send_result, local_transaction_state, local_exception)
            .await
    }

    pub(super) async fn end_transaction_owned(
        &self,
        topic: CheetahString,
        message: Option<Message>,
        send_result: &SendResult,
        local_transaction_state: LocalTransactionState,
        local_exception: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<()> {
        let id = if let Some(ref offset_msg_id) = send_result.offset_msg_id {
            MessageDecoder::decode_message_id(offset_msg_id).map_err(|e| {
                rocketmq_error::RocketMQError::IllegalArgument(format!("Failed to decode message ID: {}", e))
            })?
        } else {
            let msg_id = send_result
                .msg_id
                .as_ref()
                .ok_or_else(|| mq_client_err!("send result missing msg_id for end transaction"))?;
            MessageDecoder::decode_message_id(msg_id).map_err(|e| {
                rocketmq_error::RocketMQError::IllegalArgument(format!("Failed to decode message ID: {}", e))
            })?
        };
        let transaction_id = send_result.transaction_id.clone();
        let message_queue = send_result
            .message_queue
            .clone()
            .ok_or_else(|| mq_client_err!("send result missing message_queue for end transaction"))?;
        let runtime = self.runtime_snapshot();
        let queue = runtime.client_config.queue_with_resolved_namespace(message_queue);
        let client_instance = self.client_instance()?;
        let dest_broker_name = client_instance.get_broker_name_from_message_queue(&queue).await;
        let broker_addr = client_instance
            .find_broker_address_in_publish(dest_broker_name.as_ref())
            .ok_or_else(|| mq_client_err!(format!("broker address not found for {}", dest_broker_name)))?;
        let request_header = EndTransactionRequestHeader {
            topic,
            producer_group: runtime.producer_config.producer_group().clone(),
            tran_state_table_offset: Self::u64_to_java_long_field(
                "endTransaction",
                "tranStateTableOffset",
                send_result.queue_offset,
            )?,
            commit_log_offset: id.offset,
            commit_or_rollback: match local_transaction_state {
                LocalTransactionState::CommitMessage => MessageSysFlag::TRANSACTION_COMMIT_TYPE,
                LocalTransactionState::RollbackMessage => MessageSysFlag::TRANSACTION_ROLLBACK_TYPE,
                LocalTransactionState::Unknown => MessageSysFlag::TRANSACTION_NOT_TYPE,
            },
            from_transaction_check: false,
            msg_id: send_result.msg_id.clone().unwrap_or_default(),
            transaction_id: transaction_id.map(CheetahString::from_string),
            rpc_request_header: RpcRequestHeader {
                broker_name: Some(dest_broker_name),
                ..Default::default()
            },
        };
        if let Some(message) = message.as_ref() {
            self.do_execute_end_transaction_hook(
                message,
                &request_header.msg_id,
                &broker_addr,
                local_transaction_state,
                false,
            );
        }
        client_instance
            .mq_client_api_impl
            .load_full()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("MQClientAPIImpl"))?
            .end_transaction_oneway(
                &broker_addr,
                request_header,
                local_exception.unwrap_or_default(),
                runtime.producer_config.send_msg_timeout() as u64,
            )
            .await;
        Ok(())
    }

    pub fn do_execute_end_transaction_hook(
        &self,
        msg: &Message,
        msg_id: &CheetahString,
        broker_addr: &CheetahString,
        local_transaction_state: LocalTransactionState,
        from_transaction_check: bool,
    ) {
        if !self.has_end_transaction_hook() {
            return;
        }
        let end_transaction_context = EndTransactionContext {
            producer_group: self.runtime_snapshot().producer_config.producer_group().clone(),
            message: msg,
            msg_id: msg_id.clone(),
            transaction_id: msg.get_transaction_id().cloned().unwrap_or_default(),
            broker_addr: broker_addr.clone(),
            from_transaction_check,
            transaction_state: local_transaction_state,
        };
        self.execute_end_transaction_hook(&end_transaction_context);
    }

    pub fn execute_end_transaction_hook<'a>(&self, context: &'a EndTransactionContext<'a>) {
        let hooks = Arc::clone(&self.end_transaction_hook_list.read());
        for hook in hooks.iter() {
            hook.end_transaction(context);
        }
    }

    pub(super) fn build_end_transaction_header_for_check(
        producer_group: CheetahString,
        check_request_header: &CheckTransactionStateRequestHeader,
        msg_id: CheetahString,
        transaction_state: LocalTransactionState,
    ) -> EndTransactionRequestHeader {
        EndTransactionRequestHeader {
            topic: check_request_header.topic.clone().unwrap_or_default(),
            producer_group,
            tran_state_table_offset: check_request_header.tran_state_table_offset,
            commit_log_offset: check_request_header.commit_log_offset,
            commit_or_rollback: match transaction_state {
                LocalTransactionState::CommitMessage => MessageSysFlag::TRANSACTION_COMMIT_TYPE,
                LocalTransactionState::RollbackMessage => MessageSysFlag::TRANSACTION_ROLLBACK_TYPE,
                LocalTransactionState::Unknown => MessageSysFlag::TRANSACTION_NOT_TYPE,
            },
            from_transaction_check: true,
            msg_id,
            transaction_id: check_request_header.transaction_id.clone(),
            rpc_request_header: RpcRequestHeader {
                broker_name: check_request_header
                    .rpc_request_header
                    .clone()
                    .unwrap_or_default()
                    .broker_name,
                ..Default::default()
            },
        }
    }

    pub fn set_transaction_listener(&self, transaction_listener: ArcTransactionListener) {
        self.transaction_runtime.write().listener = Some(transaction_listener);
    }

    pub fn check_listener(&self) -> Option<ArcTransactionListener> {
        self.transaction_runtime.read().listener.clone()
    }
}

#[allow(unused_must_use)]
#[allow(unused_assignments)]
impl DefaultMQProducerImpl {
    /// Ensure transactional messages do not support delayed delivery
    pub(super) fn ensure_not_delayed_for_transactional<M>(&self, msg: &M) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait,
    {
        if msg
            .property(&CheetahString::from_static_str(MessageConst::PROPERTY_DELAY_TIME_LEVEL))
            .is_some()
            || msg
                .property(&CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DELAY_MS))
                .is_some()
            || msg
                .property(&CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DELAY_SEC))
                .is_some()
            || msg
                .property(&CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DELIVER_MS))
                .is_some()
        {
            return Err(mq_client_err!("Transactional messages do not support delayed delivery"));
        }
        Ok(())
    }

    pub fn init_transaction_env(
        &self,
        check_thread_pool_min_size: u32,
        check_thread_pool_max_size: u32,
        check_request_hold_max: u32,
    ) -> rocketmq_error::RocketMQResult<()> {
        if check_thread_pool_min_size == 0 || check_thread_pool_max_size == 0 {
            return Err(mq_client_err!(
                "transaction check thread pool min and max size must be greater than 0"
            ));
        }
        if check_thread_pool_min_size > check_thread_pool_max_size {
            return Err(mq_client_err!(
                "transaction check thread pool min size cannot exceed max size"
            ));
        }
        if check_request_hold_max == 0 {
            return Err(mq_client_err!(
                "transaction check request hold max must be greater than 0"
            ));
        }

        self.transaction_runtime.write().check_env = Some(TransactionCheckEnv {
            request_slots: Arc::new(Semaphore::new(check_request_hold_max as usize)),
            worker_slots: Arc::new(Semaphore::new(check_thread_pool_max_size as usize)),
        });
        Ok(())
    }

    pub async fn destroy_transaction_env(&self) {
        self.transaction_runtime.write().check_env = None;
    }

    #[cfg(test)]
    pub(super) fn is_transaction_env_initialized(&self) -> bool {
        self.transaction_runtime.read().check_env.is_some()
    }
}
