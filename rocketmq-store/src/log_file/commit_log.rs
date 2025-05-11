/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::mem;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use bytes::Buf;
use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use rocketmq_common::common::attribute::cq_type::CQType;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::broker::broker_role::BrokerRole;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::message::message_batch::MessageExtBatch;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::message_single::tags_string2tags_code;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageVersion;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_common::common::system_clock::SystemClock;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_common::utils::queue_type_utils::QueueTypeUtils;
use rocketmq_common::utils::time_utils;
use rocketmq_common::CRC32Utils::crc32;
use rocketmq_common::MessageDecoder;
use rocketmq_common::MessageDecoder::string_to_message_properties;
use rocketmq_common::MessageDecoder::MESSAGE_MAGIC_CODE_POSITION;
use rocketmq_common::MessageDecoder::MESSAGE_MAGIC_CODE_V2;
use rocketmq_common::MessageDecoder::SYSFLAG_POSITION;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_common::UtilAll::time_millis_to_human_string;
use rocketmq_rust::ArcMut;
use tokio::time::Instant;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::append_message_callback::DefaultAppendMessageCallback;
use crate::base::commit_log_dispatcher::CommitLogDispatcher;
use crate::base::dispatch_request::DispatchRequest;
use crate::base::flush_manager::FlushManager;
use crate::base::message_result::AppendMessageResult;
use crate::base::message_result::PutMessageResult;
use crate::base::message_status_enum::AppendMessageStatus;
use crate::base::message_status_enum::PutMessageStatus;
use crate::base::put_message_context::PutMessageContext;
use crate::base::select_result::SelectMappedBufferResult;
use crate::base::store_checkpoint::StoreCheckpoint;
use crate::base::swappable::Swappable;
use crate::base::topic_queue_lock::TopicQueueLock;
use crate::config::message_store_config::MessageStoreConfig;
use crate::consume_queue::mapped_file_queue::MappedFileQueue;
use crate::log_file::cold_data_check_service::ColdDataCheckService;
use crate::log_file::flush_manager_impl::defalut_flush_manager::DefaultFlushManager;
use crate::log_file::mapped_file::default_mapped_file_impl::DefaultMappedFile;
use crate::log_file::mapped_file::MappedFile;
use crate::message_encoder::message_ext_encoder::MessageExtEncoder;
use crate::message_store::local_file_message_store::CommitLogDispatcherDefault;
use crate::message_store::local_file_message_store::LocalFileMessageStore;
use crate::queue::consume_queue_store::ConsumeQueueStoreTrait;
use crate::queue::local_file_consume_queue_store::ConsumeQueueStore;
use crate::store_error::StoreError;

// Message's MAGIC CODE daa320a7
pub const MESSAGE_MAGIC_CODE: i32 = -626843481;

// End of file empty MAGIC CODE cbd43194
pub const BLANK_MAGIC_CODE: i32 = -875286124;

//CRC32 Format: [PROPERTY_CRC32 + NAME_VALUE_SEPARATOR + 10-digit fixed-length string +
// PROPERTY_SEPARATOR]
pub const CRC32_RESERVED_LEN: i32 = (MessageConst::PROPERTY_CRC32.len() + 1 + 10 + 1) as i32;

struct PutMessageThreadLocal {
    encoder: RefCell<Option<MessageExtEncoder>>,
    key: RefCell<String>,
}

thread_local! {
     static PUT_MESSAGE_THREAD_LOCAL: PutMessageThreadLocal = PutMessageThreadLocal{
        encoder: RefCell::new(None),
        key: RefCell::new(String::with_capacity(128)),
    };
}

fn encode_message_ext(
    message_ext: &MessageExtBrokerInner,
    message_store_config: &Arc<MessageStoreConfig>,
) -> (Option<PutMessageResult>, BytesMut) {
    PUT_MESSAGE_THREAD_LOCAL.with(|thread_local| {
        let mut encoder_ref = thread_local.encoder.borrow_mut();
        if encoder_ref.is_none() {
            let encoder = MessageExtEncoder::new(Arc::clone(message_store_config));
            *encoder_ref = Some(MessageExtEncoder::new(Arc::clone(message_store_config)));
        }
        let encoder = encoder_ref.as_mut().unwrap();
        let result = encoder.encode(message_ext);
        let bytes_mut = encoder.byte_buf();
        (result, bytes_mut)
    })
}

fn encode_message_ext_batch(
    message_ext_batch: &MessageExtBatch,
    put_message_context: &mut PutMessageContext,
    message_store_config: &Arc<MessageStoreConfig>,
) -> Option<BytesMut> {
    PUT_MESSAGE_THREAD_LOCAL.with(|thread_local| {
        let mut encoder_ref = thread_local.encoder.borrow_mut();
        if encoder_ref.is_none() {
            *encoder_ref = Some(MessageExtEncoder::new(Arc::clone(message_store_config)));
        }
        encoder_ref
            .as_mut()
            .unwrap()
            .encode_batch(message_ext_batch, put_message_context)
    })
}

fn generate_key(msg: &MessageExtBrokerInner) -> String {
    PUT_MESSAGE_THREAD_LOCAL.with(|thead_local| {
        let mut topic_queue_key = thead_local.key.borrow_mut();
        topic_queue_key.clear();
        topic_queue_key.push_str(msg.topic());
        topic_queue_key.push('-');
        topic_queue_key.push_str(&msg.queue_id().to_string());
        topic_queue_key.clone()
    })
}

pub fn get_cq_type(
    topic_config_table: &Arc<parking_lot::Mutex<HashMap<CheetahString, TopicConfig>>>,
    msg_inner: &MessageExtBrokerInner,
) -> CQType {
    let option = topic_config_table.lock().get(msg_inner.topic()).cloned();
    QueueTypeUtils::get_cq_type(&option)
}

pub fn get_message_num(
    topic_config_table: &Arc<parking_lot::Mutex<HashMap<CheetahString, TopicConfig>>>,
    msg_inner: &MessageExtBrokerInner,
) -> i16 {
    let mut message_num = 1i16;
    let cq_type = get_cq_type(topic_config_table, msg_inner);
    if MessageSysFlag::check(msg_inner.sys_flag(), MessageSysFlag::INNER_BATCH_FLAG)
        || cq_type == CQType::BatchCQ
    {
        if let Some(num) =
            msg_inner
                .message_ext_inner
                .message
                .get_property(&CheetahString::from_static_str(
                    MessageConst::PROPERTY_INNER_NUM,
                ))
        {
            message_num = num.parse().unwrap_or(1i16);
        }
    }
    // message_num
    message_num
}

pub struct CommitLog {
    mapped_file_queue: ArcMut<MappedFileQueue>,
    message_store_config: Arc<MessageStoreConfig>,
    broker_config: Arc<BrokerConfig>,
    enabled_append_prop_crc: bool,
    local_file_message_store: Option<ArcMut<LocalFileMessageStore>>,
    dispatcher: ArcMut<CommitLogDispatcherDefault>,
    confirm_offset: i64,
    store_checkpoint: Arc<StoreCheckpoint>,
    append_message_callback: Arc<DefaultAppendMessageCallback>,
    put_message_lock: Arc<tokio::sync::Mutex<()>>,
    topic_queue_lock: Arc<TopicQueueLock>,
    topic_config_table: Arc<parking_lot::Mutex<HashMap<CheetahString, TopicConfig>>>,
    consume_queue_store: ConsumeQueueStore,
    flush_manager: Arc<tokio::sync::Mutex<DefaultFlushManager>>,
    begin_time_in_lock: Arc<AtomicU64>,
    cold_data_check_service: Arc<ColdDataCheckService>,
}

impl CommitLog {
    pub fn new(
        message_store_config: Arc<MessageStoreConfig>,
        broker_config: Arc<BrokerConfig>,
        dispatcher: ArcMut<CommitLogDispatcherDefault>,
        store_checkpoint: Arc<StoreCheckpoint>,
        topic_config_table: Arc<parking_lot::Mutex<HashMap<CheetahString, TopicConfig>>>,
        consume_queue_store: ConsumeQueueStore,
    ) -> Self {
        let enabled_append_prop_crc = message_store_config.enabled_append_prop_crc;
        let store_path = message_store_config.get_store_path_commit_log();
        let mapped_file_size = message_store_config.mapped_file_size_commit_log;
        let mapped_file_queue = ArcMut::new(MappedFileQueue::new(
            store_path,
            mapped_file_size as u64,
            None,
        ));
        Self {
            mapped_file_queue: mapped_file_queue.clone(),
            message_store_config: message_store_config.clone(),
            broker_config,
            enabled_append_prop_crc,
            local_file_message_store: None,
            dispatcher,
            confirm_offset: -1,
            store_checkpoint: store_checkpoint.clone(),
            append_message_callback: Arc::new(DefaultAppendMessageCallback::new(
                message_store_config.clone(),
                topic_config_table.clone(),
            )),
            put_message_lock: Arc::new(Default::default()),
            topic_queue_lock: Arc::new(TopicQueueLock::with_size(
                message_store_config.topic_queue_lock_num,
            )),
            topic_config_table,
            consume_queue_store,
            flush_manager: Arc::new(tokio::sync::Mutex::new(DefaultFlushManager::new(
                message_store_config,
                mapped_file_queue,
                store_checkpoint,
            ))),
            begin_time_in_lock: Arc::new(AtomicU64::new(0)),
            cold_data_check_service: Arc::new(Default::default()),
        }
    }
}

#[allow(unused_variables)]
impl CommitLog {
    pub fn load(&mut self) -> bool {
        let result = self.mapped_file_queue.load();
        self.mapped_file_queue.check_self();
        info!("load commit log {}", if result { "OK" } else { "Failed" });
        result
    }

    pub fn start(&mut self) {
        let flush_manager = self.flush_manager.clone();
        tokio::spawn(async move {
            let flush_manager_weak = Arc::downgrade(&flush_manager);
            let mut guard = flush_manager.lock().await;
            if let Some(service) = guard.commit_real_time_service_mut() {
                service.set_flush_manager(Some(flush_manager_weak))
            }
            guard.start();
        });
    }

    pub fn shutdown(&mut self) {
        error!("shutdown commit log unimplemented");
    }

    pub fn destroy(&mut self) {
        error!("destroy commit log unimplemented");
    }

    pub fn get_message(&self, offset: i64, size: i32) -> Option<SelectMappedBufferResult> {
        let mapped_file_size = self.message_store_config.mapped_file_size_commit_log;
        let mapped_file = self
            .mapped_file_queue
            .find_mapped_file_by_offset(offset, offset == 0);
        match mapped_file {
            None => None,
            Some(mmap_file) => {
                let pos = offset % mapped_file_size as i64;
                let mut select_mapped_buffer_result =
                    mmap_file.select_mapped_buffer(pos as i32, size);
                if let Some(ref mut result) = select_mapped_buffer_result {
                    result.mapped_file = Some(mmap_file);
                    result.is_in_cache = self.cold_data_check_service.is_data_in_page_cache();
                }
                select_mapped_buffer_result
            }
        }
    }

    pub fn set_confirm_offset(&mut self, phy_offset: i64) {
        self.confirm_offset = phy_offset;
        self.store_checkpoint
            .set_confirm_phy_offset(phy_offset as u64);
    }

    pub async fn put_messages(
        &mut self,
        mut msg_batch: MessageExtBatch,
        this: ArcMut<Self>,
    ) -> PutMessageResult {
        msg_batch
            .message_ext_broker_inner
            .message_ext_inner
            .store_timestamp = get_current_millis() as i64;
        let tran_type =
            MessageSysFlag::get_transaction_value(msg_batch.message_ext_broker_inner.sys_flag());
        if MessageSysFlag::TRANSACTION_NOT_TYPE != tran_type {
            return PutMessageResult::new_default(PutMessageStatus::MessageIllegal);
        }
        if msg_batch
            .message_ext_broker_inner
            .message_ext_inner
            .message
            .get_delay_time_level()
            > 0
        {
            return PutMessageResult::new_default(PutMessageStatus::MessageIllegal);
        }

        //setting ip type:IPV4 OR IPV6, default is ipv4
        let born_host = msg_batch.message_ext_broker_inner.born_host();
        if born_host.is_ipv6() {
            msg_batch.message_ext_broker_inner.with_born_host_v6_flag();
        }

        let store_host = msg_batch.message_ext_broker_inner.store_host();
        if store_host.is_ipv6() {
            msg_batch.message_ext_broker_inner.with_store_host_v6_flag();
        }

        let mut _unlock_mapped_file = None;
        let mut mapped_file = self.mapped_file_queue.get_last_mapped_file();
        let curr_offset = if let Some(ref mapped_file_inner) = mapped_file {
            mapped_file_inner.get_wrote_position() as u64 + mapped_file_inner.get_file_from_offset()
        } else {
            0
        };
        let need_ack_nums = self.message_store_config.in_sync_replicas;
        let need_handle_ha = self.need_handle_ha(&msg_batch.message_ext_broker_inner);
        if need_handle_ha && self.broker_config.enable_controller_mode {
            unimplemented!("controller mode not support HA")
        } else if need_handle_ha && self.broker_config.enable_slave_acting_master {
            unimplemented!("slave acting master not support HA")
        }
        msg_batch.message_ext_broker_inner.version = MessageVersion::V1;
        let auto_message_version_on_topic_len =
            self.message_store_config.auto_message_version_on_topic_len;
        if auto_message_version_on_topic_len
            && msg_batch.message_ext_broker_inner.topic().len() > i8::MAX as usize
        {
            msg_batch.message_ext_broker_inner.version = MessageVersion::V2;
        }
        let mut put_message_context = PutMessageContext::default();
        let encoded_buff = encode_message_ext_batch(
            &msg_batch,
            &mut put_message_context,
            &self.message_store_config,
        );

        let topic_queue_key = generate_key(&msg_batch.message_ext_broker_inner);
        put_message_context.set_topic_queue_table_key(topic_queue_key.clone());
        msg_batch.encoded_buff = encoded_buff;
        let topic_queue_lock = self.topic_queue_lock.lock(topic_queue_key.as_str()).await;
        self.assign_offset(&mut msg_batch.message_ext_broker_inner);

        let lock = self.put_message_lock.lock().await;
        self.begin_time_in_lock.store(
            time_utils::get_current_millis(),
            std::sync::atomic::Ordering::Release,
        );
        let start_time = Instant::now();
        // Here settings are stored timestamp, in order to ensure an orderly global
        msg_batch
            .message_ext_broker_inner
            .message_ext_inner
            .store_timestamp = time_utils::get_current_millis() as i64;

        if mapped_file.is_none() || mapped_file.as_ref().unwrap().is_full() {
            mapped_file = self
                .mapped_file_queue
                .get_last_mapped_file_mut_start_offset(0, true);
        }

        if mapped_file.is_none() {
            drop(lock);
            error!(
                "create mapped file error, topic: {}  clientAddr: {}",
                msg_batch.message_ext_broker_inner.topic(),
                msg_batch.message_ext_broker_inner.born_host()
            );
            self.begin_time_in_lock
                .store(0, std::sync::atomic::Ordering::Release);
            return PutMessageResult::new_default(PutMessageStatus::CreateMappedFileFailed);
        }

        let result = mapped_file.as_ref().unwrap().append_messages(
            &mut msg_batch,
            self.append_message_callback.as_ref(),
            &mut put_message_context,
            self.enabled_append_prop_crc,
        );
        let put_message_result = match result.status {
            AppendMessageStatus::PutOk => {
                //onCommitLogAppend(msg, result, mappedFile); in java not support this version
                PutMessageResult::new_append_result(PutMessageStatus::PutOk, Some(result))
            }
            AppendMessageStatus::EndOfFile => {
                //onCommitLogAppend(msg, result, mappedFile); in java not support this version
                _unlock_mapped_file = mapped_file;
                mapped_file = self
                    .mapped_file_queue
                    .get_last_mapped_file_mut_start_offset(0, true);
                if mapped_file.is_none() {
                    self.begin_time_in_lock
                        .store(0, std::sync::atomic::Ordering::Release);
                    error!(
                        "create mapped file error, topic: {}  clientAddr: {}",
                        msg_batch.message_ext_broker_inner.topic(),
                        msg_batch.message_ext_broker_inner.born_host()
                    );
                    return PutMessageResult::new_append_result(
                        PutMessageStatus::CreateMappedFileFailed,
                        Some(result),
                    );
                }
                let result = mapped_file.as_ref().unwrap().append_messages(
                    &mut msg_batch,
                    self.append_message_callback.as_ref(),
                    &mut put_message_context,
                    self.enabled_append_prop_crc,
                );
                if AppendMessageStatus::PutOk == result.status {
                    PutMessageResult::new_append_result(PutMessageStatus::PutOk, Some(result))
                } else {
                    PutMessageResult::new_append_result(
                        PutMessageStatus::UnknownError,
                        Some(result),
                    )
                }
            }
            AppendMessageStatus::MessageSizeExceeded
            | AppendMessageStatus::PropertiesSizeExceeded => {
                self.begin_time_in_lock
                    .store(0, std::sync::atomic::Ordering::Release);
                PutMessageResult::new_append_result(PutMessageStatus::MessageIllegal, Some(result))
            }
            AppendMessageStatus::UnknownError => {
                self.begin_time_in_lock
                    .store(0, std::sync::atomic::Ordering::Release);
                PutMessageResult::new_append_result(PutMessageStatus::UnknownError, Some(result))
            }
        };
        let elapsed_time_in_lock = start_time.elapsed().as_millis() as u64;
        drop(lock);
        self.begin_time_in_lock
            .store(0, std::sync::atomic::Ordering::Release);
        if elapsed_time_in_lock > 500 {
            warn!(
                "[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} \
                 AppendMessageResult={}",
                elapsed_time_in_lock,
                msg_batch.message_ext_broker_inner.body_len(),
                put_message_result.append_message_result().as_ref().unwrap(),
            );
        }

        if put_message_result.put_message_status() == PutMessageStatus::PutOk {
            self.increase_offset(
                &msg_batch.message_ext_broker_inner,
                put_message_context.get_batch_size() as i16,
            );
            drop(topic_queue_lock);
            CommitLog::handle_disk_flush_and_ha(
                this,
                put_message_result,
                msg_batch.message_ext_broker_inner,
                need_ack_nums,
                need_handle_ha,
            )
            .await
        } else {
            put_message_result
        }
    }

    pub async fn put_message(
        &mut self,
        mut msg: MessageExtBrokerInner,
        this: ArcMut<Self>,
    ) -> PutMessageResult {
        // Set the storage time
        if !self.message_store_config.duplication_enable {
            msg.message_ext_inner.store_timestamp = time_utils::get_current_millis() as i64;
        }
        // Set the message body CRC (consider the most appropriate setting on the client)
        msg.message_ext_inner.body_crc = crc32(
            msg.message_ext_inner
                .message
                .body
                .as_ref()
                .unwrap_or(&Bytes::new()),
        );
        if self.enabled_append_prop_crc {
            // delete crc32 properties if exist
            msg.delete_property(MessageConst::PROPERTY_CRC32);
        }

        //setting message version
        msg.with_version(MessageVersion::V1);
        let topic = msg.topic();
        // setting auto message on topic length
        if self.message_store_config.auto_message_version_on_topic_len
            && topic.len() > i8::MAX as usize
        {
            msg.with_version(MessageVersion::V2);
        }

        //setting ip type:IPV4 OR IPV6, default is ipv4
        let born_host = msg.born_host();
        if born_host.is_ipv6() {
            msg.with_born_host_v6_flag();
        }

        let store_host = msg.store_host();
        if store_host.is_ipv6() {
            msg.with_store_host_v6_flag();
        }

        let topic_queue_key = generate_key(&msg);

        let mut _unlock_mapped_file = None;

        //get last mapped file from mapped file queue
        let mut mapped_file = self.mapped_file_queue.get_last_mapped_file();
        // current offset is physical offset
        let curr_offset = if let Some(ref mapped_file_inner) = mapped_file {
            mapped_file_inner.get_wrote_position() as u64 + mapped_file_inner.get_file_from_offset()
        } else {
            0
        };
        let need_ack_nums = self.message_store_config.in_sync_replicas;
        let need_handle_ha = self.need_handle_ha(&msg);
        if need_handle_ha && self.broker_config.enable_controller_mode {
            unimplemented!("controller mode not support HA")
        } else if need_handle_ha && self.broker_config.enable_slave_acting_master {
            unimplemented!("slave acting master not support HA")
        }

        let need_assign_offset = !(self.message_store_config.duplication_enable
            && self.message_store_config.broker_role != BrokerRole::Slave);

        let topic_queue_lock = self.topic_queue_lock.lock(topic_queue_key.as_str()).await;
        if need_assign_offset {
            self.assign_offset(&mut msg);
        }

        let (put_message_result, encoded_buff) =
            encode_message_ext(&msg, &self.message_store_config);
        if let Some(result) = put_message_result {
            return result;
        }
        msg.encoded_buff = Some(encoded_buff);
        let put_message_context = PutMessageContext::new(topic_queue_key);
        let lock = self.put_message_lock.lock().await;
        let begin_lock_timestamp = time_utils::get_current_millis();
        self.begin_time_in_lock
            .store(begin_lock_timestamp, std::sync::atomic::Ordering::Release);
        let start_time = Instant::now();
        // Here settings are stored timestamp, in order to ensure an orderly global
        if !self.message_store_config.duplication_enable {
            msg.message_ext_inner.store_timestamp = begin_lock_timestamp as i64;
        }

        if mapped_file.is_none() || mapped_file.as_ref().unwrap().is_full() {
            mapped_file = self
                .mapped_file_queue
                .get_last_mapped_file_mut_start_offset(0, true);
        }

        if mapped_file.is_none() {
            drop(lock);
            drop(topic_queue_lock);
            error!(
                "create mapped file error, topic: {}  clientAddr: {}",
                msg.topic(),
                msg.born_host()
            );
            self.begin_time_in_lock
                .store(0, std::sync::atomic::Ordering::Release);
            return PutMessageResult::new_default(PutMessageStatus::CreateMappedFileFailed);
        }

        let result = mapped_file.as_ref().unwrap().append_message(
            &mut msg,
            self.append_message_callback.as_ref(),
            &put_message_context,
        );
        let put_message_result = match result.status {
            AppendMessageStatus::PutOk => {
                //onCommitLogAppend(msg, result, mappedFile); in java not support this version
                PutMessageResult::new_append_result(PutMessageStatus::PutOk, Some(result))
            }
            AppendMessageStatus::EndOfFile => {
                //onCommitLogAppend(msg, result, mappedFile); in java not support this version
                _unlock_mapped_file = mapped_file;
                mapped_file = self
                    .mapped_file_queue
                    .get_last_mapped_file_mut_start_offset(0, true);
                if mapped_file.is_none() {
                    self.begin_time_in_lock
                        .store(0, std::sync::atomic::Ordering::Release);
                    error!(
                        "create mapped file error, topic: {}  clientAddr: {}",
                        msg.topic(),
                        msg.born_host()
                    );
                    return PutMessageResult::new_append_result(
                        PutMessageStatus::CreateMappedFileFailed,
                        Some(result),
                    );
                }
                let result = mapped_file.as_ref().unwrap().append_message(
                    &mut msg,
                    self.append_message_callback.as_ref(),
                    &put_message_context,
                );
                if AppendMessageStatus::PutOk == result.status {
                    PutMessageResult::new_append_result(PutMessageStatus::PutOk, Some(result))
                } else {
                    PutMessageResult::new_append_result(
                        PutMessageStatus::UnknownError,
                        Some(result),
                    )
                }
            }
            AppendMessageStatus::MessageSizeExceeded
            | AppendMessageStatus::PropertiesSizeExceeded => {
                self.begin_time_in_lock
                    .store(0, std::sync::atomic::Ordering::Release);
                PutMessageResult::new_append_result(PutMessageStatus::MessageIllegal, Some(result))
            }
            AppendMessageStatus::UnknownError => {
                self.begin_time_in_lock
                    .store(0, std::sync::atomic::Ordering::Release);
                PutMessageResult::new_append_result(PutMessageStatus::UnknownError, Some(result))
            }
        };
        let elapsed_time_in_lock = start_time.elapsed().as_millis() as u64;
        drop(lock);
        self.begin_time_in_lock
            .store(0, std::sync::atomic::Ordering::Release);
        if elapsed_time_in_lock > 500 {
            warn!(
                "[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} \
                 AppendMessageResult={}",
                elapsed_time_in_lock,
                msg.body_len(),
                put_message_result.append_message_result().as_ref().unwrap(),
            );
        }

        if put_message_result.put_message_status() == PutMessageStatus::PutOk {
            let message_num = get_message_num(&self.topic_config_table, &msg);
            self.increase_offset(&msg, message_num);
            drop(topic_queue_lock);
            CommitLog::handle_disk_flush_and_ha(
                this,
                put_message_result,
                msg,
                need_ack_nums,
                need_handle_ha,
            )
            .await
        } else {
            put_message_result
        }
    }

    fn increase_offset(&self, msg: &MessageExtBrokerInner, message_num: i16) {
        let tran_type = MessageSysFlag::get_transaction_value(msg.sys_flag());
        if MessageSysFlag::TRANSACTION_NOT_TYPE == tran_type
            || MessageSysFlag::TRANSACTION_COMMIT_TYPE == tran_type
        {
            self.consume_queue_store
                .increase_queue_offset(msg, message_num);
        }
    }

    #[inline]
    fn assign_offset(&self, msg: &mut MessageExtBrokerInner) {
        let tran_type = MessageSysFlag::get_transaction_value(msg.sys_flag());
        // if the message is not transaction message or transaction commit message
        if MessageSysFlag::TRANSACTION_NOT_TYPE == tran_type
            || MessageSysFlag::TRANSACTION_COMMIT_TYPE == tran_type
        {
            self.consume_queue_store.assign_queue_offset(msg);
        }
    }

    async fn handle_disk_flush_and_ha(
        this: ArcMut<Self>,
        mut put_message_result: PutMessageResult,
        msg: MessageExtBrokerInner,
        need_ack_nums: u32,
        need_handle_ha: bool,
    ) -> PutMessageResult {
        let commit_log = this.clone();
        let put_message_result_clone =
            Arc::new(put_message_result.append_message_result().unwrap().clone());
        let put_message_result_cloned = put_message_result_clone.clone();
        let disk_flush_handle = tokio::spawn(async move {
            commit_log
                .handle_disk_flush(put_message_result_clone.as_ref(), &msg)
                .await
        });

        let replica_result_handle = tokio::spawn(async move {
            if need_handle_ha {
                this.handle_ha(put_message_result_cloned.as_ref(), need_ack_nums)
                    .await
            } else {
                PutMessageStatus::PutOk
            }
        });

        match disk_flush_handle.await {
            Ok(status) => {
                put_message_result.set_put_message_status(status);
                if status == PutMessageStatus::PutOk {
                    if let Ok(replica_status) = replica_result_handle.await {
                        put_message_result.set_put_message_status(replica_status);
                    }
                }
            }
            Err(error) => {
                put_message_result.set_put_message_status(PutMessageStatus::FlushDiskTimeout);
            }
        }

        put_message_result
    }

    async fn handle_ha(
        &self,
        put_message_result: &AppendMessageResult,
        need_ack_nums: u32,
    ) -> PutMessageStatus {
        if need_ack_nums <= 1 {
            return PutMessageStatus::PutOk;
        }

        //HA service to do unimplemented

        PutMessageStatus::PutOk
    }

    async fn handle_disk_flush(
        &self,
        put_message_result: &AppendMessageResult,
        msg: &MessageExtBrokerInner,
    ) -> PutMessageStatus {
        self.flush_manager
            .lock()
            .await
            .handle_disk_flush(put_message_result, msg)
            .await
    }

    fn need_handle_ha(&self, msg_inner: &MessageExtBrokerInner) -> bool {
        if !msg_inner.is_wait_store_msg_ok() {
            /*
             No need to sync messages that special config to extra broker slaves.
             @see MessageConst.PROPERTY_WAIT_STORE_MSG_OK
            */
            return false;
        }
        if self.message_store_config.duplication_enable {
            return false;
        }
        if BrokerRole::SyncMaster != self.message_store_config.broker_role {
            // No need to check ha in async or slave broker
            return false;
        }

        true
    }

    fn on_commit_log_dispatch(
        &mut self,
        request: &mut DispatchRequest,
        do_dispatch: bool,
        is_recover: bool,
        is_file_end: bool,
    ) {
        if do_dispatch && !is_file_end {
            self.dispatcher.dispatch(request);
        }
    }

    pub fn is_multi_dispatch_msg(msg_inner: &MessageExtBrokerInner) -> bool {
        msg_inner
            .property(MessageConst::PROPERTY_INNER_MULTI_DISPATCH)
            .is_some_and(|s| !s.is_empty())
            && msg_inner
                .topic()
                .starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX)
    }

    pub async fn recover_normally(
        &mut self,
        max_phy_offset_of_consume_queue: i64,
        mut message_store: ArcMut<LocalFileMessageStore>,
    ) {
        let check_crc_on_recover = self.message_store_config.check_crc_on_recover;
        let check_dup_info = self.message_store_config.duplication_enable;
        let message_store_config = self.message_store_config.clone();
        let broker_config = self.broker_config.clone();
        // let mut mapped_file_queue = mapped_files.write().await;
        let mapped_files = self.mapped_file_queue.get_mapped_files();
        let mapped_files_inner = mapped_files.read();
        if !mapped_files_inner.is_empty() {
            // Began to recover from the last third file
            let mut index = (mapped_files_inner.len() as i32) - 3;
            if index <= 0 {
                index = 0;
            }
            let mut index = index as usize;
            //let mut mapped_file = mapped_files_inner.get(index).unwrap().lock().await;
            let mut mapped_file = mapped_files_inner.get(index).unwrap();
            let mut process_offset = mapped_file.get_file_from_offset();
            let mut mapped_file_offset = 0u64;
            //When recovering, the maximum value obtained when getting get_confirm_offset is
            // the file size of the latest file plus the value resolved from the file name.
            let mut last_valid_msg_phy_offset = self.get_confirm_offset() as u64;
            // normal recover doesn't require dispatching
            let do_dispatch = false;
            let mut current_pos = 0usize;
            loop {
                let (msg, size) = self.get_simple_message_bytes(current_pos, mapped_file.as_ref());
                if msg.is_none() {
                    break;
                }
                let mut msg_bytes = msg.unwrap();
                let mut dispatch_request = check_message_and_return_size(
                    &mut msg_bytes,
                    check_crc_on_recover,
                    check_dup_info,
                    true,
                    &message_store_config,
                    self.local_file_message_store
                        .as_ref()
                        .unwrap()
                        .max_delay_level(),
                    self.local_file_message_store
                        .as_ref()
                        .unwrap()
                        .delay_level_table_ref(),
                );
                current_pos += size;
                if dispatch_request.success && dispatch_request.msg_size > 0 {
                    last_valid_msg_phy_offset = process_offset + mapped_file_offset;
                    mapped_file_offset += dispatch_request.msg_size as u64;
                    self.on_commit_log_dispatch(&mut dispatch_request, do_dispatch, true, false);
                } else if dispatch_request.success && dispatch_request.msg_size == 0 {
                    // Come the end of the file, switch to the next file Since the
                    // return 0 representatives met last hole,
                    // this can not be included in truncate offset
                    self.on_commit_log_dispatch(&mut dispatch_request, do_dispatch, true, true);
                    index += 1;
                    if index >= mapped_files_inner.len() {
                        info!(
                            "recover last 3 physics file over, last mapped file:{} ",
                            mapped_file.get_file_name()
                        );
                        break;
                    } else {
                        mapped_file = mapped_files_inner.get(index).unwrap();
                        mapped_file_offset = 0;
                        process_offset = mapped_file.get_file_from_offset();
                        current_pos = 0;
                        info!("recover next physics file:{}", mapped_file.get_file_name());
                    }
                } else if !dispatch_request.success {
                    if dispatch_request.msg_size > 0 {
                        warn!(
                            "found a half message at {}, it will be truncated.",
                            process_offset + mapped_file_offset,
                        );
                    }
                    info!("recover physics file end,{} ", mapped_file.get_file_name());
                    break;
                }
            }
            process_offset += mapped_file_offset;
            if broker_config.enable_controller_mode {
                unimplemented!();
            } else {
                self.set_confirm_offset(last_valid_msg_phy_offset as i64);
            }

            // Clear ConsumeQueue redundant data
            if max_phy_offset_of_consume_queue as u64 >= process_offset {
                warn!(
                    "maxPhyOffsetOfConsumeQueue({}) >= processOffset({}), truncate dirty logic \
                     files",
                    max_phy_offset_of_consume_queue, process_offset
                );
                message_store.truncate_dirty_logic_files(process_offset as i64)
            }
            self.mapped_file_queue
                .set_flushed_where(process_offset as i64);
            self.mapped_file_queue
                .set_committed_where(process_offset as i64);
            self.mapped_file_queue
                .truncate_dirty_files(process_offset as i64);
        } else {
            warn!(
                "The commitlog files are deleted, and delete the consume queue
                                        files"
            );
            self.mapped_file_queue.set_flushed_where(0);
            self.mapped_file_queue.set_committed_where(0);
            message_store.consume_queue_store_mut().destroy();
            message_store.consume_queue_store_mut().load_after_destroy();
        }
    }

    fn get_simple_message_bytes<MF: MappedFile>(
        &self,
        position: usize,
        mapped_file: &MF,
    ) -> (Option<Bytes>, usize) {
        let mut bytes = mapped_file.get_bytes(position, 4);
        match bytes {
            None => (None, 0),
            Some(ref mut inner) => {
                let size = inner.get_i32();
                if size <= 0 {
                    return (None, 0);
                }
                (
                    mapped_file.get_bytes(position, size as usize),
                    size as usize,
                )
            }
        }
    }

    //Fetch and compute the newest confirmOffset.
    pub fn get_confirm_offset(&self) -> i64 {
        if self.broker_config.enable_controller_mode {
            unimplemented!()
        } else if self.broker_config.duplication_enable {
            return self.confirm_offset;
        }
        self.get_max_offset()
    }

    pub async fn recover_abnormally(
        &mut self,
        max_phy_offset_of_consume_queue: i64,
        mut message_store: ArcMut<LocalFileMessageStore>,
    ) {
        let check_crc_on_recover = self.message_store_config.check_crc_on_recover;
        let check_dup_info = self.message_store_config.duplication_enable;
        //let message_store_config = self.message_store_config.clone();
        let broker_config = self.broker_config.clone();
        // let mut mapped_file_queue = mapped_files.write().await;
        let binding = self.mapped_file_queue.get_mapped_files();
        let mapped_files_inner = binding.read();
        if !mapped_files_inner.is_empty() {
            // Began to recover from the last third file
            let mut index = (mapped_files_inner.len() as i32) - 1;
            while index >= 0 {
                let mapped_file = mapped_files_inner.get(index as usize).unwrap();
                if is_mapped_file_matched_recover(
                    &self.message_store_config,
                    mapped_file,
                    &self.store_checkpoint,
                ) {
                    break;
                }
                index -= 1;
            }
            if index <= 0 {
                index = 0;
            }
            let mut index = index as usize;
            //let mut mapped_file = mapped_files_inner.get(index).unwrap().lock().await;
            let mut mapped_file = mapped_files_inner.get(index).unwrap();
            let mut process_offset = mapped_file.get_file_from_offset();
            let mut mapped_file_offset = 0u64;
            //When recovering, the maximum value obtained when getting get_confirm_offset is
            // the file size of the latest file plus the value resolved from the file name.
            let mut last_valid_msg_phy_offset = process_offset;
            let mut last_confirm_valid_msg_phy_offset = process_offset;
            // normal recover doesn't require dispatching
            let do_dispatch = true;
            let mut current_pos = 0usize;
            loop {
                let (msg, size) = self.get_simple_message_bytes(current_pos, mapped_file.as_ref());
                if msg.is_none() {
                    break;
                }
                let mut msg_bytes = msg.unwrap();
                let mut dispatch_request = check_message_and_return_size(
                    &mut msg_bytes,
                    check_crc_on_recover,
                    check_dup_info,
                    true,
                    &self.message_store_config,
                    self.local_file_message_store
                        .as_ref()
                        .unwrap()
                        .max_delay_level(),
                    self.local_file_message_store
                        .as_ref()
                        .unwrap()
                        .delay_level_table_ref(),
                );
                current_pos += size;
                if dispatch_request.success && dispatch_request.msg_size > 0 {
                    last_valid_msg_phy_offset = process_offset + mapped_file_offset;
                    mapped_file_offset += dispatch_request.msg_size as u64;

                    if self.message_store_config.duplication_enable
                        || self.broker_config.enable_controller_mode
                    {
                        if dispatch_request.commit_log_offset + size as i64
                            <= self.get_confirm_offset()
                        {
                            self.on_commit_log_dispatch(
                                &mut dispatch_request,
                                do_dispatch,
                                true,
                                false,
                            );
                            last_confirm_valid_msg_phy_offset =
                                dispatch_request.commit_log_offset as u64 + size as u64;
                        }
                    } else {
                        self.on_commit_log_dispatch(
                            &mut dispatch_request,
                            do_dispatch,
                            true,
                            false,
                        );
                    }
                } else if dispatch_request.success && dispatch_request.msg_size == 0 {
                    // Come the end of the file, switch to the next file Since the
                    // return 0 representatives met last hole,
                    // this can not be included in truncate offset
                    self.on_commit_log_dispatch(&mut dispatch_request, do_dispatch, true, true);
                    index += 1;
                    if index >= mapped_files_inner.len() {
                        info!(
                            "recover last 3 physics file over, last mapped file:{} ",
                            mapped_file.get_file_name()
                        );
                        break;
                    } else {
                        mapped_file = mapped_files_inner.get(index).unwrap();
                        mapped_file_offset = 0;
                        process_offset = mapped_file.get_file_from_offset();
                        current_pos = 0;
                        info!("recover next physics file:{}", mapped_file.get_file_name());
                    }
                } else if !dispatch_request.success {
                    if dispatch_request.msg_size > 0 {
                        warn!(
                            "found a half message at {}, it will be truncated.",
                            process_offset + mapped_file_offset,
                        );
                    }
                    info!("recover physics file end,{} ", mapped_file.get_file_name());
                    break;
                }
            }

            // only for rocksdb mode
            // this.getMessageStore().finishCommitLogDispatch();

            process_offset += mapped_file_offset;
            if broker_config.enable_controller_mode {
                println!("TODO: finishCommitLogDispatch:{last_confirm_valid_msg_phy_offset}",);
                unimplemented!();
            } else {
                self.set_confirm_offset(last_valid_msg_phy_offset as i64);
            }

            // Clear ConsumeQueue redundant data
            if max_phy_offset_of_consume_queue as u64 >= process_offset {
                warn!(
                    "maxPhyOffsetOfConsumeQueue({}) >= processOffset({}), truncate dirty logic \
                     files",
                    max_phy_offset_of_consume_queue, process_offset
                );
                message_store.truncate_dirty_logic_files(process_offset as i64)
            }
            self.mapped_file_queue
                .set_flushed_where(process_offset as i64);
            self.mapped_file_queue
                .set_committed_where(process_offset as i64);
            self.mapped_file_queue
                .truncate_dirty_files(process_offset as i64);
        } else {
            warn!(
                "The commitlog files are deleted, and delete the consume queue
                                        files"
            );
            self.mapped_file_queue.set_flushed_where(0);
            self.mapped_file_queue.set_committed_where(0);
            message_store.consume_queue_store_mut().destroy();
            message_store.consume_queue_store_mut().load_after_destroy();
        }
    }

    pub fn get_max_offset(&self) -> i64 {
        self.mapped_file_queue.get_max_offset()
    }

    pub fn get_min_offset(&self) -> i64 {
        match self.mapped_file_queue.get_first_mapped_file() {
            None => -1,
            Some(mapped_file) => {
                if mapped_file.is_available() {
                    mapped_file.get_file_from_offset() as i64
                } else {
                    self.roll_next_file(mapped_file.get_file_from_offset() as i64)
                }
            }
        }
    }

    pub fn roll_next_file(&self, offset: i64) -> i64 {
        let mapped_file_size = self.message_store_config.mapped_file_size_commit_log as i64;
        offset + mapped_file_size - (offset % mapped_file_size)
    }

    pub fn get_data(&self, offset: i64) -> Option<SelectMappedBufferResult> {
        self.get_data_with_option(offset, offset == 0)
    }

    pub fn get_bulk_data(&self, offset: i64, size: i32) -> Option<Vec<SelectMappedBufferResult>> {
        unimplemented!("get_bulk_data not implemented")
    }

    pub fn get_data_with_option(
        &self,
        offset: i64,
        return_first_on_not_found: bool,
    ) -> Option<SelectMappedBufferResult> {
        let mapped_file_size = self.message_store_config.mapped_file_size_commit_log as i64;
        let mapped_file = self
            .mapped_file_queue
            .find_mapped_file_by_offset(offset, return_first_on_not_found);
        if let Some(mapped_file) = mapped_file {
            let pos = (offset % mapped_file_size) as i32;
            let mut result = mapped_file.select_mapped_buffer_with_position(pos);
            if let Some(ref mut result) = result {
                result.mapped_file = Some(mapped_file);
            }
            result
        } else {
            None
        }
    }

    pub fn check_self(&self) {
        self.mapped_file_queue.check_self();
    }

    pub fn lock_time_mills(&self) -> i64 {
        let begin = self
            .begin_time_in_lock
            .load(std::sync::atomic::Ordering::Acquire);
        if begin > 0 {
            (SystemClock::now() - (begin as u128)) as i64
        } else {
            0
        }
    }

    pub fn begin_time_in_lock(&self) -> &Arc<AtomicU64> {
        &self.begin_time_in_lock
    }

    pub fn remain_how_many_data_to_commit(&self) -> i64 {
        self.mapped_file_queue.remain_how_many_data_to_commit()
    }

    pub fn remain_how_many_data_to_flush(&self) -> i64 {
        self.mapped_file_queue.remain_how_many_data_to_flush()
    }

    pub fn pickup_store_timestamp(&self, offset: i64, size: i32) -> i64 {
        if offset >= self.get_min_offset() && (offset + size as i64) <= self.get_max_offset() {
            let result = self.get_message(offset, size);
            if let Some(result) = result {
                let buffer = result.get_buffer();
                let sys_flag = (&buffer[MessageDecoder::SYSFLAG_POSITION..]).get_i32();
                let born_host_length = if sys_flag & MessageSysFlag::BORNHOST_V6_FLAG == 0 {
                    4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + 8
                } else {
                    4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + 20
                };
                (&buffer[born_host_length..]).get_i64()
            } else {
                -1
            }
        } else {
            -1
        }
    }

    pub fn append_data(
        &self,
        start_offset: i64,
        data: &[u8],
        data_start: i32,
        data_length: i32,
    ) -> Result<bool, StoreError> {
        unimplemented!("append_data not implemented")
    }

    pub fn set_local_file_message_store(
        &mut self,
        local_file_message_store: ArcMut<LocalFileMessageStore>,
    ) {
        self.local_file_message_store = Some(local_file_message_store);
    }
}

pub fn check_message_and_return_size(
    bytes: &mut Bytes,
    check_crc: bool,
    check_dup_info: bool,
    read_body: bool,
    message_store_config: &Arc<MessageStoreConfig>,
    max_delay_level: i32,
    delay_level_table: &BTreeMap<i32 /* level */, i64 /* delay timeMillis */>,
) -> DispatchRequest {
    // Total size
    let total_size = bytes.get_i32();

    // message magic code
    let magic_code = bytes.get_i32();
    match magic_code {
        MESSAGE_MAGIC_CODE | MESSAGE_MAGIC_CODE_V2 => {
            // Continue processing the message
        }
        BLANK_MAGIC_CODE => {
            return DispatchRequest {
                msg_size: 0,
                success: true,
                ..Default::default()
            };
        }
        _ => {
            warn!(
                "found a illegal magic code 0x{}",
                format!("{:X}", magic_code),
            );
            return DispatchRequest {
                msg_size: -1,
                success: false,
                ..Default::default()
            };
        }
    }
    let message_version = match MessageVersion::value_of_magic_code(magic_code) {
        Ok(value) => value,
        Err(_) => {
            return DispatchRequest {
                msg_size: -1,
                success: false,
                ..Default::default()
            }
        }
    };
    let body_crc = bytes.get_i32();
    let queue_id = bytes.get_i32();
    let flag = bytes.get_i32();
    let queue_offset = bytes.get_i64();
    let physic_offset = bytes.get_i64();
    let sys_flag = bytes.get_i32();
    let born_time_stamp = bytes.get_i64();

    let born_host = if sys_flag & MessageSysFlag::BORNHOST_V6_FLAG == 0 {
        bytes.copy_to_bytes(8)
    } else {
        bytes.copy_to_bytes(20)
    };
    let store_timestamp = bytes.get_i64();

    let store_host = if sys_flag & MessageSysFlag::STOREHOSTADDRESS_V6_FLAG == 0 {
        bytes.copy_to_bytes(8)
    } else {
        bytes.copy_to_bytes(20)
    };

    let reconsume_times = bytes.get_i32();
    let prepared_transaction_offset = bytes.get_i64();
    let body_len = bytes.get_i32();
    if body_len > 0 {
        if read_body {
            //body content
            let body = bytes.copy_to_bytes(body_len as usize);
            if check_crc && !message_store_config.force_verify_prop_crc {
                let crc = crc32(body.as_ref());
                if crc != body_crc as u32 {
                    warn!("CRC check failed. bodyCRC={}, currentCRC={}", crc, body_crc);
                    return DispatchRequest {
                        msg_size: -1,
                        success: false,
                        ..Default::default()
                    };
                }
            }
        } else {
            //skip body content
            bytes.advance(body_len as usize);
        }
    }
    let topic_len = message_version.get_topic_length(bytes);
    let topic_bytes = bytes.copy_to_bytes(topic_len);
    let topic =
        CheetahString::from_string(String::from_utf8_lossy(topic_bytes.as_ref()).to_string());
    let properties_length = bytes.get_i16();
    let (tags_code, keys, uniq_key, properties_map) = if properties_length > 0 {
        let properties = bytes.copy_to_bytes(properties_length as usize);
        let properties_content = String::from_utf8_lossy(properties.as_ref()).to_string();
        //need to optimize
        let properties_map =
            string_to_message_properties(Some(&CheetahString::from_string(properties_content)));
        let keys = properties_map.get(MessageConst::PROPERTY_KEYS).cloned();
        let uniq_key = properties_map
            .get(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
            .cloned();
        if check_dup_info {
            let dup_info = properties_map.get(MessageConst::DUP_INFO).cloned();
            if dup_info.is_none() {
                warn!("DupInfo in properties check failed. dupInfo=null");
                return DispatchRequest {
                    msg_size: -1,
                    success: false,
                    ..Default::default()
                };
            } else {
                let content = dup_info.unwrap();
                let vec = content.split('_').collect::<Vec<&str>>();
                if vec.len() != 2 {
                    warn!("DupInfo in properties check failed. dupInfo={}", content);
                    return DispatchRequest {
                        msg_size: -1,
                        success: false,
                        ..Default::default()
                    };
                }
            }
        }
        let tags = properties_map.get(MessageConst::PROPERTY_TAGS);
        let mut tags_code = tags_string2tags_code(tags);

        {
            // Timing message processing
            let delay_time_level = properties_map.get(&CheetahString::from_static_str(
                MessageConst::PROPERTY_DELAY_TIME_LEVEL,
            ));
            if delay_time_level.is_some() && TopicValidator::RMQ_SYS_SCHEDULE_TOPIC == topic {
                let mut delay_level = delay_time_level.unwrap().parse::<i32>().unwrap();
                if delay_level > max_delay_level {
                    delay_level = max_delay_level;
                }
                if delay_level > 0 {
                    if let Some(delay_level) = delay_level_table.get(&delay_level) {
                        tags_code = *delay_level + store_timestamp;
                    } else {
                        tags_code = store_timestamp + 1000;
                    }
                }
            }
        }
        (
            tags_code,
            keys.unwrap_or_default(),
            uniq_key,
            properties_map,
        )
    } else {
        (0, CheetahString::new(), None, HashMap::new())
    };

    if check_crc && message_store_config.force_verify_prop_crc {
        let mut expected_crc = -1i32;
        if !properties_map.is_empty() {
            let crc_32 = properties_map.get(&CheetahString::from_static_str(
                MessageConst::PROPERTY_CRC32,
            ));
            if let Some(crc_32) = crc_32 {
                expected_crc = 0;
                for ch in crc_32.chars().rev() {
                    let num = (ch as u8 - b'0') as i32;
                    expected_crc *= 10;
                    expected_crc += num;
                }
            }
        }
        if expected_crc > 0 {
            unimplemented!("check_crc not implemented")
        }
    }

    let read_length = MessageExtEncoder::cal_msg_length(
        message_version,
        sys_flag,
        body_len,
        topic_len as i32,
        properties_length as i32,
    );

    if total_size != read_length {
        error!(
            "[BUG]read total count not equals msg total size. totalSize={}, readTotalCount={}, \
             bodyLen={}, topicLen={}, propertiesLength={}",
            total_size, read_length, body_len, topic_len, properties_length
        );
        return DispatchRequest {
            msg_size: total_size,
            success: false,
            ..Default::default()
        };
    }
    let mut dispatch_request = DispatchRequest {
        success: true,
        topic,
        queue_id,
        commit_log_offset: physic_offset,
        msg_size: total_size,
        tags_code,
        store_timestamp,
        consume_queue_offset: queue_offset,
        keys,
        uniq_key,
        sys_flag,
        prepared_transaction_offset,
        ..DispatchRequest::default()
    };
    set_batch_size_if_needed(&properties_map, &mut dispatch_request);
    dispatch_request.properties_map = Some(properties_map);
    dispatch_request
}

fn set_batch_size_if_needed(
    properties_map: &HashMap<CheetahString, CheetahString>,
    dispatch_request: &mut DispatchRequest,
) {
    if !properties_map.is_empty()
        && properties_map.contains_key(MessageConst::PROPERTY_INNER_NUM)
        && properties_map.contains_key(MessageConst::PROPERTY_INNER_BASE)
    {
        dispatch_request.msg_base_offset = properties_map
            .get(MessageConst::PROPERTY_INNER_BASE)
            .unwrap()
            .parse::<i64>()
            .unwrap();
        dispatch_request.batch_size = properties_map
            .get(MessageConst::PROPERTY_INNER_NUM)
            .unwrap()
            .parse::<i16>()
            .unwrap();
    }
}

fn is_mapped_file_matched_recover(
    message_store_config: &Arc<MessageStoreConfig>,
    mapped_file: &DefaultMappedFile,
    store_checkpoint: &StoreCheckpoint,
) -> bool {
    let magic_code = mapped_file
        .get_bytes(MESSAGE_MAGIC_CODE_POSITION, mem::size_of::<i32>())
        .unwrap_or(Bytes::from([0u8; mem::size_of::<i32>()].as_ref()))
        .get_i32();

    //check magic code
    if magic_code != MESSAGE_MAGIC_CODE && magic_code != MESSAGE_MAGIC_CODE_V2 {
        return false;
    }
    if message_store_config.is_enable_rocksdb_store() {
        unimplemented!()
    } else {
        let sys_flag = mapped_file
            .get_bytes(SYSFLAG_POSITION, mem::size_of::<i32>())
            .unwrap_or(Bytes::from([0u8; mem::size_of::<i32>()].as_ref()))
            .get_i32();
        let born_host_length = if sys_flag & MessageSysFlag::BORNHOST_V6_FLAG == 0 {
            8
        } else {
            20
        };
        let msg_store_time_pos = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + born_host_length;
        let store_timestamp = mapped_file
            .get_bytes(msg_store_time_pos, mem::size_of::<i64>())
            .unwrap_or(Bytes::from([0u8; mem::size_of::<i64>()].as_ref()))
            .get_i64();
        if store_timestamp == 0 {
            return false;
        }
        if message_store_config.message_index_enable && message_store_config.message_index_safe {
            if store_timestamp <= store_checkpoint.get_min_timestamp_index() as i64 {
                info!(
                    "find check timestamp, {} {}",
                    store_timestamp,
                    time_millis_to_human_string(store_timestamp)
                );
                return true;
            }
        } else if store_timestamp <= store_checkpoint.get_min_timestamp() as i64 {
            info!(
                "find check timestamp, {} {}",
                store_timestamp,
                time_millis_to_human_string(store_timestamp)
            );
            return true;
        }
    }
    false
}

impl Swappable for CommitLog {
    fn swap_map(
        &self,
        _reserve_num: i32,
        _force_swap_interval_ms: i64,
        _normal_swap_interval_ms: i64,
    ) {
        todo!()
    }

    fn clean_swapped_map(&self, _force_clean_swap_interval_ms: i64) {
        todo!()
    }
}
