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

use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_runtime::RuntimeContext;
use rocketmq_runtime::TaskGroup;
use rocketmq_store::BrokerAdminStore;
use rocketmq_store::BrokerReadStore;
use rocketmq_store::BrokerStorePort;
use rocketmq_store::MessageStoreConfig;
use rocketmq_store::PutMessageHook;
use rocketmq_store::PutMessageResult;
use tempfile::TempDir;

use super::*;
use crate::broker_runtime::BrokerMessageStore;
use crate::broker_runtime::BrokerRuntime;
use crate::config::broker_config::BrokerConfig;

pub(crate) struct Fixture {
    pub(crate) broker: BrokerRuntime,
    pub(crate) merge: Arc<PopBufferMergeService<BrokerMessageStore>>,
    pub(crate) processor: Arc<PopMessageProcessor<BrokerMessageStore>>,
    pub(crate) timer_fields: Arc<Mutex<Vec<(u64, u64)>>>,
    owner: TaskGroup,
    _directory: TempDir,
}

impl Fixture {
    pub(crate) async fn new() -> Self {
        Self::with_batch_ack(false).await
    }

    async fn with_batch_ack(batch: bool) -> Self {
        Self::configured(batch, false).await
    }

    async fn configured(batch: bool, kv: bool) -> Self {
        let directory = TempDir::new().unwrap();
        let root = directory.path().to_string_lossy().into_owned();
        let runtime = RuntimeContext::from_current("pop-checkpoint-retirement-test");
        let service = runtime.service_context("broker");
        let owner = service.task_group().clone();
        let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let ha_port = usize::from(socket.local_addr().unwrap().port());
        drop(socket);
        let mut broker = BrokerRuntime::new_with_service_context(
            Arc::new(BrokerConfig {
                store_path_root_dir: root.clone().into(),
                auth_config_path: directory.path().join("auth.json").to_string_lossy().into_owned().into(),
                enable_pop_buffer_merge: true,
                enable_pop_batch_ack: batch,
                pop_consumer_kv_service_init: kv,
                pop_consumer_kv_service_enable: kv,
                ..Default::default()
            }),
            Arc::new(MessageStoreConfig {
                store_path_root_dir: root.into(),
                mapped_file_size_commit_log: 1024 * 1024,
                ha_listen_port: ha_port,
                timer_wheel_enable: true,
                ..Default::default()
            }),
            service,
        );
        broker.initialize().await.unwrap();
        assert!(broker.runtime_state_mut().timer_message_store().is_some());
        broker.start_message_store_for_test().await.unwrap();
        broker.seed_pop_topic_and_group_for_test("retirement-topic", "retirement-group");
        let processor = broker
            .runtime_state_mut()
            .pop_message_processor()
            .cloned()
            .unwrap_or_else(|| broker.pop_message_processor_for_test());
        let merge = Arc::clone(processor.pop_buffer_merge_service());
        let timer_fields = Arc::new(Mutex::new(Vec::new()));
        broker.with_message_store_mut_for_test(|store| {
            store.set_put_message_hook(Box::new(TimerFieldsRecorder(Arc::clone(&timer_fields))));
        });
        Self {
            broker,
            merge,
            processor,
            timer_fields,
            owner,
            _directory: directory,
        }
    }

    async fn checkpoint(&self, queue_id: i32) -> Arc<PopCheckPointWrapper> {
        self.checkpoint_with_broker(queue_id, Some("retirement-broker")).await
    }

    async fn checkpoint_with_broker(&self, queue_id: i32, broker: Option<&str>) -> Arc<PopCheckPointWrapper> {
        let point = PopCheckPoint {
            topic: "retirement-topic".into(),
            cid: "retirement-group".into(),
            queue_id,
            pop_time: current_millis() as i64,
            invisible_time: 60_000,
            start_offset: 100,
            num: 3,
            queue_offset_diff: vec![0, 1, 2],
            broker_name: broker.map(CheetahString::from_slice),
            ..Default::default()
        };
        self.merge.add_ck(&point, 0, -1, 103).await.unwrap();
        self.merge
            .buffer
            .get(&PopCheckPointWrapper::build_keys(&point).1)
            .unwrap()
            .clone()
    }

    pub(crate) async fn retire_unacknowledged_checkpoint(&mut self) -> Arc<PopCheckPointWrapper> {
        let wrapper = self.checkpoint(0).await;
        self.merge.put_ck_to_store(&wrapper, false).await;
        self.flush();
        self.merge.scan(&mut Vec::new()).await;
        assert!(wrapper.is_ck_stored() && wrapper.retired.load(Ordering::Acquire));
        assert!(!self.merge.buffer.contains_key(wrapper.get_merge_key()));
        wrapper
    }

    fn acknowledge(&self, wrapper: &PopCheckPointWrapper, index: u8) {
        let point = wrapper.get_ck();
        assert!(self.merge.add_ack(
            wrapper.revive_queue_id,
            &AckMsg {
                ack_offset: point.ack_offset_by_index(index),
                start_offset: point.start_offset,
                consumer_group: point.cid.clone(),
                topic: point.topic.clone(),
                queue_id: point.queue_id,
                pop_time: point.pop_time,
                broker_name: point.broker_name.clone().unwrap_or_default(),
            },
        ));
    }

    async fn persist_checkpoint_fixture(&self, wrapper: &PopCheckPointWrapper) {
        // Isolate retirement from the production builder's timer encoding. The
        // state is marked stored only after a real, correctly timed CK append.
        let mut message = PopMessageProcessor::<BrokerMessageStore>::build_ck_msg(
            self.merge.context.policy.snapshot().store_host,
            wrapper.get_ck(),
            wrapper.revive_queue_id,
            self.merge.revive_topic.clone(),
        );
        message.clear_property(MessageConst::PROPERTY_TIMER_DELAY_MS);
        message.set_deliver_time_ms((wrapper.get_ck().get_revive_time() - PopAckConstants::ACK_TIME_INTERVAL) as u64);
        message.properties_string = MessageDecoder::message_properties_to_string(message.get_properties());
        let result = self.merge.context.store.put_specific(message).await.unwrap();
        assert_eq!(result.put_message_status(), PutMessageStatus::PutOk);
        wrapper.set_revive_queue_offset(result.append_message_result().unwrap().logics_offset);
        wrapper.set_ck_stored(true);
    }

    async fn persist_ack_fixture(&self, wrapper: &PopCheckPointWrapper, index: u8) {
        let point = wrapper.get_ck();
        let ack = AckMsg {
            ack_offset: point.ack_offset_by_index(index),
            start_offset: point.start_offset,
            consumer_group: point.cid.clone(),
            topic: point.topic.clone(),
            queue_id: point.queue_id,
            pop_time: point.pop_time,
            broker_name: point.broker_name.clone().unwrap_or_default(),
        };
        let mut message = MessageExtBrokerInner::default();
        message.set_topic(self.merge.revive_topic.clone());
        message.set_body(Bytes::from(ack.serialize_json().unwrap()));
        message.set_tags(PopAckConstants::ACK_TAG.into());
        message.message_ext_inner.queue_id = wrapper.revive_queue_id;
        message.message_ext_inner.born_timestamp = current_millis() as i64;
        let host = self.merge.context.policy.snapshot().store_host;
        message.message_ext_inner.born_host = host;
        message.message_ext_inner.store_host = host;
        message.set_deliver_time_ms(point.get_revive_time() as u64);
        message.properties_string = MessageDecoder::message_properties_to_string(message.get_properties());
        let result = self.merge.context.store.put_specific(message).await.unwrap();
        assert_eq!(result.put_message_status(), PutMessageStatus::PutOk);
        PopBufferMergeService::<BrokerMessageStore>::mark_bit_cas(wrapper.get_to_store_bits(), index as usize);
    }

    pub(crate) fn flush(&mut self) {
        let store = self.broker.runtime_state_mut().message_store().unwrap();
        let appended = store.get_max_phy_offset();
        assert!(
            appended > 0,
            "durability fixture must have appended a real checkpoint/ACK"
        );
        assert!(store.flush() >= appended);
    }

    fn appended_messages(&mut self, begin: i64) -> Vec<MessageExt> {
        let store = self.broker.runtime_state_mut().message_store().unwrap();
        let end = store.get_max_phy_offset();
        let mut cursor = begin;
        let mut messages = Vec::new();
        while cursor < end {
            assert!(messages.len() < 16, "bounded isolated CK/ACK append interval");
            let selected = store.select_one_message_by_offset(cursor).unwrap();
            cursor += i64::from(selected.size());
            let mut bytes = Bytes::copy_from_slice(selected.get_buffer());
            messages.push(MessageDecoder::decode(&mut bytes, true, true, false, false, true).unwrap());
        }
        messages
    }

    pub(crate) async fn close(mut self) {
        // No merge background scan is started in these tests. Its real scan and
        // offset-commit methods run in the foreground, including on the red path.
        self.broker.shutdown().await;
        let report = self.owner.shutdown(Duration::from_secs(5)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }
}

struct TimerFieldsRecorder(Arc<Mutex<Vec<(u64, u64)>>>);

impl PutMessageHook for TimerFieldsRecorder {
    fn hook_name(&self) -> &'static str {
        "pop-retirement-timer-field-recorder"
    }

    fn execute_before_put_message(&self, message: &mut dyn MessageTrait) -> Option<PutMessageResult> {
        self.0
            .lock()
            .push((message.get_deliver_time_ms(), message.get_delay_time_ms()));
        None
    }
}

fn assert_recoverable_ack_subset(messages: &[MessageExt], expected: &PopCheckPointWrapper, acknowledged_bits: i32) {
    let mut checkpoints = std::collections::BTreeMap::new();
    for message in messages {
        if message.tags().unwrap().as_str() == PopAckConstants::CK_TAG {
            let checkpoint: PopCheckPoint = serde_json::from_slice(message.get_body().unwrap()).unwrap();
            let key = PopCheckPointWrapper::build_keys(&checkpoint).1;
            assert_eq!(&key, expected.get_merge_key());
            checkpoints.insert(key, checkpoint);
        }
    }
    assert_eq!(checkpoints.len(), 1);
    let mut ack_count = 0;
    for message in messages {
        let (ack, offsets) = match message.tags().unwrap().as_str() {
            PopAckConstants::ACK_TAG => {
                let ack: AckMsg = serde_json::from_slice(message.get_body().unwrap()).unwrap();
                let offsets = vec![ack.ack_offset];
                (ack, offsets)
            }
            PopAckConstants::BATCH_ACK_TAG => {
                let batch: BatchAckMsg = serde_json::from_slice(message.get_body().unwrap()).unwrap();
                (batch.ack_msg, batch.ack_offset_list)
            }
            PopAckConstants::CK_TAG => continue,
            _ => panic!("unexpected isolated revive record"),
        };
        let key = PopCheckPointWrapper::build_merge_key(
            &ack.topic,
            &ack.consumer_group,
            ack.queue_id,
            ack.start_offset,
            ack.pop_time,
            ack.broker_name.as_str(),
        );
        // These are the six coordinates consumed by PopReviveService. A real
        // append with a different key is not a recoverable ACK for this CK.
        assert_eq!(
            &key,
            expected.get_merge_key(),
            "persisted ACK must match the complete revive identity"
        );
        let checkpoint = checkpoints.get_mut(&key).unwrap();
        for offset in offsets {
            let index = checkpoint.index_of_ack(offset);
            assert!(index >= 0);
            checkpoint.bit_map = DataConverter::set_bit(checkpoint.bit_map, index as usize, true);
            ack_count += 1;
        }
    }
    assert_eq!(ack_count, acknowledged_bits.count_ones());
    assert_eq!(
        checkpoints.get(expected.get_merge_key()).unwrap().bit_map,
        acknowledged_bits
    );
}

#[tokio::test]
async fn normal_scan_retires_durable_checkpoints_without_waiting_for_unreceived_acks() {
    let mut fixture = Fixture::new().await;
    let zero_ack = fixture.checkpoint(0).await;
    let partial_ack = fixture.checkpoint(1).await;
    let no_durable_ck = fixture.checkpoint(2).await;
    let pending_ack = fixture.checkpoint(3).await;
    let full_ack = fixture.checkpoint(4).await;

    for wrapper in [&zero_ack, &partial_ack, &pending_ack] {
        fixture.persist_checkpoint_fixture(wrapper).await;
        assert!(wrapper.is_ck_stored() && wrapper.get_revive_queue_offset() >= 0);
    }
    fixture.acknowledge(&partial_ack, 0);
    fixture.persist_ack_fixture(&partial_ack, 0).await;
    fixture.acknowledge(&pending_ack, 0);
    for index in 0..3 {
        fixture.acknowledge(&full_ack, index);
    }
    fixture.flush();
    fixture.merge.scan(&mut Vec::new()).await;

    let mut observed = Vec::new();
    for wrapper in [&zero_ack, &partial_ack, &no_durable_ck, &pending_ack, &full_ack] {
        let point = wrapper.get_ck();
        observed.push((
            fixture.merge.buffer.contains_key(wrapper.get_merge_key()),
            fixture
                .merge
                .context
                .offsets
                .query_offset(&point.cid, &point.topic, point.queue_id),
        ));
    }
    let pending_ack_bits = pending_ack.get_to_store_bits().load(Ordering::Acquire);
    let missing_ck_stored = no_durable_ck.is_ck_stored();
    fixture.close().await;

    assert_eq!(pending_ack_bits, 0, "normal scan must not invent an ACK append");
    assert!(!missing_ck_stored, "normal scan must not invent checkpoint durability");
    assert_eq!(
        observed,
        vec![(false, 103), (false, 103), (true, -1), (true, -1), (false, 103)],
        "durable CK plus all observed ACKs persisted can retire; missing CK/ACK durability cannot"
    );
}

#[tokio::test]
async fn shutdown_scan_persists_partial_ack_then_retires_checkpoint_and_offset() {
    let fixture = Fixture::new().await;
    let wrapper = fixture.checkpoint(0).await;
    fixture.acknowledge(&wrapper, 0);
    let begin = 0;
    fixture.merge.shutdown();
    // The production scan itself must append both CK and ACK before retirement.
    fixture.merge.scan(&mut Vec::new()).await;
    // The scan must have appended the CK/ACK before their production markers
    // allow retirement; flush the resulting real store before asserting.
    let mut fixture = fixture;
    fixture.flush();
    let messages = fixture.appended_messages(begin);
    let checkpoint_stored = wrapper.is_ck_stored();
    let ack_stored = wrapper.get_to_store_bits().load(Ordering::Acquire);
    let remaining_checkpoints = fixture.merge.buffer.len();
    let remaining_offsets = fixture.merge.get_offset_total_size().await;
    let point = wrapper.get_ck();
    let offset = fixture
        .merge
        .context
        .offsets
        .query_offset(&point.cid, &point.topic, point.queue_id);
    let timer_fields = fixture.timer_fields.lock().clone();
    fixture.close().await;

    assert!(checkpoint_stored);
    assert_eq!(ack_stored, 1, "partial ACK must actually reach the store path");
    assert_eq!((remaining_checkpoints, remaining_offsets, offset), (0, 0, 103));
    assert_recoverable_ack_subset(&messages, &wrapper, 1);
    assert_eq!(
        timer_fields,
        vec![
            ((point.get_revive_time() - PopAckConstants::ACK_TIME_INTERVAL) as u64, 0),
            (point.get_revive_time() as u64, 0)
        ]
    );
}

#[tokio::test]
async fn shutdown_scan_persists_batch_ack_then_retires_checkpoint_and_offset() {
    let mut fixture = Fixture::with_batch_ack(true).await;
    let wrapper = fixture.checkpoint(0).await;
    fixture.acknowledge(&wrapper, 1);
    fixture.merge.shutdown();
    fixture.merge.scan(&mut Vec::new()).await;
    fixture.flush();
    let messages = fixture.appended_messages(0);
    let state = (
        wrapper.is_ck_stored(),
        wrapper.get_to_store_bits().load(Ordering::Acquire),
        fixture.merge.buffer.len(),
        fixture.merge.get_offset_total_size().await,
        fixture
            .merge
            .context
            .offsets
            .query_offset(&wrapper.ck.cid, &wrapper.ck.topic, 0),
    );
    let timer_fields = fixture.timer_fields.lock().clone();
    let revive_time = wrapper.ck.get_revive_time() as u64;
    fixture.close().await;
    assert_eq!(state, (true, 2, 0, 0, 103));
    assert_recoverable_ack_subset(&messages, &wrapper, 2);
    assert_eq!(
        timer_fields,
        vec![
            (revive_time - PopAckConstants::ACK_TIME_INTERVAL as u64, 0),
            (revive_time, 0)
        ]
    );
}

#[tokio::test]
async fn legacy_checkpoint_without_broker_name_keeps_the_empty_revive_identity() {
    for batch in [false, true] {
        let mut fixture = Fixture::with_batch_ack(batch).await;
        let wrapper = fixture.checkpoint_with_broker(0, None).await;
        fixture.acknowledge(&wrapper, 0);
        fixture.merge.shutdown();
        fixture.merge.scan(&mut Vec::new()).await;
        fixture.flush();
        let messages = fixture.appended_messages(0);
        let state = (fixture.merge.buffer.len(), fixture.merge.get_offset_total_size().await);
        fixture.close().await;

        assert!(wrapper.ck.broker_name.is_none());
        assert_eq!(state, (0, 0));
        assert_recoverable_ack_subset(&messages, &wrapper, 1);
    }
}

#[cfg(feature = "rocksdb_store")]
#[tokio::test]
async fn kv_mirror_retirement_preserves_the_durable_revive_checkpoint_and_ack_records() {
    let mut fixture = Fixture::configured(false, true).await;
    let zero_ack = fixture.checkpoint(0).await;
    let partial_ack = fixture.checkpoint(1).await;
    let kv = Arc::clone(fixture.merge.pop_consumer_store().unwrap());
    let profiles = crate::pop::profile_store::PopConsumerProfileStore::load(Arc::clone(&kv), 8).unwrap();
    profiles
        .upsert(
            "retirement-group".into(),
            vec![
                rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData {
                    topic: "retirement-topic".into(),
                    sub_string: "*".into(),
                    ..Default::default()
                },
            ],
            rocketmq_model::common::pop_retry_policy::PopRetryPolicy::v1_only(0),
            current_millis() as i64,
        )
        .unwrap();
    let profiles_before = kv.load_profile_state().unwrap();
    assert!(profiles_before.marker.is_some());
    assert_eq!(profiles_before.records.len(), 1);
    assert_eq!(kv.scan_expired_records(0, i64::MAX, 16).unwrap().len(), 6);
    fixture.acknowledge(&partial_ack, 0);
    assert_eq!(kv.scan_expired_records(0, i64::MAX, 16).unwrap().len(), 5);
    let begin = fixture
        .broker
        .runtime_state_mut()
        .message_store()
        .unwrap()
        .get_max_phy_offset();
    fixture.merge.shutdown();
    fixture.merge.scan(&mut Vec::new()).await;
    fixture.flush();
    let store = fixture.broker.runtime_state_mut().message_store().unwrap();
    let end = store.get_max_phy_offset();
    let mut cursor = begin;
    let mut checkpoint_bodies = Vec::new();
    let mut ack_bodies = Vec::new();
    while cursor < end {
        let selected = store.select_one_message_by_offset(cursor).unwrap();
        let size = selected.size();
        let message = MessageDecoder::decode(&mut selected.into_owner_bytes(), true, true, false, false, true).unwrap();
        match message.tags().unwrap().as_str() {
            PopAckConstants::CK_TAG => {
                checkpoint_bodies.push(serde_json::from_slice::<PopCheckPoint>(message.get_body().unwrap()).unwrap())
            }
            PopAckConstants::ACK_TAG => {
                ack_bodies.push(serde_json::from_slice::<AckMsg>(message.get_body().unwrap()).unwrap())
            }
            _ => panic!("unexpected message in the isolated checkpoint/ACK append interval"),
        }
        cursor += i64::from(size);
    }
    let mirror_remaining = kv.scan_expired_records(0, i64::MAX, 16).unwrap().len();
    let profiles_after = kv.load_profile_state().unwrap();
    let state = (fixture.merge.buffer.len(), fixture.merge.get_offset_total_size().await);
    fixture.close().await;

    assert_eq!(state, (0, 0));
    assert_eq!(mirror_remaining, 0);
    assert_eq!(profiles_before.marker, profiles_after.marker);
    assert_eq!(profiles_before.records, profiles_after.records);
    assert_eq!(checkpoint_bodies.len(), 2);
    assert!(checkpoint_bodies
        .iter()
        .any(|point| point.queue_id == zero_ack.ck.queue_id && point.num == 3));
    assert!(checkpoint_bodies
        .iter()
        .any(|point| point.queue_id == partial_ack.ck.queue_id && point.num == 3));
    assert_eq!(ack_bodies.len(), 1);
    assert_eq!(ack_bodies[0].queue_id, partial_ack.ck.queue_id);
    assert_eq!(ack_bodies[0].ack_offset, 100);
}

#[tokio::test]
async fn retirement_rechecks_ack_received_after_the_scan_selected_its_candidate() {
    let mut fixture = Fixture::new().await;
    let wrapper = fixture.checkpoint(0).await;
    fixture.persist_checkpoint_fixture(&wrapper).await;
    fixture.flush();
    let (candidate_ready, wait_candidate) = tokio::sync::oneshot::channel();
    let (ack_ready, wait_ack) = tokio::sync::oneshot::channel();
    let scanner = async {
        let candidate = Arc::clone(&wrapper);
        assert!(is_ck_done_for_finish(&candidate));
        candidate_ready.send(()).unwrap();
        wait_ack.await.unwrap();
        let retired = fixture
            .merge
            .retire_checkpoint(candidate.get_merge_key(), &candidate)
            .is_some();
        let queued = fixture.merge.scan_commit_offset().await;
        (retired, queued)
    };
    let acknowledgement = async {
        wait_candidate.await.unwrap();
        fixture.acknowledge(&wrapper, 0);
        ack_ready.send(()).unwrap();
    };
    let ((retired_before_durability, queued_before_durability), ()) =
        tokio::time::timeout(Duration::from_secs(5), async { tokio::join!(scanner, acknowledgement) })
            .await
            .unwrap();
    let before = fixture
        .merge
        .context
        .offsets
        .query_offset(&wrapper.ck.cid, &wrapper.ck.topic, 0);
    fixture.persist_ack_fixture(&wrapper, 0).await;
    fixture.flush();
    // Even a currently durable subset cannot commit while its exact wrapper is
    // still in the buffer and can accept another ACK.
    let queued_before_retirement = fixture.merge.scan_commit_offset().await;
    let offset_before_retirement = fixture
        .merge
        .context
        .offsets
        .query_offset(&wrapper.ck.cid, &wrapper.ck.topic, 0);
    let retired_after_durability = fixture
        .merge
        .retire_checkpoint(wrapper.get_merge_key(), &wrapper)
        .is_some();
    let remaining = fixture.merge.scan_commit_offset().await;
    let after = fixture
        .merge
        .context
        .offsets
        .query_offset(&wrapper.ck.cid, &wrapper.ck.topic, 0);
    fixture.close().await;

    assert!(!retired_before_durability);
    assert_eq!((queued_before_durability, before), (1, -1));
    assert_eq!((queued_before_retirement, offset_before_retirement), (1, -1));
    assert!(retired_after_durability);
    assert_eq!((remaining, after), (0, 103));
}

#[tokio::test]
async fn offset_queue_does_not_commit_a_wrapper_before_its_buffer_insertion() {
    let mut fixture = Fixture::new().await;
    let point = Arc::new(PopCheckPoint {
        topic: "retirement-topic".into(),
        cid: "retirement-group".into(),
        queue_id: 1,
        pop_time: current_millis() as i64,
        invisible_time: 60_000,
        start_offset: 100,
        num: 3,
        queue_offset_diff: vec![0, 1, 2],
        broker_name: Some("retirement-broker".into()),
        ..Default::default()
    });
    let wrapper = Arc::new(PopCheckPointWrapper::new(0, -1, Arc::clone(&point), 103));
    fixture.persist_checkpoint_fixture(&wrapper).await;
    fixture.flush();
    // add_ck publishes the offset-queue entry before its buffer entry.
    fixture.merge.put_offset_queue(Arc::clone(&wrapper)).await.unwrap();
    let queued = fixture.merge.scan_commit_offset().await;
    let before = fixture.merge.context.offsets.query_offset(&point.cid, &point.topic, 1);
    fixture
        .merge
        .buffer
        .insert(wrapper.get_merge_key().clone(), Arc::clone(&wrapper));
    fixture.merge.counter.fetch_add(1, Ordering::AcqRel);
    fixture.merge.scan(&mut Vec::new()).await;
    let remaining = fixture.merge.get_offset_total_size().await;
    let after = fixture.merge.context.offsets.query_offset(&point.cid, &point.topic, 1);
    fixture.close().await;

    assert_eq!((queued, before), (1, -1));
    assert_eq!((remaining, after), (0, 103));
}

#[tokio::test]
async fn offset_only_checkpoint_keeps_its_existing_retirement_semantics() {
    let mut fixture = Fixture::new().await;
    let point = PopCheckPoint {
        topic: "retirement-topic".into(),
        cid: "retirement-group".into(),
        pop_time: current_millis() as i64,
        invisible_time: 60_000,
        broker_name: Some("retirement-broker".into()),
        ..Default::default()
    };
    fixture
        .merge
        .add_ck_just_offset(point.clone(), 0, -1, 107)
        .await
        .unwrap();
    fixture.flush();
    fixture.merge.scan(&mut Vec::new()).await;
    let state = (
        fixture.merge.buffer.len(),
        fixture.merge.get_offset_total_size().await,
        fixture.merge.context.offsets.query_offset(&point.cid, &point.topic, 0),
    );
    fixture.close().await;
    assert_eq!(state, (0, 0, 107));
}

#[tokio::test]
async fn shutdown_scan_keeps_checkpoints_and_offsets_when_real_ck_or_ack_appends_fail() {
    let mut fixture = Fixture::new().await;
    let pending_ack = fixture.checkpoint(0).await;
    let pending_ck = fixture.checkpoint(1).await;
    fixture.persist_checkpoint_fixture(&pending_ack).await;
    fixture.acknowledge(&pending_ack, 0);
    fixture.flush();
    let store = fixture.broker.runtime_state_mut().message_store().unwrap();
    let before = store.get_max_phy_offset();
    assert!(store.get_running_flags().get_and_make_not_writeable());
    fixture.merge.shutdown();
    fixture.merge.scan(&mut Vec::new()).await;
    let after = fixture
        .broker
        .runtime_state_mut()
        .message_store()
        .unwrap()
        .get_max_phy_offset();
    let state = (
        pending_ack.is_ck_stored(),
        pending_ack.get_to_store_bits().load(Ordering::Acquire),
        pending_ck.is_ck_stored(),
        fixture.merge.buffer.len(),
        fixture.merge.get_offset_total_size().await,
    );
    let offsets = [0, 1].map(|queue| {
        fixture
            .merge
            .context
            .offsets
            .query_offset(&pending_ack.ck.cid, &pending_ack.ck.topic, queue)
    });
    // Restore the injected store rejection before draining the fixture owner.
    fixture
        .broker
        .runtime_state_mut()
        .message_store()
        .unwrap()
        .get_running_flags()
        .get_and_make_writeable();
    fixture.close().await;

    assert_eq!(state, (true, 0, false, 2, 2));
    assert_eq!(offsets, [-1, -1]);
    assert_eq!(after, before, "neither rejected append may be reported durable");
}

#[tokio::test]
async fn production_checkpoint_builder_appends_with_the_absolute_revive_deadline() {
    let fixture = Fixture::new().await;
    let wrapper = fixture.checkpoint(0).await;
    let point = wrapper.get_ck();
    let message = PopMessageProcessor::<BrokerMessageStore>::build_ck_msg(
        fixture.merge.context.policy.snapshot().store_host,
        point,
        wrapper.revive_queue_id,
        fixture.merge.revive_topic.clone(),
    );
    let expected_deadline = (point.get_revive_time() - PopAckConstants::ACK_TIME_INTERVAL) as u64;
    let actual_deadline = message.get_deliver_time_ms();
    let relative_delay = message.get_delay_time_ms();
    let result = fixture.merge.context.store.put_specific(message).await.unwrap();
    let status = result.put_message_status();
    fixture.close().await;

    assert_eq!(
        status,
        PutMessageStatus::PutOk,
        "production CK must pass the actual store timer admission"
    );
    assert_eq!(actual_deadline, expected_deadline);
    assert_eq!(relative_delay, 0, "an epoch timestamp is not a relative timer delay");
}
