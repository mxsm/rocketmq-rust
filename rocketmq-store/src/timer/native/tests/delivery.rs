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

use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use cheetah_string::CheetahString;
use parking_lot::Mutex;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_protocol::common::message::message_decoder as MessageDecoder;

use super::super::delivery::NativeCommitLogRead;
use super::super::delivery::NativeCommitLogReader;
use super::super::delivery::NativeDeliveryReport;
use super::super::delivery::NativeTimerWriter;
use super::super::delivery::NativeWriteOutcome;
use super::*;

/// The stored timer message of timer `cq_offset`.
fn message(cq_offset: i64, generation: u64) -> MessageExt {
    let mut message = MessageExt::default();
    message
        .message
        .set_topic(CheetahString::from_static_str("rmq_sys_wheel_timer"));
    message.set_queue_offset(cq_offset);
    message.set_commit_log_offset(cq_offset * 100);
    message
        .message
        .set_body(Some(Bytes::from(format!("body-{cq_offset}").into_bytes())));
    message.message.set_properties(HashMap::from(
        [
            (MessageConst::PROPERTY_REAL_TOPIC, TOPIC.to_owned()),
            (MessageConst::PROPERTY_REAL_QUEUE_ID, "0".to_owned()),
            (
                MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
                format!("key-{cq_offset}"),
            ),
            (MessageConst::PROPERTY_TIMER_GENERATION, generation.to_string()),
        ]
        .map(|(key, value)| (CheetahString::from_static_str(key), CheetahString::from_string(value))),
    ));
    message
}

/// A CommitLog holding the records of timers, at the offsets [`timer`] gives them.
#[derive(Default)]
struct CommitLog {
    messages: HashMap<i64, MessageExt>,
    /// Records below this are deleted.
    cleaned_below: i64,
}

impl CommitLog {
    fn of(cq_offsets: impl IntoIterator<Item = i64>) -> Self {
        let mut commit_log = Self::default();
        for cq_offset in cq_offsets {
            commit_log.put(message(cq_offset, 0));
        }
        commit_log
    }

    fn put(&mut self, message: MessageExt) {
        self.messages.insert(message.commit_log_offset(), message);
    }
}

impl NativeCommitLogReader for CommitLog {
    fn read(&self, offset: i64, _size: i32) -> NativeCommitLogRead {
        if offset < self.cleaned_below {
            return NativeCommitLogRead::CleanedUp;
        }
        match self.messages.get(&offset) {
            Some(message) => NativeCommitLogRead::Message(message.clone()),
            None => NativeCommitLogRead::Unreadable,
        }
    }
}

/// Records the writes it is given; a write of a unique key in `stays` stays.
#[derive(Default)]
struct Writer {
    written: Mutex<Vec<MessageExtBrokerInner>>,
    stays: Mutex<HashSet<String>>,
    in_flight: AtomicUsize,
    most_in_flight: AtomicUsize,
}

impl Writer {
    fn keys(&self) -> Vec<String> {
        self.written.lock().iter().map(unique_key).collect()
    }
}

impl NativeTimerWriter for Writer {
    fn write(&self, message: MessageExtBrokerInner, _epoch: u64) -> impl Future<Output = NativeWriteOutcome> + Send {
        let outcome = if self.stays.lock().contains(&unique_key(&message)) {
            NativeWriteOutcome::Stays
        } else {
            NativeWriteOutcome::Written
        };
        self.written.lock().push(message);
        let in_flight = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.most_in_flight.fetch_max(in_flight, Ordering::SeqCst);
        async move {
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            outcome
        }
    }
}

fn unique_key(message: &MessageExtBrokerInner) -> String {
    message
        .property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
        ))
        .map(|key| key.to_string())
        .unwrap_or_default()
}

/// Prepares, writes and settles one batch of the test lane at `now_ms`, everything before it
/// indexed; returns what settling did and the timer that blocked the batch.
async fn run(
    engine: &NativeTimerEngine,
    now_ms: i64,
    commit_log: &CommitLog,
    writer: &Writer,
    skip_unknown_error: bool,
    max_in_flight: usize,
) -> (NativeDeliveryReport, Option<i64>) {
    engine.set_visible_through(now_ms);
    let batch = engine
        .prepare_due(lane(), now_ms, 1_000, commit_log, skip_unknown_error)
        .unwrap();
    let blocked_at = batch.blocked_at;
    let written = batch.write(writer, 1, max_in_flight).await;
    (engine.settle(written), blocked_at)
}

fn watermark(engine: &NativeTimerEngine, now_ms: i64) -> NativeDueCursor {
    engine.checkpoint(COMMIT_LOG_END, now_ms).unwrap().lane_watermarks[usize::from(lane())]
}

#[tokio::test]
async fn due_timers_are_written_to_their_real_topic_in_lane_order() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    ingest(
        &engine,
        &[timer(1, H0 + 2_000), timer(2, H0 + 1_000), timer(3, H0 + 2_000)],
        H0,
    );
    let writer = Writer::default();
    let (report, blocked_at) = run(&engine, H0 + 2_000, &CommitLog::of([1, 2, 3]), &writer, false, 4).await;
    assert_eq!((report.written, report.requeued, blocked_at), (3, 0, None));
    assert_eq!(writer.keys(), ["key-2", "key-1", "key-3"]);

    let written = writer.written.lock();
    let first = &written[0];
    assert_eq!(first.get_topic().as_str(), TOPIC);
    assert_eq!(first.get_body().map(|body| body.to_vec()), Some(b"body-2".to_vec()));
    assert!(first
        .property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_TIMER_DELIVERY_TOKEN
        ))
        .is_some());
    assert_eq!(
        watermark(&engine, H0 + 2_000),
        NativeDueCursor {
            due_ms: H0 + 2_000,
            cq_offset: 3,
        }
    );
}

#[tokio::test]
async fn a_recall_cancels_a_timer_only_when_it_names_the_message() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    let other_topic = NativeTimerSource {
        kind: NativeSourceKind::Recall {
            key: "OtherTopic_key-3".to_owned(),
        },
        ..timer(6, H0 + 1_000)
    };
    ingest(
        &engine,
        &[
            timer(1, H0 + 1_000),
            timer(2, H0 + 1_000),
            timer(3, H0 + 1_000),
            recall(4, 1, H0 + 1_000),
            // Of generation 0, while timer 2 is of generation 5.
            recall(5, 2, H0 + 1_000),
            other_topic,
        ],
        H0,
    );
    let mut commit_log = CommitLog::of([1, 3]);
    commit_log.put(message(2, 5));
    let writer = Writer::default();
    let (report, _) = run(&engine, H0 + 1_000, &commit_log, &writer, false, 4).await;
    assert_eq!((report.recalled, report.written), (1, 2));
    assert_eq!(writer.keys(), ["key-2", "key-3"]);
}

#[tokio::test]
async fn a_copied_timer_is_delivered_from_its_copy() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    let due_ms = D3 + 5 * MINUTE_MS;
    let copied = NativeTimerSource {
        frame: MessageDecoder::encode(&message(1, 0), false).unwrap(),
        ..timer(1, due_ms)
    };
    ingest(&engine, &[copied], H0);
    let writer = Writer::default();
    // The CommitLog dropped the record long ago.
    let (report, _) = run(&engine, due_ms, &CommitLog::default(), &writer, false, 4).await;
    assert_eq!(report.written, 1);
    let written = writer.written.lock();
    assert_eq!(
        written[0].get_body().map(|body| body.to_vec()),
        Some(b"body-1".to_vec())
    );
}

#[tokio::test]
async fn a_write_that_stays_puts_it_back_with_every_timer_after_it() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    ingest(
        &engine,
        &(1..=4).map(|cq| timer(cq, H0 + 1_000)).collect::<Vec<_>>(),
        H0,
    );
    let commit_log = CommitLog::of(1..=4);
    let writer = Writer::default();
    writer.stays.lock().insert("key-2".to_owned());
    // Timers 3 and 4 were in flight with timer 2 and land, yet go back with it.
    let (report, _) = run(&engine, H0 + 1_000, &commit_log, &writer, false, 4).await;
    assert_eq!((report.written, report.requeued), (1, 3));
    assert_eq!(writer.keys(), ["key-1", "key-2", "key-3", "key-4"]);
    assert_eq!(
        watermark(&engine, H0 + 1_000),
        NativeDueCursor {
            due_ms: H0 + 1_000,
            cq_offset: 1,
        }
    );

    // One write at a time: none starts after the one that stays.
    let writer = Writer::default();
    writer.stays.lock().insert("key-3".to_owned());
    let (report, _) = run(&engine, H0 + 1_000, &commit_log, &writer, false, 1).await;
    assert_eq!((report.written, report.requeued), (1, 2));
    assert_eq!(writer.keys(), ["key-2", "key-3"]);

    let writer = Writer::default();
    let (report, _) = run(&engine, H0 + 1_000, &commit_log, &writer, false, 4).await;
    assert_eq!((report.written, report.requeued), (2, 0));
    assert_eq!(writer.keys(), ["key-3", "key-4"]);
    assert_eq!(engine.counts().wheel, 0);
}

#[tokio::test]
async fn writes_in_flight_stay_within_the_bound() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    ingest(
        &engine,
        &(1..=5).map(|cq| timer(cq, H0 + 1_000)).collect::<Vec<_>>(),
        H0,
    );
    let writer = Writer::default();
    let (report, _) = run(&engine, H0 + 1_000, &CommitLog::of(1..=5), &writer, false, 2).await;
    assert_eq!(report.written, 5);
    assert_eq!(writer.most_in_flight.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn a_timer_that_cannot_be_read_or_routed_blocks_its_lane_unless_unknown_errors_are_skipped() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    ingest(
        &engine,
        &(1..=5).map(|cq| timer(cq, H0 + 1_000)).collect::<Vec<_>>(),
        H0,
    );
    // Timer 2's record is missing, timer 4's names no real topic, and the record at timer 5's
    // offset belongs to another timer.
    let mut commit_log = CommitLog::of([1, 3]);
    let mut unroutable = message(4, 0);
    unroutable.message.clear_property(MessageConst::PROPERTY_REAL_TOPIC);
    commit_log.put(unroutable);
    let mut misplaced = message(9, 0);
    misplaced.set_commit_log_offset(500);
    commit_log.put(misplaced);

    let writer = Writer::default();
    for _ in 0..2 {
        let (report, blocked_at) = run(&engine, H0 + 1_000, &commit_log, &writer, false, 4).await;
        assert_eq!(blocked_at, Some(2));
        assert_eq!(report.requeued, 0);
    }
    assert_eq!(writer.keys(), ["key-1"]);
    assert_eq!(engine.counts().wheel, 4);

    let (report, blocked_at) = run(&engine, H0 + 1_000, &commit_log, &writer, true, 4).await;
    assert_eq!((report.skipped, report.written, blocked_at), (3, 1, None));
    assert_eq!(writer.keys(), ["key-1", "key-3"]);
    assert_eq!(engine.counts().wheel, 0);
}

#[tokio::test]
async fn a_timer_whose_record_cleanup_deleted_leaves_without_a_write() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    ingest(
        &engine,
        &(1..=3).map(|cq| timer(cq, H0 + 1_000)).collect::<Vec<_>>(),
        H0,
    );
    let commit_log = CommitLog {
        cleaned_below: 250,
        ..CommitLog::of(1..=3)
    };
    let writer = Writer::default();
    let (report, blocked_at) = run(&engine, H0 + 1_000, &commit_log, &writer, false, 4).await;
    assert_eq!((report.cleaned_up, report.written, blocked_at), (2, 1, None));
    assert_eq!(writer.keys(), ["key-3"]);
}

#[test]
fn nothing_is_taken_past_what_the_source_stage_has_indexed() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    ingest(&engine, &[timer(1, H0 + 1_000), timer(2, H0 + 3_000)], H0);
    let poll = |engine: &NativeTimerEngine| offsets(&engine.poll_due(lane(), H0 + 5_000, 10).unwrap());
    // Right after opening, a recall of either may still be on its way.
    assert!(poll(&engine).is_empty());
    engine.set_visible_through(H0 + 1_000);
    assert_eq!(poll(&engine), vec![1]);
    engine.set_visible_through(H0 + 3_000);
    engine.set_visible_through(H0);
    assert_eq!(poll(&engine), vec![2]);
}

#[test]
fn a_timer_indexed_late_while_a_batch_is_out_lands_after_that_batch() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    ingest(&engine, &[timer(1, H0 + 1_000), timer(2, H0 + 2_000)], H0);
    let batch = due(&engine, H0 + 2_000);
    assert_eq!(offsets(&batch), vec![1, 2]);
    // Due before the batch's timers, it reaches the engine while they are being written.
    ingest(&engine, &[timer(3, H0 + 500)], H0 + 2_000);
    engine.advance_delivered(lane(), batch[1].entry.cursor());
    engine.checkpoint(COMMIT_LOG_END, H0 + 2_000).unwrap();
    drop(engine);

    let engine = open(&root, config(), H0 + 2_000);
    let due = due(&engine, H0 + 2_000);
    assert_eq!(offsets(&due), vec![3]);
    assert_eq!(due[0].entry.due_ms, H0 + 2_000);
}
