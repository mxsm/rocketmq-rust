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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::header::pop_message_response_header::PopMessageResponseHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_store::GetMessageResult;
use rocketmq_store::SelectMappedBufferResult;
use rocketmq_transport::api::v1::Channel;
use rocketmq_transport::api::v2::HandlerOutcome;
use rocketmq_transport::api::v2::ResponseBodyKind;
use rocketmq_transport::test_support::Connection;
use rocketmq_transport::test_support::TestChannelBuilder;

use super::attach_pop_response_header;
use super::deliver_pop_legacy;
use super::pop_heap_response_parts;
use super::pop_segmented_response_parts;
use super::take_pop_body_segments;

fn response_header() -> PopMessageResponseHeader {
    PopMessageResponseHeader {
        pop_time: 17,
        invisible_time: 23,
        revive_qid: 3,
        rest_num: 5,
        start_offset_info: Some(CheetahString::from_static_str("start")),
        msg_offset_info: Some(CheetahString::from_static_str("message")),
        order_count_info: Some(CheetahString::from_static_str("order")),
    }
}

fn assert_response_header(actual: &PopMessageResponseHeader, expected: &PopMessageResponseHeader) {
    assert_eq!(actual.pop_time, expected.pop_time);
    assert_eq!(actual.invisible_time, expected.invisible_time);
    assert_eq!(actual.revive_qid, expected.revive_qid);
    assert_eq!(actual.rest_num, expected.rest_num);
    assert_eq!(actual.start_offset_info, expected.start_offset_info);
    assert_eq!(actual.msg_offset_info, expected.msg_offset_info);
    assert_eq!(actual.order_count_info, expected.order_count_info);
}

#[test]
fn success_head_is_body_free_and_has_identical_heap_and_segment_metadata() {
    let expected = response_header();
    let heap_head = attach_pop_response_header(RemotingCommand::create_success_response_command(), expected.clone());
    let segment_head = attach_pop_response_header(RemotingCommand::create_success_response_command(), expected.clone());

    for head in [&heap_head, &segment_head] {
        assert!(head.body().is_none());
        let actual = head
            .read_custom_header_ref::<PopMessageResponseHeader>()
            .expect("POP response header must remain attached to the body-free head");
        assert_response_header(actual, &expected);
    }
}

#[test]
fn heap_success_builds_one_bytes_reply_without_a_channel() {
    let body = Bytes::from_static(b"heap-pop-body");
    let head = attach_pop_response_header(RemotingCommand::create_success_response_command(), response_header());
    let outcome = pop_heap_response_parts(head, Some(body.clone()))
        .expect("heap response parts")
        .into_handler_outcome()
        .expect("heap response plan");
    let HandlerOutcome::Reply(plan) = outcome else {
        panic!("immediate POP success must be represented by a reply plan");
    };

    assert_eq!(ResponseCode::Success as i32, plan.response_code());
    assert_eq!(ResponseBodyKind::Bytes, plan.body_kind());
    assert_eq!(body.len(), plan.body_len());
    assert_eq!(1, plan.body_part_count());
}

#[test]
fn non_heap_success_moves_ordered_body_only_segments_into_a_reply() {
    let first = Bytes::from_static(b"\0\0\0\x08body-one");
    let second = Bytes::from_static(b"body-two");
    let first_pointer = first.as_ptr();
    let second_pointer = second.as_ptr();
    let mut result = GetMessageResult::new();
    result.add_message_inner(SelectMappedBufferResult::from_bytes(0, first).expect("first POP selection"));
    result.add_message_inner(SelectMappedBufferResult::from_bytes(8, second).expect("second POP selection"));

    let body_segments = take_pop_body_segments(result);
    assert_eq!(2, body_segments.len());
    assert_eq!(b"\0\0\0\x08body-one"[..], body_segments[0]);
    assert_eq!(b"body-two"[..], body_segments[1]);
    assert_eq!(first_pointer, body_segments[0].as_ptr());
    assert_eq!(second_pointer, body_segments[1].as_ptr());

    let body_len = body_segments.iter().map(Bytes::len).sum::<usize>();
    let head = attach_pop_response_header(RemotingCommand::create_success_response_command(), response_header());
    let outcome = pop_segmented_response_parts(head, body_segments)
        .expect("segmented response parts")
        .into_handler_outcome()
        .expect("segmented response plan");
    let HandlerOutcome::Reply(plan) = outcome else {
        panic!("immediate non-heap POP success must be represented by a reply plan");
    };

    assert_eq!(ResponseCode::Success as i32, plan.response_code());
    assert_eq!(ResponseBodyKind::Segments, plan.body_kind());
    assert_eq!(body_len, plan.body_len());
    assert_eq!(2, plan.body_part_count());
}

struct CountingBodyOwner {
    body: &'static [u8],
    drops: Arc<AtomicUsize>,
}

impl AsRef<[u8]> for CountingBodyOwner {
    fn as_ref(&self) -> &[u8] {
        self.body
    }
}

impl Drop for CountingBodyOwner {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

async fn closed_test_channel() -> Channel {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind POP compatibility listener");
    let address = listener.local_addr().expect("POP compatibility address");
    let stream = std::net::TcpStream::connect(address).expect("connect POP compatibility stream");
    let accepted = listener.accept().expect("accept POP compatibility stream").0;
    stream.set_nonblocking(true).expect("set POP stream nonblocking");

    let mut connection = Connection::new(tokio::net::TcpStream::from_std(stream).expect("Tokio POP stream"));
    connection.shutdown().await.expect("shut down POP test connection");
    drop(accepted);

    TestChannelBuilder::new(connection, crate::test_task_group("pop-legacy-closed-channel"))
        .addresses(address, address)
        .build()
        .expect("build closed POP test channel")
}

#[tokio::test]
async fn segmented_legacy_write_failure_consumes_and_drops_body_once() {
    let channel = closed_test_channel().await;
    let drops = Arc::new(AtomicUsize::new(0));
    let body = Bytes::from_owner(CountingBodyOwner {
        body: b"segmented-pop-body",
        drops: Arc::clone(&drops),
    });
    let head = attach_pop_response_header(RemotingCommand::create_success_response_command(), response_header());
    let parts = pop_segmented_response_parts(head, vec![body]).expect("segmented POP response parts");
    assert_eq!(0, drops.load(Ordering::SeqCst));

    let result = deliver_pop_legacy(parts, &channel).await;

    assert!(result.is_err(), "closed POP connection must reject the write");
    assert_eq!(1, drops.load(Ordering::SeqCst));
}
