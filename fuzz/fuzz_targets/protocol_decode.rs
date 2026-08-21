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

#![no_main]

use std::hint::black_box;

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::protocol::header::create_topic_request_header::CreateTopicRequestHeader;
use rocketmq_protocol::protocol::header::delete_topic_request_header::DeleteTopicRequestHeader;
use rocketmq_protocol::protocol::header::get_consume_stats_request_header::GetConsumeStatsRequestHeader;
use rocketmq_protocol::protocol::header::get_max_offset_request_header::GetMaxOffsetRequestHeader;
use rocketmq_protocol::protocol::header::get_min_offset_request_header::GetMinOffsetRequestHeader;
use rocketmq_protocol::protocol::header::get_topic_stats_request_header::GetTopicStatsRequestHeader;
use rocketmq_protocol::protocol::header::namesrv::brokerid_change_request_header::NotifyMinBrokerIdChangeRequestHeader;
use rocketmq_protocol::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_protocol::protocol::header::query_topics_by_consumer_request_header::QueryTopicsByConsumerRequestHeader;
use rocketmq_protocol::protocol::header::reset_master_flush_offset_header::ResetMasterFlushOffsetHeader;
use rocketmq_protocol::RemotingCommand;
use rocketmq_rust_fuzz::corpus_bytes;

fuzz_target!(|input: &[u8]| {
    if input.len() > 1024 * 1024 {
        return;
    }
    let input = corpus_bytes(input);
    let input_len = input.len();
    let advertised_frame_len = advertised_complete_frame_len(input.as_ref());
    let mut frame = BytesMut::from(input.as_ref());

    match RemotingCommand::decode(&mut frame) {
        Ok(None) => {
            assert_eq!(
                frame.as_ref(),
                input.as_ref(),
                "incomplete frames must remain unchanged"
            );
        }
        Ok(Some(command)) => {
            if let Some(advertised_frame_len) = advertised_frame_len {
                let consumed = input_len.saturating_sub(frame.len());
                assert_eq!(
                    consumed, advertised_frame_len,
                    "a successful decoder must consume exactly one declared frame"
                );
                if let Some(trailing) = input.get(advertised_frame_len..) {
                    assert_eq!(frame.as_ref(), trailing, "bytes after the declared frame must remain");
                }
            }
            traverse_extension_fields(&command);
            decode_required_request_header(&command);
        }
        Err(_) => {}
    }
});

fn advertised_complete_frame_len(input: &[u8]) -> Option<usize> {
    const FRAME_LENGTH_BYTES: usize = 4;

    let prefix = input.get(..FRAME_LENGTH_BYTES)?;
    let declared_payload_len =
        usize::try_from(i32::from_be_bytes([prefix[0], prefix[1], prefix[2], prefix[3]])).ok()?;
    let declared_frame_len = declared_payload_len.checked_add(FRAME_LENGTH_BYTES)?;
    (declared_frame_len <= input.len()).then_some(declared_frame_len)
}

fn traverse_extension_fields(command: &RemotingCommand) {
    let Some(fields) = command.ext_fields() else {
        return;
    };

    for (key, value) in fields {
        let _ = black_box((key.as_str(), value.as_str()));
    }
}

fn decode_required_request_header(command: &RemotingCommand) {
    match RequestCode::from(command.code()) {
        RequestCode::PullMessage | RequestCode::LitePullMessage => {
            let _ = command
                .decode_required_header_fast::<PullMessageRequestHeader>("fuzz decode pull-message request header");
        }
        RequestCode::GetMaxOffset => {
            let _ = command.decode_required_header::<GetMaxOffsetRequestHeader>("fuzz decode get-max-offset header");
        }
        RequestCode::GetMinOffset => {
            let _ = command.decode_required_header::<GetMinOffsetRequestHeader>("fuzz decode get-min-offset header");
        }
        RequestCode::UpdateAndCreateTopic => {
            let _ = command.decode_required_header::<CreateTopicRequestHeader>("fuzz decode create-topic header");
        }
        RequestCode::DeleteTopicInBroker => {
            let _ = command.decode_required_header::<DeleteTopicRequestHeader>("fuzz decode delete-topic header");
        }
        RequestCode::GetTopicStatsInfo => {
            let _ = command.decode_required_header::<GetTopicStatsRequestHeader>("fuzz decode get-topic-stats header");
        }
        RequestCode::GetConsumeStats => {
            let _ = command.decode_required_header::<GetConsumeStatsRequestHeader>("fuzz decode consume-stats header");
        }
        RequestCode::QueryTopicsByConsumer => {
            let _ = command.decode_required_header::<QueryTopicsByConsumerRequestHeader>(
                "fuzz decode query-topics-by-consumer header",
            );
        }
        RequestCode::NotifyMinBrokerIdChange => {
            let _ = command.decode_required_header::<NotifyMinBrokerIdChangeRequestHeader>(
                "fuzz decode minimum-broker-id change header",
            );
        }
        RequestCode::ResetMasterFlushOffset => {
            let _ = command
                .decode_required_header::<ResetMasterFlushOffsetHeader>("fuzz decode reset-master-flush-offset header");
        }
        _ => {}
    }
}
