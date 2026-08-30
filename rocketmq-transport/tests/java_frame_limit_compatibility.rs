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

#![cfg(feature = "test-support")]

use bytes::BytesMut;
use rocketmq_protocol::protocol::encoded_frame::EncodedFrame;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_transport::api::FrameLimits;
use rocketmq_transport::test_support::RemotingCommandCodec;
use tokio_util::codec::Decoder;
use tokio_util::codec::Encoder;

const JAVA_FRAME_MAX_LENGTH: usize = 16 * 1024 * 1024;

fn java_netty_boundary_fixture() -> serde_json::Value {
    serde_json::from_str(include_str!("fixtures/java_netty_frame_limit_boundaries.json"))
        .expect("valid Java Netty boundary fixture")
}

fn command_with_total_wire_bytes(total_wire_bytes: usize) -> RemotingCommand {
    let command = RemotingCommand::create_remoting_command(105);
    let header_only = EncodedFrame::from_command(command.clone()).unwrap().encoded_len();
    command.set_body(vec![0_u8; total_wire_bytes - header_only])
}

// RocketMQ Java NettyDecoder configures LengthFieldBasedFrameDecoder with offset=0,
// length=4, adjustment=0, strip=4. Netty adds the four-byte length-field end offset before
// comparing maxFrameLength, so the configured 16 MiB is the complete wire frame, not the
// announced payload after the prefix.
#[test]
fn java_netty_frame_max_length_includes_the_four_byte_prefix() {
    let fixture = java_netty_boundary_fixture();
    assert_eq!(fixture["decoder"], "org.apache.rocketmq.remoting.netty.NettyDecoder");
    assert_eq!(fixture["maxFrameLength"], JAVA_FRAME_MAX_LENGTH);
    assert_eq!(fixture["lengthFieldOffset"], 0);
    assert_eq!(fixture["lengthFieldLength"], 4);
    assert_eq!(fixture["lengthAdjustment"], 0);
    assert_eq!(fixture["initialBytesToStrip"], 4);
    assert_eq!(fixture["cases"][0]["totalWireBytes"], JAVA_FRAME_MAX_LENGTH - 1);
    assert_eq!(fixture["cases"][0]["accepted"], true);
    assert_eq!(fixture["cases"][1]["totalWireBytes"], JAVA_FRAME_MAX_LENGTH);
    assert_eq!(fixture["cases"][1]["accepted"], true);
    assert_eq!(fixture["cases"][2]["totalWireBytes"], JAVA_FRAME_MAX_LENGTH + 1);
    assert_eq!(fixture["cases"][2]["accepted"], false);

    let limits = FrameLimits::java_compatibility();
    assert_eq!(limits.max_frame_bytes, JAVA_FRAME_MAX_LENGTH);

    let exact = command_with_total_wire_bytes(JAVA_FRAME_MAX_LENGTH);
    let mut codec = RemotingCommandCodec::with_limits(limits);
    let mut exact_wire = BytesMut::new();
    codec.encode(exact, &mut exact_wire).unwrap();
    assert_eq!(exact_wire.len(), JAVA_FRAME_MAX_LENGTH);
    assert!(codec.decode(&mut exact_wire).unwrap().is_some());

    let over = command_with_total_wire_bytes(JAVA_FRAME_MAX_LENGTH + 1);
    let mut destination = BytesMut::new();
    assert!(codec.encode(over, &mut destination).is_err());
    assert!(destination.is_empty());
}

#[test]
fn oversized_java_announcement_is_rejected_with_an_eight_byte_allocation_bound() {
    let limits = FrameLimits::java_compatibility();
    let announced_after_prefix = limits.max_frame_bytes - 4 + 1;
    let mut announced = BytesMut::with_capacity(8);
    announced.extend_from_slice(&(announced_after_prefix as i32).to_be_bytes());
    announced.extend_from_slice(&0_u32.to_be_bytes());
    let capacity_before = announced.capacity();
    let mut codec = RemotingCommandCodec::with_limits(limits);

    assert!(codec.decode(&mut announced).is_err());
    assert_eq!(capacity_before, 8);
    assert_eq!(announced.capacity(), capacity_before);
}
