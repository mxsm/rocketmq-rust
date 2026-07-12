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

use bytes::BytesMut;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_transport::codec::remoting_command_codec::FrameLimits;
use rocketmq_transport::codec::remoting_command_codec::RemotingCommandCodec;
use tokio_util::codec::Decoder;
use tokio_util::codec::Encoder;

#[test]
fn fragmented_frame_waits_and_then_decodes_without_eager_megabyte_allocation() {
    let mut codec = RemotingCommandCodec::with_limits(FrameLimits {
        max_frame_bytes: 1024,
        max_header_bytes: 512,
        max_body_bytes: 512,
        initial_read_bytes: 16,
    });
    let command = RemotingCommand::create_remoting_command(105);
    let mut encoded = BytesMut::new();
    codec.encode(command, &mut encoded).unwrap();

    let split = encoded.len() / 2;
    let tail = encoded.split_off(split);
    assert!(codec.decode(&mut encoded).unwrap().is_none());
    encoded.extend_from_slice(&tail);
    assert_eq!(codec.decode(&mut encoded).unwrap().unwrap().code(), 105);
    assert!(encoded.capacity() < 1024 * 1024);
}

#[test]
fn oversized_frame_is_rejected_before_body_allocation() {
    let mut codec = RemotingCommandCodec::with_limits(FrameLimits {
        max_frame_bytes: 32,
        max_header_bytes: 16,
        max_body_bytes: 16,
        initial_read_bytes: 8,
    });
    let mut announced = BytesMut::from(&[0, 0, 4, 0][..]);
    assert!(codec.decode(&mut announced).is_err());
    assert!(announced.capacity() < 1024);
}
