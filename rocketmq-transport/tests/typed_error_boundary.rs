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

use bytes::BytesMut;
use rocketmq_error::RocketMQError;
use rocketmq_protocol::code::BrokerRequestCode;
use rocketmq_protocol::protocol::header::extra_info_util::ExtraInfoUtil;
use rocketmq_protocol::protocol::rocketmq_serializable::RocketMQSerializable;

#[test]
fn broker_request_code_parse_returns_typed_error() {
    let err: RocketMQError = "UNKNOWN".parse::<BrokerRequestCode>().unwrap_err();

    assert!(matches!(err, RocketMQError::IllegalArgument(_)));
}

#[test]
fn remoting_decode_boundaries_return_typed_serialization_error() {
    let mut buf = BytesMut::from(&[0_u8][..]);
    let err = RocketMQSerializable::read_str(&mut buf, true, 10).unwrap_err();

    assert!(matches!(err, RocketMQError::Serialization(_)));
}

#[test]
fn extra_info_boundaries_return_typed_illegal_argument_error() {
    let err = ExtraInfoUtil::get_ck_queue_offset(&[]).unwrap_err();

    assert!(matches!(err, RocketMQError::IllegalArgument(_)));
}

#[test]
fn remoting_boundary_files_do_not_use_legacy_error_enum() {
    let files = [
        include_str!("../../rocketmq-protocol/src/code/broker_request_code.rs"),
        include_str!("../../rocketmq-transport/src/codec/remoting_command_codec.rs"),
        include_str!("../../rocketmq-protocol/src/protocol/header/extra_info_util.rs"),
        include_str!("../../rocketmq-protocol/src/protocol/rocketmq_serializable.rs"),
    ];

    for source in files {
        assert!(!source.contains(concat!("Rocket", "mqError")));
        assert!(!source.contains("DecodingError"));
        assert!(!source.contains("FromStrErr"));
        assert!(!source.contains("IllegalArgumentError"));
        assert!(!source.contains("RemotingCommandEncoderError"));
    }
}
