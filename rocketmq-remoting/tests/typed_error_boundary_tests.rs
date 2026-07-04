use bytes::BytesMut;
use rocketmq_error::RocketMQError;
use rocketmq_remoting::code::BrokerRequestCode;
use rocketmq_remoting::protocol::header::extra_info_util::ExtraInfoUtil;
use rocketmq_remoting::rocketmq_serializable::RocketMQSerializable;

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
        include_str!("../src/code/broker_request_code.rs"),
        include_str!("../src/codec/remoting_command_codec.rs"),
        include_str!("../src/protocol/header/extra_info_util.rs"),
        include_str!("../src/protocol/rocketmq_serializable.rs"),
    ];

    for source in files {
        assert!(!source.contains("RocketmqError"));
        assert!(!source.contains("DecodingError"));
        assert!(!source.contains("FromStrErr"));
        assert!(!source.contains("IllegalArgumentError"));
        assert!(!source.contains("RemotingCommandEncoderError"));
    }
}
