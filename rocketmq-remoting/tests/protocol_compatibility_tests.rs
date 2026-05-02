use bytes::Bytes;
use bytes::BytesMut;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::RemotingSysResponseCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::codec::remoting_command_codec::RemotingCommandCodec;
use rocketmq_remoting::protocol::header::client_request_header::GetRouteInfoRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use tokio_util::codec::Decoder;
use tokio_util::codec::Encoder;

const JAVA_REQUEST_CODES: &[(&str, i32)] = &[
    ("SEND_MESSAGE", 10),
    ("PULL_MESSAGE", 11),
    ("QUERY_MESSAGE", 12),
    ("QUERY_BROKER_OFFSET", 13),
    ("QUERY_CONSUMER_OFFSET", 14),
    ("UPDATE_CONSUMER_OFFSET", 15),
    ("UPDATE_AND_CREATE_TOPIC", 17),
    ("UPDATE_AND_CREATE_TOPIC_LIST", 18),
    ("GET_ALL_TOPIC_CONFIG", 21),
    ("GET_TOPIC_CONFIG_LIST", 22),
    ("GET_TOPIC_NAME_LIST", 23),
    ("UPDATE_BROKER_CONFIG", 25),
    ("GET_BROKER_CONFIG", 26),
    ("TRIGGER_DELETE_FILES", 27),
    ("GET_BROKER_RUNTIME_INFO", 28),
    ("SEARCH_OFFSET_BY_TIMESTAMP", 29),
    ("GET_MAX_OFFSET", 30),
    ("GET_MIN_OFFSET", 31),
    ("GET_EARLIEST_MSG_STORETIME", 32),
    ("VIEW_MESSAGE_BY_ID", 33),
    ("HEART_BEAT", 34),
    ("UNREGISTER_CLIENT", 35),
    ("CONSUMER_SEND_MSG_BACK", 36),
    ("END_TRANSACTION", 37),
    ("GET_CONSUMER_LIST_BY_GROUP", 38),
    ("CHECK_TRANSACTION_STATE", 39),
    ("NOTIFY_CONSUMER_IDS_CHANGED", 40),
    ("LOCK_BATCH_MQ", 41),
    ("UNLOCK_BATCH_MQ", 42),
    ("GET_ALL_CONSUMER_OFFSET", 43),
    ("GET_ALL_DELAY_OFFSET", 45),
    ("CHECK_CLIENT_CONFIG", 46),
    ("GET_CLIENT_CONFIG", 47),
    ("GET_TIMER_CHECK_POINT", 60),
    ("GET_TIMER_METRICS", 61),
    ("POP_MESSAGE", 200050),
    ("ACK_MESSAGE", 200051),
    ("BATCH_ACK_MESSAGE", 200151),
    ("PEEK_MESSAGE", 200052),
    ("CHANGE_MESSAGE_INVISIBLETIME", 200053),
    ("NOTIFICATION", 200054),
    ("POLLING_INFO", 200055),
    ("POP_ROLLBACK", 200056),
    ("POP_LITE_MESSAGE", 200070),
    ("LITE_SUBSCRIPTION_CTL", 200071),
    ("ACK_LITE_MESSAGE", 200072),
    ("NOTIFY_UNSUBSCRIBE_LITE", 200073),
    ("GET_BROKER_LITE_INFO", 200074),
    ("GET_PARENT_TOPIC_INFO", 200075),
    ("GET_LITE_TOPIC_INFO", 200076),
    ("GET_LITE_CLIENT_INFO", 200077),
    ("GET_LITE_GROUP_INFO", 200078),
    ("TRIGGER_LITE_DISPATCH", 200079),
    ("PUT_KV_CONFIG", 100),
    ("GET_KV_CONFIG", 101),
    ("DELETE_KV_CONFIG", 102),
    ("REGISTER_BROKER", 103),
    ("UNREGISTER_BROKER", 104),
    ("GET_ROUTEINFO_BY_TOPIC", 105),
    ("GET_BROKER_CLUSTER_INFO", 106),
    ("UPDATE_AND_CREATE_SUBSCRIPTIONGROUP", 200),
    ("GET_ALL_SUBSCRIPTIONGROUP_CONFIG", 201),
    ("GET_TOPIC_STATS_INFO", 202),
    ("GET_CONSUMER_CONNECTION_LIST", 203),
    ("GET_PRODUCER_CONNECTION_LIST", 204),
    ("WIPE_WRITE_PERM_OF_BROKER", 205),
    ("GET_ALL_TOPIC_LIST_FROM_NAMESERVER", 206),
    ("DELETE_SUBSCRIPTIONGROUP", 207),
    ("GET_CONSUME_STATS", 208),
    ("SUSPEND_CONSUMER", 209),
    ("RESUME_CONSUMER", 210),
    ("RESET_CONSUMER_OFFSET_IN_CONSUMER", 211),
    ("RESET_CONSUMER_OFFSET_IN_BROKER", 212),
    ("ADJUST_CONSUMER_THREAD_POOL", 213),
    ("WHO_CONSUME_THE_MESSAGE", 214),
    ("DELETE_TOPIC_IN_BROKER", 215),
    ("DELETE_TOPIC_IN_NAMESRV", 216),
    ("REGISTER_TOPIC_IN_NAMESRV", 217),
    ("GET_KVLIST_BY_NAMESPACE", 219),
    ("RESET_CONSUMER_CLIENT_OFFSET", 220),
    ("GET_CONSUMER_STATUS_FROM_CLIENT", 221),
    ("INVOKE_BROKER_TO_RESET_OFFSET", 222),
    ("INVOKE_BROKER_TO_GET_CONSUMER_STATUS", 223),
    ("QUERY_TOPIC_CONSUME_BY_WHO", 300),
    ("GET_TOPICS_BY_CLUSTER", 224),
    ("UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST", 225),
    ("QUERY_TOPICS_BY_CONSUMER", 343),
    ("QUERY_SUBSCRIPTION_BY_CONSUMER", 345),
    ("REGISTER_FILTER_SERVER", 301),
    ("REGISTER_MESSAGE_FILTER_CLASS", 302),
    ("QUERY_CONSUME_TIME_SPAN", 303),
    ("GET_SYSTEM_TOPIC_LIST_FROM_NS", 304),
    ("GET_SYSTEM_TOPIC_LIST_FROM_BROKER", 305),
    ("CLEAN_EXPIRED_CONSUMEQUEUE", 306),
    ("GET_CONSUMER_RUNNING_INFO", 307),
    ("QUERY_CORRECTION_OFFSET", 308),
    ("CONSUME_MESSAGE_DIRECTLY", 309),
    ("SEND_MESSAGE_V2", 310),
    ("GET_UNIT_TOPIC_LIST", 311),
    ("GET_HAS_UNIT_SUB_TOPIC_LIST", 312),
    ("GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST", 313),
    ("CLONE_GROUP_OFFSET", 314),
    ("VIEW_BROKER_STATS_DATA", 315),
    ("CLEAN_UNUSED_TOPIC", 316),
    ("GET_BROKER_CONSUME_STATS", 317),
    ("UPDATE_NAMESRV_CONFIG", 318),
    ("GET_NAMESRV_CONFIG", 319),
    ("SEND_BATCH_MESSAGE", 320),
    ("QUERY_CONSUME_QUEUE", 321),
    ("QUERY_DATA_VERSION", 322),
    ("RESUME_CHECK_HALF_MESSAGE", 323),
    ("SEND_REPLY_MESSAGE", 324),
    ("SEND_REPLY_MESSAGE_V2", 325),
    ("PUSH_REPLY_MESSAGE_TO_CLIENT", 326),
    ("ADD_WRITE_PERM_OF_BROKER", 327),
    ("GET_ALL_PRODUCER_INFO", 328),
    ("DELETE_EXPIRED_COMMITLOG", 329),
    ("GET_TOPIC_CONFIG", 351),
    ("GET_SUBSCRIPTIONGROUP_CONFIG", 352),
    ("UPDATE_AND_GET_GROUP_FORBIDDEN", 353),
    ("CHECK_ROCKSDB_CQ_WRITE_PROGRESS", 354),
    ("EXPORT_ROCKSDB_CONFIG_TO_JSON", 355),
    ("LITE_PULL_MESSAGE", 361),
    ("RECALL_MESSAGE", 370),
    ("QUERY_ASSIGNMENT", 400),
    ("SET_MESSAGE_REQUEST_MODE", 401),
    ("GET_ALL_MESSAGE_REQUEST_MODE", 402),
    ("UPDATE_AND_CREATE_STATIC_TOPIC", 513),
    ("GET_BROKER_MEMBER_GROUP", 901),
    ("ADD_BROKER", 902),
    ("REMOVE_BROKER", 903),
    ("BROKER_HEARTBEAT", 904),
    ("NOTIFY_MIN_BROKER_ID_CHANGE", 905),
    ("EXCHANGE_BROKER_HA_INFO", 906),
    ("GET_BROKER_HA_STATUS", 907),
    ("RESET_MASTER_FLUSH_OFFSET", 908),
    ("CONTROLLER_ALTER_SYNC_STATE_SET", 1001),
    ("CONTROLLER_ELECT_MASTER", 1002),
    ("CONTROLLER_REGISTER_BROKER", 1003),
    ("CONTROLLER_GET_REPLICA_INFO", 1004),
    ("CONTROLLER_GET_METADATA_INFO", 1005),
    ("CONTROLLER_GET_SYNC_STATE_DATA", 1006),
    ("GET_BROKER_EPOCH_CACHE", 1007),
    ("NOTIFY_BROKER_ROLE_CHANGED", 1008),
    ("UPDATE_CONTROLLER_CONFIG", 1009),
    ("GET_CONTROLLER_CONFIG", 1010),
    ("CLEAN_BROKER_DATA", 1011),
    ("CONTROLLER_GET_NEXT_BROKER_ID", 1012),
    ("CONTROLLER_APPLY_BROKER_ID", 1013),
    ("BROKER_CLOSE_CHANNEL_REQUEST", 1014),
    ("CHECK_NOT_ACTIVE_BROKER_REQUEST", 1015),
    ("GET_BROKER_LIVE_INFO_REQUEST", 1016),
    ("GET_SYNC_STATE_DATA_REQUEST", 1017),
    ("RAFT_BROKER_HEART_BEAT_EVENT_REQUEST", 1018),
    ("UPDATE_COLD_DATA_FLOW_CTR_CONFIG", 2001),
    ("REMOVE_COLD_DATA_FLOW_CTR_CONFIG", 2002),
    ("GET_COLD_DATA_FLOW_CTR_INFO", 2003),
    ("SET_COMMITLOG_READ_MODE", 2004),
    ("AUTH_CREATE_USER", 3001),
    ("AUTH_UPDATE_USER", 3002),
    ("AUTH_DELETE_USER", 3003),
    ("AUTH_GET_USER", 3004),
    ("AUTH_LIST_USER", 3005),
    ("AUTH_CREATE_ACL", 3006),
    ("AUTH_UPDATE_ACL", 3007),
    ("AUTH_DELETE_ACL", 3008),
    ("AUTH_GET_ACL", 3009),
    ("AUTH_LIST_ACL", 3010),
    ("SWITCH_TIMER_ENGINE", 5001),
];

const JAVA_RESPONSE_CODES: &[(&str, i32)] = &[
    ("FLUSH_DISK_TIMEOUT", 10),
    ("SLAVE_NOT_AVAILABLE", 11),
    ("FLUSH_SLAVE_TIMEOUT", 12),
    ("MESSAGE_ILLEGAL", 13),
    ("SERVICE_NOT_AVAILABLE", 14),
    ("VERSION_NOT_SUPPORTED", 15),
    ("NO_PERMISSION", 16),
    ("TOPIC_NOT_EXIST", 17),
    ("TOPIC_EXIST_ALREADY", 18),
    ("PULL_NOT_FOUND", 19),
    ("PULL_RETRY_IMMEDIATELY", 20),
    ("PULL_OFFSET_MOVED", 21),
    ("QUERY_NOT_FOUND", 22),
    ("SUBSCRIPTION_PARSE_FAILED", 23),
    ("SUBSCRIPTION_NOT_EXIST", 24),
    ("SUBSCRIPTION_NOT_LATEST", 25),
    ("SUBSCRIPTION_GROUP_NOT_EXIST", 26),
    ("FILTER_DATA_NOT_EXIST", 27),
    ("FILTER_DATA_NOT_LATEST", 28),
    ("INVALID_PARAMETER", 29),
    ("TRANSACTION_SHOULD_COMMIT", 200),
    ("TRANSACTION_SHOULD_ROLLBACK", 201),
    ("TRANSACTION_STATE_UNKNOW", 202),
    ("TRANSACTION_STATE_GROUP_WRONG", 203),
    ("NO_BUYER_ID", 204),
    ("NOT_IN_CURRENT_UNIT", 205),
    ("CONSUMER_NOT_ONLINE", 206),
    ("CONSUME_MSG_TIMEOUT", 207),
    ("NO_MESSAGE", 208),
    ("POLLING_FULL", 209),
    ("POLLING_TIMEOUT", 210),
    ("BROKER_NOT_EXIST", 211),
    ("BROKER_DISPATCH_NOT_COMPLETE", 212),
    ("BROADCAST_CONSUMPTION", 213),
    ("FLOW_CONTROL", 215),
    ("NOT_LEADER_FOR_QUEUE", 501),
    ("ILLEGAL_OPERATION", 604),
    ("RPC_UNKNOWN", -1000),
    ("RPC_ADDR_IS_NULL", -1002),
    ("RPC_SEND_TO_CHANNEL_FAILED", -1004),
    ("RPC_TIME_OUT", -1006),
    ("GO_AWAY", 1500),
    ("CONTROLLER_FENCED_MASTER_EPOCH", 2000),
    ("CONTROLLER_FENCED_SYNC_STATE_SET_EPOCH", 2001),
    ("CONTROLLER_INVALID_MASTER", 2002),
    ("CONTROLLER_INVALID_REPLICAS", 2003),
    ("CONTROLLER_MASTER_NOT_AVAILABLE", 2004),
    ("CONTROLLER_INVALID_REQUEST", 2005),
    ("CONTROLLER_BROKER_NOT_ALIVE", 2006),
    ("CONTROLLER_NOT_LEADER", 2007),
    ("CONTROLLER_BROKER_METADATA_NOT_EXIST", 2008),
    ("CONTROLLER_INVALID_CLEAN_BROKER_METADATA", 2009),
    ("CONTROLLER_BROKER_NEED_TO_BE_REGISTERED", 2010),
    ("CONTROLLER_MASTER_STILL_EXIST", 2011),
    ("CONTROLLER_ELECT_MASTER_FAILED", 2012),
    ("CONTROLLER_ALTER_SYNC_STATE_SET_FAILED", 2013),
    ("CONTROLLER_BROKER_ID_INVALID", 2014),
    ("CONTROLLER_JRAFT_INTERNAL_ERROR", 2015),
    ("CONTROLLER_BROKER_LIVE_INFO_NOT_EXISTS", 2016),
    ("LMQ_QUOTA_EXCEEDED", 2017),
    ("LITE_SUBSCRIPTION_QUOTA_EXCEEDED", 2018),
    ("USER_NOT_EXIST", 3001),
    ("POLICY_NOT_EXIST", 3002),
];

const RUST_LEGACY_REQUEST_CODE_EXTRAS: &[(&str, i32, RequestCode)] = &[
    (
        "UPDATE_AND_CREATE_ACL_CONFIG",
        50,
        RequestCode::UpdateAndCreateAclConfig,
    ),
    ("DELETE_ACL_CONFIG", 51, RequestCode::DeleteAclConfig),
    ("GET_BROKER_CLUSTER_ACL_INFO", 52, RequestCode::GetBrokerClusterAclInfo),
    (
        "UPDATE_GLOBAL_WHITE_ADDRS_CONFIG",
        53,
        RequestCode::UpdateGlobalWhiteAddrsConfig,
    ),
    (
        "GET_BROKER_CLUSTER_ACL_CONFIG",
        54,
        RequestCode::GetBrokerClusterAclConfig,
    ),
];

#[test]
fn protocol_compatibility_java_request_codes_are_defined() {
    assert_eq!(JAVA_REQUEST_CODES.len(), 169);

    for (name, value) in JAVA_REQUEST_CODES {
        let request_code = RequestCode::from(*value);
        assert!(
            !request_code.is_unknown(),
            "Java RequestCode {name}={value} is not defined in Rust"
        );
        assert_eq!(
            request_code.to_i32(),
            *value,
            "RequestCode round trip failed for {name}"
        );
    }
}

#[test]
fn protocol_compatibility_rust_legacy_acl_request_codes_are_preserved() {
    for (name, value, expected) in RUST_LEGACY_REQUEST_CODE_EXTRAS {
        assert_eq!(
            RequestCode::from(*value),
            *expected,
            "legacy RequestCode mismatch for {name}"
        );
        assert_eq!(expected.to_i32(), *value, "legacy RequestCode value drifted for {name}");
    }
}

#[test]
fn protocol_compatibility_unknown_request_codes_stay_unknown() {
    for value in [0, -1, 99999] {
        assert_eq!(RequestCode::from(value), RequestCode::Unknown);
    }
}

#[test]
fn protocol_compatibility_java_response_codes_round_trip() {
    assert_eq!(JAVA_RESPONSE_CODES.len(), 63);

    for (name, value) in JAVA_RESPONSE_CODES {
        let response_code = ResponseCode::from(*value);
        assert_eq!(
            response_code.to_i32(),
            *value,
            "Java ResponseCode {name}={value} is not defined in Rust"
        );
    }
}

#[test]
fn protocol_compatibility_remoting_system_response_codes_are_stable() {
    let codes = [
        (0, RemotingSysResponseCode::Success),
        (1, RemotingSysResponseCode::SystemError),
        (2, RemotingSysResponseCode::SystemBusy),
        (3, RemotingSysResponseCode::RequestCodeNotSupported),
        (4, RemotingSysResponseCode::TransactionFailed),
        (16, RemotingSysResponseCode::NoPermission),
    ];

    for (value, expected) in codes {
        assert_eq!(RemotingSysResponseCode::from(value), expected);
        assert_eq!(expected.to_i32(), value);
    }
}

#[test]
fn protocol_compatibility_request_command_codec_round_trip_preserves_wire_fields() {
    let mut codec = RemotingCommandCodec::new();
    let mut buffer = BytesMut::new();
    let body = Bytes::from_static(b"request-body");
    let command = RemotingCommand::create_request_command(
        RequestCode::GetRouteinfoByTopic,
        GetRouteInfoRequestHeader::new("TopicA", Some(true)),
    )
    .set_opaque(7)
    .set_flag(2)
    .set_body(body.clone())
    .set_remark_option(Some("request remark".to_string()));

    codec
        .encode(command, &mut buffer)
        .expect("request command should encode");
    let decoded = codec
        .decode(&mut buffer)
        .expect("request command should decode")
        .expect("encoded request command should be complete");

    assert_eq!(decoded.request_code(), RequestCode::GetRouteinfoByTopic);
    assert_eq!(decoded.opaque(), 7);
    assert_eq!(decoded.flag(), 2);
    assert!(!decoded.is_response_type());
    assert!(decoded.is_oneway_rpc());
    assert_eq!(decoded.remark().map(|value| value.as_str()), Some("request remark"));
    assert_eq!(decoded.body(), Some(&body));

    let ext_fields = decoded.ext_fields().expect("request header fields should decode");
    assert_eq!(
        ext_fields
            .iter()
            .find(|(key, _)| key.as_str() == "topic")
            .map(|(_, value)| value.as_str()),
        Some("TopicA")
    );
    assert_eq!(
        ext_fields
            .iter()
            .find(|(key, _)| key.as_str() == "acceptStandardJsonOnly")
            .map(|(_, value)| value.as_str()),
        Some("true")
    );
}

#[test]
fn protocol_compatibility_response_command_codec_round_trip_preserves_wire_fields() {
    let mut codec = RemotingCommandCodec::new();
    let mut buffer = BytesMut::new();
    let body = Bytes::from_static(b"response-body");
    let command = RemotingCommand::create_response_command_with_code_remark(ResponseCode::Success, "ok")
        .set_opaque(9)
        .set_body(body.clone());

    codec
        .encode(command, &mut buffer)
        .expect("response command should encode");
    let decoded = codec
        .decode(&mut buffer)
        .expect("response command should decode")
        .expect("encoded response command should be complete");

    assert_eq!(decoded.code(), ResponseCode::Success.to_i32());
    assert_eq!(decoded.opaque(), 9);
    assert!(decoded.is_response_type());
    assert!(!decoded.is_oneway_rpc());
    assert_eq!(decoded.remark().map(|value| value.as_str()), Some("ok"));
    assert_eq!(decoded.body(), Some(&body));
}

#[test]
fn protocol_compatibility_phase8_all_java_request_codes_codec_round_trip() {
    let mut codec = RemotingCommandCodec::new();

    for (index, (name, value)) in JAVA_REQUEST_CODES.iter().enumerate() {
        let request_code = RequestCode::from(*value);
        let body = Bytes::from(format!("phase8-request-body-{name}-{value}"));
        let expected_remark = format!("phase8 request {name}");
        let mut buffer = BytesMut::new();
        let command = RemotingCommand::create_remoting_command(request_code)
            .set_opaque(10_000 + index as i32)
            .set_remark_option(Some(expected_remark.clone()))
            .set_body(body.clone());

        codec
            .encode(command, &mut buffer)
            .unwrap_or_else(|error| panic!("Java RequestCode {name}={value} should encode: {error}"));
        let decoded = codec
            .decode(&mut buffer)
            .unwrap_or_else(|error| panic!("Java RequestCode {name}={value} should decode: {error}"))
            .unwrap_or_else(|| panic!("Java RequestCode {name}={value} should produce a complete command"));

        assert_eq!(
            decoded.request_code(),
            request_code,
            "Java RequestCode {name}={value} request_code drifted after codec round trip"
        );
        assert_eq!(
            decoded.code(),
            *value,
            "Java RequestCode {name}={value} numeric code drifted after codec round trip"
        );
        assert_eq!(decoded.opaque(), 10_000 + index as i32);
        assert_eq!(
            decoded.remark().map(|remark| remark.as_str()),
            Some(expected_remark.as_str())
        );
        assert_eq!(decoded.body(), Some(&body));
        assert!(buffer.is_empty(), "codec should consume the whole frame for {name}");
    }
}

#[test]
fn protocol_compatibility_phase8_all_java_response_codes_codec_round_trip() {
    let mut codec = RemotingCommandCodec::new();

    for (index, (name, value)) in JAVA_RESPONSE_CODES.iter().enumerate() {
        let response_code = ResponseCode::from(*value);
        let body = Bytes::from(format!("phase8-response-body-{name}-{value}"));
        let expected_remark = format!("phase8 response {name}");
        let mut buffer = BytesMut::new();
        let command = RemotingCommand::create_response_command_with_code_remark(response_code, expected_remark.clone())
            .set_opaque(20_000 + index as i32)
            .set_body(body.clone());

        codec
            .encode(command, &mut buffer)
            .unwrap_or_else(|error| panic!("Java ResponseCode {name}={value} should encode: {error}"));
        let decoded = codec
            .decode(&mut buffer)
            .unwrap_or_else(|error| panic!("Java ResponseCode {name}={value} should decode: {error}"))
            .unwrap_or_else(|| panic!("Java ResponseCode {name}={value} should produce a complete command"));

        assert_eq!(
            decoded.code(),
            *value,
            "Java ResponseCode {name}={value} numeric code drifted after codec round trip"
        );
        assert!(decoded.is_response_type());
        assert_eq!(decoded.opaque(), 20_000 + index as i32);
        assert_eq!(
            decoded.remark().map(|remark| remark.as_str()),
            Some(expected_remark.as_str())
        );
        assert_eq!(decoded.body(), Some(&body));
        assert!(buffer.is_empty(), "codec should consume the whole frame for {name}");
    }
}
