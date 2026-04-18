use rocketmq_admin_core::core::producer::CheckMessageSendRtRequest;
use rocketmq_admin_core::core::producer::ProducerInfoQueryRequest;
use rocketmq_admin_core::core::producer::SendMessageRequest;
use rocketmq_admin_core::core::producer::SendMessageStatusRequest;

#[test]
fn producer_info_query_request_trims_fields() {
    let request = ProducerInfoQueryRequest::try_new(" 127.0.0.1:10911 ").unwrap();

    assert_eq!(request.broker_addr().as_str(), "127.0.0.1:10911");
    assert_eq!(request.namesrv_addr(), None);

    let request = request.with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".to_string()));
    assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
}

#[test]
fn producer_info_query_request_rejects_blank_broker_addr() {
    assert!(ProducerInfoQueryRequest::try_new(" ").is_err());
}

#[test]
fn send_message_request_trims_fields_and_keeps_queue_target() {
    let request = SendMessageRequest::try_new(
        " TopicA ",
        " body ",
        Some(" key ".into()),
        Some(" tag ".into()),
        Some(" broker-a ".into()),
        Some(1),
        true,
    )
    .unwrap();

    assert_eq!(request.topic().as_str(), "TopicA");
    assert_eq!(request.body(), "body");
    assert_eq!(request.keys(), Some("key"));
    assert_eq!(request.tags(), Some("tag"));
    assert_eq!(request.broker_name().unwrap().as_str(), "broker-a");
    assert_eq!(request.queue_id(), Some(1));
    assert!(request.msg_trace_enable());
}

#[test]
fn send_message_request_rejects_queue_without_broker() {
    assert!(SendMessageRequest::try_new("TopicA", "body", None, None, None, Some(1), false).is_err());
}

#[test]
fn send_message_status_request_trims_broker_name() {
    let request = SendMessageStatusRequest::try_new(" broker-a ", 256, 10).unwrap();

    assert_eq!(request.broker_name().as_str(), "broker-a");
    assert_eq!(request.message_size(), 256);
    assert_eq!(request.count(), 10);
}

#[test]
fn send_message_status_request_rejects_blank_broker_name() {
    assert!(SendMessageStatusRequest::try_new(" ", 128, 50).is_err());
}

#[test]
fn check_message_send_rt_request_trims_topic() {
    let request = CheckMessageSendRtRequest::try_new(" TopicA ", 100, 512).unwrap();

    assert_eq!(request.topic().as_str(), "TopicA");
    assert_eq!(request.amount(), 100);
    assert_eq!(request.size(), 512);
}

#[test]
fn check_message_send_rt_request_requires_at_least_two_messages() {
    assert!(CheckMessageSendRtRequest::try_new("TopicA", 1, 128).is_err());
}
