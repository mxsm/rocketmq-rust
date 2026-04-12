use rocketmq_admin_core::core::connection::ConsumerConnectionQueryRequest;
use rocketmq_admin_core::core::connection::ProducerConnectionQueryRequest;

#[test]
fn consumer_connection_query_request_trims_fields() {
    let request = ConsumerConnectionQueryRequest::try_new(" group-a ", Some(" 127.0.0.1:10911 ".to_string())).unwrap();

    assert_eq!(request.consumer_group().as_str(), "group-a");
    assert_eq!(request.broker_addr(), Some("127.0.0.1:10911"));
    assert_eq!(request.namesrv_addr(), None);

    let request = request.with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".to_string()));
    assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
}

#[test]
fn consumer_connection_query_request_rejects_blank_group() {
    assert!(ConsumerConnectionQueryRequest::try_new(" ", None).is_err());
}

#[test]
fn producer_connection_query_request_trims_fields() {
    let request = ProducerConnectionQueryRequest::try_new(" producer-a ", " TopicTest ").unwrap();

    assert_eq!(request.producer_group().as_str(), "producer-a");
    assert_eq!(request.topic().as_str(), "TopicTest");

    let request = request.with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".to_string()));
    assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
}

#[test]
fn producer_connection_query_request_rejects_blank_fields() {
    assert!(ProducerConnectionQueryRequest::try_new(" ", "TopicTest").is_err());
    assert!(ProducerConnectionQueryRequest::try_new("producer-a", " ").is_err());
}
