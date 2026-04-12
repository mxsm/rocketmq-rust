use rocketmq_admin_core::core::producer::ProducerInfoQueryRequest;

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
