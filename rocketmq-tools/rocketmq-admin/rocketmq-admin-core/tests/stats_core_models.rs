use rocketmq_admin_core::core::stats::StatsAllQueryRequest;

#[test]
fn stats_all_query_request_trims_optional_topic() {
    let request = StatsAllQueryRequest::new(true, Some(" TopicA ".to_string()))
        .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".to_string()));

    assert!(request.active_topic());
    assert_eq!(request.topic().map(|topic| topic.as_str()), Some("TopicA"));
    assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
}

#[test]
fn stats_all_query_request_treats_blank_topic_as_all() {
    let request = StatsAllQueryRequest::new(false, Some(" ".to_string()));

    assert!(!request.active_topic());
    assert_eq!(request.topic(), None);
    assert_eq!(request.namesrv_addr(), None);
}
