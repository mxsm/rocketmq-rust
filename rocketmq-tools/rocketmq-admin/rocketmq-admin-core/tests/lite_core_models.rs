use rocketmq_admin_core::core::lite::BrokerLiteInfoQueryRequest;
use rocketmq_admin_core::core::lite::BrokerLiteInfoTarget;
use rocketmq_admin_core::core::lite::LiteTopicInfoQueryRequest;
use rocketmq_admin_core::core::lite::ParentTopicInfoQueryRequest;

#[test]
fn broker_lite_info_query_request_trims_broker_target() {
    let request = BrokerLiteInfoQueryRequest::try_new(Some(" 127.0.0.1:10911 ".to_string()), None).unwrap();

    assert_eq!(
        request.target(),
        &BrokerLiteInfoTarget::Broker("127.0.0.1:10911".into())
    );
    assert_eq!(request.namesrv_addr(), None);
}

#[test]
fn broker_lite_info_query_request_trims_cluster_and_namesrv() {
    let request = BrokerLiteInfoQueryRequest::try_new(None, Some(" DefaultCluster ".to_string()))
        .unwrap()
        .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".to_string()));

    assert_eq!(
        request.target(),
        &BrokerLiteInfoTarget::Cluster("DefaultCluster".into())
    );
    assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
}

#[test]
fn broker_lite_info_query_request_requires_exactly_one_target() {
    assert!(BrokerLiteInfoQueryRequest::try_new(None, None).is_err());
    assert!(BrokerLiteInfoQueryRequest::try_new(
        Some("127.0.0.1:10911".to_string()),
        Some("DefaultCluster".to_string())
    )
    .is_err());
}

#[test]
fn parent_topic_info_query_request_trims_topic_and_namesrv() {
    let request = ParentTopicInfoQueryRequest::try_new(" ParentTopic ")
        .unwrap()
        .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".to_string()));

    assert_eq!(request.parent_topic().as_str(), "ParentTopic");
    assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
}

#[test]
fn parent_topic_info_query_request_rejects_blank_topic() {
    assert!(ParentTopicInfoQueryRequest::try_new(" ").is_err());
}

#[test]
fn lite_topic_info_query_request_trims_parent_and_lite_topic() {
    let request = LiteTopicInfoQueryRequest::try_new(" ParentTopic ", " LiteTopic ")
        .unwrap()
        .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".to_string()));

    assert_eq!(request.parent_topic().as_str(), "ParentTopic");
    assert_eq!(request.lite_topic().as_str(), "LiteTopic");
    assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
}

#[test]
fn lite_topic_info_query_request_rejects_blank_fields() {
    assert!(LiteTopicInfoQueryRequest::try_new(" ", "LiteTopic").is_err());
    assert!(LiteTopicInfoQueryRequest::try_new("ParentTopic", " ").is_err());
}
