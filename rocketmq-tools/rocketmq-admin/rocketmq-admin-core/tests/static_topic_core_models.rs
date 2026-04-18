use rocketmq_admin_core::core::static_topic::RemappingStaticTopicRequest;
use rocketmq_admin_core::core::static_topic::StaticTopicMappingFileRequest;
use rocketmq_admin_core::core::static_topic::UpdateStaticTopicRequest;

#[test]
fn update_static_topic_request_trims_and_splits_targets() {
    let request = UpdateStaticTopicRequest::try_new(
        " TopicA ",
        " broker-a, broker-b ",
        " 8 ",
        Some(" ClusterA, ClusterB ".to_string()),
    )
    .unwrap()
    .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".to_string()));

    assert_eq!(request.topic().as_str(), "TopicA");
    assert_eq!(
        request
            .broker_names()
            .iter()
            .map(|name| name.as_str())
            .collect::<Vec<_>>(),
        vec!["broker-a", "broker-b"]
    );
    assert_eq!(
        request
            .cluster_names()
            .iter()
            .map(|name| name.as_str())
            .collect::<Vec<_>>(),
        vec!["ClusterA", "ClusterB"]
    );
    assert_eq!(request.queue_num(), 8);
    assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
}

#[test]
fn update_static_topic_request_rejects_invalid_fields() {
    assert!(UpdateStaticTopicRequest::try_new(" ", "broker-a", "8", None).is_err());
    assert!(UpdateStaticTopicRequest::try_new("TopicA", " ", "8", None).is_err());
    assert!(UpdateStaticTopicRequest::try_new("TopicA", "broker-a", "not-a-number", None).is_err());
}

#[test]
fn remapping_static_topic_request_accepts_broker_or_cluster_targets() {
    let broker_request =
        RemappingStaticTopicRequest::try_new(" TopicA ", Some(" broker-a, broker-b ".to_string()), None, Some(true))
            .unwrap();
    assert_eq!(broker_request.topic().as_str(), "TopicA");
    assert_eq!(
        broker_request
            .broker_names()
            .iter()
            .map(|name| name.as_str())
            .collect::<Vec<_>>(),
        vec!["broker-a", "broker-b"]
    );
    assert!(broker_request.cluster_names().is_empty());
    assert!(broker_request.force_replace());

    let cluster_request =
        RemappingStaticTopicRequest::try_new("TopicA", None, Some(" ClusterA ".to_string()), None).unwrap();
    assert!(cluster_request.broker_names().is_empty());
    assert_eq!(
        cluster_request
            .cluster_names()
            .iter()
            .map(|name| name.as_str())
            .collect::<Vec<_>>(),
        vec!["ClusterA"]
    );
    assert!(!cluster_request.force_replace());
}

#[test]
fn remapping_static_topic_request_rejects_missing_targets() {
    assert!(RemappingStaticTopicRequest::try_new("TopicA", None, None, None).is_err());
    assert!(
        RemappingStaticTopicRequest::try_new("TopicA", Some(" ".to_string()), Some(" ".to_string()), None).is_err()
    );
}

#[test]
fn static_topic_mapping_file_request_trims_topic_and_namesrv() {
    let request = StaticTopicMappingFileRequest::try_new(" TopicA ", true)
        .unwrap()
        .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".to_string()));

    assert_eq!(request.topic().as_str(), "TopicA");
    assert!(request.force_replace());
    assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
}
