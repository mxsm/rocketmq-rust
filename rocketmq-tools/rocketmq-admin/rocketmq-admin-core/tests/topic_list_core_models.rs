use cheetah_string::CheetahString;
use rocketmq_admin_core::core::topic::TopicTarget;
use rocketmq_admin_core::core::topic::UpdateTopicListRequest;
use rocketmq_common::common::config::TopicConfig as RocketMQTopicConfig;

#[test]
fn update_topic_list_request_accepts_broker_target_and_topic_configs() {
    let request = UpdateTopicListRequest::try_new(
        TopicTarget::Broker(CheetahString::from_static_str("127.0.0.1:10911")),
        vec![RocketMQTopicConfig::default()],
    )
    .unwrap()
    .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".to_string()));

    assert_eq!(
        request.target(),
        &TopicTarget::Broker(CheetahString::from_static_str("127.0.0.1:10911"))
    );
    assert_eq!(request.topic_configs().len(), 1);
    assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
}

#[test]
fn update_topic_list_request_rejects_empty_topic_config_list() {
    let error = UpdateTopicListRequest::try_new(
        TopicTarget::Cluster(CheetahString::from_static_str("DefaultCluster")),
        Vec::new(),
    )
    .unwrap_err();

    assert!(error.to_string().contains("topicConfigs must not be empty"));
}
