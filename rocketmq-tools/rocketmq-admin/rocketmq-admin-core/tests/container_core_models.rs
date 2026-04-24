use rocketmq_admin_core::core::container::ContainerAddBrokerRequest;
use rocketmq_admin_core::core::container::ContainerRemoveBrokerRequest;

#[test]
fn container_add_broker_request_trims_fields() {
    let request = ContainerAddBrokerRequest::try_new(" 127.0.0.1:10911 ", " /tmp/broker.conf ").expect("valid request");

    assert_eq!(request.broker_container_addr().as_str(), "127.0.0.1:10911");
    assert_eq!(request.broker_config_path().as_str(), "/tmp/broker.conf");
}

#[test]
fn container_remove_broker_request_trims_identity_and_rejects_negative_broker_id() {
    let request = ContainerRemoveBrokerRequest::try_new(" 127.0.0.1:10911 ", " DefaultCluster ", " broker-a ", 1)
        .expect("valid request");

    assert_eq!(request.broker_container_addr().as_str(), "127.0.0.1:10911");
    assert_eq!(request.cluster_name().as_str(), "DefaultCluster");
    assert_eq!(request.broker_name().as_str(), "broker-a");
    assert_eq!(request.broker_id(), 1);
    assert_eq!(request.broker_identity(), "DefaultCluster:broker-a:1");
    assert!(ContainerRemoveBrokerRequest::try_new("127.0.0.1:10911", "DefaultCluster", "broker-a", -1).is_err());
}
