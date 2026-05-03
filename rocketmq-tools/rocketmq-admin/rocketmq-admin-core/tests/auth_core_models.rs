use rocketmq_admin_core::core::auth::AuthTarget;
use rocketmq_admin_core::core::auth::CopyAclRequest;
use rocketmq_admin_core::core::auth::CreateAclRequest;
use rocketmq_admin_core::core::auth::CreateUserRequest;
use rocketmq_admin_core::core::auth::DeleteAclRequest;
use rocketmq_admin_core::core::auth::ListAclRequest;
use rocketmq_admin_core::core::auth::ListUsersRequest;
use rocketmq_admin_core::core::auth::UpdateAclRequest;
use rocketmq_admin_core::core::auth::UpdateUserRequest;

#[test]
fn auth_user_requests_trim_fields_and_validate_targets() {
    let create = CreateUserRequest::try_new(
        Some(" 127.0.0.1:10911 ".to_string()),
        None,
        " alice ",
        " secret ",
        Some(" NORMAL ".to_string()),
    )
    .unwrap()
    .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".to_string()));

    assert!(matches!(create.target(), AuthTarget::BrokerAddr(addr) if addr.as_str() == "127.0.0.1:10911"));
    assert_eq!(create.username().as_str(), "alice");
    assert_eq!(create.password().as_str(), "secret");
    assert_eq!(create.user_type().as_str(), "NORMAL");
    assert_eq!(create.namesrv_addr(), Some("127.0.0.1:9876"));

    assert!(CreateUserRequest::try_new(None, None, "alice", "secret", None).is_err());
    assert!(CreateUserRequest::try_new(
        Some("127.0.0.1:10911".to_string()),
        Some("DefaultCluster".to_string()),
        "alice",
        "secret",
        None
    )
    .is_err());
    assert!(CreateUserRequest::try_new(Some("127.0.0.1:10911".to_string()), None, " ", "secret", None).is_err());
    assert!(CreateUserRequest::try_new(Some("127.0.0.1:10911".to_string()), None, "alice", " ", None).is_err());
}

#[test]
fn auth_user_update_and_list_requests_preserve_optional_fields() {
    let update = UpdateUserRequest::try_new(
        None,
        Some(" DefaultCluster ".to_string()),
        " alice ",
        Some(" secret ".to_string()),
        Some(" NORMAL ".to_string()),
        Some(" ENABLE ".to_string()),
    )
    .unwrap();

    assert!(matches!(update.target(), AuthTarget::ClusterName(cluster) if cluster.as_str() == "DefaultCluster"));
    assert_eq!(update.username().as_str(), "alice");
    assert_eq!(update.password(), Some("secret"));
    assert_eq!(update.user_type(), Some("NORMAL"));
    assert_eq!(update.user_status(), Some("ENABLE"));
    assert!(UpdateUserRequest::try_new(None, Some("DefaultCluster".to_string()), "alice", None, None, None).is_err());

    let list =
        ListUsersRequest::try_new(None, Some(" DefaultCluster ".to_string()), Some(" alice ".to_string())).unwrap();
    assert!(matches!(list.target(), AuthTarget::ClusterName(cluster) if cluster.as_str() == "DefaultCluster"));
    assert_eq!(list.filter(), Some("alice"));
}

#[test]
fn auth_acl_create_request_trims_fields_and_builds_acl_info() {
    let request = CreateAclRequest::try_new(
        Some(" 127.0.0.1:10911 ".to_string()),
        None,
        " user:alice ",
        " Topic:order-topic, Topic:user-topic ",
        " PUB,SUB ",
        " ALLOW ",
        Some(" 127.0.0.1, 127.0.0.2 ".to_string()),
    )
    .unwrap()
    .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".to_string()));

    assert!(matches!(request.target(), AuthTarget::BrokerAddr(addr) if addr.as_str() == "127.0.0.1:10911"));
    assert_eq!(request.subject().as_str(), "user:alice");
    assert_eq!(
        request
            .resources()
            .iter()
            .map(|resource| resource.as_str())
            .collect::<Vec<_>>(),
        vec!["Topic:order-topic", "Topic:user-topic"]
    );
    assert_eq!(
        request
            .actions()
            .iter()
            .map(|action| action.as_str())
            .collect::<Vec<_>>(),
        vec!["PUB", "SUB"]
    );
    assert_eq!(request.decision().as_str(), "ALLOW");
    assert_eq!(
        request
            .source_ips()
            .iter()
            .map(|source_ip| source_ip.as_str())
            .collect::<Vec<_>>(),
        vec!["127.0.0.1", "127.0.0.2"]
    );
    assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));

    let acl_info = request.build_acl_info();
    assert_eq!(
        acl_info.subject.as_ref().map(|subject| subject.as_str()),
        Some("user:alice")
    );
    let entries = acl_info
        .policies
        .as_ref()
        .and_then(|policies| policies.first())
        .and_then(|policy| policy.entries.as_ref())
        .unwrap();
    let entry = entries.first().unwrap();
    assert_eq!(
        entry.resource.as_ref().map(|resource| resource.as_str()),
        Some("Topic:order-topic,Topic:user-topic")
    );
    assert_eq!(
        entry.actions.as_ref().map(|actions| {
            actions
                .iter()
                .map(|action| action.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }),
        Some("PUB,SUB".to_string())
    );
    assert_eq!(entry.decision.as_ref().map(|decision| decision.as_str()), Some("ALLOW"));
    assert_eq!(
        entry
            .source_ips
            .as_ref()
            .unwrap()
            .iter()
            .map(|source_ip| source_ip.as_str())
            .collect::<Vec<_>>(),
        vec!["127.0.0.1", "127.0.0.2"]
    );
}

#[test]
fn auth_acl_requests_validate_required_fields_and_targets() {
    assert!(CreateAclRequest::try_new(None, None, "user:alice", "Topic:A", "PUB", "ALLOW", None).is_err());
    assert!(CreateAclRequest::try_new(
        Some("127.0.0.1:10911".to_string()),
        Some("DefaultCluster".to_string()),
        "user:alice",
        "Topic:A",
        "PUB",
        "ALLOW",
        None,
    )
    .is_err());
    assert!(CreateAclRequest::try_new(
        Some("127.0.0.1:10911".to_string()),
        None,
        " ",
        "Topic:A",
        "PUB",
        "ALLOW",
        None
    )
    .is_err());
    assert!(CreateAclRequest::try_new(
        Some("127.0.0.1:10911".to_string()),
        None,
        "user:alice",
        " ",
        "PUB",
        "ALLOW",
        None
    )
    .is_err());
    assert!(CreateAclRequest::try_new(
        Some("127.0.0.1:10911".to_string()),
        None,
        "user:alice",
        "Topic:A",
        " ",
        "ALLOW",
        None
    )
    .is_err());
    assert!(CreateAclRequest::try_new(
        Some("127.0.0.1:10911".to_string()),
        None,
        "user:alice",
        "Topic:A",
        "PUB",
        " ",
        None
    )
    .is_err());
    assert!(UpdateAclRequest::try_new(
        None,
        Some("DefaultCluster".to_string()),
        "user:alice",
        "Topic:A",
        "SUB",
        "DENY",
        None,
    )
    .is_ok());
}

#[test]
fn auth_acl_delete_list_and_copy_requests_trim_fields() {
    let delete = DeleteAclRequest::try_new(
        None,
        Some(" DefaultCluster ".to_string()),
        " user:alice ",
        Some(" Topic:A ".to_string()),
    )
    .unwrap();
    assert!(matches!(delete.target(), AuthTarget::ClusterName(cluster) if cluster.as_str() == "DefaultCluster"));
    assert_eq!(delete.subject().as_str(), "user:alice");
    assert_eq!(delete.resource(), Some("Topic:A"));

    let list =
        ListAclRequest::try_new(None, Some(" DefaultCluster ".to_string()), Some(" user: ".to_string())).unwrap();
    assert!(matches!(list.target(), AuthTarget::ClusterName(cluster) if cluster.as_str() == "DefaultCluster"));
    assert_eq!(list.subject_filter(), Some("user:"));
    assert_eq!(list.resource_filter(), None);

    let copy = CopyAclRequest::try_new(
        " 127.0.0.1:10911 ",
        " 127.0.0.2:10911 ",
        Some(" user:alice, user:bob ".to_string()),
    )
    .unwrap();
    assert_eq!(copy.from_broker().as_str(), "127.0.0.1:10911");
    assert_eq!(copy.to_broker().as_str(), "127.0.0.2:10911");
    assert_eq!(
        copy.subjects()
            .unwrap()
            .iter()
            .map(|subject| subject.as_str())
            .collect::<Vec<_>>(),
        vec!["user:alice", "user:bob"]
    );
}
