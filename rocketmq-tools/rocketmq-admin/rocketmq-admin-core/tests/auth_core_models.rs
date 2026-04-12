use rocketmq_admin_core::core::auth::AuthTarget;
use rocketmq_admin_core::core::auth::CreateUserRequest;
use rocketmq_admin_core::core::auth::ListUsersRequest;
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
