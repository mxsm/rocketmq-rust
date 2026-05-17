use std::collections::HashMap;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_auth::authentication::acl_signer;
use rocketmq_auth::authentication::enums::subject_type::SubjectType;
use rocketmq_auth::authentication::enums::user_status::UserStatus;
use rocketmq_auth::authentication::enums::user_type::UserType;
use rocketmq_auth::authentication::model::user::User;
use rocketmq_auth::authentication::provider::AuthenticationMetadataProvider;
use rocketmq_auth::authorization::metadata_provider::AuthorizationMetadataProvider;
use rocketmq_auth::authorization::model::acl::Acl;
use rocketmq_auth::authorization::model::policy::Policy;
use rocketmq_auth::authorization::model::resource::Resource;
use rocketmq_auth::config::AuthConfig;
use rocketmq_auth::AuthRuntime;
use rocketmq_auth::AuthRuntimeBuilder;
use rocketmq_common::common::action::Action;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use tempfile::TempDir;

fn temp_auth_config(temp: &TempDir) -> AuthConfig {
    AuthConfig {
        auth_config_path: CheetahString::from(temp.path().join("auth-store").to_string_lossy().as_ref()),
        ..AuthConfig::default()
    }
}

fn send_message_command(
    topic: &str,
    access_key: Option<&str>,
    signature: Option<&str>,
    extra_fields: &[(&str, &str)],
    body: Option<&[u8]>,
) -> RemotingCommand {
    let mut fields = HashMap::new();
    fields.insert(CheetahString::from_static_str("topic"), CheetahString::from(topic));
    if let Some(access_key) = access_key {
        fields.insert(
            CheetahString::from_static_str("AccessKey"),
            CheetahString::from(access_key),
        );
    }
    if let Some(signature) = signature {
        fields.insert(
            CheetahString::from_static_str("Signature"),
            CheetahString::from(signature),
        );
    }
    for (key, value) in extra_fields {
        fields.insert(CheetahString::from(*key), CheetahString::from(*value));
    }

    let command = RemotingCommand::create_remoting_command(RequestCode::SendMessage.to_i32()).set_ext_fields(fields);
    if let Some(body) = body {
        command.set_body(body.to_vec())
    } else {
        command
    }
}

async fn seed_user(runtime: &AuthRuntime, username: &str, secret: &str, user_type: UserType) {
    let mut user = User::of_with_type(username, secret, user_type);
    user.set_user_status(UserStatus::Enable);
    runtime
        .provider_registry()
        .authentication_metadata_provider()
        .create_user(user)
        .await
        .expect("test user should be created");
}

async fn allow_topic_pub(runtime: &AuthRuntime, username: &str, topic: &str) {
    let acl = Acl::of(
        username,
        SubjectType::User,
        Policy::of(
            vec![Resource::of_topic(topic)],
            vec![Action::Pub],
            None,
            rocketmq_auth::authorization::enums::decision::Decision::Allow,
        ),
    );
    runtime
        .provider_registry()
        .authorization_metadata_provider()
        .create_acl(acl)
        .await
        .expect("test acl should be created");
}

#[tokio::test]
async fn remoting_signature_matches_java_sorted_value_only_content() {
    let temp = TempDir::new().expect("temp dir should be created");
    let runtime = AuthRuntimeBuilder::new(AuthConfig {
        authentication_enabled: true,
        ..temp_auth_config(&temp)
    })
    .build()
    .await
    .expect("auth runtime should build");

    seed_user(&runtime, "alice", "secret", UserType::Normal).await;

    let signature = acl_signer::cal_signature(b"aliceATopicAZbody", "secret").expect("signature should calculate");
    let command = send_message_command(
        "TopicA",
        Some("alice"),
        Some(signature.as_str()),
        &[("zeta", "Z"), ("alpha", "A")],
        Some(b"body"),
    );
    runtime
        .check_remoting(&(), &command)
        .await
        .expect("Java value-only signed content should authenticate");

    let key_value_signature = acl_signer::cal_signature(b"AccessKeyalicealphaAtopicTopicAzetaZbody", "secret")
        .expect("signature should calculate");
    let command = send_message_command(
        "TopicA",
        Some("alice"),
        Some(key_value_signature.as_str()),
        &[("zeta", "Z"), ("alpha", "A")],
        Some(b"body"),
    );
    runtime
        .check_remoting(&(), &command)
        .await
        .expect_err("key+value signed content must not match Java remoting signature semantics");
}

#[tokio::test]
async fn plain_acl_camel_case_fields_match_java_legacy_semantics() {
    let temp = TempDir::new().expect("temp dir should be created");
    let acl_file = temp.path().join("plain_acl.yml");
    std::fs::write(
        &acl_file,
        r#"
globalWhiteRemoteAddresses:
  - 10.10.*.*
accounts:
  - accessKey: alice
    secretKey: secret
    whiteRemoteAddress: 192.168.0.*
    admin: false
    defaultTopicPerm: DENY
    defaultGroupPerm: SUB
    topicPerms:
      - TopicA=PUB
  - accessKey: admin
    secretKey: admin-secret
    admin: true
    defaultTopicPerm: DENY
"#,
    )
    .expect("acl file should be written");

    let runtime = AuthRuntimeBuilder::new(AuthConfig {
        acl_file: CheetahString::from(acl_file.to_string_lossy().as_ref()),
        authentication_enabled: true,
        authorization_enabled: true,
        ..temp_auth_config(&temp)
    })
    .build()
    .await
    .expect("auth runtime should build");

    assert!(runtime
        .is_acl_white_remote_address(None, Some("10.10.1.2"))
        .expect("white list check should succeed"));
    assert!(runtime
        .is_acl_white_remote_address(Some("alice"), Some("192.168.0.7"))
        .expect("white list check should succeed"));

    let signature = acl_signer::cal_signature(b"aliceTopicA", "secret").expect("signature should calculate");
    let command = send_message_command("TopicA", Some("alice"), Some(signature.as_str()), &[], None);
    runtime
        .check_remoting_with_source_ip(&(), &command, Some("172.16.0.1"), None)
        .await
        .expect("custom TopicA=PUB should override default topic DENY");

    let signature = acl_signer::cal_signature(b"aliceTopicB", "secret").expect("signature should calculate");
    let command = send_message_command("TopicB", Some("alice"), Some(signature.as_str()), &[], None);
    runtime
        .check_remoting_with_source_ip(&(), &command, Some("172.16.0.1"), None)
        .await
        .expect_err("defaultTopicPerm=DENY should reject non-custom topic");

    let command = send_message_command("TopicB", Some("alice"), Some(""), &[], None);
    runtime
        .check_remoting_with_source_ip(&(), &command, Some("192.168.0.7"), None)
        .await
        .expect("account whiteRemoteAddress should bypass signature and ACL checks");

    let command = send_message_command("TopicB", None, None, &[], None);
    runtime
        .check_remoting_with_source_ip(&(), &command, Some("10.10.1.2"), None)
        .await
        .expect("globalWhiteRemoteAddresses should bypass missing credentials");

    let signature = acl_signer::cal_signature(b"adminTopicB", "admin-secret").expect("signature should calculate");
    let command = send_message_command("TopicB", Some("admin"), Some(signature.as_str()), &[], None);
    runtime
        .check_remoting_with_source_ip(&(), &command, Some("172.16.0.1"), None)
        .await
        .expect("admin=true should map to SUPER user and bypass ACL DENY");
}

#[tokio::test]
async fn custom_deny_takes_precedence_over_custom_allow_for_same_resource() {
    let temp = TempDir::new().expect("temp dir should be created");
    let runtime = AuthRuntimeBuilder::new(AuthConfig {
        authentication_enabled: true,
        authorization_enabled: true,
        ..temp_auth_config(&temp)
    })
    .build()
    .await
    .expect("auth runtime should build");

    seed_user(&runtime, "alice", "secret", UserType::Normal).await;

    let allow = Policy::of(
        vec![Resource::of_topic("TopicA")],
        vec![Action::Pub],
        None,
        rocketmq_auth::authorization::enums::decision::Decision::Allow,
    );
    let deny = Policy::of(
        vec![Resource::of_topic("TopicA")],
        vec![Action::Pub],
        None,
        rocketmq_auth::authorization::enums::decision::Decision::Deny,
    );
    runtime
        .provider_registry()
        .authorization_metadata_provider()
        .create_acl(Acl::of_with_policies("alice", SubjectType::User, vec![allow, deny]))
        .await
        .expect("test acl should be created");

    let signature = acl_signer::cal_signature(b"aliceTopicA", "secret").expect("signature should calculate");
    let command = send_message_command("TopicA", Some("alice"), Some(signature.as_str()), &[], None);
    runtime
        .check_remoting_with_source_ip(&(), &command, Some("172.16.0.1"), None)
        .await
        .expect_err("DENY should have higher priority than ALLOW for the same custom resource");
}

#[tokio::test]
async fn super_user_short_circuits_acl_lookup() {
    let temp = TempDir::new().expect("temp dir should be created");
    let runtime = AuthRuntimeBuilder::new(AuthConfig {
        authentication_enabled: true,
        authorization_enabled: true,
        ..temp_auth_config(&temp)
    })
    .build()
    .await
    .expect("auth runtime should build");

    seed_user(&runtime, "root", "secret", UserType::Super).await;
    allow_topic_pub(&runtime, "alice", "TopicA").await;

    let signature = acl_signer::cal_signature(b"rootTopicWithoutAcl", "secret").expect("signature should calculate");
    let command = send_message_command("TopicWithoutAcl", Some("root"), Some(signature.as_str()), &[], None);
    runtime
        .check_remoting_with_source_ip(&(), &command, Some("172.16.0.1"), None)
        .await
        .expect("SUPER user should pass without a matching ACL");
}

#[tokio::test]
async fn manual_acl_reload_is_safe_during_concurrent_authorization() {
    let temp = TempDir::new().expect("temp dir should be created");
    let acl_file = temp.path().join("plain_acl.yml");
    std::fs::write(
        &acl_file,
        r#"
accounts:
  - accessKey: alice
    secretKey: first
    defaultTopicPerm: DENY
    topicPerms:
      - TopicA=PUB
"#,
    )
    .expect("acl file should be written");

    let runtime = Arc::new(
        AuthRuntimeBuilder::new(AuthConfig {
            acl_file: CheetahString::from(acl_file.to_string_lossy().as_ref()),
            authentication_enabled: true,
            authorization_enabled: true,
            ..temp_auth_config(&temp)
        })
        .build()
        .await
        .expect("auth runtime should build"),
    );

    let old_signature = acl_signer::cal_signature(b"aliceTopicA", "first").expect("signature should calculate");
    let old_command = send_message_command("TopicA", Some("alice"), Some(old_signature.as_str()), &[], None);
    runtime
        .check_remoting_with_source_ip(&(), &old_command, Some("172.16.0.1"), Some("channel-a"))
        .await
        .expect("initial ACL should authorize TopicA with the first secret");
    let generation_before_reload = runtime.acl_generation();

    let mut workers = Vec::new();
    for _ in 0..8 {
        let runtime = Arc::clone(&runtime);
        let old_signature = old_signature.clone();
        workers.push(tokio::spawn(async move {
            for _ in 0..25 {
                let command = send_message_command("TopicA", Some("alice"), Some(old_signature.as_str()), &[], None);
                let channel_context = ();
                let _ = runtime
                    .check_remoting_with_source_ip(&channel_context, &command, Some("172.16.0.1"), Some("channel-a"))
                    .await;
                tokio::task::yield_now().await;
            }
        }));
    }

    std::fs::write(
        &acl_file,
        r#"
accounts:
  - accessKey: alice
    secretKey: second
    defaultTopicPerm: DENY
    topicPerms:
      - TopicB=PUB
"#,
    )
    .expect("acl file should be updated");
    runtime
        .reload_acl_file()
        .await
        .expect("manual ACL reload should succeed");
    assert!(
        runtime.acl_generation() > generation_before_reload,
        "successful reload should advance ACL generation for cache invalidation"
    );

    for worker in workers {
        worker.await.expect("concurrent authorization worker should not panic");
    }

    let old_command = send_message_command("TopicA", Some("alice"), Some(old_signature.as_str()), &[], None);
    runtime
        .check_remoting_with_source_ip(&(), &old_command, Some("172.16.0.1"), Some("channel-a"))
        .await
        .expect_err("reloaded ACL should reject the old secret and old topic");

    let new_signature = acl_signer::cal_signature(b"aliceTopicB", "second").expect("signature should calculate");
    let new_command = send_message_command("TopicB", Some("alice"), Some(new_signature.as_str()), &[], None);
    runtime
        .check_remoting_with_source_ip(&(), &new_command, Some("172.16.0.1"), Some("channel-a"))
        .await
        .expect("reloaded ACL should authorize TopicB with the second secret");

    runtime.shutdown().await.expect("runtime should shut down");
}

#[tokio::test]
async fn failed_acl_reload_keeps_previous_working_snapshot() {
    let temp = TempDir::new().expect("temp dir should be created");
    let acl_file = temp.path().join("plain_acl.yml");
    std::fs::write(
        &acl_file,
        r#"
accounts:
  - accessKey: alice
    secretKey: first
    defaultTopicPerm: DENY
    topicPerms:
      - TopicA=PUB
"#,
    )
    .expect("acl file should be written");

    let runtime = AuthRuntimeBuilder::new(AuthConfig {
        acl_file: CheetahString::from(acl_file.to_string_lossy().as_ref()),
        authentication_enabled: true,
        authorization_enabled: true,
        ..temp_auth_config(&temp)
    })
    .build()
    .await
    .expect("auth runtime should build");
    let generation_before_reload = runtime.acl_generation();

    std::fs::write(
        &acl_file,
        r#"
accounts:
  - accessKey: alice
    defaultTopicPerm: DENY
    topicPerms:
      - TopicB=PUB
"#,
    )
    .expect("acl file should be updated");
    runtime
        .reload_acl_file()
        .await
        .expect_err("invalid ACL reload should fail");
    assert_eq!(
        runtime.acl_generation(),
        generation_before_reload,
        "failed reload must not advance ACL generation"
    );

    let old_signature = acl_signer::cal_signature(b"aliceTopicA", "first").expect("signature should calculate");
    let old_command = send_message_command("TopicA", Some("alice"), Some(old_signature.as_str()), &[], None);
    runtime
        .check_remoting_with_source_ip(&(), &old_command, Some("172.16.0.1"), Some("channel-a"))
        .await
        .expect("failed reload must keep the previous working ACL snapshot");

    let new_signature = acl_signer::cal_signature(b"aliceTopicB", "first").expect("signature should calculate");
    let new_command = send_message_command("TopicB", Some("alice"), Some(new_signature.as_str()), &[], None);
    runtime
        .check_remoting_with_source_ip(&(), &new_command, Some("172.16.0.1"), Some("channel-a"))
        .await
        .expect_err("failed reload must not partially apply the invalid ACL file");
}

#[tokio::test]
async fn successful_acl_reload_removes_deleted_file_accounts_and_cached_acls() {
    let temp = TempDir::new().expect("temp dir should be created");
    let acl_file = temp.path().join("plain_acl.yml");
    std::fs::write(
        &acl_file,
        r#"
accounts:
  - accessKey: alice
    secretKey: first
    defaultTopicPerm: DENY
    topicPerms:
      - TopicA=PUB
  - accessKey: bob
    secretKey: second
    defaultTopicPerm: DENY
    topicPerms:
      - TopicB=PUB
"#,
    )
    .expect("acl file should be written");

    let runtime = AuthRuntimeBuilder::new(AuthConfig {
        acl_file: CheetahString::from(acl_file.to_string_lossy().as_ref()),
        authentication_enabled: true,
        authorization_enabled: true,
        ..temp_auth_config(&temp)
    })
    .build()
    .await
    .expect("auth runtime should build");

    let bob_signature = acl_signer::cal_signature(b"bobTopicB", "second").expect("signature should calculate");
    let bob_command = send_message_command("TopicB", Some("bob"), Some(bob_signature.as_str()), &[], None);
    runtime
        .check_remoting_with_source_ip(&(), &bob_command, Some("172.16.0.1"), Some("channel-b"))
        .await
        .expect("initial ACL should authorize bob and populate provider caches");
    let generation_before_reload = runtime.acl_generation();

    std::fs::write(
        &acl_file,
        r#"
accounts:
  - accessKey: alice
    secretKey: first
    defaultTopicPerm: DENY
    topicPerms:
      - TopicA=PUB
"#,
    )
    .expect("acl file should be updated");
    runtime
        .reload_acl_file()
        .await
        .expect("valid ACL reload should succeed");
    assert!(
        runtime.acl_generation() > generation_before_reload,
        "successful reload should advance ACL generation for cache invalidation"
    );

    runtime
        .check_remoting_with_source_ip(&(), &bob_command, Some("172.16.0.1"), Some("channel-b"))
        .await
        .expect_err("successful reload must remove deleted ACL-file accounts and invalidate cached ACLs");
}
