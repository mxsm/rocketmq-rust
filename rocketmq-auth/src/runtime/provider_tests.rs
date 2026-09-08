// Copyright 2026 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Mutex;
use std::task::Poll;

use rocketmq_runtime::ShutdownDeadline;
use tokio::sync::Notify;

use super::*;
use crate::AclImportAccount;
use crate::AclMetadataPort;
use crate::AclMetadataRead;
use crate::AclSnapshotImport;
use crate::ProviderBundle;
use crate::ProviderControl;
use crate::ProviderFuture;
use crate::Subject;
use crate::SubjectKey;
use crate::UserMetadataPort;
use crate::UserMetadataRead;

#[derive(Default)]
struct FakeProvider {
    users: Mutex<HashMap<String, User>>,
    acls: Mutex<HashMap<String, Acl>>,
    events: Arc<Mutex<Vec<String>>>,
    name: &'static str,
    fail: Mutex<Option<&'static str>>,
    hold_read: AtomicBool,
    hold_close: AtomicBool,
    hold_initialize: AtomicBool,
    release_read: Notify,
    release_close: Notify,
    release_initialize: Notify,
    stop_after_import: AtomicBool,
    context: Mutex<Option<ChildServiceContext>>,
}

impl FakeProvider {
    fn step(&self, step: &'static str) -> AuthServiceResult<()> {
        self.events.lock().unwrap().push(format!("{}.{step}", self.name));
        if *self.fail.lock().unwrap() == Some(step) {
            return Err(AuthServiceError::with_source(
                AuthOperation::InitializeProvider,
                AuthFailureKind::Unavailable,
                std::io::Error::other("private-provider-credential"),
            ));
        }
        Ok(())
    }

    fn bundle(self: &Arc<Self>) -> ProviderBundle {
        ProviderBundle::new(self.clone(), self.clone(), vec![self.clone()])
    }
}

impl UserMetadataRead for FakeProvider {
    fn lookup_user<'a>(&'a self, username: &'a str) -> ProviderFuture<'a, User> {
        Box::pin(async move {
            self.step("read")?;
            if self.hold_read.load(Ordering::Acquire) {
                self.release_read.notified().await;
            }
            self.users
                .lock()
                .unwrap()
                .get(username)
                .cloned()
                .ok_or_else(|| AuthServiceError::new(AuthOperation::ManageMetadata, AuthFailureKind::NotFound))
        })
    }
}

impl UserMetadataPort for FakeProvider {
    fn create_user(&self, user: User) -> ProviderFuture<'_, ()> {
        Box::pin(async move {
            self.step("create")?;
            self.users.lock().unwrap().insert(user.username().to_string(), user);
            Ok(())
        })
    }

    fn update_user(&self, user: User) -> ProviderFuture<'_, ()> {
        self.create_user(user)
    }

    fn delete_user<'a>(&'a self, username: &'a str) -> ProviderFuture<'a, ()> {
        Box::pin(async move {
            self.users.lock().unwrap().remove(username);
            Ok(())
        })
    }

    fn list_user<'a>(&'a self, _: Option<&'a str>) -> ProviderFuture<'a, Vec<User>> {
        Box::pin(async { Ok(self.users.lock().unwrap().values().cloned().collect()) })
    }
}

impl AclMetadataRead for FakeProvider {
    fn lookup_acl<'a>(&'a self, subject: &'a SubjectKey) -> ProviderFuture<'a, Option<Acl>> {
        Box::pin(async move { Ok(self.acls.lock().unwrap().get(subject.subject_key()).cloned()) })
    }
}

impl AclMetadataPort for FakeProvider {
    fn create_acl(&self, acl: Acl) -> ProviderFuture<'_, ()> {
        Box::pin(async move {
            self.acls.lock().unwrap().insert(acl.subject_key().to_owned(), acl);
            Ok(())
        })
    }

    fn update_acl(&self, acl: Acl) -> ProviderFuture<'_, ()> {
        self.create_acl(acl)
    }

    fn delete_acl<'a>(&'a self, subject: &'a SubjectKey) -> ProviderFuture<'a, ()> {
        Box::pin(async move {
            self.acls.lock().unwrap().remove(subject.subject_key());
            Ok(())
        })
    }

    fn list_acl<'a>(&'a self, _: Option<&'a str>, _: Option<&'a str>) -> ProviderFuture<'a, Vec<Acl>> {
        Box::pin(async { Ok(self.acls.lock().unwrap().values().cloned().collect()) })
    }
}

impl ProviderControl for FakeProvider {
    fn initialize<'a>(&'a self, _: &'a AuthConfig, context: ChildServiceContext) -> ProviderFuture<'a, ()> {
        Box::pin(async move {
            *self.context.lock().unwrap() = Some(context);
            self.step("initialize")?;
            if self.hold_initialize.load(Ordering::Acquire) {
                self.release_initialize.notified().await;
            }
            Ok(())
        })
    }

    fn flush(&self) -> ProviderFuture<'_, ()> {
        Box::pin(async { self.step("flush") })
    }

    fn close(&self) -> ProviderFuture<'_, ()> {
        Box::pin(async {
            self.step("close")?;
            if self.hold_close.load(Ordering::Acquire) {
                self.release_close.notified().await;
            }
            Ok(())
        })
    }
}

impl AclSnapshotImport for FakeProvider {
    fn import<'a>(&'a self, accounts: &'a [AclImportAccount], _: &'a [SubjectKey]) -> ProviderFuture<'a, ()> {
        Box::pin(async move {
            self.step("import")?;
            for account in accounts {
                self.create_user(account.user.clone()).await?;
                if let Some(acl) = &account.acl {
                    self.create_acl(acl.clone()).await?;
                }
            }
            if self.stop_after_import.load(Ordering::Acquire) {
                let context = self.context.lock().unwrap().clone().unwrap();
                context.task_group().shutdown(Duration::from_secs(1)).await;
            }
            Ok(())
        })
    }
}

fn builder(config: AuthConfig, bundle: ProviderBundle) -> AuthRuntimeBuilder {
    let context = rocketmq_runtime::RuntimeContext::from_current("provider-test");
    AuthRuntimeBuilder::new(config, context.service_context("auth")).with_provider_bundle(bundle)
}

async fn assert_pending<F: Future>(mut future: Pin<&mut F>) {
    assert!(std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx).is_pending())).await);
}

fn startup_failure(error: &AuthServiceError) -> &AuthStartupFailure {
    error.source().unwrap().downcast_ref().unwrap()
}

#[tokio::test]
async fn same_named_provider_instances_isolate_metadata_strategies_and_cached_decisions() {
    use crate::authentication::acl_signer;
    use crate::authorization::context::default_authorization_context::DefaultAuthorizationContext;
    use crate::AuthenticationMetadataManager;
    use crate::AuthorizationStrategy;
    use rocketmq_security_api::Action;

    let allowed = Arc::new(FakeProvider::default());
    let denied = Arc::new(FakeProvider::default());
    let resource = Resource::of_topic("orders");
    for (provider, password, decision) in [
        (&allowed, "first", crate::PolicyDecision::Allow),
        (&denied, "second", crate::PolicyDecision::Deny),
    ] {
        let mut user = User::of_with_type("alice", password, UserType::Normal);
        user.set_user_status(UserStatus::Enable);
        provider.users.lock().unwrap().insert("alice".into(), user.clone());
        let policy = Policy::of(vec![resource.clone()], vec![Action::Pub], None, decision);
        provider
            .create_acl(Acl::of_subject_and_policy(&user, policy))
            .await
            .unwrap();
    }
    let config = AuthConfig {
        config_name: "same-label".into(),
        authentication_enabled: true,
        authorization_enabled: true,
        authentication_strategy: "stateful".into(),
        stateful_authorization_cache_negative_enable: true,
        ..Default::default()
    };
    let first = builder(config.clone(), allowed.bundle()).build().await.unwrap();
    let second = builder(config.clone(), denied.bundle()).build().await.unwrap();
    let first_authn =
        crate::AuthenticationFactory::get_strategy(&config, Some(Arc::new(first.provider_registry.clone())))
            .await
            .unwrap();
    let second_authn =
        crate::AuthenticationFactory::get_strategy(&config, Some(Arc::new(second.provider_registry.clone())))
            .await
            .unwrap();
    let mut authn = crate::DefaultAuthenticationContext::new();
    authn.base.set_channel_id(Some("same-channel".into()));
    authn.set_username("alice".into());
    authn.set_content(b"signed-content".to_vec());
    authn.set_signature(acl_signer::cal_signature(b"signed-content", "first").unwrap().into());
    for _ in 0..2 {
        first_authn.authenticate(&authn).await.unwrap();
        assert_eq!(
            second_authn.authenticate(&authn).await.unwrap_err().kind(),
            AuthFailureKind::Unauthenticated
        );
    }
    let first_authz =
        crate::StatefulAuthorizationStrategy::new(config.clone(), Some(Box::new(first.provider_registry.clone())))
            .unwrap();
    let second_authz =
        crate::StatefulAuthorizationStrategy::new(config.clone(), Some(Box::new(second.provider_registry.clone())))
            .unwrap();
    let mut authz = DefaultAuthorizationContext::of("alice", SubjectType::User, resource, Action::Pub, "127.0.0.1");
    authz.set_channel_id("same-channel");
    for _ in 0..2 {
        assert_eq!(
            first_authz.evaluate(&authz).await.unwrap(),
            AuthorizationDecision::Allow
        );
        assert!(matches!(
            second_authz.evaluate(&authz).await.unwrap(),
            AuthorizationDecision::Deny(_)
        ));
    }
    let mut manager = crate::AuthenticationMetadataManagerImpl::with_registry(&first.provider_registry);
    assert_eq!(manager.get_user("alice").await.unwrap().password().unwrap(), "first");
    manager.shutdown().await.unwrap();
    let legacy = crate::AuthenticationFactory::get_metadata_provider_with_service(
        &config,
        Some(Arc::new(second.provider_registry.clone())),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(legacy.get_user("alice").await.unwrap().password().unwrap(), "second");
    *allowed.fail.lock().unwrap() = Some("read");
    first.provider_registry.advance_acl_generation();
    assert_eq!(
        first_authn.authenticate(&authn).await.unwrap_err().kind(),
        AuthFailureKind::Unavailable
    );
    assert_eq!(
        first_authz.evaluate(&authz).await.unwrap_err().kind(),
        AuthFailureKind::Unavailable
    );
    first.shutdown().await.unwrap();
    assert_eq!(
        first_authn.authenticate(&authn).await.unwrap_err().kind(),
        AuthFailureKind::Unavailable
    );
    assert_eq!(
        first_authz.evaluate(&authz).await.unwrap_err().kind(),
        AuthFailureKind::Unavailable
    );
    assert!(matches!(
        second_authz.evaluate(&authz).await.unwrap(),
        AuthorizationDecision::Deny(_)
    ));
    second.shutdown().await.unwrap();
}

#[tokio::test]
async fn provider_startup_failures_close_attempted_controls_in_reverse_order() {
    for failpoint in ["initialize", "create", "import"] {
        let events = Arc::new(Mutex::new(Vec::new()));
        let first = Arc::new(FakeProvider {
            name: "first",
            events: events.clone(),
            ..Default::default()
        });
        let second = Arc::new(FakeProvider {
            name: "second",
            events: events.clone(),
            ..Default::default()
        });
        let untouched = Arc::new(FakeProvider {
            name: "untouched",
            events: events.clone(),
            ..Default::default()
        });
        *second.fail.lock().unwrap() = Some(failpoint);
        let mut config = AuthConfig::default();
        let temp = tempfile::tempdir().unwrap();
        if failpoint == "create" {
            config.init_authentication_user = r#"{"username":"seed","password":"secret"}"#.into();
        }
        if failpoint == "import" {
            let path = temp.path().join("acl.yml");
            std::fs::write(&path, "accounts:\n  - accessKey: seed\n    secretKey: secret\n").unwrap();
            config.acl_file = path.to_string_lossy().as_ref().into();
        }
        let bundle = ProviderBundle::new(second.clone(), second.clone(), vec![first, second.clone(), untouched])
            .with_snapshot_import(second);
        let error = builder(config, bundle).build().await.err().unwrap();
        assert_eq!(error.kind(), AuthFailureKind::Unavailable);
        let failure = startup_failure(&error);
        assert!(failure.cleanup_error().is_none());
        assert!(failure.primary().source().is_some());
        assert!(!format!("{error:?} {error} {failure:?} {failure}").contains("private-provider-credential"));
        let closed: Vec<_> = events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event.ends_with(".close"))
            .cloned()
            .collect();
        if failpoint == "initialize" {
            assert_eq!(closed, ["second.close", "first.close"]);
        } else {
            assert_eq!(closed, ["untouched.close", "second.close", "first.close"]);
        }
        let previous = events.lock().unwrap().clone();
        failure
            .retry_cleanup_until(ShutdownDeadline::after(Duration::from_secs(1)))
            .await
            .unwrap();
        assert_eq!(*events.lock().unwrap(), previous);
    }
}

#[tokio::test]
async fn provider_missing_import_fails_before_initialization_or_local_fallback() {
    let provider = Arc::new(FakeProvider::default());
    let temp = tempfile::tempdir().unwrap();
    let config = AuthConfig {
        acl_file: "unused-acl.yml".into(),
        auth_config_path: temp.path().to_string_lossy().as_ref().into(),
        ..Default::default()
    };
    let error = builder(config, provider.bundle()).build().await.err().unwrap();
    assert_eq!(error.kind(), AuthFailureKind::Unsupported);
    assert!(provider.events.lock().unwrap().is_empty());
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn provider_configuration_is_validated_before_initialization() {
    let provider = Arc::new(FakeProvider::default());
    let config = AuthConfig {
        authentication_provider: "unsupported-authenticator".into(),
        ..Default::default()
    };
    let error = builder(config, provider.bundle()).build().await.err().unwrap();
    assert_eq!(error.kind(), AuthFailureKind::InvalidConfiguration);
    assert!(provider.events.lock().unwrap().is_empty());
}

#[tokio::test]
async fn provider_initialization_cannot_reopen_admission_after_shutdown_starts() {
    let provider = Arc::new(FakeProvider::default());
    provider.hold_initialize.store(true, Ordering::Release);
    let registry = ProviderRegistry::from_bundle(provider.bundle(), false);
    let runtime_context = rocketmq_runtime::RuntimeContext::from_current("initialize-stop-race");
    let context = runtime_context.service_context("auth");
    let config = AuthConfig::default();
    let mut initialize = Box::pin(registry.owner.initialize(&config, &context));
    assert_pending(initialize.as_mut()).await;
    let error = registry
        .shutdown_until(ShutdownDeadline::after(Duration::ZERO))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), AuthFailureKind::Timeout);
    provider.release_initialize.notify_one();
    assert_eq!(initialize.await.unwrap_err().kind(), AuthFailureKind::Unavailable);
    assert!(registry.admission().enter().is_err());
    registry
        .shutdown_until(ShutdownDeadline::after(Duration::from_secs(1)))
        .await
        .unwrap();
    assert_eq!(*provider.events.lock().unwrap(), [".initialize", ".flush", ".close"]);
    assert!(context.task_group().shutdown(Duration::from_secs(1)).await.is_healthy());
}

#[tokio::test]
async fn provider_watcher_start_failure_rolls_back_the_prepared_candidate() {
    let provider = Arc::new(FakeProvider::default());
    provider.stop_after_import.store(true, Ordering::Release);
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("acl.yml");
    std::fs::write(&path, "accounts:\n  - accessKey: seed\n    secretKey: secret\n").unwrap();
    let config = AuthConfig {
        acl_file: path.to_string_lossy().as_ref().into(),
        acl_file_watch_enabled: true,
        ..Default::default()
    };
    let error = builder(config, provider.bundle().with_snapshot_import(provider.clone()))
        .build()
        .await
        .err()
        .expect("watcher admission failure must fail startup");
    let failure = startup_failure(&error);
    assert!(failure.primary().source().is_some());
    assert!(failure.cleanup_error().is_none());
    let events = provider.events.lock().unwrap();
    assert_eq!(*events, [".initialize", ".import", ".create", ".close"]);
}

#[tokio::test]
async fn provider_shutdown_drains_requests_and_resumes_the_same_close_future() {
    let provider = Arc::new(FakeProvider::default());
    provider.users.lock().unwrap().insert("user".into(), User::of("user"));
    let runtime = builder(AuthConfig::default(), provider.bundle()).build().await.unwrap();
    let metadata_io = runtime.owned_metadata_io.as_ref().unwrap().clone();
    let clone = runtime.clone();
    let users = runtime.provider_registry.authentication_metadata_provider();
    provider.hold_read.store(true, Ordering::Release);
    let read = users.get_user("user");
    tokio::pin!(read);
    assert_pending(read.as_mut()).await;
    let mut stop = Box::pin(runtime.shutdown());
    assert_pending(stop.as_mut()).await;
    assert!(metadata_io.snapshot().accepting);
    assert_eq!(
        users.get_user("user").await.unwrap_err().kind(),
        AuthFailureKind::Unavailable
    );
    assert!(!provider
        .events
        .lock()
        .unwrap()
        .iter()
        .any(|event| event.ends_with(".flush")));
    provider.release_read.notify_one();
    assert_eq!(read.await.unwrap().username(), "user");
    provider.hold_close.store(true, Ordering::Release);
    assert_pending(stop.as_mut()).await;
    // Cancel the caller, preserving the finalizer future in the shared owner.
    drop(stop);
    let error = clone
        .shutdown_until(ShutdownDeadline::after(Duration::ZERO))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), AuthFailureKind::Timeout);
    assert!(
        metadata_io.snapshot().accepting,
        "provider close still owns the I/O lane"
    );
    provider.release_close.notify_one();
    clone.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
    assert!(!metadata_io.snapshot().accepting);
    let events = provider.events.lock().unwrap();
    assert_eq!(events.iter().filter(|event| event.ends_with(".flush")).count(), 1);
    assert_eq!(events.iter().filter(|event| event.ends_with(".close")).count(), 1);
}

#[tokio::test]
async fn provider_startup_retains_primary_and_all_cleanup_failures_for_retry() {
    let first = Arc::new(FakeProvider::default());
    let second = Arc::new(FakeProvider::default());
    *first.fail.lock().unwrap() = Some("close");
    *second.fail.lock().unwrap() = Some("close");
    let config = AuthConfig {
        acl_file: "import-not-supported.yml".into(),
        ..Default::default()
    };
    // Already initialized registry models a caller-supplied candidate; both
    // controls must be finalized even though validation fails before publication.
    let registry = ProviderRegistry::from_bundle(
        ProviderBundle::new(first.clone(), first.clone(), vec![first.clone(), second.clone()]),
        true,
    );
    let context = rocketmq_runtime::RuntimeContext::from_current("rollback-test");
    let error = AuthRuntimeBuilder::new(config, context.service_context("auth"))
        .with_provider_registry(registry)
        .build()
        .await
        .err()
        .unwrap();
    let failure = startup_failure(&error);
    assert_eq!(failure.primary().kind(), AuthFailureKind::Unsupported);
    let cleanup = failure
        .cleanup_error()
        .unwrap()
        .source()
        .unwrap()
        .downcast_ref::<crate::ProviderCleanupErrors>()
        .unwrap();
    assert_eq!(cleanup.failures().len(), 2);
    *first.fail.lock().unwrap() = None;
    *second.fail.lock().unwrap() = None;
    failure
        .retry_cleanup_until(ShutdownDeadline::after(Duration::from_secs(1)))
        .await
        .unwrap();
}
