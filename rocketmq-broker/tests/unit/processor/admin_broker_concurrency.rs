//  Copyright 2023 The RocketMQ Rust Authors
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

use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;

use cheetah_string::CheetahString;
use parking_lot::Mutex;
use rocketmq_auth::AuthConfig;
use rocketmq_auth::ProviderRegistry;
use rocketmq_auth::User;
use rocketmq_auth::UserStatus;
use rocketmq_auth::UserType;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::body::user_info::UserInfo;
use rocketmq_protocol::protocol::header::create_user_request_header::CreateUserRequestHeader;
use rocketmq_protocol::protocol::header::get_user_request_headers::GetUserRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::RemotingSerializable;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_security_api::AuthenticatedRequestContext;
use rocketmq_security_api::Decision;
use rocketmq_security_api::Principal;
use rocketmq_security_api::RequestPolicy;
use rocketmq_store::MessageStoreConfig;
use rocketmq_store::StorePorts;
use rocketmq_transport::api::AdmissionController;
use rocketmq_transport::api::AdmissionLimits;
use rocketmq_transport::api::AuthorizedCommandDispatcher;
use rocketmq_transport::api::EmbeddedDispatchOutcome;
use rocketmq_transport::api::HandlerOutcome;
use rocketmq_transport::api::IngressRequestView;
use rocketmq_transport::api::RejectRequestDecision;
use rocketmq_transport::api::RemotingRequest;
use rocketmq_transport::api::RequestOrdering;
use rocketmq_transport::api::RequestProcessor;
use rocketmq_transport::api::ResponseObservation;
use rocketmq_transport::api::TransportSecurity;
use rocketmq_transport::test_support::EmbeddedRequestHarness;

use super::AdminBrokerProcessor;
use crate::auth::auth_admin_service::AuthAdminService;
use crate::broker_runtime::BrokerRuntime;
use crate::config::broker_config::BrokerConfig;
use crate::processor::dispatcher::BrokerProcessorType;
use crate::transaction::queue::default_transactional_message_service::DefaultTransactionalMessageService;

type TestAdminLeaf = BrokerProcessorType<StorePorts, DefaultTransactionalMessageService<StorePorts>>;

#[derive(Clone, Debug, Eq, PartialEq)]
struct ObservedAdminOperation {
    original_code: i32,
    command_code: i32,
    original_opaque: i32,
    username: Option<String>,
    response_code: Option<i32>,
}

#[derive(Clone)]
struct ObservedAdminProcessor {
    inner: TestAdminLeaf,
    operations: Arc<Mutex<Vec<ObservedAdminOperation>>>,
    mutate_command_code: Option<i32>,
}

impl RequestProcessor for ObservedAdminProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        if let Some(code) = self.mutate_command_code {
            request.command_mut().set_code_mut(code);
        }
        let original = request.original_identity();
        let command_code = request.command().code();
        let username = request
            .command()
            .ext_fields()
            .and_then(|fields| fields.get("username"))
            .map(ToString::to_string);
        let outcome = RequestProcessor::process(&mut self.inner, request).await?;
        let response_code = match &outcome {
            HandlerOutcome::Reply(plan) => Some(plan.response_code()),
            HandlerOutcome::Deferred(_) | HandlerOutcome::NoReply(_) => None,
        };
        self.operations.lock().push(ObservedAdminOperation {
            original_code: original.original_code(),
            command_code,
            original_opaque: original.original_opaque(),
            username,
            response_code,
        });
        Ok(outcome)
    }

    fn reject_request(&self, code: i32) -> RejectRequestDecision {
        RequestProcessor::reject_request(&self.inner, code)
    }

    fn request_ordering(&self, request: IngressRequestView<'_>) -> RequestOrdering {
        RequestProcessor::request_ordering(&self.inner, request)
    }

    fn observe_response(&self, observation: ResponseObservation) {
        RequestProcessor::observe_response(&self.inner, observation);
    }
}

struct AllowEmbeddedAdminPolicy;

impl RequestPolicy for AllowEmbeddedAdminPolicy {
    fn evaluate_authenticated(&self, _context: AuthenticatedRequestContext<'_>) -> Decision {
        Decision::Allow
    }
}

struct AdminFixture {
    owner: Option<RuntimeOwner>,
    harness: Option<EmbeddedRequestHarness<ObservedAdminProcessor>>,
    operations: Arc<Mutex<Vec<ObservedAdminOperation>>>,
    auth_admin_service: Arc<AuthAdminService>,
    temp_root: PathBuf,
}

impl AdminFixture {
    fn new(mutate_command_code: Option<i32>) -> Box<Self> {
        static NEXT_TEMP_ROOT: AtomicU64 = AtomicU64::new(1);

        let temp_root = std::env::temp_dir().join(format!(
            "rocketmq-admin-concurrency-{}-{}",
            std::process::id(),
            NEXT_TEMP_ROOT.fetch_add(1, Ordering::Relaxed)
        ));
        let broker_config = Arc::new(BrokerConfig {
            store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
            auth_config_path: temp_root.join("auth.json").to_string_lossy().into_owned().into(),
            ..BrokerConfig::default()
        });
        let message_store_config = Arc::new(MessageStoreConfig {
            store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
            ..MessageStoreConfig::default()
        });
        let runtime = BrokerRuntime::new(broker_config, message_store_config);
        let provider_registry =
            ProviderRegistry::local(&AuthConfig::default()).expect("create in-memory auth registry");
        let auth_admin_service = Arc::new(AuthAdminService::with_provider_registry(provider_registry));
        let processor: TestAdminLeaf = BrokerProcessorType::AdminBroker(Arc::new(AdminBrokerProcessor::new(
            runtime.admin_runtime_for_test(),
            Arc::clone(&auth_admin_service),
        )));
        let operations = Arc::new(Mutex::new(Vec::new()));
        let processor = ObservedAdminProcessor {
            inner: processor,
            operations: Arc::clone(&operations),
            mutate_command_code,
        };
        let mut runtime_config = RuntimeConfig::server_default("admin-concurrency-test");
        runtime_config.thread_stack_size = Some(16 * 1024 * 1024);
        let owner = RuntimeOwner::plan(runtime_config)
            .expect("test runtime configuration is valid")
            .build()
            .expect("Admin concurrency test runtime");
        let dispatcher = Arc::new(AuthorizedCommandDispatcher::new(
            processor,
            Vec::new(),
            Arc::new(TransportSecurity::secure_enforced(
                Some(Arc::new(AllowEmbeddedAdminPolicy)),
                None,
            )),
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
        ));
        let harness = EmbeddedRequestHarness::new(
            dispatcher,
            owner
                .root_context()
                .component("admin-concurrency-request")
                .task_group()
                .clone(),
            Principal::new("broker-proxy"),
        );
        drop(runtime);
        Box::new(Self {
            owner: Some(owner),
            harness: Some(harness),
            operations,
            auth_admin_service,
            temp_root,
        })
    }

    fn harness(&self) -> &EmbeddedRequestHarness<ObservedAdminProcessor> {
        self.harness.as_ref().expect("Admin harness must remain installed")
    }

    async fn seed_user(&self, username: &'static str) {
        let mut user = User::of_with_type(username, format!("{username}-password"), UserType::Normal);
        user.set_user_status(UserStatus::Enable);
        self.auth_admin_service
            .create_user(user)
            .await
            .expect("seed Admin read fixture user");
    }

    async fn finish(mut self: Box<Self>) {
        drop(self.harness.take());
        let owner = self.owner.take().expect("Admin runtime owner must remain installed");
        assert!(owner.shutdown_tasks().await.is_healthy());
        assert!(owner.shutdown_background().is_healthy());
        let temp_root = self.temp_root.clone();
        drop(self);
        let _ = std::fs::remove_dir_all(temp_root);
    }
}

fn get_user_request(username: &'static str, opaque: i32) -> RemotingCommand {
    let mut request = RemotingCommand::create_request_command(
        RequestCode::AuthGetUser,
        GetUserRequestHeader {
            username: CheetahString::from_static_str(username),
        },
    )
    .set_opaque(opaque);
    request.make_custom_header_to_net();
    request
}

fn create_user_request(username: &'static str, opaque: i32) -> RemotingCommand {
    let mut request = RemotingCommand::create_request_command(
        RequestCode::AuthCreateUser,
        CreateUserRequestHeader {
            username: CheetahString::from_static_str(username),
        },
    )
    .set_body(
        UserInfo {
            username: None,
            password: Some(CheetahString::from_static_str("mutation-password")),
            user_type: Some(CheetahString::from_static_str("Normal")),
            user_status: Some(CheetahString::from_static_str("enable")),
        }
        .encode()
        .expect("encode Admin create-user body"),
    )
    .set_opaque(opaque);
    request.make_custom_header_to_net();
    request
}

fn expect_success_reply(outcome: EmbeddedDispatchOutcome, expected_body: bool) {
    let EmbeddedDispatchOutcome::Reply(plan) = outcome else {
        panic!("Admin operation must produce one immediate response")
    };
    assert_eq!(ResponseCode::from(plan.response_code()), ResponseCode::Success);
    assert_eq!(plan.body_len() > 0, expected_body);
}

fn run_admin_test(name: &'static str, body: fn(&RuntimeOwner)) {
    thread::Builder::new()
        .name(name.to_owned())
        .stack_size(16 * 1024 * 1024)
        .spawn(move || {
            let owner = RuntimeOwner::plan(RuntimeConfig::server_default(format!("{name}-driver")))
                .expect("test runtime configuration is valid")
                .build()
                .expect("Admin test driver runtime");
            body(&owner);
            assert!(owner.block_on(owner.shutdown_tasks()).is_healthy());
            assert!(owner.shutdown_background().is_healthy());
        })
        .expect("start Admin test thread")
        .join()
        .expect("Admin test thread must not panic");
}

#[test]
fn shared_dispatcher_preserves_concurrent_read_and_mutation_identity() {
    run_admin_test("admin-concurrency", |owner| {
        owner.block_on(shared_dispatcher_concurrency_body());
    });
}

async fn shared_dispatcher_concurrency_body() {
    const FIRST_READ_OPAQUE: i32 = 72_001;
    const SECOND_READ_OPAQUE: i32 = 72_002;
    const FIRST_MUTATION_OPAQUE: i32 = 72_101;
    const SECOND_MUTATION_OPAQUE: i32 = 72_102;

    let fixture = AdminFixture::new(None);
    fixture.seed_user("read-alpha").await;
    fixture.seed_user("read-beta").await;

    let first_read = Box::pin(
        fixture
            .harness()
            .dispatch(None, get_user_request("read-alpha", FIRST_READ_OPAQUE)),
    );
    let second_read = Box::pin(
        fixture
            .harness()
            .dispatch(None, get_user_request("read-beta", SECOND_READ_OPAQUE)),
    );
    let (first_read, second_read) = tokio::join!(first_read, second_read);
    expect_success_reply(first_read.expect("first concurrent Admin read"), true);
    expect_success_reply(second_read.expect("second concurrent Admin read"), true);

    let first_mutation = Box::pin(
        fixture
            .harness()
            .dispatch(None, create_user_request("write-alpha", FIRST_MUTATION_OPAQUE)),
    );
    let second_mutation = Box::pin(
        fixture
            .harness()
            .dispatch(None, create_user_request("write-beta", SECOND_MUTATION_OPAQUE)),
    );
    let (first_mutation, second_mutation) = tokio::join!(first_mutation, second_mutation);
    expect_success_reply(first_mutation.expect("first concurrent Admin mutation"), false);
    expect_success_reply(second_mutation.expect("second concurrent Admin mutation"), false);

    for username in ["write-alpha", "write-beta"] {
        let user = fixture
            .auth_admin_service
            .get_user(username)
            .await
            .expect("read concurrent Admin mutation state")
            .expect("concurrent Admin mutation must not be lost");
        assert_eq!(user.username.as_deref(), Some(username));
        assert_eq!(user.password.as_deref(), Some("mutation-password"));
    }

    let mut operations = fixture.operations.lock().clone();
    operations.sort_by_key(|operation| operation.original_opaque);
    assert_eq!(
        operations,
        vec![
            ObservedAdminOperation {
                original_code: RequestCode::AuthGetUser as i32,
                command_code: RequestCode::AuthGetUser as i32,
                original_opaque: FIRST_READ_OPAQUE,
                username: Some("read-alpha".to_owned()),
                response_code: Some(ResponseCode::Success as i32),
            },
            ObservedAdminOperation {
                original_code: RequestCode::AuthGetUser as i32,
                command_code: RequestCode::AuthGetUser as i32,
                original_opaque: SECOND_READ_OPAQUE,
                username: Some("read-beta".to_owned()),
                response_code: Some(ResponseCode::Success as i32),
            },
            ObservedAdminOperation {
                original_code: RequestCode::AuthCreateUser as i32,
                command_code: RequestCode::AuthCreateUser as i32,
                original_opaque: FIRST_MUTATION_OPAQUE,
                username: Some("write-alpha".to_owned()),
                response_code: Some(ResponseCode::Success as i32),
            },
            ObservedAdminOperation {
                original_code: RequestCode::AuthCreateUser as i32,
                command_code: RequestCode::AuthCreateUser as i32,
                original_opaque: SECOND_MUTATION_OPAQUE,
                username: Some("write-beta".to_owned()),
                response_code: Some(ResponseCode::Success as i32),
            },
        ]
    );

    fixture.finish().await;
}

#[test]
fn dispatch_uses_original_code_after_mutable_command_code_changes() {
    run_admin_test("admin-original-code", |owner| {
        owner.block_on(original_code_mutation_body());
    });
}

async fn original_code_mutation_body() {
    const OPAQUE: i32 = 72_201;

    let fixture = AdminFixture::new(Some(RequestCode::AuthDeleteUser as i32));
    fixture.seed_user("original-code-user").await;

    let outcome = Box::pin(
        fixture
            .harness()
            .dispatch(None, get_user_request("original-code-user", OPAQUE)),
    )
    .await
    .expect("Admin dispatch after command-code mutation");
    expect_success_reply(outcome, true);
    assert!(fixture
        .auth_admin_service
        .get_user("original-code-user")
        .await
        .expect("read original-code behavior state")
        .is_some());
    assert_eq!(
        fixture.operations.lock().as_slice(),
        &[ObservedAdminOperation {
            original_code: RequestCode::AuthGetUser as i32,
            command_code: RequestCode::AuthDeleteUser as i32,
            original_opaque: OPAQUE,
            username: Some("original-code-user".to_owned()),
            response_code: Some(ResponseCode::Success as i32),
        }]
    );

    fixture.finish().await;
}
