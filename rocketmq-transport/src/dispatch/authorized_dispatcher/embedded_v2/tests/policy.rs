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

use std::net::SocketAddr;

use crate::runtime::RPCHook;

use super::*;

#[derive(Default)]
struct PrincipalPolicy {
    seen: Mutex<Vec<(String, bool, Option<String>)>>,
}

impl RequestPolicy for PrincipalPolicy {
    fn evaluate_authenticated(&self, context: AuthenticatedRequestContext<'_>) -> Decision {
        self.seen.lock().expect("policy lock").push((
            context.principal().id().to_owned(),
            context.request().peer().is_none(),
            context.request().fields().get("principal").map(ToString::to_string),
        ));
        Decision::Allow
    }
}

#[tokio::test]
async fn supplied_principal_and_peerless_origin_are_authoritative_over_command_headers() {
    let fixture = EmbeddedFixture::new("embedded-v2-security-origin");
    let (processor, state, _) = TestProcessor::new(Behavior::Reply);
    let policy = Arc::new(PrincipalPolicy::default());
    let security = Arc::new(TransportSecurity::secure_enforced(Some(policy.clone()), None));
    let dispatcher = AuthorizedCommandDispatcherV2::new(
        processor,
        Vec::new(),
        security,
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
    );
    let (mut command, _) = request(false);
    command.add_ext_field("principal", "forged-header");

    assert!(matches!(
        dispatcher
            .dispatch_embedded_v2(&fixture.task_group, Principal::new("trusted-broker"), None, command)
            .await
            .expect("authenticated embedded dispatch"),
        EmbeddedDispatchOutcome::Reply(_)
    ));
    assert_eq!(
        policy.seen.lock().expect("policy lock").as_slice(),
        [("trusted-broker".to_owned(), true, Some("forged-header".to_owned()))]
    );
    assert_eq!(state.processes.load(Ordering::SeqCst), 1);
    fixture.shutdown().await;
}

pub(super) struct DenyPolicy {
    pub(super) evaluations: AtomicUsize,
    pub(super) peerless: AtomicUsize,
}

impl RequestPolicy for DenyPolicy {
    fn evaluate_authenticated(&self, context: AuthenticatedRequestContext<'_>) -> Decision {
        self.evaluations.fetch_add(1, Ordering::SeqCst);
        self.peerless
            .fetch_add(usize::from(context.request().peer().is_none()), Ordering::SeqCst);
        Decision::deny("test denial")
    }
}

#[tokio::test]
async fn embedded_security_denial_is_peerless_exactly_once_and_development_exemption_fails_closed() {
    let fixture = EmbeddedFixture::new("embedded-v2-security-deny");
    let (processor, state, _) = TestProcessor::new(Behavior::Reply);
    let policy = Arc::new(DenyPolicy {
        evaluations: AtomicUsize::new(0),
        peerless: AtomicUsize::new(0),
    });
    let dispatcher = AuthorizedCommandDispatcherV2::new(
        processor,
        Vec::new(),
        Arc::new(TransportSecurity::secure_enforced(Some(policy.clone()), None)),
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
    );
    let outcome = dispatcher
        .dispatch_embedded_v2(
            &fixture.task_group,
            Principal::new("trusted-broker"),
            None,
            request(false).0,
        )
        .await
        .expect("security denial becomes an embedded reply");
    let EmbeddedDispatchOutcome::Reply(plan) = outcome else {
        panic!("security denial must return a reply")
    };
    assert_eq!(plan.response_code(), ResponseCode::NoPermission.to_i32());
    assert_eq!(policy.evaluations.load(Ordering::SeqCst), 1);
    assert_eq!(policy.peerless.load(Ordering::SeqCst), 1);
    assert_eq!(state.orderings.load(Ordering::SeqCst), 1);
    assert_eq!(state.clones.load(Ordering::SeqCst), 0);
    assert_eq!(state.processes.load(Ordering::SeqCst), 0);
    assert_eq!(state.observations.lock().expect("observation lock").len(), 1);

    let (processor, state, _) = TestProcessor::new(Behavior::Reply);
    let development = AuthorizedCommandDispatcherV2::new(
        processor,
        Vec::new(),
        Arc::new(TransportSecurity::development_insecure_loopback(None, None)),
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
    );
    let outcome = development
        .dispatch_embedded_v2(
            &fixture.task_group,
            Principal::new("trusted-broker"),
            None,
            request(false).0,
        )
        .await
        .expect("missing embedded policy fails closed with a denial reply");
    let EmbeddedDispatchOutcome::Reply(plan) = outcome else {
        panic!("fail-closed security must return a reply")
    };
    assert_eq!(plan.response_code(), ResponseCode::NoPermission.to_i32());
    assert_eq!(state.clones.load(Ordering::SeqCst), 0);
    assert_eq!(state.processes.load(Ordering::SeqCst), 0);
    fixture.shutdown().await;
}

struct AddressHook {
    calls: Arc<AtomicUsize>,
}

impl RPCHook for AddressHook {
    fn do_before_request(
        &self,
        _remote_addr: SocketAddr,
        _request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn do_after_response(
        &self,
        _remote_addr: SocketAddr,
        _request: &RemotingCommand,
        _response: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn embedded_v2_never_invokes_legacy_address_hooks() {
    let fixture = EmbeddedFixture::new("embedded-v2-no-address-hook");
    let (processor, state, _) = TestProcessor::new(Behavior::Reply);
    let calls = Arc::new(AtomicUsize::new(0));
    let dispatcher = AuthorizedCommandDispatcherV2::new(
        processor,
        vec![Arc::new(AddressHook {
            calls: Arc::clone(&calls),
        })],
        Arc::new(TransportSecurity::secure_enforced(Some(Arc::new(AllowPolicy)), None)),
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
    );
    assert!(matches!(
        dispatcher
            .dispatch_embedded_v2(
                &fixture.task_group,
                Principal::new("broker-proxy"),
                None,
                request(false).0
            )
            .await
            .expect("embedded dispatch"),
        EmbeddedDispatchOutcome::Reply(_)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(state.processes.load(Ordering::SeqCst), 1);
    fixture.shutdown().await;
}
