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

use std::sync::atomic::AtomicU64;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::LanguageCode;
use rocketmq_runtime::RuntimeContext;
use rocketmq_runtime::TaskGroup;
use rocketmq_security_api::PeerInfo;
use rocketmq_security_api::Principal;
use tokio::sync::watch;

use super::super::AuthenticationState;
use super::super::EmbeddedCaller;
use super::super::RequestContext;
use super::super::RequestOrigin;
use super::*;
use crate::connection::ConnectionState;
use crate::proxy_protocol::ProxyProtocolMetadata;
use crate::session_view::EmbeddedSessionRecord;
use crate::session_view::SessionId;

struct Fixture {
    original: OriginalRequestIdentity,
    received_at: Instant,
    context: RequestContext,
    lifecycle: RequestLifecycleProvenance,
    command: RemotingCommand,
}

fn address(value: &str) -> std::net::SocketAddr {
    value.parse().expect("test address must parse")
}

struct NetworkSessionHarness {
    owner: u64,
    local_addr: std::net::SocketAddr,
    remote_addr: std::net::SocketAddr,
    transport_peer_addr: std::net::SocketAddr,
    state_tx: watch::Sender<ConnectionState>,
    closed_tx: watch::Sender<bool>,
}

impl NetworkSessionHarness {
    fn new(owner: u64, remote_addr: std::net::SocketAddr, transport_peer_addr: std::net::SocketAddr) -> Self {
        let (state_tx, _) = watch::channel(ConnectionState::Healthy);
        let (closed_tx, _) = watch::channel(false);
        Self {
            owner,
            local_addr: address("192.0.2.10:10911"),
            remote_addr,
            transport_peer_addr,
            state_tx,
            closed_tx,
        }
    }

    fn provenance(
        &self,
        parent_task_group: &TaskGroup,
        proxy_protocol: Option<&ProxyProtocolMetadata>,
    ) -> RequestLifecycleProvenance {
        RequestLifecycleProvenance::network_for_test(
            self.owner,
            self.local_addr,
            self.remote_addr,
            self.transport_peer_addr,
            proxy_protocol,
            self.state_tx.subscribe(),
            self.closed_tx.subscribe(),
            parent_task_group,
        )
    }
}

fn network_fixture(
    parent_task_group: &TaskGroup,
    owner: u64,
    deadline: Option<crate::deadline::RequestDeadline>,
    one_way: bool,
) -> (Fixture, NetworkSessionHarness) {
    let peer_address = address("198.51.100.44:43123");
    let mut command = RemotingCommand::create_remoting_command(17)
        .set_opaque(91)
        .set_language(LanguageCode::JAVA)
        .set_version(123)
        .set_body(Bytes::from_static(b"request-body"));
    command.add_ext_field("tenant", "ingress-tenant");
    if one_way {
        command.mark_oneway_rpc_ref();
    }
    let original =
        OriginalRequestIdentity::capture(owner, &AtomicU64::new(1), &command).expect("test identity must allocate");
    let received_at = Instant::now();
    let session = NetworkSessionHarness::new(owner, peer_address, peer_address);
    let context = RequestContext::network(
        PeerInfo::new(peer_address, true),
        Some(Principal::new("network-user")),
        deadline,
    );

    (
        Fixture {
            original,
            received_at,
            context,
            lifecycle: session.provenance(parent_task_group, None),
            command,
        },
        session,
    )
}

fn embedded_fixture(
    parent_task_group: &TaskGroup,
    owner: u64,
    deadline: Option<crate::deadline::RequestDeadline>,
) -> (Fixture, EmbeddedSessionRecord) {
    let mut command = RemotingCommand::create_remoting_command(18)
        .set_opaque(92)
        .set_language(LanguageCode::RUST)
        .set_version(124);
    command.add_ext_field("tenant", "embedded-tenant");
    let original =
        OriginalRequestIdentity::capture(owner, &AtomicU64::new(1), &command).expect("test identity must allocate");
    let received_at = Instant::now();
    let session = EmbeddedSessionRecord::new(owner);
    let context = RequestContext::try_embedded_with_caller(
        EmbeddedCaller::BrokerProxy,
        Some(Principal::new("embedded-user")),
        deadline,
    )
    .expect("embedded fixture must have a principal");

    (
        Fixture {
            original,
            received_at,
            context,
            lifecycle: RequestLifecycleProvenance::from_embedded_session(&session, parent_task_group),
            command,
        },
        session,
    )
}

fn builder(fixture: Fixture) -> RemotingRequestBuilder {
    RemotingRequestBuilder::new(
        fixture.original,
        fixture.received_at,
        fixture.context,
        fixture.lifecycle,
        fixture.command,
    )
}

fn build(fixture: Fixture) -> Result<RemotingRequest, RemotingRequestBuildError> {
    builder(fixture).build()
}

async fn shutdown(runtime: RuntimeContext) {
    let report = runtime.shutdown_tasks(Duration::from_secs(1)).await;
    assert!(report.is_healthy(), "{}", report.to_json());
}

#[tokio::test]
async fn builder_assembles_a_network_request_with_one_canonical_deadline() {
    let runtime = RuntimeContext::from_current("remoting-request-network");
    let deadline = crate::deadline::RequestDeadline::after(Duration::from_secs(5));
    let (fixture, _session) = network_fixture(runtime.root_group(), 41, Some(deadline), false);
    let received_at = fixture.received_at;
    let request = build(fixture).expect("matching trusted network facts must build a request");

    assert_eq!(request.original_identity().original_code(), 17);
    assert_eq!(request.original_identity().original_opaque(), 91);
    assert_eq!(request.meta().received_at(), received_at);
    assert_eq!(request.meta().deadline(), Some(deadline));
    assert_eq!(request.control().deadline(), Some(deadline));
    assert_eq!(request.session().id(), SessionId::from_session_owner(41));
    assert!(matches!(request.origin(), RequestOrigin::Network { .. }));
    assert!(matches!(request.session(), SessionView::Network { .. }));

    shutdown(runtime).await;
}

#[tokio::test]
async fn builder_assembles_an_authenticated_embedded_request() {
    let runtime = RuntimeContext::from_current("remoting-request-embedded");
    let (fixture, _session) = embedded_fixture(runtime.root_group(), 42, None);
    let request = build(fixture).expect("matching authenticated embedded facts must build a request");

    assert!(matches!(request.origin(), RequestOrigin::Embedded { .. }));
    assert!(matches!(request.session(), SessionView::Embedded { .. }));
    assert_eq!(
        request.authentication().principal().map(Principal::id),
        Some("embedded-user")
    );

    shutdown(runtime).await;
}

#[tokio::test(start_paused = true)]
async fn builder_control_observes_its_session_not_a_same_deadline_decoy() {
    let runtime = RuntimeContext::from_current("remoting-request-session-provenance");
    let deadline = crate::deadline::RequestDeadline::after(Duration::from_secs(5));
    let (fixture, actual_session) = network_fixture(runtime.root_group(), 46, Some(deadline), false);
    let (_decoy_fixture, decoy_session) = network_fixture(runtime.root_group(), 47, Some(deadline), false);
    let request = build(fixture).expect("matching trusted facts must build a request");

    assert_eq!(request.meta().deadline(), Some(deadline));
    assert_eq!(request.control().deadline(), request.meta().deadline());
    decoy_session
        .closed_tx
        .send(true)
        .expect("decoy session publisher must remain subscribed");
    assert!(!request.control().is_cancelled());
    assert!(
        tokio::time::timeout(Duration::from_secs(1), request.control().cancelled())
            .await
            .is_err()
    );

    actual_session
        .closed_tx
        .send(true)
        .expect("actual session publisher must remain subscribed");
    request.control().cancelled().await;
    assert!(request.control().is_cancelled());

    shutdown(runtime).await;
}

#[tokio::test(start_paused = true)]
async fn builder_control_observes_its_parent_not_a_decoy_parent() {
    let runtime = RuntimeContext::from_current("remoting-request-parent-provenance");
    let actual_parent = runtime
        .service_context("remoting-request-actual-parent")
        .task_group()
        .clone();
    let decoy_parent = runtime
        .service_context("remoting-request-decoy-parent")
        .task_group()
        .clone();
    let (fixture, _session) = network_fixture(&actual_parent, 48, None, false);
    let request = build(fixture).expect("matching trusted facts must build a request");

    decoy_parent.cancel();
    assert!(!request.control().is_cancelled());
    assert!(
        tokio::time::timeout(Duration::from_secs(1), request.control().cancelled())
            .await
            .is_err()
    );

    actual_parent.cancel();
    request.control().cancelled().await;
    assert!(request.control().is_cancelled());

    shutdown(runtime).await;
}

#[tokio::test]
async fn builder_rejects_response_and_every_captured_command_fact_mismatch() {
    let runtime = RuntimeContext::from_current("remoting-request-command-mismatch");

    let (fixture, _session) = network_fixture(runtime.root_group(), 51, None, false);
    let mut response = fixture.command;
    response.mark_response_type_ref();
    assert_eq!(
        RemotingRequestBuilder::new(
            fixture.original,
            fixture.received_at,
            fixture.context,
            fixture.lifecycle,
            response,
        )
        .build()
        .err(),
        Some(RemotingRequestBuildError::ResponseCommand)
    );

    let (fixture, _session) = network_fixture(runtime.root_group(), 52, None, false);
    let mut code = fixture.command;
    code.set_code_ref(99);
    assert_eq!(
        RemotingRequestBuilder::new(
            fixture.original,
            fixture.received_at,
            fixture.context,
            fixture.lifecycle,
            code,
        )
        .build()
        .err(),
        Some(RemotingRequestBuildError::OriginalCommandMismatch)
    );

    let (fixture, _session) = network_fixture(runtime.root_group(), 53, None, false);
    let mut opaque = fixture.command;
    opaque.set_opaque_mut(100);
    assert_eq!(
        RemotingRequestBuilder::new(
            fixture.original,
            fixture.received_at,
            fixture.context,
            fixture.lifecycle,
            opaque,
        )
        .build()
        .err(),
        Some(RemotingRequestBuildError::OriginalCommandMismatch)
    );

    let (fixture, _session) = network_fixture(runtime.root_group(), 54, None, false);
    let mut one_way = fixture.command;
    one_way.mark_oneway_rpc_ref();
    assert_eq!(
        RemotingRequestBuilder::new(
            fixture.original,
            fixture.received_at,
            fixture.context,
            fixture.lifecycle,
            one_way,
        )
        .build()
        .err(),
        Some(RemotingRequestBuildError::OriginalCommandMismatch)
    );

    let (fixture, _session) = network_fixture(runtime.root_group(), 55, None, false);
    let language = fixture.command.set_language(LanguageCode::RUST);
    assert_eq!(
        RemotingRequestBuilder::new(
            fixture.original,
            fixture.received_at,
            fixture.context,
            fixture.lifecycle,
            language,
        )
        .build()
        .err(),
        Some(RemotingRequestBuildError::OriginalCommandMismatch)
    );

    let (fixture, _session) = network_fixture(runtime.root_group(), 56, None, false);
    let version = fixture.command.set_version(124);
    assert_eq!(
        RemotingRequestBuilder::new(
            fixture.original,
            fixture.received_at,
            fixture.context,
            fixture.lifecycle,
            version,
        )
        .build()
        .err(),
        Some(RemotingRequestBuildError::OriginalCommandMismatch)
    );

    shutdown(runtime).await;
}

#[tokio::test]
async fn builder_rejects_owner_and_origin_session_shape_mismatches() {
    let runtime = RuntimeContext::from_current("remoting-request-invalid-facts");

    let (mut fixture, _session) = network_fixture(runtime.root_group(), 61, None, false);
    let decoy_session = NetworkSessionHarness::new(62, address("198.51.100.44:43123"), address("198.51.100.44:43123"));
    fixture.lifecycle = decoy_session.provenance(runtime.root_group(), None);
    assert_eq!(
        build(fixture).err(),
        Some(RemotingRequestBuildError::SessionOwnerMismatch)
    );

    let (mut fixture, _session) = network_fixture(runtime.root_group(), 64, None, false);
    let embedded = EmbeddedSessionRecord::new(64);
    fixture.lifecycle = RequestLifecycleProvenance::from_embedded_session(&embedded, runtime.root_group());
    assert_eq!(
        build(fixture).err(),
        Some(RemotingRequestBuildError::NetworkSessionMismatch)
    );

    let (mut fixture, _session) = embedded_fixture(runtime.root_group(), 65, None);
    let network = NetworkSessionHarness::new(65, address("198.51.100.44:43123"), address("198.51.100.44:43123"));
    fixture.lifecycle = network.provenance(runtime.root_group(), None);
    assert_eq!(
        build(fixture).err(),
        Some(RemotingRequestBuildError::EmbeddedSessionMismatch)
    );

    shutdown(runtime).await;
}

#[tokio::test]
async fn builder_uses_effective_proxy_peer_and_rejects_mismatched_network_peers() {
    let runtime = RuntimeContext::from_current("remoting-request-effective-peer");
    let effective_peer = address("198.51.100.44:43123");
    let transport_peer = address("203.0.113.9:31000");
    let metadata = ProxyProtocolMetadata {
        transport_peer,
        source: effective_peer,
        destination: address("192.0.2.10:10911"),
        tlvs: Default::default(),
    };
    let (mut fixture, _session) = network_fixture(runtime.root_group(), 71, None, false);
    let proxied_session = NetworkSessionHarness::new(71, metadata.source, metadata.transport_peer);
    fixture.lifecycle = proxied_session.provenance(runtime.root_group(), Some(&metadata));
    assert!(
        build(fixture).is_ok(),
        "effective peer must be accepted even when transport peer differs"
    );

    let (mut fixture, _session) = network_fixture(runtime.root_group(), 72, None, false);
    let mismatched_session = NetworkSessionHarness::new(72, address("198.51.100.45:43123"), transport_peer);
    fixture.lifecycle = mismatched_session.provenance(runtime.root_group(), None);
    assert_eq!(
        build(fixture).err(),
        Some(RemotingRequestBuildError::NetworkPeerMismatch)
    );

    shutdown(runtime).await;
}

#[tokio::test]
async fn builder_rejects_embedded_requests_without_authenticated_ingress() {
    let runtime = RuntimeContext::from_current("remoting-request-embedded-authentication");
    let (mut fixture, _session) = embedded_fixture(runtime.root_group(), 81, None);
    let mut parts = fixture.context.into_parts();
    parts.authentication = AuthenticationState::Anonymous;
    fixture.context = RequestContext::from_parts(parts);

    assert_eq!(
        build(fixture).err(),
        Some(RemotingRequestBuildError::MissingEmbeddedAuthentication)
    );

    shutdown(runtime).await;
}

#[tokio::test]
async fn builder_ingress_view_borrows_pre_mutation_extension_fields_without_body_copy() {
    let runtime = RuntimeContext::from_current("remoting-request-ingress-view");
    let (fixture, _session) = network_fixture(runtime.root_group(), 91, None, false);
    let body_ptr = fixture.command.body().expect("fixture has a body").as_ptr();
    let builder = builder(fixture);

    {
        let ingress = builder.ingress_view();
        assert_eq!(ingress.original_identity().original_code(), 17);
        assert_eq!(
            ingress
                .ext_fields()
                .and_then(|fields| fields.get(&CheetahString::from_static_str("tenant"))),
            Some(&CheetahString::from_static_str("ingress-tenant"))
        );
    }

    let mut request = builder.build().expect("builder ownership transfers after ordering");
    assert_eq!(
        request.command().body().expect("request retains body").as_ptr(),
        body_ptr
    );
    let original = request.original_identity();
    request.command_mut().set_code_ref(99);
    request.command_mut().set_opaque_mut(100);
    request.command_mut().add_ext_field("tenant", "mutated-tenant");
    assert_eq!(request.original_identity(), original);

    shutdown(runtime).await;
}

#[tokio::test]
async fn request_extensions_remain_lazy_replace_values_and_preserve_rejected_values() {
    let runtime = RuntimeContext::from_current("remoting-request-extensions");
    let (fixture, _session) = network_fixture(runtime.root_group(), 101, None, false);
    let mut request = build(fixture).expect("matching trusted facts must build a request");

    assert_eq!(request.extension::<String>(), None);
    assert_eq!(request.try_insert_extension("first".to_owned()), Ok(None));
    assert_eq!(request.extension::<String>(), Some(&"first".to_owned()));
    assert_eq!(
        request.try_insert_extension("second".to_owned()),
        Ok(Some("first".to_owned()))
    );
    let deadline = crate::deadline::RequestDeadline::after(Duration::from_secs(1));
    assert_eq!(request.try_insert_extension(deadline), Err(deadline));

    shutdown(runtime).await;
}

#[tokio::test]
async fn request_extensions_reject_reserved_request_model_types() {
    let runtime = RuntimeContext::from_current("remoting-request-reserved-extensions");
    let (fixture, _session) = network_fixture(runtime.root_group(), 106, None, false);
    let mut request = build(fixture).expect("matching trusted facts must build a request");
    let (nested_fixture, _nested_session) = network_fixture(runtime.root_group(), 107, None, false);
    let nested = build(nested_fixture).expect("matching trusted facts must build a request");
    let nested_identity = nested.original_identity();

    let returned = match request.try_insert_extension(nested) {
        Err(value) => value,
        Ok(_) => panic!("request aggregates must remain reserved extension types"),
    };

    assert_eq!(returned.original_identity(), nested_identity);
    assert!(request.extension::<RemotingRequest>().is_none());

    let returned = match request.try_insert_extension(DeferredSlot::default()) {
        Err(value) => value,
        Ok(_) => panic!("deferred slots must remain reserved extension types"),
    };
    assert!(!returned.is_reserved());
    assert!(request.extension::<DeferredSlot>().is_none());

    shutdown(runtime).await;
}

#[tokio::test]
async fn one_way_requests_cannot_reserve_deferred_response_state() {
    let runtime = RuntimeContext::from_current("remoting-request-one-way-defer");
    let (fixture, _session) = network_fixture(runtime.root_group(), 111, None, true);
    assert_eq!(
        builder(fixture).reserve_deferred_response().build().err(),
        Some(RemotingRequestBuildError::OneWayDeferredResponse)
    );

    let (fixture, _session) = network_fixture(runtime.root_group(), 112, None, false);
    let request = builder(fixture)
        .reserve_deferred_response()
        .build()
        .expect("non-one-way request may retain the inert deferred slot");
    assert!(request.has_reserved_deferred_response());

    shutdown(runtime).await;
}
