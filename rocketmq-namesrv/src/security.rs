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

use std::sync::Arc;

use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_security_api::AuthenticatedRequestContext;
use rocketmq_security_api::AuthorizationDecision;
use rocketmq_security_api::AuthorizationDenial;
use rocketmq_security_api::IngressDecision;
use rocketmq_security_api::IngressPolicy;
use rocketmq_security_api::LayerEvaluation;
use rocketmq_security_api::RequestPolicy;
use rocketmq_security_api::SecurityBootstrapOutcome;
use rocketmq_security_api::SecurityBootstrapProfile;
use rocketmq_transport::api::TransportSecurity;

/// Low-cardinality authorization classes for every NameServer request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NameServerRequestClass {
    /// Topic route lookup served from the NameServer data plane.
    RouteRead,
    /// Broker registration, heartbeat, and unregistration control traffic.
    BrokerControl,
    /// Read-only cluster, Topic, KV, or configuration administration.
    AdminRead,
    /// Mutating KV, Topic, permission, or configuration administration.
    AdminWrite,
}

impl NameServerRequestClass {
    /// Returns the stable low-cardinality label used by logs and metrics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RouteRead => "route-read",
            Self::BrokerControl => "broker-control",
            Self::AdminRead => "admin-read",
            Self::AdminWrite => "admin-write",
        }
    }
}

/// Classifies the complete request surface currently handled by NameServer.
///
/// Unknown and non-NameServer request codes return `None` and must be denied
/// before reaching the default request processor.
#[must_use]
pub const fn classify_namesrv_request(code: RequestCode) -> Option<NameServerRequestClass> {
    Some(match code {
        RequestCode::GetRouteinfoByTopic => NameServerRequestClass::RouteRead,
        RequestCode::RegisterBroker | RequestCode::UnregisterBroker | RequestCode::BrokerHeartbeat => {
            NameServerRequestClass::BrokerControl
        }
        RequestCode::GetKvConfig
        | RequestCode::QueryDataVersion
        | RequestCode::GetBrokerMemberGroup
        | RequestCode::GetBrokerClusterInfo
        | RequestCode::GetAllTopicListFromNameserver
        | RequestCode::GetKvlistByNamespace
        | RequestCode::GetTopicsByCluster
        | RequestCode::GetSystemTopicListFromNs
        | RequestCode::GetUnitTopicList
        | RequestCode::GetHasUnitSubTopicList
        | RequestCode::GetHasUnitSubUnunitTopicList
        | RequestCode::GetNamesrvConfig => NameServerRequestClass::AdminRead,
        RequestCode::PutKvConfig
        | RequestCode::DeleteKvConfig
        | RequestCode::WipeWritePermOfBroker
        | RequestCode::AddWritePermOfBroker
        | RequestCode::DeleteTopicInNamesrv
        | RequestCode::RegisterTopicInNamesrv
        | RequestCode::UpdateNamesrvConfig => NameServerRequestClass::AdminWrite,
        _ => return None,
    })
}

/// Transport policy that admits only the enumerated NameServer protocol
/// surface after the composition root has installed protocol authorization.
///
/// Detailed Topic, cluster, and administrative ACL checks remain the
/// responsibility of `rocketmq_auth::AuthRuntime`; this policy prevents an
/// unknown request from bypassing that protocol boundary through the default
/// transport dispatcher.
#[doc(hidden)]
pub struct NameServerTransportPolicy;

/// Builds the NameServer transport boundary selected by a validated bootstrap outcome.
///
/// A [`SecurityBootstrapOutcome::Disabled`] result selects the legacy
/// development-compatible transport behavior. It is an explicit migration
/// choice, not a validated loopback-listener proof. A validated development
/// outcome has already proved every supplied listener is loopback-only.
///
/// This is hidden from the public documentation because the bootstrap binary is
/// the only production composition root. It remains public so that binary and
/// library targets use the exact same outcome-to-transport projection.
#[doc(hidden)]
#[must_use]
pub fn build_namesrv_transport_security(outcome: SecurityBootstrapOutcome) -> Arc<TransportSecurity> {
    match outcome {
        SecurityBootstrapOutcome::Disabled => Arc::new(TransportSecurity::development_insecure_loopback(None, None)),
        SecurityBootstrapOutcome::Validated(validated) => match validated.profile() {
            SecurityBootstrapProfile::SecureEnforced => Arc::new(
                TransportSecurity::secure_enforced(None, None).with_ingress_policy(Arc::new(NameServerTransportPolicy)),
            ),
            SecurityBootstrapProfile::DevelopmentInsecureLoopback => {
                Arc::new(TransportSecurity::development_insecure_loopback(None, None))
            }
        },
    }
}

impl IngressPolicy for NameServerTransportPolicy {
    fn evaluate_ingress(
        &self,
        request: rocketmq_security_api::SecurityRequestView<'_>,
    ) -> LayerEvaluation<IngressDecision> {
        match classify_namesrv_request(RequestCode::from(request.code())) {
            Some(_) => Ok(IngressDecision::AllowToContinue),
            None => Ok(IngressDecision::Deny),
        }
    }
}

impl RequestPolicy for NameServerTransportPolicy {
    fn evaluate_authenticated(&self, context: AuthenticatedRequestContext<'_>) -> AuthorizationDecision {
        match classify_namesrv_request(RequestCode::from(context.request().code())) {
            Some(_) => AuthorizationDecision::Allow,
            None => AuthorizationDecision::Deny(AuthorizationDenial::PermissionDenied),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rocketmq_security_api::{evaluate_request, Action, Principal, RequestContext, Resource, SecurityRequestView};

    use super::*;

    #[test]
    fn classifier_covers_the_nameserver_request_surface() {
        let expected = [
            (RequestCode::GetRouteinfoByTopic, NameServerRequestClass::RouteRead),
            (RequestCode::RegisterBroker, NameServerRequestClass::BrokerControl),
            (RequestCode::UnregisterBroker, NameServerRequestClass::BrokerControl),
            (RequestCode::BrokerHeartbeat, NameServerRequestClass::BrokerControl),
            (RequestCode::GetKvConfig, NameServerRequestClass::AdminRead),
            (RequestCode::QueryDataVersion, NameServerRequestClass::AdminRead),
            (RequestCode::GetBrokerMemberGroup, NameServerRequestClass::AdminRead),
            (RequestCode::GetBrokerClusterInfo, NameServerRequestClass::AdminRead),
            (
                RequestCode::GetAllTopicListFromNameserver,
                NameServerRequestClass::AdminRead,
            ),
            (RequestCode::GetKvlistByNamespace, NameServerRequestClass::AdminRead),
            (RequestCode::GetTopicsByCluster, NameServerRequestClass::AdminRead),
            (RequestCode::GetSystemTopicListFromNs, NameServerRequestClass::AdminRead),
            (RequestCode::GetUnitTopicList, NameServerRequestClass::AdminRead),
            (RequestCode::GetHasUnitSubTopicList, NameServerRequestClass::AdminRead),
            (
                RequestCode::GetHasUnitSubUnunitTopicList,
                NameServerRequestClass::AdminRead,
            ),
            (RequestCode::GetNamesrvConfig, NameServerRequestClass::AdminRead),
            (RequestCode::PutKvConfig, NameServerRequestClass::AdminWrite),
            (RequestCode::DeleteKvConfig, NameServerRequestClass::AdminWrite),
            (RequestCode::WipeWritePermOfBroker, NameServerRequestClass::AdminWrite),
            (RequestCode::AddWritePermOfBroker, NameServerRequestClass::AdminWrite),
            (RequestCode::DeleteTopicInNamesrv, NameServerRequestClass::AdminWrite),
            (RequestCode::RegisterTopicInNamesrv, NameServerRequestClass::AdminWrite),
            (RequestCode::UpdateNamesrvConfig, NameServerRequestClass::AdminWrite),
        ];
        for (request, class) in expected {
            assert_eq!(classify_namesrv_request(request), Some(class));
        }
    }

    #[test]
    fn classifier_fails_closed_for_unknown_or_foreign_codes() {
        assert_eq!(classify_namesrv_request(RequestCode::Unknown), None);
        assert_eq!(classify_namesrv_request(RequestCode::SendMessage), None);
    }

    #[test]
    fn request_classes_have_stable_metric_labels() {
        for (class, label) in [
            (NameServerRequestClass::RouteRead, "route-read"),
            (NameServerRequestClass::BrokerControl, "broker-control"),
            (NameServerRequestClass::AdminRead, "admin-read"),
            (NameServerRequestClass::AdminWrite, "admin-write"),
        ] {
            assert_eq!(class.as_str(), label);
        }
    }

    #[test]
    fn transport_policy_allows_nameserver_codes_and_denies_foreign_or_unknown_codes() {
        let fields = HashMap::new();
        let principal = Principal::new("namesrv-client");
        for (code, allowed) in [
            (RequestCode::GetRouteinfoByTopic.to_i32(), true),
            (RequestCode::RegisterBroker.to_i32(), true),
            (RequestCode::GetNamesrvConfig.to_i32(), true),
            (RequestCode::UpdateNamesrvConfig.to_i32(), true),
            (RequestCode::PullMessage.to_i32(), false),
            (i32::MAX, false),
        ] {
            let view = SecurityRequestView::new(code, 1, &fields, None, None);
            assert_eq!(
                NameServerTransportPolicy.evaluate_ingress(view).unwrap(),
                if allowed {
                    IngressDecision::AllowToContinue
                } else {
                    IngressDecision::Deny
                },
                "ingress code {code}"
            );
            let context = RequestContext::new(view, Some(&principal), Resource::topic("topic"), Action::Publish);
            assert_eq!(
                evaluate_request(&NameServerTransportPolicy, &context),
                if allowed {
                    AuthorizationDecision::Allow
                } else {
                    AuthorizationDecision::Deny(AuthorizationDenial::PermissionDenied)
                },
                "authenticated code {code}"
            );
        }
    }

    #[test]
    fn unauthenticated_requests_are_denied_before_policy_evaluation() {
        struct MustNotEvaluate;
        impl RequestPolicy for MustNotEvaluate {
            fn evaluate_authenticated(&self, _: AuthenticatedRequestContext<'_>) -> AuthorizationDecision {
                panic!("missing identity must be rejected before invoking the policy");
            }
        }

        let fields = HashMap::new();
        let view = SecurityRequestView::new(RequestCode::GetRouteinfoByTopic.to_i32(), 1, &fields, None, None);
        let context = RequestContext::new(view, None, Resource::topic("topic"), Action::Publish);
        let denied = AuthorizationDecision::Deny(AuthorizationDenial::SubjectUnknown);
        assert_eq!(evaluate_request(&NameServerTransportPolicy, &context), denied);
        assert_eq!(evaluate_request(&MustNotEvaluate, &context), denied);
    }
}
