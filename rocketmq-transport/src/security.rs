// Copyright 2023 The RocketMQ Rust Authors
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
use std::sync::Arc;
use std::sync::OnceLock;

use cheetah_string::CheetahString;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_security_api::evaluate_request;
use rocketmq_security_api::Action;
use rocketmq_security_api::AuthorizationDecision;
use rocketmq_security_api::AuthorizationDenial;
use rocketmq_security_api::IngressDecision;
use rocketmq_security_api::IngressPolicy;
use rocketmq_security_api::LayerEvaluation;
use rocketmq_security_api::LayerFailureKind;
use rocketmq_security_api::OutboundSigner;
use rocketmq_security_api::PeerInfo;
use rocketmq_security_api::Principal;
use rocketmq_security_api::RequestContext;
use rocketmq_security_api::RequestPolicy;
use rocketmq_security_api::Resource;
use rocketmq_security_api::SecurityBootstrapProfile;
use rocketmq_security_api::SecurityContractViolation;
use rocketmq_security_api::SecurityOperation;
use rocketmq_security_api::SecurityProviderError;
use rocketmq_security_api::SecurityRequestView;

fn empty_fields() -> &'static HashMap<CheetahString, CheetahString> {
    static EMPTY: OnceLock<HashMap<CheetahString, CheetahString>> = OnceLock::new();
    EMPTY.get_or_init(HashMap::new)
}

/// Borrows security-relevant fields without copying the protocol command or body.
pub fn request_view<'a>(command: &'a RemotingCommand, peer: Option<&'a PeerInfo>) -> SecurityRequestView<'a> {
    let fields: &'a HashMap<CheetahString, CheetahString> = match command.ext_fields() {
        Some(fields) => fields,
        None => empty_fields(),
    };
    SecurityRequestView::new(
        command.code(),
        command.version(),
        fields,
        command.body().map(bytes::Bytes::as_ref),
        peer,
    )
}

/// Injected transport ports; provider implementations remain in composition crates.
pub struct TransportSecurity {
    profile: SecurityBootstrapProfile,
    ingress_policy: Option<Arc<dyn IngressPolicy>>,
    policy: Option<Arc<dyn RequestPolicy>>,
    signer: Option<Arc<dyn OutboundSigner>>,
}

impl TransportSecurity {
    /// Returns the process security profile selected by the composition root.
    #[must_use]
    pub(crate) const fn profile(&self) -> SecurityBootstrapProfile {
        self.profile
    }

    /// Returns whether this adapter was constructed for a secure-enforced process.
    #[must_use]
    pub const fn is_secure_enforced(&self) -> bool {
        matches!(self.profile, SecurityBootstrapProfile::SecureEnforced)
    }

    /// Creates an explicitly insecure transport adapter for loopback-only development.
    ///
    /// The listener address restriction is enforced by the process security bootstrap before bind.
    pub fn development_insecure_loopback(
        policy: Option<Arc<dyn RequestPolicy>>,
        signer: Option<Arc<dyn OutboundSigner>>,
    ) -> Self {
        Self {
            profile: SecurityBootstrapProfile::DevelopmentInsecureLoopback,
            ingress_policy: None,
            policy,
            signer,
        }
    }

    /// Creates a fail-closed transport adapter for a securely bootstrapped process.
    pub fn secure_enforced(policy: Option<Arc<dyn RequestPolicy>>, signer: Option<Arc<dyn OutboundSigner>>) -> Self {
        Self {
            profile: SecurityBootstrapProfile::SecureEnforced,
            ingress_policy: None,
            policy,
            signer,
        }
    }

    /// Installs a coarse ingress policy for the transport dispatch boundary.
    ///
    /// When configured, this policy is evaluated before a request reaches the
    /// service processor. The resource policy is evaluated when no ingress
    /// policy has been installed.
    #[must_use]
    pub fn with_ingress_policy(mut self, ingress_policy: Arc<dyn IngressPolicy>) -> Self {
        self.ingress_policy = Some(ingress_policy);
        self
    }

    /// Projects a request onto the coarse ingress continuation contract.
    ///
    /// This method does not authenticate or evaluate detailed resource policy.
    /// A missing secure ingress policy is unavailable and must be resolved as a
    /// fail-closed denial by the caller.
    pub fn authorize_ingress(
        &self,
        command: &RemotingCommand,
        peer: Option<&PeerInfo>,
    ) -> LayerEvaluation<IngressDecision> {
        match &self.ingress_policy {
            Some(policy) => policy.evaluate_ingress(request_view(command, peer)),
            None => match self.profile {
                SecurityBootstrapProfile::DevelopmentInsecureLoopback => Ok(IngressDecision::AllowToContinue),
                SecurityBootstrapProfile::SecureEnforced => Err(LayerFailureKind::Unavailable),
            },
        }
    }

    pub fn authorize(
        &self,
        command: &RemotingCommand,
        peer: Option<&PeerInfo>,
        principal: Option<&Principal>,
        resource: Resource,
        action: Action,
    ) -> LayerEvaluation<AuthorizationDecision> {
        let Some(policy) = &self.policy else {
            return match self.profile {
                SecurityBootstrapProfile::DevelopmentInsecureLoopback => Ok(AuthorizationDecision::Allow),
                SecurityBootstrapProfile::SecureEnforced => Err(LayerFailureKind::Unavailable),
            };
        };
        let context = RequestContext::new(request_view(command, peer), principal, resource, action);
        Ok(evaluate_request(policy.as_ref(), &context))
    }

    pub(crate) fn authorize_for_dispatch(
        &self,
        command: &RemotingCommand,
        peer: Option<&PeerInfo>,
        principal: Option<&Principal>,
        resource: Resource,
        action: Action,
    ) -> LayerEvaluation<AuthorizationDecision> {
        if self.ingress_policy.is_some() {
            return match self.authorize_ingress(command, peer) {
                Ok(IngressDecision::AllowToContinue) => Ok(AuthorizationDecision::Allow),
                Ok(IngressDecision::Deny) => Ok(AuthorizationDecision::Deny(AuthorizationDenial::PermissionDenied)),
                Err(failure) => Err(failure),
            };
        }
        self.authorize(command, peer, principal, resource, action)
    }

    pub(crate) fn authorize_embedded_for_dispatch(
        &self,
        command: &RemotingCommand,
        principal: &Principal,
        resource: Resource,
        action: Action,
    ) -> LayerEvaluation<AuthorizationDecision> {
        if self.ingress_policy.is_some() {
            return match self.authorize_ingress(command, None) {
                Ok(IngressDecision::AllowToContinue) => Ok(AuthorizationDecision::Allow),
                Ok(IngressDecision::Deny) => Ok(AuthorizationDecision::Deny(AuthorizationDenial::PermissionDenied)),
                Err(failure) => Err(failure),
            };
        }
        let Some(policy) = &self.policy else {
            return Err(LayerFailureKind::Unavailable);
        };
        let context = RequestContext::new(request_view(command, None), Some(principal), resource, action);
        Ok(evaluate_request(policy.as_ref(), &context))
    }

    pub fn sign(&self, command: &mut RemotingCommand, peer: Option<&PeerInfo>) -> Result<(), SecurityProviderError> {
        let Some(signer) = &self.signer else {
            return match self.profile {
                SecurityBootstrapProfile::DevelopmentInsecureLoopback => Ok(()),
                SecurityBootstrapProfile::SecureEnforced => Err(SecurityProviderError::contract(
                    SecurityOperation::SignRequest,
                    SecurityContractViolation::SigningProviderRequired,
                )),
            };
        };
        let signature = signer.sign(request_view(command, peer))?;
        command.ensure_ext_fields_initialized();
        for (key, value) in signature.fields() {
            command.add_ext_field(key.clone(), value.expose_secret().clone());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct StaticIngress(LayerEvaluation<IngressDecision>);

    impl IngressPolicy for StaticIngress {
        fn evaluate_ingress(&self, _request: SecurityRequestView<'_>) -> LayerEvaluation<IngressDecision> {
            self.0
        }
    }

    fn security_with_ingress(decision: LayerEvaluation<IngressDecision>) -> TransportSecurity {
        TransportSecurity::secure_enforced(None, None).with_ingress_policy(Arc::new(StaticIngress(decision)))
    }

    #[test]
    fn dispatch_ingress_deny_has_closed_reason_and_failures_remain_typed() {
        let command = RemotingCommand::create_remoting_command(10);
        let principal = Principal::new("authenticated");
        let resource = Resource::topic("TopicA");

        assert_eq!(
            security_with_ingress(Ok(IngressDecision::Deny)).authorize_for_dispatch(
                &command,
                None,
                Some(&principal),
                resource.clone(),
                Action::Manage,
            ),
            Ok(AuthorizationDecision::Deny(AuthorizationDenial::PermissionDenied))
        );
        assert_eq!(
            security_with_ingress(Ok(IngressDecision::Deny)).authorize_embedded_for_dispatch(
                &command,
                &principal,
                resource.clone(),
                Action::Manage,
            ),
            Ok(AuthorizationDecision::Deny(AuthorizationDenial::PermissionDenied))
        );

        for failure in [
            LayerFailureKind::Unavailable,
            LayerFailureKind::Error,
            LayerFailureKind::Timeout,
        ] {
            assert_eq!(
                security_with_ingress(Err(failure)).authorize_for_dispatch(
                    &command,
                    None,
                    Some(&principal),
                    resource.clone(),
                    Action::Manage,
                ),
                Err(failure)
            );
            assert_eq!(
                security_with_ingress(Err(failure)).authorize_embedded_for_dispatch(
                    &command,
                    &principal,
                    resource.clone(),
                    Action::Manage,
                ),
                Err(failure)
            );
        }
    }
}
