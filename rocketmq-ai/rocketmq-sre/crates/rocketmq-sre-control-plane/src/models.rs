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

mod config;
mod lifecycle;
mod lifecycle_repository;
mod model;
mod repository;
mod service;
mod smoke_repository;

use rocketmq_sre_model_gateway::ProviderRejection;
use rocketmq_sre_model_gateway::ProviderStatusOutcome;

use crate::ControlPlaneRequestFailure;

pub(crate) fn provider_rejection_failure(rejection: ProviderRejection) -> ControlPlaneRequestFailure {
    match rejection {
        ProviderRejection::InvalidRequest => {
            ControlPlaneRequestFailure::validation("model_provider_invalid_request", "provider rejected request")
        }
        ProviderRejection::AuthenticationFailed => ControlPlaneRequestFailure::unauthorized(),
        ProviderRejection::AuthorizationFailed => {
            ControlPlaneRequestFailure::forbidden("model_provider_authorization_failed", "provider denied request")
        }
        ProviderRejection::PolicyDenied => {
            ControlPlaneRequestFailure::forbidden("model_provider_policy_denied", "provider policy denied request")
        }
        ProviderRejection::SafetyRefusal => {
            ControlPlaneRequestFailure::forbidden("model_provider_safety_refusal", "provider refused request")
        }
        ProviderRejection::CapabilityUnsupported => ControlPlaneRequestFailure::validation(
            "model_provider_capability_unsupported",
            "provider capability is unsupported",
        ),
        ProviderRejection::DataResidencyDenied => ControlPlaneRequestFailure::forbidden(
            "model_provider_data_residency_denied",
            "provider data-residency policy denied request",
        ),
        ProviderRejection::Cancelled => {
            ControlPlaneRequestFailure::conflict_code("model_provider_cancelled", "provider request was cancelled")
        }
        ProviderRejection::SchemaValidationFailed => ControlPlaneRequestFailure::validation(
            "model_provider_schema_validation_failed",
            "provider response failed schema validation",
        ),
        ProviderRejection::SecretAccessDenied => ControlPlaneRequestFailure::forbidden(
            "model_provider_secret_access_denied",
            "provider credential access was denied",
        ),
        ProviderRejection::UnsupportedWireVersion => ControlPlaneRequestFailure::validation(
            "model_provider_unsupported_wire_version",
            "provider wire version is unsupported",
        ),
        ProviderRejection::MutualTlsFailed => ControlPlaneRequestFailure::forbidden(
            "model_provider_mutual_tls_failed",
            "provider mutual TLS authentication failed",
        ),
        ProviderRejection::ProfileInvalid => {
            ControlPlaneRequestFailure::validation("model_provider_profile_invalid", "provider profile is invalid")
        }
    }
}

pub(crate) fn provider_configuration_failure(outcome: ProviderStatusOutcome) -> ControlPlaneRequestFailure {
    match outcome {
        ProviderStatusOutcome::Rejected { rejection, .. } => provider_rejection_failure(rejection),
        ProviderStatusOutcome::Operational(source) => ControlPlaneRequestFailure::configuration_source(source),
    }
}

pub(crate) use lifecycle::ModelProfileLifecyclePage;
pub(crate) use lifecycle::ModelProfileLifecycleTransitionRequest;
pub(crate) use lifecycle::ModelProfileLifecycleView;
pub(crate) use lifecycle::ModelProfileRollbackRequest;
pub(crate) use lifecycle::ProviderSmokeResultView;
pub(crate) use model::ConversationAnswerDecision;
pub(crate) use model::ConversationToolDecision;
pub(crate) use model::ModelCapabilitiesStatus;
pub(crate) use model::ModelCriticDecision;
pub(crate) use model::ModelDiagnosisDecision;
pub(crate) use model::ModelInvocationListQuery;
pub(crate) use model::ModelInvocationPage;
pub(crate) use service::ModelGatewayService;
