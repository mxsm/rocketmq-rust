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

//! Narrow exact-address management requests. No request payload implements Debug or Serde.

#[derive(Clone, Copy)]
pub enum AclReadKind {
    Users,
    Policies,
}

/// Transient Broker data whose Debug output cannot expose passwords or full policies.
pub struct AclDocuments(Vec<serde_json::Value>);
impl AclDocuments {
    pub fn new(values: Vec<serde_json::Value>) -> Self {
        Self(values)
    }
    pub fn into_values(self) -> Vec<serde_json::Value> {
        self.0
    }
}
impl std::fmt::Debug for AclDocuments {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AclDocuments")
            .field("count", &self.0.len())
            .finish()
    }
}

/// A confirmed Broker rejection is distinct from an unconfirmed transport or generic error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AclRemoteFailure {
    AccessDenied,
    Unsupported,
    Invalid,
    Unconfirmed,
}

/// Classifies the retained wire response, never the descriptor's outgoing remoting projection.
pub fn classify_acl_failure(error: &super::AdminError) -> AclRemoteFailure {
    let broker_code = error.diagnostic_view().ok().and_then(|view| {
        view.fields().find_map(|field| match (field.name(), field.value()) {
            ("broker_code", rocketmq_error::ViewValueRef::I64(code)) => Some(code),
            _ => None,
        })
    });
    match broker_code {
        Some(16) => AclRemoteFailure::AccessDenied,
        Some(3) => AclRemoteFailure::Unsupported,
        Some(29 | 3001) => AclRemoteFailure::Invalid,
        _ if error.failure() == super::AdminFailure::InvalidArgument => AclRemoteFailure::Invalid,
        _ => AclRemoteFailure::Unconfirmed,
    }
}

#[derive(Clone)]
pub struct AclEntryWrite {
    pub resource: String,
    pub actions: Vec<String>,
    pub source_ips: Vec<String>,
    pub decision: String,
}
#[derive(Clone)]
pub enum AclExactWrite {
    User {
        create: bool,
        username: String,
        password: Option<String>,
        user_type: String,
        status: String,
    },
    DeleteUser {
        username: String,
    },
    Policy {
        create: bool,
        subject: String,
        policy_type: String,
        entry: AclEntryWrite,
    },
    DeleteEntry {
        subject: String,
        policy_type: String,
        resource: String,
    },
}
impl AclExactWrite {
    /// # Errors
    /// Rejects empty identities and unsupported enum values before any RPC.
    pub fn validate(&self, address: &str) -> super::AdminResult<()> {
        let valid = match self {
            Self::User {
                create,
                username,
                password,
                user_type,
                status,
            } => {
                !username.is_empty()
                    && ["Normal", "Super"].contains(&user_type.as_str())
                    && ["enable", "disable"].contains(&status.as_str())
                    && (!*create || password.as_ref().is_some_and(|password| !password.is_empty()))
                    && password.as_ref().is_none_or(|password| !password.is_empty())
            }
            Self::DeleteUser { username } => !username.is_empty(),
            Self::Policy {
                subject,
                policy_type,
                entry,
                ..
            } => {
                canonical_subject(subject)
                    && ["Custom", "Default"].contains(&policy_type.as_str())
                    && canonical_resource(&entry.resource)
                    && !entry.actions.is_empty()
                    && entry.actions.iter().all(|action| {
                        [
                            "All",
                            "Pub",
                            "Sub",
                            "Create",
                            "Update",
                            "Delete",
                            "Get",
                            "List",
                            "Publish",
                            "Subscribe",
                            "Describe",
                            "Manage",
                        ]
                        .contains(&action.as_str())
                    })
                    && ["Allow", "Deny"].contains(&entry.decision.as_str())
            }
            Self::DeleteEntry {
                subject,
                policy_type,
                resource,
            } => {
                canonical_subject(subject)
                    && canonical_resource(resource)
                    && ["Custom", "Default"].contains(&policy_type.as_str())
            }
        };
        if address.trim().is_empty() || !valid {
            Err(super::AdminError::invalid_argument(
                "acl",
                "Invalid structured exact-target ACL operation",
            ))
        } else {
            Ok(())
        }
    }
}

fn canonical_subject(subject: &str) -> bool {
    subject.len() <= 255
        && subject
            .strip_prefix("User:")
            .is_some_and(|name| !name.is_empty() && name.trim() == name && !name.chars().any(char::is_control))
}
fn canonical_resource(resource: &str) -> bool {
    resource.len() <= 512
        && (resource == "*"
            || resource.split_once(':').is_some_and(|(kind, name)| {
                ["Topic", "Group", "Cluster"].contains(&kind)
                    && !name.is_empty()
                    && name.trim() == name
                    && !name.ends_with("**")
                    && !name.chars().any(char::is_control)
            }))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn acl_document_debug_redacts_payload_while_unknown_fields_survive_projection_transfer() {
        let documents = AclDocuments::new(vec![
            serde_json::json!({"password":"backend-private", "policies":[{"resource":"private-resource"}], "futureField":true}),
        ]);
        let debug = format!("{documents:?}");
        assert!(!debug.contains("backend-private") && !debug.contains("private-resource"));
        assert_eq!(documents.into_values()[0]["futureField"], true);
    }

    #[test]
    fn acl_exact_policy_validation_requires_canonical_identity_and_writable_actions() {
        for (subject, resource, action, valid) in [
            ("User:alice", "Topic:orders", "Pub", true),
            ("alice", "Topic:orders", "Pub", false),
            ("User:alice", "topic:orders", "Pub", false),
            ("User:alice", "Topic:orders", "Any", false),
        ] {
            let operation = AclExactWrite::Policy {
                create: true,
                subject: subject.into(),
                policy_type: "Custom".into(),
                entry: AclEntryWrite {
                    resource: resource.into(),
                    actions: vec![action.into()],
                    source_ips: vec![],
                    decision: "Allow".into(),
                },
            };
            assert_eq!(operation.validate("broker:10911").is_ok(), valid);
        }
    }

    #[test]
    fn acl_failure_classification_uses_original_broker_code_instead_of_outgoing_projection() {
        for (code, expected) in [
            (16, AclRemoteFailure::AccessDenied),
            (3, AclRemoteFailure::Unsupported),
            (29, AclRemoteFailure::Invalid),
            (3001, AclRemoteFailure::Invalid),
            (1, AclRemoteFailure::Unconfirmed),
        ] {
            let error = super::super::AdminError::from_error(
                "acl",
                rocketmq_error::Error::new(&rocketmq_error::BROKER_OPERATION_FAILED).with_context(
                    rocketmq_error::ErrorContext::new().with_i64(rocketmq_error::fields::BROKER_CODE, code),
                ),
            );
            if code == 16 {
                assert_ne!(error.remoting_response_code().as_i32(), 16);
            }
            assert_eq!(classify_acl_failure(&error), expected);
        }
        let provider_failure = super::super::AdminError::from_error(
            "acl",
            rocketmq_error::Error::new(&rocketmq_error::SECURITY_PROVIDER_UNAVAILABLE),
        );
        assert_eq!(provider_failure.remoting_response_code().as_i32(), 16);
        assert_eq!(classify_acl_failure(&provider_failure), AclRemoteFailure::Unconfirmed);
    }

    #[test]
    #[cfg(feature = "client-adapter")]
    fn acl_sdk_failure_promotion_keeps_wire_code_classification_and_redacts_details() {
        for (code, expected) in [
            (16, AclRemoteFailure::AccessDenied),
            (3, AclRemoteFailure::Unsupported),
            (1, AclRemoteFailure::Unconfirmed),
        ] {
            let sdk = rocketmq_client_rust::ClientError::broker_operation_failed(
                "acl_workspace",
                code,
                "private-acl-material",
            );
            let error = super::super::AdminError::from_error(
                "acl_workspace",
                crate::IntoCanonicalError::into_canonical_error(sdk),
            );
            assert_eq!(classify_acl_failure(&error), expected);
            assert!(!format!("{error:?} {error}").contains("private-acl-material"));
            assert!(std::error::Error::source(&error).is_some());
        }
    }
}
