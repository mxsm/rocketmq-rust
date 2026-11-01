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

use super::lifecycle::AdminSession;
use crate::core::{
    acl_workspace::{AclDocuments, AclReadKind},
    AdminError, AdminResult,
};
use rocketmq_client_rust::{AclWorkspaceReadKind, MQAdminAclWorkspaceReadExt as _};

impl AdminSession {
    /// Reads one advertised Broker without selector expansion. Returned documents may contain secrets.
    ///
    /// # Errors
    /// Returns authorization, session, protocol, or transport failures.
    pub async fn acl_documents_at(&self, address: &str, kind: AclReadKind) -> AdminResult<AclDocuments> {
        self.ensure_open()?;
        if address.is_empty() {
            return Err(AdminError::invalid_argument("address", "Broker address is required"));
        }
        let kind = match kind {
            AclReadKind::Users => AclWorkspaceReadKind::Users,
            AclReadKind::Policies => AclWorkspaceReadKind::Policies,
        };
        self.inner
            .read_acl_documents_at(address.into(), kind)
            .await
            .map(|documents| AclDocuments::new(documents.into_values()))
            .map_err(|source| {
                AdminError::from_error(
                    "acl_workspace_read",
                    crate::IntoCanonicalError::into_canonical_error(source),
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use crate::core::{
        acl_workspace::{classify_acl_failure, AclRemoteFailure},
        AdminError,
    };

    #[test]
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
            let error = AdminError::from_error("acl_workspace", crate::IntoCanonicalError::into_canonical_error(sdk));
            assert_eq!(classify_acl_failure(&error), expected);
            assert!(!format!("{error:?} {error}").contains("private-acl-material"));
            assert!(std::error::Error::source(&error).is_some());
        }
    }
}
