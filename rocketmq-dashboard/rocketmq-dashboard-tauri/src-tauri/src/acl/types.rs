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

use crate::error::{DashboardError, DashboardResult};
use rocketmq_admin_core::core::dashboard::{DashboardAclUserMutationRequest, TargetSelector};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct AclScope {
    pub(crate) cluster_name: String,
    pub(crate) broker_name: String,
    pub(crate) broker_addr: String,
}
impl AclScope {
    pub(crate) fn validate(&self) -> DashboardResult<()> {
        if [&self.cluster_name, &self.broker_name, &self.broker_addr]
            .into_iter()
            .any(|value| value.trim().is_empty())
        {
            return Err(DashboardError::Validation(
                "Select an explicit ACL Broker scope.".into(),
            ));
        }
        Ok(())
    }
    pub(crate) fn selector(&self) -> TargetSelector {
        TargetSelector {
            cluster_name: Some(self.cluster_name.clone()),
            broker_name: Some(self.broker_name.clone()),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum AclUserType {
    Normal,
    Super,
}
impl AclUserType {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Super => "super",
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum AclUserStatus {
    Enable,
    Disable,
}
impl AclUserStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Enable => "enable",
            Self::Disable => "disable",
        }
    }
}

// Passwords are input-only and deliberately absent from Debug and response models.
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct AclUserChange {
    pub(crate) scope: AclScope,
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) user_type: AclUserType,
    pub(crate) user_status: Option<AclUserStatus>,
}
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct AclUserDelete {
    pub(crate) scope: AclScope,
    pub(crate) username: String,
}
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AclUserOperation {
    Create,
    Update,
    Delete,
}
impl AclUserChange {
    pub(crate) fn to_core(&self, operation: AclUserOperation) -> DashboardResult<DashboardAclUserMutationRequest> {
        self.scope.validate()?;
        validate_username(&self.username)?;
        if self.password.trim().is_empty() {
            return Err(DashboardError::Validation(
                "A nonempty password is required by the ACL user API.".into(),
            ));
        }
        match operation {
            AclUserOperation::Create if self.user_status.is_some_and(|status| status != AclUserStatus::Enable) => {
                return Err(DashboardError::Validation(
                    "New ACL users are enabled; update their status after creation.".into(),
                ));
            }
            AclUserOperation::Update if self.user_status.is_none() => {
                return Err(DashboardError::Validation(
                    "User status is required when updating.".into(),
                ));
            }
            AclUserOperation::Delete => return Err(DashboardError::Internal("User change cannot represent deletion")),
            _ => {}
        }
        Ok(DashboardAclUserMutationRequest {
            selector: self.scope.selector(),
            username: self.username.clone(),
            password: self.password.clone(),
            user_type: self.user_type.as_str().into(),
            user_status: self.user_status.map(|status| status.as_str().into()),
        })
    }
}
pub(crate) fn validate_username(value: &str) -> DashboardResult<()> {
    if value.trim().is_empty() || value != value.trim() || value.chars().any(char::is_control) {
        return Err(DashboardError::Validation(
            "Provide a nonempty ACL username without surrounding whitespace.".into(),
        ));
    }
    Ok(())
}
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AclUser {
    pub(crate) username: String,
    pub(crate) user_type: Option<String>,
    pub(crate) user_status: Option<String>,
}
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AclUserResult {
    pub(crate) scope: AclScope,
    pub(crate) username: String,
    pub(crate) operation: AclUserOperation,
    pub(crate) success: bool,
    pub(crate) users: Option<Vec<AclUser>>,
    pub(crate) read_back_error: Option<&'static str>,
}
impl crate::audit::types::AuditReceipt for AclUserResult {
    fn summary(&self) -> crate::audit::types::Summary {
        crate::audit::types::Summary::count(usize::from(self.success), usize::from(!self.success))
    }
}
