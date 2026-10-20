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

//! Security administration contracts.

use std::fmt;

use serde::Deserialize;
use serde::Serialize;

use crate::core::error::required;
use crate::core::AdminFuture;
use crate::core::AdminResult;

#[derive(Clone, PartialEq, Eq)]
pub struct AdminCredentials {
    access_key: String,
    secret_key: String,
    security_token: Option<String>,
}

impl AdminCredentials {
    pub fn try_new(
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
        security_token: Option<String>,
    ) -> AdminResult<Self> {
        Ok(Self {
            access_key: required("accessKey", access_key)?,
            secret_key: required("secretKey", secret_key)?,
            security_token: security_token
                .map(|token| token.trim().to_owned())
                .filter(|token| !token.is_empty()),
        })
    }

    #[cfg(any(feature = "read-client-adapter", feature = "mutation-client-adapter", test))]
    pub(crate) fn access_key(&self) -> &str {
        &self.access_key
    }

    #[cfg(any(feature = "read-client-adapter", feature = "mutation-client-adapter", test))]
    pub(crate) fn secret_key(&self) -> &str {
        &self.secret_key
    }

    #[cfg(any(feature = "read-client-adapter", feature = "mutation-client-adapter", test))]
    pub(crate) fn security_token(&self) -> Option<&str> {
        self.security_token.as_deref()
    }
}

impl fmt::Debug for AdminCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminCredentials")
            .field("access_key", &"<redacted>")
            .field("secret_key", &"<redacted>")
            .field("security_token", &self.security_token.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListUsersRequest {
    pub broker_addr: String,
    pub filter: String,
}

impl ListUsersRequest {
    pub fn try_new(broker_addr: impl Into<String>, filter: impl Into<String>) -> AdminResult<Self> {
        Ok(Self {
            broker_addr: required("brokerAddr", broker_addr)?,
            filter: filter.into().trim().to_string(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserSummary {
    pub username: Option<String>,
    pub user_type: Option<String>,
    pub user_status: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListUsersResult {
    pub users: Vec<UserSummary>,
}

pub trait SecurityAdmin: Send {
    fn list_users<'a>(&'a mut self, request: &'a ListUsersRequest) -> AdminFuture<'a, ListUsersResult>;
}

/// Maximum rows retained by one ACL user observation. A row is one user on one Broker.
pub const MAX_ACL_USER_ROWS: usize = 500;
/// Maximum Brokers one ACL user observation asks.
pub const MAX_ACL_USER_BROKERS: usize = 64;

/// Selects the cluster whose ACL users are listed. It carries no address.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AclUserQueryRequest {
    pub cluster: String,
}

impl AclUserQueryRequest {
    pub fn try_new(cluster: impl Into<String>) -> AdminResult<Self> {
        Ok(Self {
            cluster: required("cluster", cluster)?,
        })
    }
}

/// One ACL user as one Broker holds it. It carries neither a password nor an address.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AclUserObservation {
    pub broker_name: String,
    pub username: String,
    pub user_type: Option<String>,
    pub user_status: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AclUserQueryResult {
    /// The rows in ascending order of user name and Broker, at most [`MAX_ACL_USER_ROWS`].
    pub users: Vec<AclUserObservation>,
    /// How many distinct user names the Brokers listed, before the retained-row cap.
    pub user_count: usize,
    /// Whether rows beyond the retained-row cap were left out.
    pub truncated: bool,
    pub queried_broker_count: usize,
    /// Logical names of the Brokers that could not be asked or did not answer.
    pub unanswered_brokers: Vec<String>,
}

/// Secret-free read of the ACL users the masters of one cluster hold.
///
/// A Broker returns the password of every user it lists. It never reaches a result of this
/// trait: the read capability drops it before the adapter sees a user.
pub trait AclUserQueryAdmin: Send {
    fn query_acl_users<'a>(&'a mut self, request: &'a AclUserQueryRequest) -> AdminFuture<'a, AclUserQueryResult>;
}

#[cfg(test)]
mod tests {
    use super::AdminCredentials;

    #[test]
    fn admin_credentials_validate_trim_and_redact_secret_values() {
        let credentials = AdminCredentials::try_new(
            " access-value-42 ",
            " secret-value-42 ",
            Some(" token-value-42 ".to_string()),
        )
        .unwrap();

        assert_eq!(credentials.access_key(), "access-value-42");
        assert_eq!(credentials.secret_key(), "secret-value-42");
        assert_eq!(credentials.security_token(), Some("token-value-42"));

        let debug = format!("{credentials:?}");
        assert!(!debug.contains("access-value-42"));
        assert!(!debug.contains("secret-value-42"));
        assert!(!debug.contains("token-value-42"));
        assert!(debug.contains("<redacted>"));
    }

    #[test]
    fn admin_credentials_reject_incomplete_required_fields() {
        assert!(AdminCredentials::try_new(" ", "secret", None).is_err());
        assert!(AdminCredentials::try_new("access", " ", None).is_err());
    }
}
