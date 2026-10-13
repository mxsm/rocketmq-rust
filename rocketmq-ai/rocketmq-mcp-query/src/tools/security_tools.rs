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

//! The ACL users of a cluster. This module exists only with the `security-admin` feature.

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ListAclUsersArgs {
    /// Logical cluster name configured on this server. It is a name, not a NameServer address.
    #[schemars(length(min = 1))]
    pub cluster: String,
}

/// One ACL user as one Broker holds it. It never carries a password.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct AclUserRow {
    /// The name of the user, as the Broker holds it.
    pub username: String,
    /// The Broker that holds the user. Each Broker keeps its own users.
    pub broker_name: String,
    /// The kind of user, `Super` or `Normal`.
    pub user_type: Option<String>,
    /// Whether the user may sign requests, `enable` or `disable`.
    pub user_status: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct ListAclUsersOutput {
    pub cluster: String,
    /// How many distinct user names the Brokers listed.
    pub user_count: usize,
    /// One row for each user on each Broker, in ascending order of user name and Broker, at most 500.
    pub users: Vec<AclUserRow>,
    /// Whether rows beyond 500 were left out. `user_count` still covers them.
    pub truncated: bool,
    /// How many Brokers the cluster has, those in `unanswered_brokers` included.
    pub queried_broker_count: usize,
    /// The Brokers that have no master to ask or did not answer. Their users are missing.
    pub unanswered_brokers: Vec<String>,
    pub generated_at: String,
}
