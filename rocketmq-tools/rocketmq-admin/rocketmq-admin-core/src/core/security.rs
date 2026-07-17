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

use serde::Deserialize;
use serde::Serialize;

use crate::core::error::required;
use crate::core::AdminFuture;
use crate::core::AdminResult;

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
