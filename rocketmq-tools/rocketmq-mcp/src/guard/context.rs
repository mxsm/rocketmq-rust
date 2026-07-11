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

use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    pub id: String,
    pub roles: BTreeSet<String>,
    pub scopes: BTreeSet<String>,
    pub allowed_clusters: Option<BTreeSet<String>>,
}

impl Principal {
    pub fn local(profile: &str) -> Self {
        let mut scopes = BTreeSet::from(["rocketmq:read".to_string(), "rocketmq:diagnose".to_string()]);
        if profile.eq_ignore_ascii_case("operator") {
            scopes.insert("rocketmq:plan".to_string());
        }
        Self {
            id: "local-stdio".to_string(),
            roles: [profile.to_string()].into_iter().collect(),
            scopes,
            allowed_clusters: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestContext {
    pub principal: Principal,
    pub client: Option<String>,
}

impl RequestContext {
    pub fn local(profile: &str) -> Self {
        Self {
            principal: Principal::local(profile),
            client: Some("stdio".to_string()),
        }
    }
}
