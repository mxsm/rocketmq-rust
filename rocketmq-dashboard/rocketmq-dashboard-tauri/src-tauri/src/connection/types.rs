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

use rocketmq_dashboard_common::{NameServerConfigSnapshot, ProxyConfigSnapshot};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EndpointKind {
    NameServer,
    Proxy,
}
impl EndpointKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::NameServer => "nameserver",
            Self::Proxy => "proxy",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct EndpointView {
    pub(crate) endpoint_id: String,
    pub(crate) kind: EndpointKind,
    pub(crate) address: String,
    pub(crate) environment_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ConnectionSettingsView {
    pub(crate) revision: i64,
    pub(crate) endpoints: Vec<EndpointView>,
    pub(crate) current_nameserver_id: Option<String>,
    pub(crate) current_proxy_id: Option<String>,
    pub(crate) environment_id: Option<String>,
    pub(crate) nameserver: NameServerConfigSnapshot,
    pub(crate) proxy: ProxyConfigSnapshot,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ReplaceNameServersRequest {
    pub(crate) addresses: Vec<String>,
    pub(crate) current_endpoint: Option<NameServerSelection>,
    pub(crate) expected_revision: i64,
}

pub(crate) enum ConnectionChange {
    Add {
        kind: EndpointKind,
        address: String,
    },
    Switch {
        kind: EndpointKind,
        address: String,
    },
    Delete {
        kind: EndpointKind,
        address: String,
    },
    Vip(bool),
    Tls(bool),
    Replace {
        addresses: Vec<String>,
        current_endpoint: Option<NameServerSelection>,
    },
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ConnectionMutationResult {
    pub(crate) message: &'static str,
    pub(crate) settings: ConnectionSettingsView,
}
impl crate::audit::types::AuditReceipt for ConnectionMutationResult {
    fn summary(&self) -> crate::audit::types::Summary {
        crate::audit::types::Summary::count(1, 0)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub(crate) enum NameServerSelection {
    ExistingId(String),
    Address(String),
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ConnectionProjection<T> {
    #[serde(flatten)]
    pub(crate) value: T,
    pub(crate) settings: ConnectionSettingsView,
}
