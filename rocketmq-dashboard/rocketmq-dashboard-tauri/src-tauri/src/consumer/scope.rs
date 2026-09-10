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

use crate::connection::{ConnectionSettingsView, EndpointKind};
use crate::error::{DashboardError, DashboardResult};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ConsumerQueryScope {
    NameServer,
    Proxy {
        #[serde(rename = "endpointId")]
        endpoint_id: String,
    },
}

impl ConsumerQueryScope {
    pub(crate) fn address(&self, settings: &ConnectionSettingsView) -> DashboardResult<Option<String>> {
        match self {
            Self::NameServer => Ok(None),
            Self::Proxy { endpoint_id } => {
                let endpoint = settings
                    .endpoints
                    .iter()
                    .find(|endpoint| {
                        endpoint.kind == EndpointKind::Proxy
                            && &endpoint.endpoint_id == endpoint_id
                            && settings.current_proxy_id.as_ref() == Some(endpoint_id)
                            && settings.proxy.current_proxy_addr.as_ref() == Some(&endpoint.address)
                    })
                    .ok_or_else(|| {
                        DashboardError::Configuration("select a configured Proxy before querying consumers".into())
                    })?;
                Ok(Some(endpoint.address.clone()))
            }
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ScopedConsumerListRequest {
    pub(crate) scope: ConsumerQueryScope,
    #[serde(default)]
    pub(crate) skip_sys_group: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ScopedConsumerGroupRequest {
    pub(crate) scope: ConsumerQueryScope,
    pub(crate) consumer_group: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::EndpointView;
    use rocketmq_dashboard_common::{NameServerConfigSnapshot, ProxyConfigSnapshot};

    #[test]
    fn only_the_current_configured_proxy_is_resolved() {
        let mut settings = ConnectionSettingsView {
            revision: 1,
            credentials_configured: false,
            current_nameserver_id: None,
            environment_id: None,
            current_proxy_id: Some("proxy-a".into()),
            nameserver: NameServerConfigSnapshot::default(),
            proxy: ProxyConfigSnapshot {
                current_proxy_addr: Some("127.0.0.1:8080".into()),
                proxy_addr_list: vec!["127.0.0.1:8080".into()],
            },
            endpoints: vec![EndpointView {
                endpoint_id: "proxy-a".into(),
                kind: EndpointKind::Proxy,
                address: "127.0.0.1:8080".into(),
                environment_id: None,
            }],
        };
        let scope = ConsumerQueryScope::Proxy {
            endpoint_id: "proxy-a".into(),
        };
        assert_eq!(scope.address(&settings).unwrap().as_deref(), Some("127.0.0.1:8080"));
        assert_eq!(ConsumerQueryScope::NameServer.address(&settings).unwrap(), None);
        assert!(
            ConsumerQueryScope::Proxy {
                endpoint_id: "unsaved".into()
            }
            .address(&settings)
            .is_err()
        );
        settings.current_proxy_id = None;
        assert!(scope.address(&settings).is_err());
        settings.current_proxy_id = Some("proxy-a".into());
        settings.endpoints[0].kind = EndpointKind::NameServer;
        assert!(scope.address(&settings).is_err());
    }

    #[test]
    fn query_contract_rejects_arbitrary_addresses_and_missing_scope() {
        assert!(serde_json::from_str::<ScopedConsumerListRequest>(r#"{"address":"127.0.0.1:8080"}"#).is_err());
        assert!(
            serde_json::from_str::<ScopedConsumerListRequest>(
                r#"{"scope":{"mode":"name_server"},"address":"127.0.0.1:8080"}"#
            )
            .is_err()
        );
        assert!(
            serde_json::from_str::<ScopedConsumerGroupRequest>(
                r#"{"consumerGroup":"orders","scope":{"mode":"proxy","endpointId":"proxy-a"}}"#
            )
            .is_ok()
        );
    }
}
