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

pub(crate) use crate::error::{DashboardError, DashboardResult};
use rocketmq_admin_core::client_adapter::{AdminBuilder, ClientRuntime};
use rocketmq_admin_core::core::security::AdminCredentials;
use rocketmq_dashboard_common::NameServerConfigSnapshot;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Clone, Default)]
pub(crate) struct AdminConnectionConfig {
    credentials: Option<AdminCredentials>,
}

#[derive(Clone, Copy)]
pub(crate) enum AdminPurpose {
    Acl,
    Cluster,
    Consumer,
    Message,
    Producer,
    Topic,
    Probe,
}
impl AdminPurpose {
    fn name(self) -> &'static str {
        match self {
            Self::Acl => "acl",
            Self::Cluster => "cluster",
            Self::Consumer => "consumer",
            Self::Message => "message",
            Self::Producer => "producer",
            Self::Topic => "topic",
            Self::Probe => "probe",
        }
    }
    fn timeout_millis(self) -> u64 {
        match self {
            Self::Probe => 1500,
            Self::Acl | Self::Cluster | Self::Consumer | Self::Message | Self::Producer | Self::Topic => 5000,
        }
    }
}

impl AdminConnectionConfig {
    pub(crate) fn from_environment() -> DashboardResult<Self> {
        let read = |key| match std::env::var(key) {
            Ok(value) => Ok(Some(value)),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(_) => Err(DashboardError::Configuration(
                "invalid RocketMQ credential encoding".into(),
            )),
        };
        Self::from_values(
            read("DASHBOARD_TAURI_ROCKETMQ_ACCESS_KEY")?,
            read("DASHBOARD_TAURI_ROCKETMQ_SECRET_KEY")?,
            read("DASHBOARD_TAURI_ROCKETMQ_SECURITY_TOKEN")?,
        )
    }

    fn from_values(
        access_key: Option<String>,
        secret_key: Option<String>,
        security_token: Option<String>,
    ) -> DashboardResult<Self> {
        let credentials = match (access_key, secret_key, security_token) {
            (None, None, None) => None,
            (Some(access), Some(secret), token) => {
                Some(AdminCredentials::try_new(access, secret, token).map_err(|_| {
                    DashboardError::Configuration("RocketMQ access and secret keys must both be nonempty".into())
                })?)
            }
            _ => {
                return Err(DashboardError::Configuration(
                    "RocketMQ access and secret keys must be configured together".into(),
                ));
            }
        };
        Ok(Self { credentials })
    }

    pub(crate) fn credentials_configured(&self) -> bool {
        self.credentials.is_some()
    }

    pub(crate) fn builder(
        &self,
        runtime: Arc<ClientRuntime>,
        snapshot: &NameServerConfigSnapshot,
        address: &str,
        purpose: AdminPurpose,
    ) -> AdminBuilder {
        let mut builder = AdminBuilder::new(runtime)
            .admin_group(format!("dashboard-{}-{}", purpose.name(), Uuid::new_v4()))
            .namesrv_addr(address)
            .vip_channel_enabled(snapshot.use_vip_channel)
            .use_tls(snapshot.use_tls)
            .timeout_millis(purpose.timeout_millis());
        if let Some(credentials) = &self.credentials {
            builder = builder.credentials(credentials.clone());
        }
        builder
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn configuration_requires_complete_nonempty_credentials_and_redacts_values() {
        assert!(
            !AdminConnectionConfig::from_values(None, None, None)
                .unwrap()
                .credentials_configured()
        );
        for (access, secret, token) in [
            (Some("access-sentinel"), None, None),
            (None, Some("secret-sentinel"), None),
            (None, None, Some("token-sentinel")),
            (Some(""), Some("secret-sentinel"), None),
        ] {
            let error = AdminConnectionConfig::from_values(
                access.map(str::to_owned),
                secret.map(str::to_owned),
                token.map(str::to_owned),
            )
            .unwrap_err();
            let public = serde_json::to_string(&crate::error::CommandError::from(error)).unwrap();
            assert!(!public.contains("sentinel"));
        }
        let config = AdminConnectionConfig::from_values(
            Some("access-sentinel".into()),
            Some("secret-sentinel".into()),
            Some("token-sentinel".into()),
        )
        .unwrap();
        assert!(config.credentials_configured());
        assert!(!format!("{config:?}").contains("sentinel"));
    }

    #[test]
    fn common_builder_keeps_transport_settings_for_signed_and_unsigned_paths() {
        let snapshot = NameServerConfigSnapshot {
            use_vip_channel: true,
            use_tls: true,
            ..Default::default()
        };
        let signed =
            AdminConnectionConfig::from_values(Some("access-sentinel".into()), Some("secret-sentinel".into()), None)
                .unwrap();
        for config in [AdminConnectionConfig::default(), signed] {
            let builder = config.builder(
                crate::nameserver::runtime::test_client_runtime(),
                &snapshot,
                "127.0.0.1:9876",
                AdminPurpose::Probe,
            );
            let debug = format!("{builder:?}");
            assert!(!debug.contains("sentinel"));
            assert!(debug.contains("1500"));
            assert!(!debug.contains("127.0.0.1:9876"));
            assert!(debug.contains("use_tls: true"));
            assert!(debug.contains("vip_channel_enabled: true"));
        }
    }

    #[test]
    #[ignore = "requires the isolated deploy/dev/acl Docker fixture"]
    fn local_acl_query_accepts_configured_credentials_and_rejects_invalid_credentials() {
        use rocketmq_admin_core::client_adapter::{ClientRuntimeConfig, TelemetryHandle};
        use rocketmq_admin_core::core::security::{ListUsersRequest, SecurityAdmin};
        use rocketmq_runtime::{RuntimeConfig, RuntimeOwner};

        let owner = RuntimeOwner::plan(RuntimeConfig::server_default("tauri-acl-smoke"))
            .unwrap()
            .build()
            .unwrap();
        let runtime = ClientRuntime::try_new(
            owner.root_context().component("client"),
            ClientRuntimeConfig::default(),
            TelemetryHandle::noop(),
        )
        .unwrap();
        let snapshot = NameServerConfigSnapshot {
            current_namesrv: Some("127.0.0.1:29876".into()),
            namesrv_addr_list: vec!["127.0.0.1:29876".into()],
            use_vip_channel: false,
            use_tls: false,
        };
        let outcomes = owner.block_on(async {
            let mut outcomes = Vec::new();
            for secret in [Some("tauri-dev-secret"), None, Some("incorrect-dev-secret")] {
                let config = AdminConnectionConfig::from_values(
                    secret.map(|_| "tauri-dev-admin".into()),
                    secret.map(str::to_owned),
                    None,
                )
                .unwrap();
                let result = match config
                    .builder(runtime.clone(), &snapshot, "127.0.0.1:29876", AdminPurpose::Cluster)
                    .build_and_start()
                    .await
                {
                    Ok(mut session) => {
                        let request = ListUsersRequest::try_new("127.0.0.1:22911", "").unwrap();
                        let result = async {
                            use rocketmq_admin_core::core::topic::{
                                DeleteTopicAdminRequest, TopicAdmin, TopicSendRequest, UpsertTopicRequest,
                            };
                            let users = session.list_users(&request).await?;
                            if secret == Some("tauri-dev-secret") {
                                for message_type in [None, Some("TRANSACTION".to_string())] {
                                    let topic = format!("TauriAclSend{}", Uuid::new_v4().simple());
                                    session
                                        .upsert_topic(&UpsertTopicRequest {
                                            cluster_names: vec!["TauriAclDebugCluster".into()],
                                            broker_names: vec![],
                                            topic: topic.clone(),
                                            write_queue_nums: 1,
                                            read_queue_nums: 1,
                                            perm: 6,
                                            order: false,
                                            message_type,
                                        })
                                        .await?;
                                    let sent = session
                                        .send_topic_test_message(&TopicSendRequest {
                                            topic: topic.clone(),
                                            key: String::new(),
                                            tag: String::new(),
                                            message_body: "signed-desktop-smoke".into(),
                                            trace_enabled: false,
                                        })
                                        .await;
                                    let deleted = session
                                        .delete_topic(&DeleteTopicAdminRequest {
                                            topic,
                                            cluster_name: Some("TauriAclDebugCluster".into()),
                                            broker_name: None,
                                        })
                                        .await;
                                    let sent = sent?;
                                    deleted?;
                                    if sent.send_status.split(" (").next() != Some("SendOk")
                                        || sent.message_id.is_none()
                                    {
                                        return Err(rocketmq_admin_core::core::AdminError::backend(
                                            "acl_send_smoke",
                                            "expected a successful signed send receipt",
                                        ));
                                    }
                                }
                            }
                            Ok(users)
                        }
                        .await;
                        session.shutdown().await;
                        result
                    }
                    Err(error) => Err(error),
                };
                outcomes.push(result);
            }
            runtime.shutdown().await;
            outcomes
        });
        owner.shutdown_runtime_blocking().unwrap();
        assert!(
            outcomes[0]
                .as_ref()
                .unwrap()
                .users
                .iter()
                .any(|user| { user.username.as_deref() == Some("tauri-dev-admin") })
        );
        assert!(outcomes[1].is_err(), "anonymous requests must be rejected");
        assert!(outcomes[2].is_err(), "incorrect signatures must be rejected");
    }
}
