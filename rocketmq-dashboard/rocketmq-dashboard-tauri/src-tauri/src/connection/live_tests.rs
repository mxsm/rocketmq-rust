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

use super::{AdminConnectionConfig, AdminPurpose};
use rocketmq_admin_core::client_adapter::{ClientRuntime, ClientRuntimeConfig, TelemetryHandle};
use rocketmq_admin_core::core::dashboard::{DashboardAdmin, DashboardBrokerTarget};
use rocketmq_dashboard_common::NameServerConfigSnapshot;
use rocketmq_runtime::{RuntimeConfig, RuntimeOwner};

#[test]
#[ignore = "requires deploy/dev/tls with SSL_CERT_FILE pointing to its ca.pem"]
fn local_tls_cluster_queries_require_encryption() {
    let owner = RuntimeOwner::plan(RuntimeConfig::server_default("tauri-tls-smoke"))
        .unwrap()
        .build()
        .unwrap();
    let runtime = ClientRuntime::try_new(
        owner.root_context().component("client"),
        ClientRuntimeConfig::default(),
        TelemetryHandle::noop(),
    )
    .unwrap();
    let outcomes = owner.block_on(async {
        let mut outcomes = Vec::new();
        for use_tls in [true, false] {
            let snapshot = NameServerConfigSnapshot {
                current_namesrv: Some("127.0.0.1:39786".into()),
                namesrv_addr_list: vec!["127.0.0.1:39786".into()],
                use_vip_channel: false,
                use_tls,
            };
            let outcome = match AdminConnectionConfig::default()
                .builder(runtime.clone(), &snapshot, "127.0.0.1:39786", AdminPurpose::Cluster)
                .build_and_start()
                .await
            {
                Ok(mut session) => {
                    let result = async {
                        let brokers = session.dashboard_list_brokers().await?;
                        let target = DashboardBrokerTarget {
                            broker_name: "tauri-tls-broker".into(),
                            broker_addr: Some("127.0.0.1:33911".into()),
                        };
                        let config = session.dashboard_broker_config(&target).await?;
                        let status = session.dashboard_broker_runtime(&target).await?;
                        Ok::<_, rocketmq_admin_core::core::AdminError>((brokers, config, status))
                    }
                    .await;
                    session.shutdown().await;
                    result
                }
                Err(error) => Err(error),
            };
            outcomes.push(outcome);
        }
        runtime.shutdown().await;
        outcomes
    });
    owner.shutdown_runtime_blocking().unwrap();

    let (brokers, config, status) = outcomes[0].as_ref().expect("trusted TLS queries must succeed");
    assert!(brokers.items.iter().any(|broker| {
        broker.cluster_name == "TauriTlsDebugCluster"
            && broker.broker_name == "tauri-tls-broker"
            && broker.address == "127.0.0.1:33911"
            && broker.runtime_error.is_none()
    }));
    assert!(!config.entries.is_empty());
    assert!(!status.entries.is_empty());
    assert!(
        outcomes[1].is_err(),
        "the enforcing Broker must reject plaintext administration"
    );
}
