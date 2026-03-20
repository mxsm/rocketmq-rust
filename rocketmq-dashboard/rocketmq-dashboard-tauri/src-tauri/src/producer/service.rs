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

use crate::nameserver::NameServerRuntimeState;
use crate::producer::admin::ManagedProducerAdmin;
use crate::producer::types::ProducerConnectionItem;
use crate::producer::types::ProducerConnectionView;
use crate::producer::types::ProducerError;
use crate::producer::types::ProducerResult;
use rocketmq_admin_core::admin::default_mq_admin_ext::DefaultMQAdminExt;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::common::mq_version::RocketMqVersion;
use rocketmq_dashboard_common::ProducerConnectionQueryRequest;
use rocketmq_remoting::protocol::body::producer_connection::ProducerConnection;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub(crate) struct ProducerManager {
    runtime: Arc<NameServerRuntimeState>,
    admin_session: Arc<Mutex<Option<ManagedProducerAdmin>>>,
}

impl ProducerManager {
    pub(crate) fn new(runtime: Arc<NameServerRuntimeState>) -> Self {
        Self {
            runtime,
            admin_session: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) async fn query_producer_connections(
        &self,
        request: ProducerConnectionQueryRequest,
    ) -> ProducerResult<ProducerConnectionView> {
        self.validate_query_request(&request)?;

        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut session_guard = self.admin_session.lock().await;
            self.ensure_admin_session(&mut session_guard).await?;

            let result = {
                let session = session_guard
                    .as_mut()
                    .expect("producer admin session should be initialized before use");
                self.query_producer_connections_with_admin(&mut session.admin, request.clone())
                    .await
            };

            let should_reset = Self::should_reset_session(&result);
            if should_reset {
                self.reset_admin_session(&mut session_guard, "query_producer_connections failed")
                    .await;
            }
            drop(session_guard);

            match result {
                Ok(response) => return Ok(response),
                Err(error) if should_reset && attempt < 2 => {
                    log::warn!("Retrying `query_producer_connections` after reconnect: {}", error);
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn validate_query_request(&self, request: &ProducerConnectionQueryRequest) -> ProducerResult<()> {
        if request.topic.trim().is_empty() {
            return Err(ProducerError::Validation("Topic is required.".into()));
        }
        if request.producer_group.trim().is_empty() {
            return Err(ProducerError::Validation("Producer group is required.".into()));
        }
        Ok(())
    }

    async fn ensure_admin_session(&self, session_slot: &mut Option<ManagedProducerAdmin>) -> ProducerResult<()> {
        let generation = self.runtime.generation();
        let needs_reconnect = session_slot
            .as_ref()
            .is_none_or(|session| !session.matches_generation(generation));

        if needs_reconnect {
            self.reset_admin_session(session_slot, "refreshing producer admin session")
                .await;
            let session = ManagedProducerAdmin::connect(&self.runtime).await?;
            log::info!(
                "Connected producer admin session for namesrv `{}` at generation {}",
                session.snapshot.current_namesrv.as_deref().unwrap_or_default(),
                session.generation
            );
            *session_slot = Some(session);
        }

        Ok(())
    }

    async fn reset_admin_session(&self, session_slot: &mut Option<ManagedProducerAdmin>, reason: &str) {
        if let Some(mut session) = session_slot.take() {
            log::info!(
                "Shutting down producer admin session for namesrv `{}`: {}",
                session.snapshot.current_namesrv.as_deref().unwrap_or_default(),
                reason
            );
            session.shutdown().await;
        }
    }

    fn should_reset_session<T>(result: &ProducerResult<T>) -> bool {
        matches!(result, Err(ProducerError::RocketMQ(_)))
    }

    async fn query_producer_connections_with_admin(
        &self,
        admin: &mut DefaultMQAdminExt,
        request: ProducerConnectionQueryRequest,
    ) -> ProducerResult<ProducerConnectionView> {
        let connection = admin
            .examine_producer_connection_info(request.producer_group.clone().into(), request.topic.clone().into())
            .await
            .map_err(|error| ProducerError::RocketMQ(error.to_string()))?;

        Ok(build_connection_view(request, connection))
    }
}

fn build_connection_view(
    request: ProducerConnectionQueryRequest,
    connection: ProducerConnection,
) -> ProducerConnectionView {
    let mut connections: Vec<ProducerConnectionItem> = connection
        .connection_set()
        .iter()
        .map(|item| ProducerConnectionItem {
            client_id: item.get_client_id().to_string(),
            client_addr: item.get_client_addr().to_string(),
            language: item.get_language().to_string(),
            version: item.get_version(),
            version_desc: RocketMqVersion::from_ordinal(item.get_version() as u32)
                .name()
                .to_string(),
        })
        .collect();
    connections.sort_by(|left, right| left.client_id.cmp(&right.client_id));

    ProducerConnectionView {
        topic: request.topic,
        producer_group: request.producer_group,
        connection_count: connections.len(),
        connections,
    }
}

#[cfg(test)]
mod tests {
    use super::build_connection_view;
    use cheetah_string::CheetahString;
    use rocketmq_common::common::mq_version::RocketMqVersion;
    use rocketmq_dashboard_common::ProducerConnectionQueryRequest;
    use rocketmq_remoting::protocol::LanguageCode;
    use rocketmq_remoting::protocol::body::connection::Connection;
    use rocketmq_remoting::protocol::body::producer_connection::ProducerConnection;

    #[test]
    fn build_connection_view_sorts_connections_and_maps_version_desc() {
        let mut producer_connection = ProducerConnection::new();

        let mut second = Connection::new();
        second.set_client_id(CheetahString::from("client-b"));
        second.set_client_addr(CheetahString::from("127.0.0.1:10911"));
        second.set_language(LanguageCode::JAVA);
        second.set_version(ordinal_for_v5_0_0());

        let mut first = Connection::new();
        first.set_client_id(CheetahString::from("client-a"));
        first.set_client_addr(CheetahString::from("127.0.0.1:10912"));
        first.set_language(LanguageCode::RUST);
        first.set_version(ordinal_for_v5_0_0());

        producer_connection.connection_set_mut().insert(second);
        producer_connection.connection_set_mut().insert(first);

        let view = build_connection_view(
            ProducerConnectionQueryRequest {
                topic: "TopicTest".to_string(),
                producer_group: "producer-group".to_string(),
            },
            producer_connection,
        );

        assert_eq!(view.connection_count, 2);
        assert_eq!(view.connections[0].client_id, "client-a");
        assert_eq!(view.connections[1].client_id, "client-b");
        assert_eq!(
            view.connections[0].version_desc,
            RocketMqVersion::from_ordinal(ordinal_for_v5_0_0() as u32).name()
        );
    }

    const fn ordinal_for_v5_0_0() -> i32 {
        433
    }
}
