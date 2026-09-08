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

use std::collections::HashSet;
use std::sync::Arc;

use crate::IntoCanonicalError;
use cheetah_string::CheetahString;
use rocketmq_client_rust::ClientRuntime;
use rocketmq_client_rust::DefaultMQAdminExt;
use rocketmq_client_rust::{RouteAdmin as _, TopicAdmin as _};
use rocketmq_error::Result;

use super::TopicService;
use crate::client_adapter::services::admin::AdminBuilder;
use crate::client_adapter::services::resolver::BrokerAddressResolver;
use crate::client_adapter::services::{stable_error_code, stable_error_message};
use crate::core::security::AdminCredentials;
use crate::core::topic::{
    execute_deletion, DeleteTopicRequest, DeleteTopicResult, TopicDeletionBackend, TopicOperationFailure,
};

impl DeleteTopicRequest {
    fn admin_builder(&self) -> AdminBuilder {
        match self.namesrv_addr() {
            Some(addr) => AdminBuilder::new().namesrv_addr(addr),
            None => AdminBuilder::new(),
        }
    }
}

impl TopicService {
    /// Compatibility entrypoint. An explicit client runtime is required for execution.
    pub async fn delete_topic_by_request(request: DeleteTopicRequest) -> Result<DeleteTopicResult> {
        let request = request.validated()?;
        let mut admin = request.admin_builder().build_and_start().await?;
        let result = Self::delete_topic_with_admin(&mut admin, &request).await;
        admin.shutdown().await;
        result
    }

    /// Deletes one cluster Topic within a caller-owned client runtime and session.
    pub async fn delete_topic_by_request_with_credentials(
        request: DeleteTopicRequest,
        credentials: Option<AdminCredentials>,
        client_runtime: Arc<ClientRuntime>,
    ) -> Result<DeleteTopicResult> {
        let request = request.validated()?;
        let mut builder = request.admin_builder().client_runtime(client_runtime);
        if let Some(credentials) = credentials {
            builder = builder.credentials(credentials);
        }
        let mut admin = builder.build_and_start().await?;
        let result = Self::delete_topic_with_admin(&mut admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub(crate) async fn delete_topic_with_admin(
        admin: &mut DefaultMQAdminExt,
        request: &DeleteTopicRequest,
    ) -> Result<DeleteTopicResult> {
        execute_deletion(&mut SdkTopicDeletion(admin), request).await
    }
}

struct SdkTopicDeletion<'a>(&'a mut DefaultMQAdminExt);

impl TopicDeletionBackend for SdkTopicDeletion<'_> {
    async fn master_targets(&mut self, cluster: &CheetahString) -> Result<Vec<CheetahString>> {
        let info = self
            .0
            .examine_broker_cluster_info()
            .await
            .map_err(IntoCanonicalError::into_canonical_error)?;
        BrokerAddressResolver::fetch_master_addr_by_cluster_name(&info, cluster)
    }

    async fn delete_broker(
        &mut self,
        topic: &CheetahString,
        broker: &CheetahString,
    ) -> std::result::Result<(), TopicOperationFailure> {
        self.0
            .delete_topic_in_broker_list(HashSet::from([broker.clone()]), vec![topic.clone()])
            .await
            .map_err(|error| TopicOperationFailure {
                broker_addr: broker.clone(),
                error_code: stable_error_code(&error),
                error: stable_error_message(&error),
            })
    }

    async fn delete_route(&mut self, topic: &CheetahString, cluster: &CheetahString) -> Result<()> {
        let nameservers = self.0.get_name_server_address_list().await.into_iter().collect();
        self.0
            .delete_topic_in_name_server(nameservers, Some(cluster.clone()), topic.clone())
            .await
            .map_err(IntoCanonicalError::into_canonical_error)
    }
}
