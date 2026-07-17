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

//! Backend service compatibility exports and local composition.

use std::sync::Arc;

use crate::config::LocalConfig;
use crate::config::ProxyMode;
use crate::local::local_service_manager_from_config;

pub use rocketmq_proxy_cluster::service::*;
pub use rocketmq_proxy_core::service::*;
pub use rocketmq_proxy_core::ResourceIdentity;

pub struct LocalServiceManager {
    route_service: Arc<dyn RouteService>,
    metadata_service: Arc<dyn MetadataService>,
    assignment_service: Arc<dyn AssignmentService>,
    message_service: Arc<dyn MessageService>,
    consumer_service: Arc<dyn ConsumerService>,
    transaction_service: Arc<dyn TransactionService>,
}

impl LocalServiceManager {
    pub fn new(route_service: Arc<dyn RouteService>, metadata_service: Arc<dyn MetadataService>) -> Self {
        Self::with_services(
            route_service,
            metadata_service,
            Arc::new(DefaultAssignmentService),
            Arc::new(DefaultMessageService),
            Arc::new(DefaultConsumerService),
            Arc::new(DefaultTransactionService),
        )
    }

    pub fn with_assignment_service(
        route_service: Arc<dyn RouteService>,
        metadata_service: Arc<dyn MetadataService>,
        assignment_service: Arc<dyn AssignmentService>,
    ) -> Self {
        Self::with_services(
            route_service,
            metadata_service,
            assignment_service,
            Arc::new(DefaultMessageService),
            Arc::new(DefaultConsumerService),
            Arc::new(DefaultTransactionService),
        )
    }

    pub fn with_services(
        route_service: Arc<dyn RouteService>,
        metadata_service: Arc<dyn MetadataService>,
        assignment_service: Arc<dyn AssignmentService>,
        message_service: Arc<dyn MessageService>,
        consumer_service: Arc<dyn ConsumerService>,
        transaction_service: Arc<dyn TransactionService>,
    ) -> Self {
        Self {
            route_service,
            metadata_service,
            assignment_service,
            message_service,
            consumer_service,
            transaction_service,
        }
    }

    pub fn from_local_config(config: LocalConfig, assignment_strategy_name: impl Into<String>) -> Self {
        local_service_manager_from_config(config, assignment_strategy_name)
    }
}

impl Default for LocalServiceManager {
    fn default() -> Self {
        Self::new(
            Arc::new(StaticRouteService::default()),
            Arc::new(StaticMetadataService::default()),
        )
    }
}

impl ServiceManager for LocalServiceManager {
    fn mode(&self) -> ProxyMode {
        ProxyMode::Local
    }

    fn route_service(&self) -> Arc<dyn RouteService> {
        Arc::clone(&self.route_service)
    }

    fn metadata_service(&self) -> Arc<dyn MetadataService> {
        Arc::clone(&self.metadata_service)
    }

    fn assignment_service(&self) -> Arc<dyn AssignmentService> {
        Arc::clone(&self.assignment_service)
    }

    fn message_service(&self) -> Arc<dyn MessageService> {
        Arc::clone(&self.message_service)
    }

    fn consumer_service(&self) -> Arc<dyn ConsumerService> {
        Arc::clone(&self.consumer_service)
    }

    fn transaction_service(&self) -> Arc<dyn TransactionService> {
        Arc::clone(&self.transaction_service)
    }
}
