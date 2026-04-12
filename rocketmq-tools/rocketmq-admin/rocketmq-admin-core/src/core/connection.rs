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

//! Producer and consumer connection admin service models and operations.

use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_remoting::protocol::body::consumer_connection::ConsumerConnection;
use rocketmq_remoting::protocol::body::producer_connection::ProducerConnection;
use rocketmq_remoting::runtime::RPCHook;
use serde::Deserialize;
use serde::Serialize;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::core::admin::AdminBuilder;
use crate::core::RocketMQResult;
use crate::core::ToolsError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerConnectionQueryRequest {
    consumer_group: CheetahString,
    broker_addr: Option<CheetahString>,
    namesrv_addr: Option<String>,
}

impl ConsumerConnectionQueryRequest {
    pub fn try_new(consumer_group: impl Into<String>, broker_addr: Option<String>) -> RocketMQResult<Self> {
        Ok(Self {
            consumer_group: trim_required_cheetah("consumerGroup", consumer_group)?,
            broker_addr: trim_optional_string(broker_addr).map(CheetahString::from),
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn consumer_group(&self) -> &CheetahString {
        &self.consumer_group
    }

    pub fn broker_addr(&self) -> Option<&str> {
        self.broker_addr.as_ref().map(|broker_addr| broker_addr.as_str())
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        builder_with_namesrv(self.namesrv_addr())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumerConnectionQueryResult {
    pub connection: ConsumerConnection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerConnectionQueryRequest {
    producer_group: CheetahString,
    topic: CheetahString,
    namesrv_addr: Option<String>,
}

impl ProducerConnectionQueryRequest {
    pub fn try_new(producer_group: impl Into<String>, topic: impl Into<String>) -> RocketMQResult<Self> {
        Ok(Self {
            producer_group: trim_required_cheetah("producerGroup", producer_group)?,
            topic: trim_required_cheetah("topic", topic)?,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn producer_group(&self) -> &CheetahString {
        &self.producer_group
    }

    pub fn topic(&self) -> &CheetahString {
        &self.topic
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        builder_with_namesrv(self.namesrv_addr())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProducerConnectionQueryResult {
    pub connection: ProducerConnection,
}

pub struct ConnectionService;

impl ConnectionService {
    pub async fn query_consumer_connection_by_request_with_rpc_hook(
        request: ConsumerConnectionQueryRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<ConsumerConnectionQueryResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::query_consumer_connection_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn query_consumer_connection_with_admin(
        admin: &DefaultMQAdminExt,
        request: &ConsumerConnectionQueryRequest,
    ) -> RocketMQResult<ConsumerConnectionQueryResult> {
        let connection = admin
            .examine_consumer_connection_info(request.consumer_group.clone(), request.broker_addr.clone())
            .await?;
        Ok(ConsumerConnectionQueryResult { connection })
    }

    pub async fn query_producer_connection_by_request_with_rpc_hook(
        request: ProducerConnectionQueryRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<ProducerConnectionQueryResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::query_producer_connection_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn query_producer_connection_with_admin(
        admin: &DefaultMQAdminExt,
        request: &ProducerConnectionQueryRequest,
    ) -> RocketMQResult<ProducerConnectionQueryResult> {
        let connection = admin
            .examine_producer_connection_info(request.producer_group.clone(), request.topic.clone())
            .await?;
        Ok(ProducerConnectionQueryResult { connection })
    }
}

fn trim_optional_string(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn trim_required_cheetah(field: &'static str, value: impl Into<String>) -> RocketMQResult<CheetahString> {
    let value = value.into();
    let value = value.trim();
    if value.is_empty() {
        return Err(ToolsError::validation_error(field, format!("{field} must not be empty")).into());
    }
    Ok(CheetahString::from(value))
}

fn builder_with_namesrv(namesrv_addr: Option<&str>) -> AdminBuilder {
    let builder = AdminBuilder::new();
    match namesrv_addr {
        Some(addr) => builder.namesrv_addr(addr),
        None => builder,
    }
}

fn admin_builder_with_rpc_hook(builder: AdminBuilder, rpc_hook: Option<Arc<dyn RPCHook>>) -> AdminBuilder {
    match rpc_hook {
        Some(hook) => builder.rpc_hook(hook),
        None => builder,
    }
}
