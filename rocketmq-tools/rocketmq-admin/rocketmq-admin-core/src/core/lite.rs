//! Lite message queue admin service models and operations.

use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_remoting::protocol::body::get_broker_lite_info_response_body::GetBrokerLiteInfoResponseBody;
use rocketmq_remoting::protocol::body::get_lite_topic_info_response_body::GetLiteTopicInfoResponseBody;
use rocketmq_remoting::protocol::body::get_parent_topic_info_response_body::GetParentTopicInfoResponseBody;
use rocketmq_remoting::runtime::RPCHook;
use serde::Deserialize;
use serde::Serialize;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::core::admin::AdminBuilder;
use crate::core::resolver::BrokerAddressResolver;
use crate::core::RocketMQResult;
use crate::core::ToolsError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BrokerLiteInfoTarget {
    Broker(CheetahString),
    Cluster(CheetahString),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerLiteInfoQueryRequest {
    target: BrokerLiteInfoTarget,
    namesrv_addr: Option<String>,
}

impl BrokerLiteInfoQueryRequest {
    pub fn try_new(broker_addr: Option<String>, cluster_name: Option<String>) -> RocketMQResult<Self> {
        let broker_addr = trim_optional_string(broker_addr);
        let cluster_name = trim_optional_string(cluster_name);
        let target = match (broker_addr, cluster_name) {
            (Some(broker_addr), None) => BrokerLiteInfoTarget::Broker(CheetahString::from(broker_addr)),
            (None, Some(cluster_name)) => BrokerLiteInfoTarget::Cluster(CheetahString::from(cluster_name)),
            (None, None) => {
                return Err(ToolsError::validation_error(
                    "target",
                    "either brokerAddr or clusterName must be provided",
                )
                .into());
            }
            (Some(_), Some(_)) => {
                return Err(ToolsError::validation_error(
                    "target",
                    "brokerAddr and clusterName cannot be provided together",
                )
                .into());
            }
        };

        Ok(Self {
            target,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn target(&self) -> &BrokerLiteInfoTarget {
        &self.target
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        let builder = AdminBuilder::new();
        match self.namesrv_addr() {
            Some(addr) => builder.namesrv_addr(addr),
            None => builder,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerLiteInfoEntry {
    pub broker_addr: CheetahString,
    pub body: Option<GetBrokerLiteInfoResponseBody>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BrokerLiteInfoQueryResult {
    pub entries: Vec<BrokerLiteInfoEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParentTopicInfoQueryRequest {
    parent_topic: CheetahString,
    namesrv_addr: Option<String>,
}

impl ParentTopicInfoQueryRequest {
    pub fn try_new(parent_topic: impl Into<String>) -> RocketMQResult<Self> {
        Ok(Self {
            parent_topic: trim_required_cheetah("parentTopic", parent_topic)?,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn parent_topic(&self) -> &CheetahString {
        &self.parent_topic
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        let builder = AdminBuilder::new();
        match self.namesrv_addr() {
            Some(addr) => builder.namesrv_addr(addr),
            None => builder,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParentTopicInfoEntry {
    pub broker_name: CheetahString,
    pub body: Option<GetParentTopicInfoResponseBody>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ParentTopicInfoQueryResult {
    pub parent_topic: CheetahString,
    pub entries: Vec<ParentTopicInfoEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiteTopicInfoQueryRequest {
    parent_topic: CheetahString,
    lite_topic: CheetahString,
    namesrv_addr: Option<String>,
}

impl LiteTopicInfoQueryRequest {
    pub fn try_new(parent_topic: impl Into<String>, lite_topic: impl Into<String>) -> RocketMQResult<Self> {
        Ok(Self {
            parent_topic: trim_required_cheetah("parentTopic", parent_topic)?,
            lite_topic: trim_required_cheetah("liteTopic", lite_topic)?,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn parent_topic(&self) -> &CheetahString {
        &self.parent_topic
    }

    pub fn lite_topic(&self) -> &CheetahString {
        &self.lite_topic
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        let builder = AdminBuilder::new();
        match self.namesrv_addr() {
            Some(addr) => builder.namesrv_addr(addr),
            None => builder,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiteTopicInfoEntry {
    pub broker_name: CheetahString,
    pub body: Option<GetLiteTopicInfoResponseBody>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LiteTopicInfoQueryResult {
    pub parent_topic: CheetahString,
    pub lite_topic: CheetahString,
    pub entries: Vec<LiteTopicInfoEntry>,
}

pub struct LiteService;

impl LiteService {
    pub async fn query_broker_lite_info_by_request_with_rpc_hook(
        request: BrokerLiteInfoQueryRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<BrokerLiteInfoQueryResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::query_broker_lite_info_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn query_broker_lite_info_with_admin(
        admin: &DefaultMQAdminExt,
        request: &BrokerLiteInfoQueryRequest,
    ) -> RocketMQResult<BrokerLiteInfoQueryResult> {
        match request.target() {
            BrokerLiteInfoTarget::Broker(broker_addr) => {
                let body = admin.get_broker_lite_info(broker_addr.clone()).await?;
                Ok(BrokerLiteInfoQueryResult {
                    entries: vec![BrokerLiteInfoEntry {
                        broker_addr: broker_addr.clone(),
                        body: Some(body),
                        error: None,
                    }],
                })
            }
            BrokerLiteInfoTarget::Cluster(cluster_name) => {
                let cluster_info = admin.examine_broker_cluster_info().await?;
                let broker_addrs =
                    BrokerAddressResolver::fetch_master_addr_by_cluster_name(&cluster_info, cluster_name)?;
                let mut entries = Vec::with_capacity(broker_addrs.len());
                for broker_addr in broker_addrs {
                    match admin.get_broker_lite_info(broker_addr.clone()).await {
                        Ok(body) => entries.push(BrokerLiteInfoEntry {
                            broker_addr,
                            body: Some(body),
                            error: None,
                        }),
                        Err(error) => entries.push(BrokerLiteInfoEntry {
                            broker_addr,
                            body: None,
                            error: Some(error.to_string()),
                        }),
                    }
                }
                Ok(BrokerLiteInfoQueryResult { entries })
            }
        }
    }

    pub async fn query_parent_topic_info_by_request_with_rpc_hook(
        request: ParentTopicInfoQueryRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<ParentTopicInfoQueryResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::query_parent_topic_info_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn query_parent_topic_info_with_admin(
        admin: &DefaultMQAdminExt,
        request: &ParentTopicInfoQueryRequest,
    ) -> RocketMQResult<ParentTopicInfoQueryResult> {
        let route = admin
            .examine_topic_route_info(request.parent_topic().clone())
            .await?
            .ok_or_else(|| {
                ToolsError::internal(format!(
                    "Topic route not found for parentTopic '{}'",
                    request.parent_topic()
                ))
            })?;

        let mut entries = Vec::new();
        for broker_data in &route.broker_datas {
            let Some(broker_addr) = broker_data.select_broker_addr() else {
                continue;
            };
            let broker_name = broker_data.broker_name().clone();
            match admin
                .get_parent_topic_info(broker_addr, request.parent_topic().clone())
                .await
            {
                Ok(body) => entries.push(ParentTopicInfoEntry {
                    broker_name,
                    body: Some(body),
                    error: None,
                }),
                Err(error) => entries.push(ParentTopicInfoEntry {
                    broker_name,
                    body: None,
                    error: Some(error.to_string()),
                }),
            }
        }

        Ok(ParentTopicInfoQueryResult {
            parent_topic: request.parent_topic().clone(),
            entries,
        })
    }

    pub async fn query_lite_topic_info_by_request_with_rpc_hook(
        request: LiteTopicInfoQueryRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<LiteTopicInfoQueryResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::query_lite_topic_info_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn query_lite_topic_info_with_admin(
        admin: &DefaultMQAdminExt,
        request: &LiteTopicInfoQueryRequest,
    ) -> RocketMQResult<LiteTopicInfoQueryResult> {
        let route = admin
            .examine_topic_route_info(request.parent_topic().clone())
            .await?
            .ok_or_else(|| {
                ToolsError::internal(format!(
                    "Topic route not found for parentTopic '{}'",
                    request.parent_topic()
                ))
            })?;

        let mut entries = Vec::new();
        for broker_data in &route.broker_datas {
            let Some(broker_addr) = broker_data.select_broker_addr() else {
                continue;
            };
            let broker_name = broker_data.broker_name().clone();
            match admin
                .get_lite_topic_info(
                    broker_addr,
                    request.parent_topic().clone(),
                    request.lite_topic().clone(),
                )
                .await
            {
                Ok(body) => entries.push(LiteTopicInfoEntry {
                    broker_name,
                    body: Some(body),
                    error: None,
                }),
                Err(error) => entries.push(LiteTopicInfoEntry {
                    broker_name,
                    body: None,
                    error: Some(error.to_string()),
                }),
            }
        }

        Ok(LiteTopicInfoQueryResult {
            parent_topic: request.parent_topic().clone(),
            lite_topic: request.lite_topic().clone(),
            entries,
        })
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

fn admin_builder_with_rpc_hook(builder: AdminBuilder, rpc_hook: Option<Arc<dyn RPCHook>>) -> AdminBuilder {
    match rpc_hook {
        Some(hook) => builder.rpc_hook(hook),
        None => builder,
    }
}
