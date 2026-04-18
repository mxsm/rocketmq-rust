//! Lite message queue admin service models and operations.

use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_remoting::protocol::body::get_broker_lite_info_response_body::GetBrokerLiteInfoResponseBody;
use rocketmq_remoting::protocol::body::get_lite_client_info_response_body::GetLiteClientInfoResponseBody;
use rocketmq_remoting::protocol::body::get_lite_group_info_response_body::GetLiteGroupInfoResponseBody;
use rocketmq_remoting::protocol::body::get_lite_topic_info_response_body::GetLiteTopicInfoResponseBody;
use rocketmq_remoting::protocol::body::get_parent_topic_info_response_body::GetParentTopicInfoResponseBody;
use rocketmq_remoting::protocol::body::lite_lag_info::LiteLagInfo;
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiteGroupInfoQueryRequest {
    parent_topic: CheetahString,
    group: CheetahString,
    lite_topic: Option<CheetahString>,
    top_k: i32,
    namesrv_addr: Option<String>,
}

impl LiteGroupInfoQueryRequest {
    pub fn try_new(
        parent_topic: impl Into<String>,
        group: impl Into<String>,
        lite_topic: Option<String>,
        top_k: Option<i32>,
    ) -> RocketMQResult<Self> {
        Ok(Self {
            parent_topic: trim_required_cheetah("parentTopic", parent_topic)?,
            group: trim_required_cheetah("group", group)?,
            lite_topic: trim_optional_string(lite_topic).map(CheetahString::from),
            top_k: top_k.unwrap_or(20),
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

    pub fn group(&self) -> &CheetahString {
        &self.group
    }

    pub fn lite_topic(&self) -> Option<&CheetahString> {
        self.lite_topic.as_ref()
    }

    pub fn query_by_lite_topic(&self) -> bool {
        self.lite_topic.is_some()
    }

    pub fn top_k(&self) -> i32 {
        self.top_k
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
pub struct LiteGroupInfoEntry {
    pub broker_name: CheetahString,
    pub body: Option<GetLiteGroupInfoResponseBody>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiteGroupInfoQueryResult {
    pub parent_topic: CheetahString,
    pub group: CheetahString,
    pub lite_topic: Option<CheetahString>,
    pub total_lag_count: i64,
    pub earliest_unconsumed_timestamp: i64,
    pub lag_count_top_k: Vec<LiteLagInfo>,
    pub lag_timestamp_top_k: Vec<LiteLagInfo>,
    pub entries: Vec<LiteGroupInfoEntry>,
}

impl LiteGroupInfoQueryResult {
    pub fn query_by_lite_topic(&self) -> bool {
        self.lite_topic.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiteClientInfoQueryRequest {
    parent_topic: CheetahString,
    group: CheetahString,
    client_id: CheetahString,
    namesrv_addr: Option<String>,
}

impl LiteClientInfoQueryRequest {
    pub fn try_new(
        parent_topic: impl Into<String>,
        group: impl Into<String>,
        client_id: impl Into<String>,
    ) -> RocketMQResult<Self> {
        Ok(Self {
            parent_topic: trim_required_cheetah("parentTopic", parent_topic)?,
            group: trim_required_cheetah("group", group)?,
            client_id: trim_required_cheetah("clientId", client_id)?,
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

    pub fn group(&self) -> &CheetahString {
        &self.group
    }

    pub fn client_id(&self) -> &CheetahString {
        &self.client_id
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
pub struct LiteClientInfoEntry {
    pub broker_name: CheetahString,
    pub body: Option<GetLiteClientInfoResponseBody>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiteClientInfoQueryResult {
    pub parent_topic: CheetahString,
    pub group: CheetahString,
    pub client_id: CheetahString,
    pub entries: Vec<LiteClientInfoEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriggerLiteDispatchRequest {
    parent_topic: CheetahString,
    group: CheetahString,
    client_id: Option<CheetahString>,
    broker_name: Option<CheetahString>,
    namesrv_addr: Option<String>,
}

impl TriggerLiteDispatchRequest {
    pub fn try_new(
        parent_topic: impl Into<String>,
        group: impl Into<String>,
        client_id: Option<String>,
        broker_name: Option<String>,
    ) -> RocketMQResult<Self> {
        Ok(Self {
            parent_topic: trim_required_cheetah("parentTopic", parent_topic)?,
            group: trim_required_cheetah("group", group)?,
            client_id: trim_optional_string(client_id).map(CheetahString::from),
            broker_name: trim_optional_string(broker_name).map(CheetahString::from),
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

    pub fn group(&self) -> &CheetahString {
        &self.group
    }

    pub fn client_id(&self) -> Option<&CheetahString> {
        self.client_id.as_ref()
    }

    pub fn broker_name(&self) -> Option<&CheetahString> {
        self.broker_name.as_ref()
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
pub struct TriggerLiteDispatchEntry {
    pub broker_name: CheetahString,
    pub dispatched: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerLiteDispatchResult {
    pub parent_topic: CheetahString,
    pub group: CheetahString,
    pub client_id: Option<CheetahString>,
    pub broker_name: Option<CheetahString>,
    pub entries: Vec<TriggerLiteDispatchEntry>,
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

    pub async fn query_lite_group_info_by_request_with_rpc_hook(
        request: LiteGroupInfoQueryRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<LiteGroupInfoQueryResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::query_lite_group_info_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn query_lite_group_info_with_admin(
        admin: &DefaultMQAdminExt,
        request: &LiteGroupInfoQueryRequest,
    ) -> RocketMQResult<LiteGroupInfoQueryResult> {
        let route = admin
            .examine_topic_route_info(request.parent_topic().clone())
            .await?
            .ok_or_else(|| {
                ToolsError::internal(format!(
                    "Topic route not found for parentTopic '{}'",
                    request.parent_topic()
                ))
            })?;

        let mut total_lag_count = 0;
        let mut earliest_unconsumed_timestamp = current_millis() as i64;
        let mut lag_count_top_k = Vec::new();
        let mut lag_timestamp_top_k = Vec::new();
        let mut entries = Vec::new();

        for broker_data in &route.broker_datas {
            let Some(broker_addr) = broker_data.select_broker_addr() else {
                continue;
            };
            let broker_name = broker_data.broker_name().clone();
            let lite_topic = request.lite_topic().cloned().unwrap_or_default();

            match admin
                .get_lite_group_info(broker_addr, request.group().clone(), lite_topic, request.top_k())
                .await
            {
                Ok(body) => {
                    if body.total_lag_count() > 0 {
                        total_lag_count += body.total_lag_count();
                    }
                    if body.earliest_unconsumed_timestamp() > 0 {
                        earliest_unconsumed_timestamp =
                            earliest_unconsumed_timestamp.min(body.earliest_unconsumed_timestamp());
                    }
                    lag_count_top_k.extend_from_slice(body.lag_count_top_k());
                    lag_timestamp_top_k.extend_from_slice(body.lag_timestamp_top_k());
                    entries.push(LiteGroupInfoEntry {
                        broker_name,
                        body: Some(body),
                        error: None,
                    });
                }
                Err(error) => entries.push(LiteGroupInfoEntry {
                    broker_name,
                    body: None,
                    error: Some(error.to_string()),
                }),
            }
        }

        lag_count_top_k.sort_by_key(|item| std::cmp::Reverse(item.lag_count()));
        lag_timestamp_top_k.sort_by_key(|item| item.earliest_unconsumed_timestamp());

        Ok(LiteGroupInfoQueryResult {
            parent_topic: request.parent_topic().clone(),
            group: request.group().clone(),
            lite_topic: request.lite_topic().cloned(),
            total_lag_count,
            earliest_unconsumed_timestamp,
            lag_count_top_k,
            lag_timestamp_top_k,
            entries,
        })
    }

    pub async fn query_lite_client_info_by_request_with_rpc_hook(
        request: LiteClientInfoQueryRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<LiteClientInfoQueryResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::query_lite_client_info_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn query_lite_client_info_with_admin(
        admin: &DefaultMQAdminExt,
        request: &LiteClientInfoQueryRequest,
    ) -> RocketMQResult<LiteClientInfoQueryResult> {
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
                .get_lite_client_info(
                    broker_addr,
                    request.parent_topic().clone(),
                    request.group().clone(),
                    request.client_id().clone(),
                )
                .await
            {
                Ok(body) => entries.push(LiteClientInfoEntry {
                    broker_name,
                    body: Some(body),
                    error: None,
                }),
                Err(error) => entries.push(LiteClientInfoEntry {
                    broker_name,
                    body: None,
                    error: Some(error.to_string()),
                }),
            }
        }

        Ok(LiteClientInfoQueryResult {
            parent_topic: request.parent_topic().clone(),
            group: request.group().clone(),
            client_id: request.client_id().clone(),
            entries,
        })
    }

    pub async fn trigger_lite_dispatch_by_request_with_rpc_hook(
        request: TriggerLiteDispatchRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<TriggerLiteDispatchResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::trigger_lite_dispatch_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn trigger_lite_dispatch_with_admin(
        admin: &DefaultMQAdminExt,
        request: &TriggerLiteDispatchRequest,
    ) -> RocketMQResult<TriggerLiteDispatchResult> {
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
            if let Some(filter) = request.broker_name() {
                if filter != broker_data.broker_name() {
                    continue;
                }
            }

            let broker_name = broker_data.broker_name().clone();
            let client_id = request.client_id().cloned().unwrap_or_default();
            match admin
                .trigger_lite_dispatch(broker_addr, request.group().clone(), client_id)
                .await
            {
                Ok(()) => entries.push(TriggerLiteDispatchEntry {
                    broker_name,
                    dispatched: true,
                    error: None,
                }),
                Err(error) => entries.push(TriggerLiteDispatchEntry {
                    broker_name,
                    dispatched: false,
                    error: Some(error.to_string()),
                }),
            }
        }

        Ok(TriggerLiteDispatchResult {
            parent_topic: request.parent_topic().clone(),
            group: request.group().clone(),
            client_id: request.client_id().cloned(),
            broker_name: request.broker_name().cloned(),
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
