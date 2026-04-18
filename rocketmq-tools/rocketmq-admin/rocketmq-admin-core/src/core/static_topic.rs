//! Static topic admin service models and operations.

use std::collections::HashSet;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_utils::TopicQueueMappingUtils;
use rocketmq_remoting::protocol::static_topic::topic_remapping_detail_wrapper;
use rocketmq_remoting::protocol::static_topic::topic_remapping_detail_wrapper::TopicRemappingDetailWrapper;
use rocketmq_remoting::rpc::client_metadata::ClientMetadata;
use rocketmq_remoting::runtime::RPCHook;
use serde::Deserialize;
use serde::Serialize;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::admin::mq_admin_utils::MQAdminUtils;
use crate::core::admin::AdminBuilder;
use crate::core::RocketMQResult;
use crate::core::ToolsError;

const DEFAULT_BLOCK_SEQ_SIZE: i64 = 10000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateStaticTopicRequest {
    topic: CheetahString,
    broker_names: Vec<CheetahString>,
    cluster_names: Vec<CheetahString>,
    queue_num: i32,
    namesrv_addr: Option<String>,
}

impl UpdateStaticTopicRequest {
    pub fn try_new(
        topic: impl Into<String>,
        broker_names: impl Into<String>,
        queue_num: impl AsRef<str>,
        cluster_names: Option<String>,
    ) -> RocketMQResult<Self> {
        let broker_names = split_csv_required("brokerAddr", broker_names.into())?;
        let queue_num = queue_num
            .as_ref()
            .trim()
            .parse::<i32>()
            .map_err(|_| ToolsError::validation_error("totalQueueNum", "totalQueueNum must be a valid i32"))?;

        Ok(Self {
            topic: trim_required_cheetah("topic", topic)?,
            broker_names,
            cluster_names: split_csv_optional(cluster_names),
            queue_num,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn topic(&self) -> &CheetahString {
        &self.topic
    }

    pub fn broker_names(&self) -> &[CheetahString] {
        &self.broker_names
    }

    pub fn cluster_names(&self) -> &[CheetahString] {
        &self.cluster_names
    }

    pub fn queue_num(&self) -> i32 {
        self.queue_num
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        builder_with_namesrv(self.namesrv_addr())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemappingStaticTopicRequest {
    topic: CheetahString,
    broker_names: Vec<CheetahString>,
    cluster_names: Vec<CheetahString>,
    force_replace: bool,
    namesrv_addr: Option<String>,
}

impl RemappingStaticTopicRequest {
    pub fn try_new(
        topic: impl Into<String>,
        broker_names: Option<String>,
        cluster_names: Option<String>,
        force_replace: Option<bool>,
    ) -> RocketMQResult<Self> {
        let broker_names = split_csv_optional(broker_names);
        let cluster_names = split_csv_optional(cluster_names);
        if broker_names.is_empty() && cluster_names.is_empty() {
            return Err(ToolsError::validation_error("target", "either brokers or clusters must be provided").into());
        }

        Ok(Self {
            topic: trim_required_cheetah("topic", topic)?,
            broker_names,
            cluster_names,
            force_replace: force_replace.unwrap_or(false),
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn topic(&self) -> &CheetahString {
        &self.topic
    }

    pub fn broker_names(&self) -> &[CheetahString] {
        &self.broker_names
    }

    pub fn cluster_names(&self) -> &[CheetahString] {
        &self.cluster_names
    }

    pub fn force_replace(&self) -> bool {
        self.force_replace
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        builder_with_namesrv(self.namesrv_addr())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StaticTopicMappingFileRequest {
    topic: CheetahString,
    force_replace: bool,
    namesrv_addr: Option<String>,
}

impl StaticTopicMappingFileRequest {
    pub fn try_new(topic: impl Into<String>, force_replace: bool) -> RocketMQResult<Self> {
        Ok(Self {
            topic: trim_required_cheetah("topic", topic)?,
            force_replace,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn topic(&self) -> &CheetahString {
        &self.topic
    }

    pub fn force_replace(&self) -> bool {
        self.force_replace
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        builder_with_namesrv(self.namesrv_addr())
    }
}

#[derive(Debug)]
pub struct StaticTopicMappingPlan {
    pub old_mapping: TopicRemappingDetailWrapper,
    pub new_mapping: TopicRemappingDetailWrapper,
}

pub struct StaticTopicService;

impl StaticTopicService {
    pub async fn update_static_topic_by_request_with_rpc_hook(
        request: UpdateStaticTopicRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<StaticTopicMappingPlan> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::update_static_topic_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn update_static_topic_with_admin(
        admin: &DefaultMQAdminExt,
        request: &UpdateStaticTopicRequest,
    ) -> RocketMQResult<StaticTopicMappingPlan> {
        let cluster_info = admin.examine_broker_cluster_info().await?;
        let cluster_addr_table = cluster_info
            .cluster_addr_table
            .as_ref()
            .ok_or_else(|| RocketMQError::Internal("The Cluster info is empty".to_string()))?;
        if cluster_addr_table.is_empty() {
            return Err(RocketMQError::Internal("The Cluster info is empty".to_string()));
        }

        let mut target_brokers: HashSet<CheetahString> = request.broker_names().iter().cloned().collect();
        for cluster in request.cluster_names() {
            if let Some(brokers) = cluster_addr_table.get(cluster) {
                target_brokers.extend(brokers.iter().cloned());
            }
        }
        if target_brokers.is_empty() {
            return Err(RocketMQError::Internal("Find none brokers, do nothing".to_string()));
        }

        let mut broker_config_map = MQAdminUtils::examine_topic_config_all(request.topic(), admin).await?;
        let mut max_epoch_and_num = (current_millis(), request.queue_num());
        if !broker_config_map.is_empty() {
            let new_max_epoch_and_num =
                TopicQueueMappingUtils::check_name_epoch_num_consistence(request.topic(), &broker_config_map)?;
            max_epoch_and_num.0 = new_max_epoch_and_num.0 as u64;
            max_epoch_and_num.1 = new_max_epoch_and_num.1;
        }

        let old_mapping = TopicRemappingDetailWrapper::new(
            request.topic().clone(),
            CheetahString::from_static_str(topic_remapping_detail_wrapper::TYPE_CREATE_OR_UPDATE),
            max_epoch_and_num.0,
            broker_config_map.clone(),
            HashSet::new(),
            HashSet::new(),
        );

        target_brokers.extend(broker_config_map.keys().cloned());
        let new_mapping = TopicQueueMappingUtils::create_topic_config_mapping(
            request.topic().as_str(),
            request.queue_num(),
            &target_brokers,
            &mut broker_config_map,
        )?;

        MQAdminUtils::complete_no_target_brokers(broker_config_map.clone(), admin).await?;
        MQAdminUtils::update_topic_config_mapping_all(&broker_config_map, admin, false).await?;

        Ok(StaticTopicMappingPlan {
            old_mapping,
            new_mapping,
        })
    }

    pub async fn update_static_topic_from_mapping_by_request_with_rpc_hook(
        request: StaticTopicMappingFileRequest,
        wrapper: TopicRemappingDetailWrapper,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<()> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::update_static_topic_from_mapping_with_admin(&admin, &request, wrapper).await;
        admin.shutdown().await;
        result
    }

    pub async fn update_static_topic_from_mapping_with_admin(
        admin: &DefaultMQAdminExt,
        request: &StaticTopicMappingFileRequest,
        wrapper: TopicRemappingDetailWrapper,
    ) -> RocketMQResult<()> {
        TopicQueueMappingUtils::check_name_epoch_num_consistence(request.topic(), wrapper.broker_config_map())?;
        let mapping_details = TopicQueueMappingUtils::get_mapping_detail_from_config(
            wrapper.broker_config_map().values().cloned().collect(),
        )?;
        TopicQueueMappingUtils::check_and_build_mapping_items(mapping_details, request.force_replace(), true)?;

        MQAdminUtils::complete_no_target_brokers(wrapper.broker_config_map().clone(), admin).await?;
        MQAdminUtils::update_topic_config_mapping_all(wrapper.broker_config_map(), admin, request.force_replace()).await
    }

    pub async fn remapping_static_topic_by_request_with_rpc_hook(
        request: RemappingStaticTopicRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<StaticTopicMappingPlan> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::remapping_static_topic_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn remapping_static_topic_with_admin(
        admin: &DefaultMQAdminExt,
        request: &RemappingStaticTopicRequest,
    ) -> RocketMQResult<StaticTopicMappingPlan> {
        let cluster_info = admin.examine_broker_cluster_info().await?;
        let cluster_addr_table = cluster_info
            .cluster_addr_table
            .as_ref()
            .ok_or_else(|| RocketMQError::Internal("The Cluster info is empty".to_string()))?;
        if cluster_addr_table.is_empty() {
            return Err(RocketMQError::Internal("The Cluster info is empty".to_string()));
        }

        let client_metadata = ClientMetadata::new();
        client_metadata.refresh_cluster_info(Some(&cluster_info));

        let mut target_brokers: HashSet<CheetahString> = request.broker_names().iter().cloned().collect();
        for cluster in request.cluster_names() {
            if let Some(brokers) = cluster_addr_table.get(cluster) {
                target_brokers.extend(brokers.iter().cloned());
            }
        }
        if target_brokers.is_empty() {
            return Err(RocketMQError::Internal("Find none brokers, do nothing".to_string()));
        }

        for broker in &target_brokers {
            if client_metadata.find_master_broker_addr(broker).is_none() {
                return Err(RocketMQError::Internal(format!(
                    "Can't find addr for broker {}",
                    broker
                )));
            }
        }

        let mut broker_config_map = MQAdminUtils::examine_topic_config_all(request.topic(), admin).await?;
        if broker_config_map.is_empty() {
            return Err(RocketMQError::Internal(
                "No topic route to do the remapping".to_string(),
            ));
        }

        let max_epoch_and_num =
            TopicQueueMappingUtils::check_name_epoch_num_consistence(request.topic(), &broker_config_map)?;
        let old_mapping = TopicRemappingDetailWrapper::new(
            request.topic().clone(),
            CheetahString::from_static_str(topic_remapping_detail_wrapper::TYPE_CREATE_OR_UPDATE),
            max_epoch_and_num.0 as u64,
            broker_config_map.clone(),
            HashSet::new(),
            HashSet::new(),
        );

        let new_mapping = TopicQueueMappingUtils::remapping_static_topic(
            request.topic().as_str(),
            &mut broker_config_map,
            &target_brokers,
        )?;

        MQAdminUtils::complete_no_target_brokers(broker_config_map.clone(), admin).await?;
        MQAdminUtils::remapping_static_topic(
            request.topic(),
            new_mapping.broker_to_map_in(),
            new_mapping.broker_to_map_out(),
            &mut broker_config_map,
            DEFAULT_BLOCK_SEQ_SIZE,
            request.force_replace(),
            admin,
        )
        .await?;

        Ok(StaticTopicMappingPlan {
            old_mapping,
            new_mapping,
        })
    }

    pub async fn remapping_static_topic_from_mapping_by_request_with_rpc_hook(
        request: StaticTopicMappingFileRequest,
        mut wrapper: TopicRemappingDetailWrapper,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<()> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::remapping_static_topic_from_mapping_with_admin(&admin, &request, &mut wrapper).await;
        admin.shutdown().await;
        result
    }

    pub async fn remapping_static_topic_from_mapping_with_admin(
        admin: &DefaultMQAdminExt,
        request: &StaticTopicMappingFileRequest,
        wrapper: &mut TopicRemappingDetailWrapper,
    ) -> RocketMQResult<()> {
        TopicQueueMappingUtils::check_name_epoch_num_consistence(request.topic(), wrapper.broker_config_map())?;
        let mapping_details = TopicQueueMappingUtils::get_mapping_detail_from_config(
            wrapper.broker_config_map().values().cloned().collect(),
        )?;
        TopicQueueMappingUtils::check_and_build_mapping_items(mapping_details, false, true)?;

        let brokers_to_map_in = wrapper.broker_to_map_in().clone();
        let brokers_to_map_out = wrapper.broker_to_map_out().clone();

        MQAdminUtils::remapping_static_topic(
            request.topic(),
            &brokers_to_map_in,
            &brokers_to_map_out,
            wrapper.broker_config_map_mut(),
            DEFAULT_BLOCK_SEQ_SIZE,
            request.force_replace(),
            admin,
        )
        .await
    }
}

fn split_csv_required(field: &'static str, value: impl Into<String>) -> RocketMQResult<Vec<CheetahString>> {
    let values = split_csv_optional(Some(value.into()));
    if values.is_empty() {
        return Err(ToolsError::validation_error(field, format!("{field} must not be empty")).into());
    }
    Ok(values)
}

fn split_csv_optional(value: Option<String>) -> Vec<CheetahString> {
    value
        .into_iter()
        .flat_map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(CheetahString::from)
                .collect::<Vec<_>>()
        })
        .collect()
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
