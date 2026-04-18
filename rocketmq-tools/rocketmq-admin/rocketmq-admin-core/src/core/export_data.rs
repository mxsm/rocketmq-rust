//! Export admin service models and operations.

use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_remoting::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_remoting::protocol::body::subscription_group_wrapper::SubscriptionGroupWrapper;
use rocketmq_remoting::protocol::body::topic_info_wrapper::TopicConfigSerializeWrapper;
use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_remoting::runtime::RPCHook;
#[cfg(feature = "rocksdb-export")]
use rocksdb::Options;
#[cfg(feature = "rocksdb-export")]
use rocksdb::DB;
use serde::Deserialize;
use serde::Serialize;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::core::admin::AdminBuilder;
use crate::core::resolver::BrokerAddressResolver;
use crate::core::RocketMQError;
use crate::core::RocketMQResult;
use crate::core::ToolsError;

const EXPORT_BROKER_PROPERTY_KEYS: &[&str] = &[
    "brokerClusterName",
    "brokerId",
    "brokerName",
    "brokerRole",
    "fileReservedTime",
    "filterServerNums",
    "flushDiskType",
    "maxMessageSize",
    "messageDelayLevel",
    "msgTraceTopicName",
    "slaveReadEnable",
    "traceOn",
    "traceTopicEnable",
    "useTLS",
    "autoCreateTopicEnable",
    "autoCreateSubscriptionGroup",
];
const DEFAULT_EXPORT_POP_RECORD_TIMEOUT_MILLIS: u64 = 30000;
const TOPICS_JSON_CONFIG: &str = "topics";
const SUBSCRIPTION_GROUP_JSON_CONFIG: &str = "subscriptionGroups";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportConfigsRequest {
    cluster_name: CheetahString,
    namesrv_addr: Option<String>,
}

impl ExportConfigsRequest {
    pub fn try_new(cluster_name: impl Into<String>) -> RocketMQResult<Self> {
        Ok(Self {
            cluster_name: trim_required_cheetah("clusterName", cluster_name)?,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn cluster_name(&self) -> &CheetahString {
        &self.cluster_name
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportConfigsResult {
    pub name_servers: Vec<String>,
    pub broker_configs: HashMap<String, HashMap<String, String>>,
    pub master_broker_size: i64,
    pub slave_broker_size: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExportMetadataTarget {
    Broker(CheetahString),
    Cluster(CheetahString),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExportMetadataScope {
    Topic,
    SubscriptionGroup,
    All,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportMetadataRequest {
    target: ExportMetadataTarget,
    scope: ExportMetadataScope,
    special_topic: bool,
    timeout_millis: u64,
    namesrv_addr: Option<String>,
}

impl ExportMetadataRequest {
    pub fn try_new(
        cluster_name: Option<String>,
        broker_addr: Option<String>,
        topic_only: bool,
        subscription_group_only: bool,
        special_topic: bool,
    ) -> RocketMQResult<Self> {
        let cluster_name = trim_optional_string(cluster_name);
        let broker_addr = trim_optional_string(broker_addr);
        let target = match (cluster_name, broker_addr) {
            (Some(cluster_name), None) => ExportMetadataTarget::Cluster(CheetahString::from(cluster_name)),
            (None, Some(broker_addr)) => ExportMetadataTarget::Broker(CheetahString::from(broker_addr)),
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

        let scope = if topic_only {
            ExportMetadataScope::Topic
        } else if subscription_group_only {
            ExportMetadataScope::SubscriptionGroup
        } else {
            ExportMetadataScope::All
        };

        if matches!(target, ExportMetadataTarget::Broker(_)) && matches!(scope, ExportMetadataScope::All) {
            return Err(ToolsError::validation_error(
                "scope",
                "broker target requires topic or subscriptionGroup scope",
            )
            .into());
        }

        Ok(Self {
            target,
            scope,
            special_topic,
            timeout_millis: 10000,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn target(&self) -> &ExportMetadataTarget {
        &self.target
    }

    pub fn scope(&self) -> ExportMetadataScope {
        self.scope
    }

    pub fn special_topic(&self) -> bool {
        self.special_topic
    }

    pub fn timeout_millis(&self) -> u64 {
        self.timeout_millis
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExportMetadataInRocksDbConfigType {
    Topics,
    SubscriptionGroups,
}

impl ExportMetadataInRocksDbConfigType {
    pub fn from_config_type(config_type: &str) -> Option<Self> {
        if config_type.eq_ignore_ascii_case(TOPICS_JSON_CONFIG) {
            Some(Self::Topics)
        } else if config_type.eq_ignore_ascii_case(SUBSCRIPTION_GROUP_JSON_CONFIG) {
            Some(Self::SubscriptionGroups)
        } else {
            None
        }
    }

    pub fn table_key(self) -> &'static str {
        match self {
            Self::Topics => "topicConfigTable",
            Self::SubscriptionGroups => "subscriptionGroupTable",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportMetadataInRocksDbRequest {
    path: PathBuf,
    config_type: String,
    json_enable: bool,
}

impl ExportMetadataInRocksDbRequest {
    pub fn new(path: impl Into<String>, config_type: impl Into<String>, json_enable: bool) -> Self {
        Self {
            path: PathBuf::from(path.into().trim()),
            config_type: config_type.into().trim().to_string(),
            json_enable,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn config_type(&self) -> &str {
        &self.config_type
    }

    pub fn json_enable(&self) -> bool {
        self.json_enable
    }

    pub fn normalized_config_type(&self) -> Option<ExportMetadataInRocksDbConfigType> {
        ExportMetadataInRocksDbConfigType::from_config_type(&self.config_type)
    }

    pub fn full_path(&self) -> PathBuf {
        self.path.join(&self.config_type)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportMetadataInRocksDbEntry {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExportMetadataInRocksDbResult {
    InvalidPath,
    InvalidConfigType {
        config_type: String,
    },
    Data {
        config_type: ExportMetadataInRocksDbConfigType,
        json_enable: bool,
        entries: Vec<ExportMetadataInRocksDbEntry>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExportPopRecordTarget {
    Broker(CheetahString),
    Cluster(CheetahString),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportPopRecordRequest {
    target: ExportPopRecordTarget,
    dry_run: bool,
    timeout_millis: u64,
    namesrv_addr: Option<String>,
}

impl ExportPopRecordRequest {
    pub fn try_new(cluster_name: Option<String>, broker_addr: Option<String>, dry_run: bool) -> RocketMQResult<Self> {
        let cluster_name = trim_optional_string(cluster_name);
        let broker_addr = trim_optional_string(broker_addr);
        let target = match (cluster_name, broker_addr) {
            (Some(cluster_name), None) => ExportPopRecordTarget::Cluster(CheetahString::from(cluster_name)),
            (None, Some(broker_addr)) => ExportPopRecordTarget::Broker(CheetahString::from(broker_addr)),
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
            dry_run,
            timeout_millis: DEFAULT_EXPORT_POP_RECORD_TIMEOUT_MILLIS,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn with_timeout_millis(mut self, timeout_millis: u64) -> Self {
        self.timeout_millis = timeout_millis;
        self
    }

    pub fn target(&self) -> &ExportPopRecordTarget {
        &self.target
    }

    pub fn dry_run(&self) -> bool {
        self.dry_run
    }

    pub fn timeout_millis(&self) -> u64 {
        self.timeout_millis
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportPopRecordTargetResult {
    pub broker_name: String,
    pub broker_addr: String,
    pub dry_run: bool,
    pub exported: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportPopRecordResult {
    pub targets: Vec<ExportPopRecordTargetResult>,
}

#[derive(Debug)]
pub enum ExportMetadataResult {
    BrokerTopic {
        wrapper: TopicConfigSerializeWrapper,
    },
    BrokerSubscriptionGroup {
        wrapper: SubscriptionGroupWrapper,
    },
    Cluster {
        scope: ExportMetadataScope,
        topic_config_table: HashMap<CheetahString, TopicConfig>,
        subscription_group_table: HashMap<CheetahString, SubscriptionGroupConfig>,
        export_time_millis: u64,
    },
}

pub struct ExportService;

impl ExportService {
    pub fn export_metadata_in_rocksdb_by_request(
        request: &ExportMetadataInRocksDbRequest,
    ) -> RocketMQResult<ExportMetadataInRocksDbResult> {
        if request.path().as_os_str().is_empty() || !request.path().exists() {
            return Ok(ExportMetadataInRocksDbResult::InvalidPath);
        }

        let Some(config_type) = request.normalized_config_type() else {
            return Ok(ExportMetadataInRocksDbResult::InvalidConfigType {
                config_type: request.config_type().to_string(),
            });
        };

        #[cfg(not(feature = "rocksdb-export"))]
        {
            let _ = config_type;
            return Err(RocketMQError::Internal(
                "ExportService: RocksDB metadata export requires the rocksdb-export feature".to_string(),
            ));
        }

        #[cfg(feature = "rocksdb-export")]
        {
            let full_path = request.full_path();
            let mut opts = Options::default();
            opts.create_if_missing(false);

            let db = DB::open_for_read_only(&opts, &full_path, false).map_err(|error| {
                RocketMQError::Internal(format!(
                    "ExportService: failed to open RocksDB path {}: {}",
                    full_path.display(),
                    error
                ))
            })?;

            let entries = iterate_rocksdb_metadata(&db)?;
            drop(db);

            Ok(ExportMetadataInRocksDbResult::Data {
                config_type,
                json_enable: request.json_enable(),
                entries,
            })
        }
    }

    pub async fn export_configs_by_request_with_rpc_hook(
        request: ExportConfigsRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<ExportConfigsResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::export_configs_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn export_configs_with_admin(
        admin: &DefaultMQAdminExt,
        request: &ExportConfigsRequest,
    ) -> RocketMQResult<ExportConfigsResult> {
        let name_server_address_list = admin.get_name_server_address_list().await;
        let name_servers = name_server_address_list
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();

        let cluster_info = admin.examine_broker_cluster_info().await?;
        let master_and_slave_map =
            BrokerAddressResolver::fetch_master_and_slave_distinguish(&cluster_info, request.cluster_name().as_str())?;

        let mut master_broker_size = 0;
        let mut slave_broker_size = 0;
        let mut broker_configs = HashMap::new();

        for (master_addr, slave_addrs) in &master_and_slave_map {
            if master_addr.as_str() == BrokerAddressResolver::NO_MASTER_PLACEHOLDER {
                slave_broker_size += slave_addrs.len() as i64;
                continue;
            }

            let master_properties = admin.get_broker_config(master_addr.clone()).await.map_err(|error| {
                RocketMQError::Internal(format!(
                    "ExportService: Failed to get broker config for {}: {}",
                    master_addr, error
                ))
            })?;

            master_broker_size += 1;
            slave_broker_size += slave_addrs.len() as i64;

            let broker_name = master_properties
                .get(&CheetahString::from_static_str("brokerName"))
                .map(ToString::to_string)
                .unwrap_or_else(|| master_addr.to_string());
            broker_configs.insert(broker_name, filter_export_broker_properties(&master_properties));
        }

        Ok(ExportConfigsResult {
            name_servers,
            broker_configs,
            master_broker_size,
            slave_broker_size,
        })
    }

    pub async fn export_metadata_by_request_with_rpc_hook(
        request: ExportMetadataRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<ExportMetadataResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::export_metadata_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn export_metadata_with_admin(
        admin: &DefaultMQAdminExt,
        request: &ExportMetadataRequest,
    ) -> RocketMQResult<ExportMetadataResult> {
        match request.target() {
            ExportMetadataTarget::Broker(broker_addr) => match request.scope() {
                ExportMetadataScope::Topic => {
                    let wrapper = admin
                        .get_user_topic_config(broker_addr.clone(), request.special_topic(), request.timeout_millis())
                        .await?;
                    Ok(ExportMetadataResult::BrokerTopic { wrapper })
                }
                ExportMetadataScope::SubscriptionGroup => {
                    let wrapper = admin
                        .get_user_subscription_group(broker_addr.clone(), request.timeout_millis())
                        .await?;
                    Ok(ExportMetadataResult::BrokerSubscriptionGroup { wrapper })
                }
                ExportMetadataScope::All => Err(ToolsError::validation_error(
                    "scope",
                    "broker target requires topic or subscriptionGroup scope",
                )
                .into()),
            },
            ExportMetadataTarget::Cluster(cluster_name) => {
                let cluster_info = admin.examine_broker_cluster_info().await?;
                let master_set =
                    BrokerAddressResolver::fetch_master_addr_by_cluster_name(&cluster_info, cluster_name.as_str())?;

                let mut topic_config_table = HashMap::new();
                let mut subscription_group_table = HashMap::new();

                for addr in &master_set {
                    let topic_config_wrapper = admin
                        .get_user_topic_config(addr.clone(), request.special_topic(), request.timeout_millis())
                        .await?;
                    let subscription_group_wrapper = admin
                        .get_user_subscription_group(addr.clone(), request.timeout_millis())
                        .await?;

                    if let Some(topic_table) = topic_config_wrapper.topic_config_table() {
                        merge_topic_configs(&mut topic_config_table, topic_table);
                    }
                    merge_subscription_groups(&mut subscription_group_table, &subscription_group_wrapper);
                }

                Ok(ExportMetadataResult::Cluster {
                    scope: request.scope(),
                    topic_config_table,
                    subscription_group_table,
                    export_time_millis: current_millis(),
                })
            }
        }
    }

    pub async fn export_pop_records_by_request_with_rpc_hook(
        request: ExportPopRecordRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<ExportPopRecordResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::export_pop_records_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn export_pop_records_with_admin(
        admin: &DefaultMQAdminExt,
        request: &ExportPopRecordRequest,
    ) -> RocketMQResult<ExportPopRecordResult> {
        let targets = match request.target() {
            ExportPopRecordTarget::Broker(broker_addr) => {
                vec![Self::resolve_export_pop_record_broker_target(admin, broker_addr).await]
            }
            ExportPopRecordTarget::Cluster(cluster_name) => {
                let cluster_info = admin.examine_broker_cluster_info().await?;
                resolve_export_pop_record_targets_from_cluster_info(&cluster_info, cluster_name.as_str())
            }
        };

        let mut results = Vec::with_capacity(targets.len());
        for target in targets {
            let (exported, error) = if request.dry_run() {
                (false, None)
            } else {
                match admin
                    .export_pop_records(
                        CheetahString::from(target.broker_addr.as_str()),
                        request.timeout_millis(),
                    )
                    .await
                {
                    Ok(()) => (true, None),
                    Err(error) => (false, Some(error.to_string())),
                }
            };

            results.push(ExportPopRecordTargetResult {
                broker_name: target.broker_name,
                broker_addr: target.broker_addr,
                dry_run: request.dry_run(),
                exported,
                error,
            });
        }

        Ok(ExportPopRecordResult { targets: results })
    }

    async fn resolve_export_pop_record_broker_target(
        admin: &DefaultMQAdminExt,
        broker_addr: &CheetahString,
    ) -> ExportPopRecordBrokerTarget {
        let broker_name = admin
            .get_broker_config(broker_addr.clone())
            .await
            .ok()
            .and_then(|properties| {
                properties
                    .get(&CheetahString::from_static_str("brokerName"))
                    .map(ToString::to_string)
            })
            .unwrap_or_default();

        ExportPopRecordBrokerTarget {
            broker_name,
            broker_addr: broker_addr.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExportPopRecordBrokerTarget {
    broker_name: String,
    broker_addr: String,
}

fn resolve_export_pop_record_targets_from_cluster_info(
    cluster_info: &ClusterInfo,
    cluster_name: &str,
) -> Vec<ExportPopRecordBrokerTarget> {
    let Some(cluster_addr_table) = cluster_info.cluster_addr_table.as_ref() else {
        return Vec::new();
    };
    let Some(broker_name_set) = cluster_addr_table.get(cluster_name) else {
        return Vec::new();
    };
    let Some(broker_addr_table) = cluster_info.broker_addr_table.as_ref() else {
        return Vec::new();
    };

    let mut targets = Vec::new();
    for broker_name in broker_name_set {
        let Some(broker_data) = broker_addr_table.get(broker_name) else {
            continue;
        };

        for broker_addr in broker_data.broker_addrs().values() {
            targets.push(ExportPopRecordBrokerTarget {
                broker_name: broker_name.to_string(),
                broker_addr: broker_addr.to_string(),
            });
        }
    }

    targets.sort_by(|left, right| {
        left.broker_name
            .cmp(&right.broker_name)
            .then_with(|| left.broker_addr.cmp(&right.broker_addr))
    });
    targets.dedup();
    targets
}

#[cfg(feature = "rocksdb-export")]
fn iterate_rocksdb_metadata(db: &DB) -> RocketMQResult<Vec<ExportMetadataInRocksDbEntry>> {
    let mut entries = Vec::new();
    let iter = db.iterator(rocksdb::IteratorMode::Start);
    for item in iter {
        let (key, value) =
            item.map_err(|error| RocketMQError::Internal(format!("ExportService: RocksDB iterator error: {error}")))?;
        entries.push(ExportMetadataInRocksDbEntry {
            key: String::from_utf8_lossy(&key).to_string(),
            value: String::from_utf8_lossy(&value).to_string(),
        });
    }
    Ok(entries)
}

pub fn filter_export_broker_properties(properties: &HashMap<CheetahString, CheetahString>) -> HashMap<String, String> {
    let mut filtered = HashMap::new();
    for key in EXPORT_BROKER_PROPERTY_KEYS {
        if let Some(value) = properties.get(&CheetahString::from(*key)) {
            filtered.insert((*key).to_string(), value.to_string());
        }
    }
    filtered
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

fn merge_topic_configs(target: &mut HashMap<CheetahString, TopicConfig>, source: &HashMap<CheetahString, TopicConfig>) {
    for (key, value) in source {
        if let Some(existing) = target.get(key) {
            let mut merged = value.clone();
            merged.write_queue_nums = existing.write_queue_nums + value.write_queue_nums;
            merged.read_queue_nums = existing.read_queue_nums + value.read_queue_nums;
            target.insert(key.clone(), merged);
        } else {
            target.insert(key.clone(), value.clone());
        }
    }
}

fn merge_subscription_groups(
    target: &mut HashMap<CheetahString, SubscriptionGroupConfig>,
    source: &SubscriptionGroupWrapper,
) {
    for entry in source.get_subscription_group_table().iter() {
        let key = entry.key().clone();
        let value = entry.value();
        if let Some(existing) = target.get(&key) {
            let mut merged = (**value).clone();
            merged.set_retry_queue_nums(existing.retry_queue_nums() + value.retry_queue_nums());
            target.insert(key, merged);
        } else {
            target.insert(key, (**value).clone());
        }
    }
}

fn admin_builder_with_rpc_hook(builder: AdminBuilder, rpc_hook: Option<Arc<dyn RPCHook>>) -> AdminBuilder {
    match rpc_hook {
        Some(hook) => builder.rpc_hook(hook),
        None => builder,
    }
}
