//! Export admin service models and operations.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mq_version::RocketMqVersion;
use rocketmq_common::common::mq_version::CURRENT_VERSION;
use rocketmq_common::common::stats::Stats;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_remoting::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_remoting::protocol::body::kv_table::KVTable;
use rocketmq_remoting::protocol::body::subscription_group_wrapper::SubscriptionGroupWrapper;
use rocketmq_remoting::protocol::body::topic_info_wrapper::TopicConfigSerializeWrapper;
use rocketmq_remoting::protocol::subscription::broker_stats_data::BrokerStatsData;
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
const DEFAULT_EXPORT_METRICS_TIMEOUT_MILLIS: u64 = 10000;
const DEFAULT_EXPORT_POP_RECORD_TIMEOUT_MILLIS: u64 = 30000;
const TOPICS_JSON_CONFIG: &str = "topics";
const SUBSCRIPTION_GROUP_JSON_CONFIG: &str = "subscriptionGroups";
const CONSUMER_OFFSETS_JSON_CONFIG: &str = "consumerOffsets";

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
pub struct ExportMetricsRequest {
    cluster_name: CheetahString,
    timeout_millis: u64,
    namesrv_addr: Option<String>,
}

impl ExportMetricsRequest {
    pub fn try_new(cluster_name: impl Into<String>) -> RocketMQResult<Self> {
        Ok(Self {
            cluster_name: trim_required_cheetah("clusterName", cluster_name)?,
            timeout_millis: DEFAULT_EXPORT_METRICS_TIMEOUT_MILLIS,
            namesrv_addr: None,
        })
    }

    pub fn with_timeout_millis(mut self, timeout_millis: u64) -> Self {
        self.timeout_millis = timeout_millis;
        self
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn cluster_name(&self) -> &CheetahString {
        &self.cluster_name
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

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportMetricsResult {
    pub evaluate_report: BTreeMap<String, ExportMetricsBrokerReport>,
    pub total_data: ExportMetricsTotals,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportMetricsBrokerReport {
    pub runtime_env: ExportMetricsRuntimeEnv,
    pub runtime_quota: ExportMetricsRuntimeQuota,
    pub runtime_version: ExportMetricsRuntimeVersion,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportMetricsRuntimeEnv {
    pub cpu_num: Option<String>,
    #[serde(rename = "totalMemKBytes")]
    pub total_mem_kbytes: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportMetricsRuntimeQuota {
    pub disk_ratio: ExportMetricsDiskRatio,
    pub tps: ExportMetricsTps,
    pub one_day_num: ExportMetricsOneDayNum,
    pub message_average_size: Option<String>,
    pub topic_size: usize,
    pub group_size: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportMetricsDiskRatio {
    pub commit_log_disk_ratio: Option<String>,
    pub consume_queue_disk_ratio: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportMetricsTps {
    pub normal_in_tps: f64,
    pub normal_out_tps: f64,
    pub trans_in_tps: f64,
    pub schedule_in_tps: f64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportMetricsOneDayNum {
    pub normal_one_day_in_num: i64,
    pub normal_one_day_out_num: i64,
    pub trans_one_day_in_num: u64,
    pub schedule_one_day_in_num: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportMetricsRuntimeVersion {
    pub rocketmq_version: String,
    pub client_info: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportMetricsTotals {
    pub total_tps: ExportMetricsTotalTps,
    pub total_one_day_num: ExportMetricsOneDayNum,
}

impl ExportMetricsTotals {
    pub fn accumulate(&mut self, quota: &ExportMetricsRuntimeQuota) {
        self.total_tps.total_normal_in_tps += quota.tps.normal_in_tps;
        self.total_tps.total_normal_out_tps += quota.tps.normal_out_tps;
        self.total_tps.total_trans_in_tps += quota.tps.trans_in_tps;
        self.total_tps.total_schedule_in_tps += quota.tps.schedule_in_tps;
        self.total_one_day_num.normal_one_day_in_num += quota.one_day_num.normal_one_day_in_num;
        self.total_one_day_num.normal_one_day_out_num += quota.one_day_num.normal_one_day_out_num;
        self.total_one_day_num.trans_one_day_in_num += quota.one_day_num.trans_one_day_in_num;
        self.total_one_day_num.schedule_one_day_in_num += quota.one_day_num.schedule_one_day_in_num;
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportMetricsTotalTps {
    pub total_normal_in_tps: f64,
    pub total_normal_out_tps: f64,
    pub total_trans_in_tps: f64,
    pub total_schedule_in_tps: f64,
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
    ConsumerOffsets,
}

impl ExportMetadataInRocksDbConfigType {
    pub fn from_config_type(config_type: &str) -> Option<Self> {
        if config_type.eq_ignore_ascii_case(TOPICS_JSON_CONFIG) {
            Some(Self::Topics)
        } else if config_type.eq_ignore_ascii_case(SUBSCRIPTION_GROUP_JSON_CONFIG) {
            Some(Self::SubscriptionGroups)
        } else if config_type.eq_ignore_ascii_case(CONSUMER_OFFSETS_JSON_CONFIG) {
            Some(Self::ConsumerOffsets)
        } else {
            None
        }
    }

    pub fn table_key(self) -> &'static str {
        match self {
            Self::Topics => "topicConfigTable",
            Self::SubscriptionGroups => "subscriptionGroupTable",
            Self::ConsumerOffsets => "offsetTable",
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

#[derive(Debug, Serialize)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExportFileOverwritePolicy {
    CreateNew,
    Overwrite,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportFileWriteRequest {
    output_path: PathBuf,
    overwrite_policy: ExportFileOverwritePolicy,
}

impl ExportFileWriteRequest {
    pub fn try_new(
        output_path: impl Into<String>,
        overwrite_policy: ExportFileOverwritePolicy,
    ) -> RocketMQResult<Self> {
        let output_path = output_path.into();
        let output_path = output_path.trim();
        if output_path.is_empty() {
            return Err(ToolsError::validation_error("outputPath", "outputPath must not be empty").into());
        }

        Ok(Self {
            output_path: PathBuf::from(output_path),
            overwrite_policy,
        })
    }

    pub fn output_path(&self) -> &Path {
        &self.output_path
    }

    pub fn overwrite_policy(&self) -> ExportFileOverwritePolicy {
        self.overwrite_policy
    }

    pub fn overwrite(&self) -> bool {
        matches!(self.overwrite_policy, ExportFileOverwritePolicy::Overwrite)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportFileWriteResult {
    output_path: PathBuf,
    bytes_written: u64,
    overwritten: bool,
}

impl ExportFileWriteResult {
    pub fn output_path(&self) -> &Path {
        &self.output_path
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    pub fn overwritten(&self) -> bool {
        self.overwritten
    }
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

            let entries = Self::convert_rocksdb_metadata_entries(config_type, iterate_rocksdb_metadata(&db)?)?;
            drop(db);

            Ok(ExportMetadataInRocksDbResult::Data {
                config_type,
                json_enable: request.json_enable(),
                entries,
            })
        }
    }

    pub fn convert_rocksdb_metadata_entries(
        config_type: ExportMetadataInRocksDbConfigType,
        entries: Vec<ExportMetadataInRocksDbEntry>,
    ) -> RocketMQResult<Vec<ExportMetadataInRocksDbEntry>> {
        match config_type {
            ExportMetadataInRocksDbConfigType::Topics | ExportMetadataInRocksDbConfigType::SubscriptionGroups => {
                Ok(entries)
            }
            ExportMetadataInRocksDbConfigType::ConsumerOffsets => {
                entries.into_iter().map(convert_consumer_offset_entry).collect()
            }
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

    pub async fn export_metrics_by_request_with_rpc_hook(
        request: ExportMetricsRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<ExportMetricsResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::export_metrics_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn export_metrics_with_admin(
        admin: &DefaultMQAdminExt,
        request: &ExportMetricsRequest,
    ) -> RocketMQResult<ExportMetricsResult> {
        let cluster_info = admin.examine_broker_cluster_info().await?;
        let broker_names = resolve_export_metrics_broker_names(&cluster_info, request.cluster_name().as_str())?;
        let broker_addr_table = cluster_info
            .broker_addr_table
            .as_ref()
            .ok_or_else(|| RocketMQError::Internal("ExportService: broker address table is empty".to_string()))?;

        let mut result = ExportMetricsResult::default();
        for broker_name in broker_names {
            let Some(broker_data) = broker_addr_table.get(&broker_name) else {
                continue;
            };
            let Some(master_addr) = broker_data.broker_addrs().get(&mix_all::MASTER_ID) else {
                continue;
            };

            let runtime_stats = admin.fetch_broker_runtime_stats(master_addr.clone()).await?;
            let broker_config = admin.get_broker_config(master_addr.clone()).await?;
            let subscription_group_wrapper = admin
                .get_user_subscription_group(master_addr.clone(), request.timeout_millis())
                .await?;
            let topic_config_wrapper = admin
                .get_user_topic_config(master_addr.clone(), false, request.timeout_millis())
                .await?;
            let trans_stats_data = admin
                .view_broker_stats_data(
                    master_addr.clone(),
                    CheetahString::from_static_str(Stats::TOPIC_PUT_NUMS),
                    CheetahString::from_static_str(TopicValidator::RMQ_SYS_TRANS_HALF_TOPIC),
                )
                .await
                .ok();
            let schedule_stats_data = admin
                .view_broker_stats_data(
                    master_addr.clone(),
                    CheetahString::from_static_str(Stats::TOPIC_PUT_NUMS),
                    CheetahString::from_static_str(TopicValidator::RMQ_SYS_SCHEDULE_TOPIC),
                )
                .await
                .ok();
            let client_info = collect_export_metrics_client_info(admin, &subscription_group_wrapper).await;
            let topic_size = topic_config_wrapper
                .topic_config_table()
                .map(HashMap::len)
                .unwrap_or_default();
            let group_size = subscription_group_wrapper.get_subscription_group_table().len();
            let report = Self::build_export_metrics_broker_report(
                &runtime_stats,
                &broker_config,
                topic_size,
                group_size,
                trans_stats_data.as_ref(),
                schedule_stats_data.as_ref(),
                client_info,
            );

            result.total_data.accumulate(&report.runtime_quota);
            result.evaluate_report.insert(broker_name.to_string(), report);
        }

        Ok(result)
    }

    pub fn build_export_metrics_broker_report(
        runtime_stats: &KVTable,
        broker_config: &HashMap<CheetahString, CheetahString>,
        topic_size: usize,
        group_size: usize,
        trans_stats_data: Option<&BrokerStatsData>,
        schedule_stats_data: Option<&BrokerStatsData>,
        client_info: Vec<String>,
    ) -> ExportMetricsBrokerReport {
        let trans_one_day_in_num = trans_stats_data
            .map(crate::core::stats::StatsService::compute_24_hour_sum)
            .unwrap_or_default();
        let schedule_one_day_in_num = schedule_stats_data
            .map(crate::core::stats::StatsService::compute_24_hour_sum)
            .unwrap_or_default();

        ExportMetricsBrokerReport {
            runtime_env: ExportMetricsRuntimeEnv {
                cpu_num: map_value(broker_config, "clientCallbackExecutorThreads"),
                total_mem_kbytes: kv_value(runtime_stats, "totalMemKBytes"),
            },
            runtime_quota: ExportMetricsRuntimeQuota {
                disk_ratio: ExportMetricsDiskRatio {
                    commit_log_disk_ratio: kv_value(runtime_stats, "commitLogDiskRatio"),
                    consume_queue_disk_ratio: kv_value(runtime_stats, "consumeQueueDiskRatio"),
                },
                tps: ExportMetricsTps {
                    normal_in_tps: parse_leading_f64(kv_value(runtime_stats, "putTps").as_deref()),
                    normal_out_tps: parse_leading_f64(kv_value(runtime_stats, "getTransferredTps").as_deref()),
                    trans_in_tps: trans_stats_data
                        .map(|stats| stats.get_stats_minute().get_tps())
                        .unwrap_or_default(),
                    schedule_in_tps: schedule_stats_data
                        .map(|stats| stats.get_stats_minute().get_tps())
                        .unwrap_or_default(),
                },
                one_day_num: ExportMetricsOneDayNum {
                    normal_one_day_in_num: kv_i64(runtime_stats, "msgPutTotalTodayMorning")
                        - kv_i64(runtime_stats, "msgPutTotalYesterdayMorning"),
                    normal_one_day_out_num: kv_i64(runtime_stats, "msgGetTotalTodayMorning")
                        - kv_i64(runtime_stats, "msgGetTotalYesterdayMorning"),
                    trans_one_day_in_num,
                    schedule_one_day_in_num,
                },
                message_average_size: kv_value(runtime_stats, "putMessageAverageSize"),
                topic_size,
                group_size,
            },
            runtime_version: ExportMetricsRuntimeVersion {
                rocketmq_version: CURRENT_VERSION.name().to_string(),
                client_info: normalize_client_info(client_info),
            },
        }
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

    pub fn write_json_export_file<T>(
        request: &ExportFileWriteRequest,
        value: &T,
    ) -> RocketMQResult<ExportFileWriteResult>
    where
        T: Serialize + ?Sized,
    {
        let output_path = request.output_path();
        if output_path.is_dir() {
            return Err(RocketMQError::Internal(format!(
                "ExportService: output path {} is a directory",
                output_path.display()
            )));
        }

        if let Some(parent) = output_path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
            if !parent.exists() {
                return Err(RocketMQError::Internal(format!(
                    "ExportService: output directory {} does not exist",
                    parent.display()
                )));
            }
        }

        let overwritten = output_path.exists();
        if overwritten && !request.overwrite() {
            return Err(RocketMQError::Internal(format!(
                "ExportService: output file {} already exists; enable overwrite to replace it",
                output_path.display()
            )));
        }

        let bytes = serde_json::to_vec_pretty(value)
            .map_err(|error| RocketMQError::Internal(format!("ExportService: failed to serialize export: {error}")))?;
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(request.overwrite())
            .create_new(!request.overwrite())
            .open(output_path)
            .map_err(|error| {
                RocketMQError::Internal(format!(
                    "ExportService: failed to open output file {}: {}",
                    output_path.display(),
                    error
                ))
            })?;
        file.write_all(&bytes).map_err(|error| {
            RocketMQError::Internal(format!(
                "ExportService: failed to write output file {}: {}",
                output_path.display(),
                error
            ))
        })?;

        Ok(ExportFileWriteResult {
            output_path: output_path.to_path_buf(),
            bytes_written: bytes.len() as u64,
            overwritten,
        })
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

fn resolve_export_metrics_broker_names(
    cluster_info: &ClusterInfo,
    cluster_name: &str,
) -> RocketMQResult<Vec<CheetahString>> {
    let broker_names = cluster_info
        .cluster_addr_table
        .as_ref()
        .and_then(|cluster_addr_table| cluster_addr_table.get(cluster_name))
        .ok_or_else(|| {
            RocketMQError::Internal(format!(
                "ExportService: cluster {} does not exist or has no brokers",
                cluster_name
            ))
        })?;
    let mut broker_names = broker_names.iter().cloned().collect::<Vec<_>>();
    broker_names.sort();
    Ok(broker_names)
}

async fn collect_export_metrics_client_info(
    admin: &DefaultMQAdminExt,
    subscription_group_wrapper: &SubscriptionGroupWrapper,
) -> Vec<String> {
    let mut client_info = BTreeSet::new();
    for entry in subscription_group_wrapper.get_subscription_group_table().iter() {
        let group_name = entry.value().group_name().clone();
        let Ok(connection) = admin.examine_consumer_connection_info(group_name, None).await else {
            continue;
        };
        for connection in connection.get_connection_set() {
            let version = if connection.get_version() < 0 {
                RocketMqVersion::from_ordinal(0)
            } else {
                RocketMqVersion::from_ordinal(connection.get_version() as u32)
            };
            client_info.insert(format!("{}%{}", connection.get_language(), version.name()));
        }
    }
    client_info.into_iter().collect()
}

fn kv_value(runtime_stats: &KVTable, key: &str) -> Option<String> {
    runtime_stats
        .table
        .get(&CheetahString::from(key))
        .map(ToString::to_string)
}

fn map_value(properties: &HashMap<CheetahString, CheetahString>, key: &str) -> Option<String> {
    properties.get(&CheetahString::from(key)).map(ToString::to_string)
}

fn kv_i64(runtime_stats: &KVTable, key: &str) -> i64 {
    kv_value(runtime_stats, key)
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or_default()
}

fn parse_leading_f64(value: Option<&str>) -> f64 {
    value
        .and_then(|value| value.split_whitespace().next())
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or_default()
}

fn normalize_client_info(client_info: Vec<String>) -> Vec<String> {
    let mut client_info = client_info
        .into_iter()
        .filter(|value| !value.trim().is_empty())
        .collect::<Vec<_>>();
    client_info.sort();
    client_info.dedup();
    client_info
}

fn convert_consumer_offset_entry(entry: ExportMetadataInRocksDbEntry) -> RocketMQResult<ExportMetadataInRocksDbEntry> {
    let wrapper = serde_json::from_str::<serde_json::Value>(&entry.value).map_err(|error| {
        RocketMQError::Internal(format!(
            "ExportService: failed to parse consumerOffsets entry {}: {}",
            entry.key, error
        ))
    })?;
    let offset_table = wrapper
        .get("offsetTable")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let value = serde_json::to_string(&offset_table).map_err(|error| {
        RocketMQError::Internal(format!(
            "ExportService: failed to serialize consumerOffsets entry {}: {}",
            entry.key, error
        ))
    })?;

    Ok(ExportMetadataInRocksDbEntry { key: entry.key, value })
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
