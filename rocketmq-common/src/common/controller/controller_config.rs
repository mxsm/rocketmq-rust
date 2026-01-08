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

use std::env;
use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;

use serde::Deserialize;
use serde::Serialize;

use crate::common::metrics::MetricsExporterType;
use crate::common::mix_all::ROCKETMQ_HOME_ENV;
use crate::utils::env_utils::EnvUtils;

/// Controller type constant
pub const RAFT_CONTROLLER: &str = "Raft";

/// Controller configuration
///
/// This configuration defines the behavior and parameters of the RocketMQ controller.
/// It manages broker election, state synchronization, and cluster coordination.

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftPeer {
    /// Node ID
    pub id: u64,

    /// Peer address
    pub addr: SocketAddr,
}

/// Storage backend type
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum StorageBackendType {
    /// RocksDB storage
    RocksDB,

    /// File-based storage
    File,

    /// In-memory storage (for testing)
    Memory,
}

impl fmt::Display for StorageBackendType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RocksDB => write!(f, "rocks_db"),
            Self::File => write!(f, "file"),
            Self::Memory => write!(f, "memory"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ControllerConfig {
    // --- shared Controller fields (existing) ---
    /// RocketMQ home directory
    ///
    /// Defaults to ROCKETMQ_HOME environment variable or current directory
    pub rocketmq_home: String,

    /// Configuration file storage path
    ///
    /// Defaults to $HOME/controller/controller.properties
    pub config_store_path: PathBuf,

    /// Controller type: "Raft"
    ///
    /// Default: Raft
    /// Note: In Rust implementation, "Raft" uses tikv/raft or openraft
    pub controller_type: String,

    /// Interval of periodic scanning for non-active broker (milliseconds)
    ///
    /// Default: 5000 (5 seconds)
    pub scan_not_active_broker_interval: u64,

    /// Number of threads to handle broker or operation requests (like REGISTER_BROKER)
    ///
    /// Default: 16
    pub controller_thread_pool_nums: usize,

    /// Capacity of queue to hold client requests
    ///
    /// Default: 50000
    pub controller_request_thread_pool_queue_capacity: usize,

    /// Mapped file size for controller storage
    ///
    /// Default: 1GB (1024 * 1024 * 1024)
    pub mapped_file_size: usize,

    /// Controller storage path
    ///
    /// Default: empty string (will be set based on rocketmq_home)
    pub controller_store_path: String,

    /// Max retry count for electing master when failed because of network or system error
    ///
    /// Default: 3
    pub elect_master_max_retry_count: u32,

    /// Whether the controller can elect a master which is not in the syncStateSet
    ///
    /// Default: false
    pub enable_elect_unclean_master: bool,

    /// Whether process read event
    ///
    /// Default: false
    pub is_process_read_event: bool,

    /// Whether notify broker when its role changed
    ///
    /// Default: true
    pub notify_broker_role_changed: bool,

    /// Interval of periodic scanning for non-active master in each broker-set (milliseconds)
    ///
    /// Default: 5000 (5 seconds)
    pub scan_inactive_master_interval: u64,

    /// Metrics exporter type
    ///
    /// Default: Disable
    pub metrics_exporter_type: MetricsExporterType,

    /// Metrics gRPC exporter target address
    pub metrics_grpc_exporter_target: String,

    /// Metrics gRPC exporter header
    pub metrics_grpc_exporter_header: String,

    /// Metric gRPC exporter timeout (milliseconds)
    ///
    /// Default: 3000 (3 seconds)
    pub metric_grpc_exporter_time_out_in_mills: u64,

    /// Metric gRPC exporter interval (milliseconds)
    ///
    /// Default: 60000 (60 seconds)
    pub metric_grpc_exporter_interval_in_mills: u64,

    /// Metric logging exporter interval (milliseconds)
    ///
    /// Default: 10000 (10 seconds)
    pub metric_logging_exporter_interval_in_mills: u64,

    /// Metrics Prometheus exporter port
    ///
    /// Default: 5557
    pub metrics_prom_exporter_port: u16,

    /// Metrics Prometheus exporter host
    pub metrics_prom_exporter_host: String,

    /// Metrics label (CSV format: Key:Value,Key:Value)
    ///
    /// Example: "instance_id:xxx,uid:xxx"
    pub metrics_label: String,

    /// Whether metrics are in delta mode
    ///
    /// Default: false
    pub metrics_in_delta: bool,

    /// Configuration blacklist (configs that cannot be updated via command)
    ///
    /// These configs can only be changed by restarting the process.
    /// Default: "configBlackList;configStorePath"
    pub config_black_list: String,

    // --- node-specific fields (added for controller usage) ---
    /// Node id for this controller process (optional in shared config)
    pub node_id: u64,

    /// Listen address for RPC/raft on this node
    pub listen_addr: SocketAddr,

    /// Peer list used for raft bootstrapping
    pub raft_peers: Vec<RaftPeer>,

    /// Election timeout (ms) used by Raft
    pub election_timeout_ms: u64,

    /// Heartbeat interval (ms) used by Raft
    pub heartbeat_interval_ms: u64,

    /// Local storage path for controller artifacts
    pub storage_path: String,

    /// Storage backend to use for local controller artifacts
    pub storage_backend: StorageBackendType,

    /// Whether the controller may elect an unclean master on this node
    pub enable_elect_unclean_master_local: bool,
}

impl Default for ControllerConfig {
    fn default() -> Self {
        // Get ROCKETMQ_HOME from environment variable or use current directory
        let rocketmq_home = env::var(ROCKETMQ_HOME_ENV)
            .ok()
            .or_else(|| env::var("rocketmq.home.dir").ok())
            .unwrap_or_else(EnvUtils::get_rocketmq_home);

        // Get user home directory
        let user_home = dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .to_string_lossy()
            .to_string();

        Self {
            rocketmq_home,
            config_store_path: PathBuf::from(&user_home)
                .join("controller")
                .join("controller.properties"),
            controller_type: RAFT_CONTROLLER.to_string(),
            scan_not_active_broker_interval: 5 * 1000,
            controller_thread_pool_nums: 16,
            controller_request_thread_pool_queue_capacity: 50000,
            mapped_file_size: 1024 * 1024 * 1024,
            controller_store_path: String::new(),
            elect_master_max_retry_count: 3,
            enable_elect_unclean_master: false,
            is_process_read_event: false,
            notify_broker_role_changed: true,
            scan_inactive_master_interval: 5 * 1000,
            metrics_exporter_type: MetricsExporterType::Disable,
            metrics_grpc_exporter_target: String::new(),
            metrics_grpc_exporter_header: String::new(),
            metric_grpc_exporter_time_out_in_mills: 3 * 1000,
            metric_grpc_exporter_interval_in_mills: 60 * 1000,
            metric_logging_exporter_interval_in_mills: 10 * 1000,
            metrics_prom_exporter_port: 5557,
            metrics_prom_exporter_host: String::new(),
            metrics_label: String::new(),
            metrics_in_delta: false,
            config_black_list: "configBlackList;configStorePath".to_string(),
            node_id: 1,
            listen_addr: "127.0.0.1:60109".parse().unwrap(),
            raft_peers: Vec::new(),
            election_timeout_ms: 1000,
            heartbeat_interval_ms: 300,
            storage_path: String::new(),
            storage_backend: StorageBackendType::Memory,
            enable_elect_unclean_master_local: false,
        }
    }
}

impl ControllerConfig {
    /// Create a new ControllerConfig with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Set RocketMQ home directory
    pub fn with_rocketmq_home(mut self, path: impl Into<String>) -> Self {
        self.rocketmq_home = path.into();
        self
    }

    /// Set configuration store path
    pub fn with_config_store_path(mut self, path: PathBuf) -> Self {
        self.config_store_path = path;
        self
    }

    /// Set controller type
    pub fn with_controller_type(mut self, controller_type: impl Into<String>) -> Self {
        self.controller_type = controller_type.into();
        self
    }

    /// Set scan not active broker interval
    pub fn with_scan_not_active_broker_interval(mut self, interval_ms: u64) -> Self {
        self.scan_not_active_broker_interval = interval_ms;
        self
    }

    /// Set controller thread pool size
    pub fn with_controller_thread_pool_nums(mut self, nums: usize) -> Self {
        self.controller_thread_pool_nums = nums;
        self
    }

    /// Set controller request queue capacity
    pub fn with_controller_request_queue_capacity(mut self, capacity: usize) -> Self {
        self.controller_request_thread_pool_queue_capacity = capacity;
        self
    }

    /// Set controller storage path
    pub fn with_controller_store_path(mut self, path: impl Into<String>) -> Self {
        self.controller_store_path = path.into();
        self
    }

    /// Set mapped file size
    pub fn with_mapped_file_size(mut self, size: usize) -> Self {
        self.mapped_file_size = size;
        self
    }

    /// Set elect master max retry count
    pub fn with_elect_master_max_retry_count(mut self, count: u32) -> Self {
        self.elect_master_max_retry_count = count;
        self
    }

    /// Enable or disable elect unclean master
    pub fn with_enable_elect_unclean_master(mut self, enable: bool) -> Self {
        self.enable_elect_unclean_master = enable;
        self
    }

    /// Enable or disable process read event
    pub fn with_process_read_event(mut self, enable: bool) -> Self {
        self.is_process_read_event = enable;
        self
    }

    /// Enable or disable notify broker role changed
    pub fn with_notify_broker_role_changed(mut self, enable: bool) -> Self {
        self.notify_broker_role_changed = enable;
        self
    }

    /// Set scan inactive master interval
    pub fn with_scan_inactive_master_interval(mut self, interval_ms: u64) -> Self {
        self.scan_inactive_master_interval = interval_ms;
        self
    }

    /// Set metrics exporter type
    pub fn with_metrics_exporter_type(mut self, exporter_type: MetricsExporterType) -> Self {
        self.metrics_exporter_type = exporter_type;
        self
    }

    /// Set metrics gRPC exporter configuration
    pub fn with_metrics_grpc_exporter(mut self, target: impl Into<String>, header: impl Into<String>) -> Self {
        self.metrics_grpc_exporter_target = target.into();
        self.metrics_grpc_exporter_header = header.into();
        self
    }

    /// Set metrics Prometheus exporter configuration
    pub fn with_metrics_prom_exporter(mut self, host: impl Into<String>, port: u16) -> Self {
        self.metrics_prom_exporter_host = host.into();
        self.metrics_prom_exporter_port = port;
        self
    }

    /// Set metrics label
    pub fn with_metrics_label(mut self, label: impl Into<String>) -> Self {
        self.metrics_label = label.into();
        self
    }

    /// Set node id and listen address for node-specific usage
    pub fn with_node_info(mut self, node_id: u64, listen_addr: SocketAddr) -> Self {
        self.node_id = node_id;
        self.listen_addr = listen_addr;
        self
    }

    /// Set raft peers for bootstrapping
    pub fn with_raft_peers(mut self, peers: Vec<RaftPeer>) -> Self {
        self.raft_peers = peers;
        self
    }

    /// Set election timeout (ms)
    pub fn with_election_timeout_ms(mut self, ms: u64) -> Self {
        self.election_timeout_ms = ms;
        self
    }

    /// Set heartbeat interval (ms)
    pub fn with_heartbeat_interval_ms(mut self, ms: u64) -> Self {
        self.heartbeat_interval_ms = ms;
        self
    }

    /// Set local storage path for this node
    pub fn with_storage_path(mut self, path: impl Into<String>) -> Self {
        self.storage_path = path.into();
        self
    }

    /// Set whether to enable elect unclean master locally
    pub fn with_enable_elect_unclean_master_local(mut self, enable: bool) -> Self {
        self.enable_elect_unclean_master_local = enable;
        self
    }

    /// Set storage backend for this node
    pub fn with_storage_backend(mut self, backend: StorageBackendType) -> Self {
        self.storage_backend = backend;
        self
    }

    /// Set configuration blacklist
    pub fn with_config_black_list(mut self, blacklist: impl Into<String>) -> Self {
        self.config_black_list = blacklist.into();
        self
    }

    /// Convenience constructor for a node-specific config (node id + listen addr)
    pub fn new_node(node_id: u64, listen_addr: SocketAddr) -> Self {
        Self::default().with_node_info(node_id, listen_addr)
    }

    /// Test config helper
    pub fn test_config() -> Self {
        Self::default().with_node_info(1, "127.0.0.1:60109".parse().unwrap())
    }

    /// Check if a configuration key is in the blacklist
    pub fn is_config_in_blacklist(&self, key: &str) -> bool {
        self.config_black_list.split(';').any(|item| item.trim() == key)
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.controller_type != RAFT_CONTROLLER {
            return Err(format!(
                "Invalid controller type: {}. Must be '{}'",
                self.controller_type, RAFT_CONTROLLER
            ));
        }

        if self.controller_thread_pool_nums == 0 {
            return Err("controller_thread_pool_nums must be greater than 0".to_string());
        }

        if self.controller_request_thread_pool_queue_capacity == 0 {
            return Err("controller_request_thread_pool_queue_capacity must be greater than 0".to_string());
        }

        if self.mapped_file_size == 0 {
            return Err("mapped_file_size must be greater than 0".to_string());
        }

        if self.elect_master_max_retry_count == 0 {
            return Err("elect_master_max_retry_count must be greater than 0".to_string());
        }

        Ok(())
    }

    /// Convert configuration to properties string format
    ///
    /// Returns a string in properties format (key=value\n) using camelCase naming
    pub fn to_properties_string(&self) -> String {
        use std::fmt::Write;

        let mut result = String::with_capacity(2048);

        writeln!(result, "rocketmqHome={}", self.rocketmq_home).unwrap();
        writeln!(result, "configStorePath={}", self.config_store_path.display()).unwrap();
        writeln!(result, "controllerType={}", self.controller_type).unwrap();
        writeln!(
            result,
            "scanNotActiveBrokerInterval={}",
            self.scan_not_active_broker_interval
        )
        .unwrap();
        writeln!(result, "controllerThreadPoolNums={}", self.controller_thread_pool_nums).unwrap();
        writeln!(
            result,
            "controllerRequestThreadPoolQueueCapacity={}",
            self.controller_request_thread_pool_queue_capacity
        )
        .unwrap();
        writeln!(result, "mappedFileSize={}", self.mapped_file_size).unwrap();
        writeln!(result, "controllerStorePath={}", self.controller_store_path).unwrap();
        writeln!(result, "electMasterMaxRetryCount={}", self.elect_master_max_retry_count).unwrap();
        writeln!(result, "enableElectUncleanMaster={}", self.enable_elect_unclean_master).unwrap();
        writeln!(result, "isProcessReadEvent={}", self.is_process_read_event).unwrap();
        writeln!(result, "notifyBrokerRoleChanged={}", self.notify_broker_role_changed).unwrap();
        writeln!(
            result,
            "scanInactiveMasterInterval={}",
            self.scan_inactive_master_interval
        )
        .unwrap();
        writeln!(result, "metricsExporterType={}", self.metrics_exporter_type).unwrap();
        writeln!(
            result,
            "metricsGrpcExporterTarget={}",
            self.metrics_grpc_exporter_target
        )
        .unwrap();
        writeln!(
            result,
            "metricsGrpcExporterHeader={}",
            self.metrics_grpc_exporter_header
        )
        .unwrap();
        writeln!(
            result,
            "metricGrpcExporterTimeOutInMills={}",
            self.metric_grpc_exporter_time_out_in_mills
        )
        .unwrap();
        writeln!(
            result,
            "metricGrpcExporterIntervalInMills={}",
            self.metric_grpc_exporter_interval_in_mills
        )
        .unwrap();
        writeln!(
            result,
            "metricLoggingExporterIntervalInMills={}",
            self.metric_logging_exporter_interval_in_mills
        )
        .unwrap();
        writeln!(result, "metricsPromExporterPort={}", self.metrics_prom_exporter_port).unwrap();
        writeln!(result, "metricsPromExporterHost={}", self.metrics_prom_exporter_host).unwrap();
        writeln!(result, "metricsLabel={}", self.metrics_label).unwrap();
        writeln!(result, "metricsInDelta={}", self.metrics_in_delta).unwrap();
        writeln!(result, "configBlackList={}", self.config_black_list).unwrap();
        writeln!(result, "nodeId={}", self.node_id).unwrap();
        writeln!(result, "listenAddr={}", self.listen_addr).unwrap();

        let peers: Vec<String> = self
            .raft_peers
            .iter()
            .map(|peer| format!("{}-{}", peer.id, peer.addr))
            .collect();
        writeln!(result, "raftPeers={}", peers.join(";")).unwrap();

        writeln!(result, "electionTimeoutMs={}", self.election_timeout_ms).unwrap();
        writeln!(result, "heartbeatIntervalMs={}", self.heartbeat_interval_ms).unwrap();
        writeln!(result, "storagePath={}", self.storage_path).unwrap();
        writeln!(result, "storageBackend={}", self.storage_backend).unwrap();
        writeln!(
            result,
            "enableElectUncleanMasterLocal={}",
            self.enable_elect_unclean_master_local
        )
        .unwrap();

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_controller_config_default() {
        let config = ControllerConfig::default();

        assert_eq!(config.controller_type, RAFT_CONTROLLER);
        assert_eq!(config.scan_not_active_broker_interval, 5000);
        assert_eq!(config.controller_thread_pool_nums, 16);
        assert_eq!(config.controller_request_thread_pool_queue_capacity, 50000);
        assert_eq!(config.mapped_file_size, 1024 * 1024 * 1024);
        assert_eq!(config.elect_master_max_retry_count, 3);
        assert!(!config.enable_elect_unclean_master);
        assert!(!config.is_process_read_event);
        assert!(config.notify_broker_role_changed);
        assert_eq!(config.metrics_prom_exporter_port, 5557);
    }

    #[test]
    fn test_controller_config_builder() {
        let config = ControllerConfig::new()
            .with_controller_type(RAFT_CONTROLLER)
            .with_scan_not_active_broker_interval(10000)
            .with_controller_thread_pool_nums(32)
            .with_enable_elect_unclean_master(true);

        assert_eq!(config.controller_type, RAFT_CONTROLLER);
        assert_eq!(config.scan_not_active_broker_interval, 10000);
        assert_eq!(config.controller_thread_pool_nums, 32);
        assert!(config.enable_elect_unclean_master);
    }

    #[test]
    fn test_config_validation_invalid_type() {
        let config = ControllerConfig {
            controller_type: "InvalidType".to_string(),
            ..Default::default()
        };

        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_success() {
        let config = ControllerConfig::new();

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_blacklist() {
        let config = ControllerConfig::default();

        assert!(config.is_config_in_blacklist("configBlackList"));
        assert!(config.is_config_in_blacklist("configStorePath"));
        assert!(!config.is_config_in_blacklist("controllerType"));
    }

    #[test]
    fn test_metrics_config() {
        let config = ControllerConfig::new()
            .with_metrics_exporter_type(MetricsExporterType::Prom)
            .with_metrics_prom_exporter("localhost", 9090)
            .with_metrics_label("instance_id:test,uid:123");

        assert_eq!(config.metrics_exporter_type, MetricsExporterType::Prom);
        assert_eq!(config.metrics_prom_exporter_host, "localhost");
        assert_eq!(config.metrics_prom_exporter_port, 9090);
        assert_eq!(config.metrics_label, "instance_id:test,uid:123");
    }

    #[test]
    fn test_to_properties_string_basic() {
        let config = ControllerConfig::default();
        let result = config.to_properties_string();

        assert!(result.contains("rocketmqHome="));
        assert!(result.contains("controllerType=Raft"));
        assert!(result.contains("scanNotActiveBrokerInterval=5000"));
        assert!(result.contains("controllerThreadPoolNums=16"));
        assert!(result.contains("notifyBrokerRoleChanged=true"));
        assert!(result.contains("enableElectUncleanMaster=false"));
    }

    #[test]
    fn test_to_properties_string_format() {
        let config = ControllerConfig::default();
        let result = config.to_properties_string();

        let lines: Vec<&str> = result.lines().collect();
        assert!(!lines.is_empty());

        for line in lines {
            assert!(line.contains('='), "Each line should contain '=': {}", line);
            let parts: Vec<&str> = line.split('=').collect();
            assert_eq!(parts.len(), 2, "Each line should have exactly one '=': {}", line);
        }
    }

    #[test]
    fn test_to_properties_string_with_custom_config() {
        let config = ControllerConfig::new()
            .with_controller_type(RAFT_CONTROLLER)
            .with_scan_not_active_broker_interval(10000)
            .with_controller_thread_pool_nums(32)
            .with_enable_elect_unclean_master(true)
            .with_storage_backend(StorageBackendType::RocksDB);

        let result = config.to_properties_string();

        assert!(result.contains("controllerType=Raft"));
        assert!(result.contains("scanNotActiveBrokerInterval=10000"));
        assert!(result.contains("controllerThreadPoolNums=32"));
        assert!(result.contains("enableElectUncleanMaster=true"));
        assert!(result.contains("storageBackend=rocks_db"));
    }

    #[test]
    fn test_to_properties_string_with_raft_peers() {
        let config = ControllerConfig::new().with_raft_peers(vec![
            RaftPeer {
                id: 1,
                addr: "127.0.0.1:9877".parse().unwrap(),
            },
            RaftPeer {
                id: 2,
                addr: "127.0.0.1:9878".parse().unwrap(),
            },
        ]);

        let result = config.to_properties_string();

        assert!(result.contains("raftPeers=1-127.0.0.1:9877;2-127.0.0.1:9878"));
    }
}
