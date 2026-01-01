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

use rocketmq_remoting::code::request_code::RequestCode;

// Label constants
pub const LABEL_ADDRESS: &str = "address";
pub const LABEL_GROUP: &str = "group";
pub const LABEL_PEER_ID: &str = "peer_id";
pub const LABEL_AGGREGATION: &str = "aggregation";
pub const AGGREGATION_DELTA: &str = "delta";

pub const OPEN_TELEMETRY_METER_NAME: &str = "controller";

// Gauge constants
pub const GAUGE_ROLE: &str = "role";
pub const GAUGE_DLEDGER_DISK_USAGE: &str = "dledger_disk_usage"; // unit: B
pub const GAUGE_ACTIVE_BROKER_NUM: &str = "active_broker_num";

// Counter constants
pub const COUNTER_REQUEST_TOTAL: &str = "request_total";
pub const COUNTER_DLEDGER_OP_TOTAL: &str = "dledger_op_total";
pub const COUNTER_ELECTION_TOTAL: &str = "election_total";

// Histogram constants
pub const HISTOGRAM_REQUEST_LATENCY: &str = "request_latency"; // unit: us
pub const HISTOGRAM_DLEDGER_OP_LATENCY: &str = "dledger_op_latency"; // unit: us

// Operation label constants
pub const LABEL_CLUSTER_NAME: &str = "cluster";
pub const LABEL_BROKER_SET: &str = "broker_set";
pub const LABEL_REQUEST_TYPE: &str = "request_type";
pub const LABEL_REQUEST_HANDLE_STATUS: &str = "request_handle_status";
pub const LABEL_DLEDGER_OPERATION: &str = "dledger_operation";
pub const LABEL_DLEDGER_OPERATION_STATUS: &str = "dLedger_operation_status";
pub const LABEL_ELECTION_RESULT: &str = "election_result";

/// Helper functions for RequestCode to get lowercase names for metrics
pub trait RequestType {
    /// Get lowercase name for controller request types
    /// Returns None if the request code is not a controller request
    fn get_controller_request_name(&self) -> Option<&'static str>;

    /// Check if this is a controller request
    fn is_controller_request(&self) -> bool;
}

impl RequestType for RequestCode {
    fn get_controller_request_name(&self) -> Option<&'static str> {
        match *self {
            RequestCode::ControllerAlterSyncStateSet => Some("controller_alter_sync_state_set"),
            RequestCode::ControllerElectMaster => Some("controller_elect_master"),
            RequestCode::ControllerRegisterBroker => Some("controller_register_broker"),
            RequestCode::ControllerGetReplicaInfo => Some("controller_get_replica_info"),
            RequestCode::ControllerGetMetadataInfo => Some("controller_get_metadata_info"),
            RequestCode::ControllerGetSyncStateData => Some("controller_get_sync_state_data"),
            RequestCode::GetBrokerEpochCache => Some("controller_get_broker_epoch_cache"),
            RequestCode::NotifyBrokerRoleChanged => Some("controller_notify_broker_role_changed"),
            RequestCode::BrokerHeartbeat => Some("controller_broker_heartbeat"),
            RequestCode::UpdateControllerConfig => Some("controller_update_controller_config"),
            RequestCode::GetControllerConfig => Some("controller_get_controller_config"),
            RequestCode::CleanBrokerData => Some("controller_clean_broker_data"),
            RequestCode::ControllerGetNextBrokerId => Some("controller_get_next_broker_id"),
            RequestCode::ControllerApplyBrokerId => Some("controller_apply_broker_id"),
            _ => None,
        }
    }

    fn is_controller_request(&self) -> bool {
        self.get_controller_request_name().is_some()
    }
}

/// Request handle status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RequestHandleStatus {
    Success,
    Failed,
    Timeout,
}

impl RequestHandleStatus {
    /// Get lowercase name
    pub const fn get_lower_case_name(&self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failed => "failed",
            Self::Timeout => "timeout",
        }
    }
}

/// DLedger operation types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DLedgerOperation {
    Append,
}

impl DLedgerOperation {
    /// Get lowercase name
    pub const fn get_lower_case_name(&self) -> &'static str {
        match self {
            Self::Append => "append",
        }
    }
}

/// DLedger operation status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DLedgerOperationStatus {
    Success,
    Failed,
    Timeout,
}

impl DLedgerOperationStatus {
    /// Get lowercase name
    pub const fn get_lower_case_name(&self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failed => "failed",
            Self::Timeout => "timeout",
        }
    }
}

/// Election result types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ElectionResult {
    NewMasterElected,
    KeepCurrentMaster,
    NoMasterElected,
}

impl ElectionResult {
    /// Get lowercase name
    pub const fn get_lower_case_name(&self) -> &'static str {
        match self {
            Self::NewMasterElected => "new_master_elected",
            Self::KeepCurrentMaster => "keep_current_master",
            Self::NoMasterElected => "no_master_elected",
        }
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_remoting::code::request_code::RequestCode;

    use super::*;

    #[test]
    fn test_request_code_get_controller_request_name() {
        assert_eq!(
            RequestCode::ControllerAlterSyncStateSet.get_controller_request_name(),
            Some("controller_alter_sync_state_set")
        );
        assert_eq!(
            RequestCode::ControllerElectMaster.get_controller_request_name(),
            Some("controller_elect_master")
        );
        assert_eq!(
            RequestCode::BrokerHeartbeat.get_controller_request_name(),
            Some("controller_broker_heartbeat")
        );
        assert_eq!(RequestCode::SendMessage.get_controller_request_name(), None);
    }

    #[test]
    fn test_request_code_is_controller_request() {
        assert!(RequestCode::ControllerAlterSyncStateSet.is_controller_request());
        assert!(RequestCode::ControllerElectMaster.is_controller_request());
        assert!(RequestCode::BrokerHeartbeat.is_controller_request());
        assert!(!RequestCode::SendMessage.is_controller_request());
        assert!(!RequestCode::PullMessage.is_controller_request());
    }

    #[test]
    fn test_request_handle_status() {
        assert_eq!(RequestHandleStatus::Success.get_lower_case_name(), "success");
        assert_eq!(RequestHandleStatus::Failed.get_lower_case_name(), "failed");
        assert_eq!(RequestHandleStatus::Timeout.get_lower_case_name(), "timeout");
    }

    #[test]
    fn test_dledger_operation() {
        assert_eq!(DLedgerOperation::Append.get_lower_case_name(), "append");
    }

    #[test]
    fn test_dledger_operation_status() {
        assert_eq!(DLedgerOperationStatus::Success.get_lower_case_name(), "success");
        assert_eq!(DLedgerOperationStatus::Failed.get_lower_case_name(), "failed");
        assert_eq!(DLedgerOperationStatus::Timeout.get_lower_case_name(), "timeout");
    }

    #[test]
    fn test_election_result() {
        assert_eq!(
            ElectionResult::NewMasterElected.get_lower_case_name(),
            "new_master_elected"
        );
        assert_eq!(
            ElectionResult::KeepCurrentMaster.get_lower_case_name(),
            "keep_current_master"
        );
        assert_eq!(
            ElectionResult::NoMasterElected.get_lower_case_name(),
            "no_master_elected"
        );
    }

    #[test]
    fn test_constants() {
        assert_eq!(LABEL_ADDRESS, "address");
        assert_eq!(LABEL_GROUP, "group");
        assert_eq!(OPEN_TELEMETRY_METER_NAME, "controller");
        assert_eq!(GAUGE_ROLE, "role");
        assert_eq!(COUNTER_REQUEST_TOTAL, "request_total");
        assert_eq!(HISTOGRAM_REQUEST_LATENCY, "request_latency");
    }
}
