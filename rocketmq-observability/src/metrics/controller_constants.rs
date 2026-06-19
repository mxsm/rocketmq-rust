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
    use super::*;

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
