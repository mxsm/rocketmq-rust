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

#[allow(unused_imports)]
pub use rocketmq_observability::metrics::controller_constants::*;

/// Helper functions for RequestCode to get lowercase names for metrics.
pub trait RequestType {
    /// Get lowercase name for controller request types.
    ///
    /// Returns None if the request code is not a controller request.
    fn get_controller_request_name(&self) -> Option<&'static str>;

    /// Check if this is a controller request.
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

#[cfg(test)]
mod tests {
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
    fn phase7_non_dledger_controller_request_set_has_stable_names() {
        let expected_controller_requests = [
            (
                RequestCode::ControllerAlterSyncStateSet,
                "controller_alter_sync_state_set",
            ),
            (RequestCode::ControllerElectMaster, "controller_elect_master"),
            (RequestCode::ControllerRegisterBroker, "controller_register_broker"),
            (RequestCode::ControllerGetReplicaInfo, "controller_get_replica_info"),
            (RequestCode::ControllerGetMetadataInfo, "controller_get_metadata_info"),
            (
                RequestCode::ControllerGetSyncStateData,
                "controller_get_sync_state_data",
            ),
            (RequestCode::GetBrokerEpochCache, "controller_get_broker_epoch_cache"),
            (
                RequestCode::NotifyBrokerRoleChanged,
                "controller_notify_broker_role_changed",
            ),
            (RequestCode::BrokerHeartbeat, "controller_broker_heartbeat"),
            (
                RequestCode::UpdateControllerConfig,
                "controller_update_controller_config",
            ),
            (RequestCode::GetControllerConfig, "controller_get_controller_config"),
            (RequestCode::CleanBrokerData, "controller_clean_broker_data"),
            (RequestCode::ControllerGetNextBrokerId, "controller_get_next_broker_id"),
            (RequestCode::ControllerApplyBrokerId, "controller_apply_broker_id"),
        ];

        for (request_code, expected_name) in expected_controller_requests {
            assert_eq!(request_code.get_controller_request_name(), Some(expected_name));
            assert!(request_code.is_controller_request());
        }
    }

    #[test]
    fn phase7_dledger_request_codes_stay_outside_regular_controller_metrics() {
        for request_code in [
            RequestCode::BrokerCloseChannelRequest,
            RequestCode::CheckNotActiveBrokerRequest,
            RequestCode::GetBrokerLiveInfoRequest,
            RequestCode::GetSyncStateDataRequest,
            RequestCode::RaftBrokerHeartBeatEventRequest,
        ] {
            assert_eq!(request_code.get_controller_request_name(), None);
            assert!(
                !request_code.is_controller_request(),
                "{request_code:?} is DLedger/raft-scoped and should not be classified as a regular controller request"
            );
        }
    }
}
