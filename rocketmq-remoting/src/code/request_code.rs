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

/// Macro to define RequestCode enum with automatic conversion implementations.
/// This reduces code duplication and makes it easier to maintain.
macro_rules! define_request_code {
    (
        $(#[$enum_meta:meta])*
        pub enum $enum_name:ident {
            $(
                $(#[$variant_meta:meta])*
                $variant:ident = $value:expr
            ),* $(,)?
        }
    ) => {
        $(#[$enum_meta])*
        #[repr(i32)]
        pub enum $enum_name {
            $(
                $(#[$variant_meta])*
                $variant = $value,
            )*
        }

        impl From<$enum_name> for i32 {
            #[inline]
            fn from(value: $enum_name) -> Self {
                value as i32
            }
        }

        impl From<i32> for $enum_name {
            #[inline]
            fn from(value: i32) -> Self {
                match value {
                    $($value => $enum_name::$variant,)*
                    _ => $enum_name::Unknown,
                }
            }
        }

        impl $enum_name {
            /// Convert to i32 value
            #[inline]
            pub const fn to_i32(self) -> i32 {
                self as i32
            }

            /// Check if the request code is unknown
            #[inline]
            pub const fn is_unknown(&self) -> bool {
                matches!(self, Self::Unknown)
            }
        }
    };
}

define_request_code! {
    #[derive(Debug, Eq, PartialEq, Clone, Copy, Hash)]
    pub enum RequestCode {
        SendMessage = 10,
        PullMessage = 11,
        QueryMessage = 12,
        QueryBrokerOffset = 13, // Not used in Java
        QueryConsumerOffset = 14,
        UpdateConsumerOffset = 15,
        UpdateAndCreateTopic = 17,
        UpdateAndCreateTopicList = 18,
        GetAllTopicConfig = 21,
        GetTopicConfigList = 22, // Not used in Java
        GetTopicNameList = 23,   // Not used in Java
        UpdateBrokerConfig = 25,
        GetBrokerConfig = 26,
        TriggerDeleteFiles = 27, // Not used in Java
        GetBrokerRuntimeInfo = 28,
        SearchOffsetByTimestamp = 29,
        GetMaxOffset = 30,
        GetMinOffset = 31,
        GetEarliestMsgStoreTime = 32,
        ViewMessageById = 33,
        HeartBeat = 34,
        UnregisterClient = 35,
        ConsumerSendMsgBack = 36,
        EndTransaction = 37,
        GetConsumerListByGroup = 38,
        CheckTransactionState = 39,
        NotifyConsumerIdsChanged = 40,
        LockBatchMq = 41,
        UnlockBatchMq = 42,
        GetAllConsumerOffset = 43,
        GetAllDelayOffset = 45,
        CheckClientConfig = 46,
        GetClientConfig = 47, // Not used in Java
        UpdateAndCreateAclConfig = 50,
        DeleteAclConfig = 51,
        GetBrokerClusterAclInfo = 52,
        UpdateGlobalWhiteAddrsConfig = 53,
        GetBrokerClusterAclConfig = 54, // Deprecated
        GetTimerCheckPoint = 60,
        GetTimerMetrics = 61,

        PopMessage = 200050,
        AckMessage = 200051,
        BatchAckMessage = 200151,
        PeekMessage = 200052,
        ChangeMessageInvisibleTime = 200053,
        Notification = 200054,
        PollingInfo = 200055,

        PutKvConfig = 100,
        GetKvConfig = 101,
        DeleteKvConfig = 102,
        RegisterBroker = 103,
        UnregisterBroker = 104,
        GetRouteinfoByTopic = 105,
        GetBrokerClusterInfo = 106,
        UpdateAndCreateSubscriptionGroup = 200,
        GetAllSubscriptionGroupConfig = 201,
        GetTopicStatsInfo = 202,
        GetConsumerConnectionList = 203,
        GetProducerConnectionList = 204,
        WipeWritePermOfBroker = 205,
        GetAllTopicListFromNameserver = 206,
        DeleteSubscriptionGroup = 207,
        GetConsumeStats = 208,

        SuspendConsumer = 209,               // Not used in Java
        ResumeConsumer = 210,                // Not used in Java
        ResetConsumerOffsetInConsumer = 211, // Not used in Java
        ResetConsumerOffsetInBroker = 212,   // Not used in Java
        AdjustConsumerThreadPool = 213,      // Not used in Java
        WhoConsumeTheMessage = 214,          // Not used in Java

        DeleteTopicInBroker = 215,
        DeleteTopicInNamesrv = 216,
        RegisterTopicInNamesrv = 217,
        GetKvlistByNamespace = 219,
        ResetConsumerClientOffset = 220,
        GetConsumerStatusFromClient = 221,
        InvokeBrokerToResetOffset = 222,
        InvokeBrokerToGetConsumerStatus = 223,
        UpdateAndCreateSubscriptionGroupList = 225,

        QueryTopicConsumeByWho = 300,
        GetTopicsByCluster = 224,
        QueryTopicsByConsumer = 343,
        QuerySubscriptionByConsumer = 345,

        RegisterFilterServer = 301,       // Not used in Java
        RegisterMessageFilterClass = 302, // Not used in Java

        QueryConsumeTimeSpan = 303,
        GetSystemTopicListFromNs = 304,
        GetSystemTopicListFromBroker = 305,
        CleanExpiredConsumequeue = 306,
        GetConsumerRunningInfo = 307,
        QueryCorrectionOffset = 308,
        ConsumeMessageDirectly = 309,
        SendMessageV2 = 310,
        GetUnitTopicList = 311,
        GetHasUnitSubTopicList = 312,
        GetHasUnitSubUnunitTopicList = 313,
        CloneGroupOffset = 314,
        ViewBrokerStatsData = 315,
        CleanUnusedTopic = 316,
        GetBrokerConsumeStats = 317,
        UpdateNamesrvConfig = 318,
        GetNamesrvConfig = 319,
        SendBatchMessage = 320,
        QueryConsumeQueue = 321,
        QueryDataVersion = 322,
        ResumeCheckHalfMessage = 323,
        SendReplyMessage = 324,
        SendReplyMessageV2 = 325,
        PushReplyMessageToClient = 326,
        AddWritePermOfBroker = 327,
        GetTopicConfig = 351,
        GetSubscriptionGroupConfig = 352,
        UpdateAndGetGroupForbidden = 353,
        CheckRocksdbCqWriteProgress = 354,

        LitePullMessage = 361,

        QueryAssignment = 400,
        SetMessageRequestMode = 401,
        GetAllMessageRequestMode = 402,
        UpdateAndCreateStaticTopic = 513,
        GetBrokerMemberGroup = 901,
        AddBroker = 902,
        RemoveBroker = 903,
        BrokerHeartbeat = 904,
        NotifyMinBrokerIdChange = 905,
        ExchangeBrokerHaInfo = 906,
        GetBrokerHaStatus = 907,
        ResetMasterFlushOffset = 908,
        GetAllProducerInfo = 328,
        DeleteExpiredCommitlog = 329,

        UpdateColdDataFlowCtrConfig = 2001,
        RemoveColdDataFlowCtrConfig = 2002,
        GetColdDataFlowCtrInfo = 2003,
        SetCommitlogReadMode = 2004,

        // Controller codes
        ControllerAlterSyncStateSet = 1001,
        ControllerElectMaster = 1002,
        ControllerRegisterBroker = 1003,
        ControllerGetReplicaInfo = 1004,
        ControllerGetMetadataInfo = 1005,
        ControllerGetSyncStateData = 1006,
        GetBrokerEpochCache = 1007,
        NotifyBrokerRoleChanged = 1008,
        UpdateControllerConfig = 1009,
        GetControllerConfig = 1010,

        CleanBrokerData = 1011,
        ControllerGetNextBrokerId = 1012,
        ControllerApplyBrokerId = 1013,
        BrokerCloseChannelRequest = 1014,
        CheckNotActiveBrokerRequest = 1015,
        GetBrokerLiveInfoRequest = 1016,
        GetSyncStateDataRequest = 1017,
        RaftBrokerHeartBeatEventRequest = 1018,

        // Auth codes
        AuthCreateUser = 3001,
        AuthUpdateUser = 3002,
        AuthDeleteUser = 3003,
        AuthGetUser = 3004,
        AuthListUser = 3005,
        AuthCreateAcl = 3006,
        AuthUpdateAcl = 3007,
        AuthDeleteAcl = 3008,
        AuthGetAcl = 3009,
        AuthListAcl = 3010,

        Unknown = -9999999,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_code_to_i32() {
        assert_eq!(RequestCode::SendMessage.to_i32(), 10);
        assert_eq!(RequestCode::PullMessage.to_i32(), 11);
        assert_eq!(RequestCode::HeartBeat.to_i32(), 34);
        assert_eq!(RequestCode::PopMessage.to_i32(), 200050);
        assert_eq!(RequestCode::ControllerAlterSyncStateSet.to_i32(), 1001);
        assert_eq!(RequestCode::AuthCreateUser.to_i32(), 3001);
        assert_eq!(RequestCode::Unknown.to_i32(), -9999999);
    }

    #[test]
    fn test_i32_to_request_code() {
        assert_eq!(RequestCode::from(10), RequestCode::SendMessage);
        assert_eq!(RequestCode::from(11), RequestCode::PullMessage);
        assert_eq!(RequestCode::from(34), RequestCode::HeartBeat);
        assert_eq!(RequestCode::from(200050), RequestCode::PopMessage);
        assert_eq!(RequestCode::from(1001), RequestCode::ControllerAlterSyncStateSet);
        assert_eq!(RequestCode::from(3001), RequestCode::AuthCreateUser);
        assert_eq!(RequestCode::from(-9999999), RequestCode::Unknown);
    }

    #[test]
    fn test_unknown_code_conversion() {
        // Any unknown i32 value should return Unknown
        assert_eq!(RequestCode::from(99999), RequestCode::Unknown);
        assert_eq!(RequestCode::from(-1), RequestCode::Unknown);
        assert_eq!(RequestCode::from(0), RequestCode::Unknown);
        assert_eq!(RequestCode::from(999), RequestCode::Unknown);
    }

    #[test]
    fn test_is_unknown() {
        assert!(RequestCode::Unknown.is_unknown());
        assert!(!RequestCode::SendMessage.is_unknown());
        assert!(!RequestCode::HeartBeat.is_unknown());
        assert!(!RequestCode::PopMessage.is_unknown());

        // Test with unknown code conversion
        let unknown_code = RequestCode::from(12345);
        assert!(unknown_code.is_unknown());
    }

    #[test]
    fn test_request_code_from_trait() {
        let code: i32 = RequestCode::SendMessage.into();
        assert_eq!(code, 10);

        let code: i32 = RequestCode::Unknown.into();
        assert_eq!(code, -9999999);
    }

    #[test]
    fn test_round_trip_conversion() {
        // Test that converting to i32 and back gives the same value
        let codes = vec![
            RequestCode::SendMessage,
            RequestCode::PullMessage,
            RequestCode::QueryMessage,
            RequestCode::HeartBeat,
            RequestCode::PopMessage,
            RequestCode::RegisterBroker,
            RequestCode::ControllerElectMaster,
            RequestCode::AuthCreateUser,
            RequestCode::Unknown,
        ];

        for code in codes {
            let i32_val = code.to_i32();
            let converted_back = RequestCode::from(i32_val);
            assert_eq!(code, converted_back, "Round trip failed for {:?}", code);
        }
    }

    #[test]
    fn test_derive_traits() {
        // Test Debug
        let code = RequestCode::SendMessage;
        assert_eq!(format!("{:?}", code), "SendMessage");

        // Test Clone and Copy
        let code1 = RequestCode::HeartBeat;
        let code2 = code1;
        let code3 = code1;
        assert_eq!(code1, code2);
        assert_eq!(code1, code3);

        // Test PartialEq and Eq
        assert_eq!(RequestCode::SendMessage, RequestCode::SendMessage);
        assert_ne!(RequestCode::SendMessage, RequestCode::PullMessage);

        // Test Hash (by using in a HashSet)
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(RequestCode::SendMessage);
        set.insert(RequestCode::SendMessage); // Duplicate
        set.insert(RequestCode::PullMessage);
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_special_codes() {
        // Test Pop message range (200050-200055)
        assert_eq!(RequestCode::from(200050), RequestCode::PopMessage);
        assert_eq!(RequestCode::from(200051), RequestCode::AckMessage);
        assert_eq!(RequestCode::from(200151), RequestCode::BatchAckMessage);
        assert_eq!(RequestCode::from(200052), RequestCode::PeekMessage);
        assert_eq!(RequestCode::from(200053), RequestCode::ChangeMessageInvisibleTime);
        assert_eq!(RequestCode::from(200054), RequestCode::Notification);
        assert_eq!(RequestCode::from(200055), RequestCode::PollingInfo);

        // Test Controller codes (1001-1018)
        assert_eq!(RequestCode::from(1001), RequestCode::ControllerAlterSyncStateSet);
        assert_eq!(RequestCode::from(1018), RequestCode::RaftBrokerHeartBeatEventRequest);

        // Test Auth codes (3001-3010)
        assert_eq!(RequestCode::from(3001), RequestCode::AuthCreateUser);
        assert_eq!(RequestCode::from(3010), RequestCode::AuthListAcl);

        // Test Cold data flow codes (2001-2004)
        assert_eq!(RequestCode::from(2001), RequestCode::UpdateColdDataFlowCtrConfig);
        assert_eq!(RequestCode::from(2004), RequestCode::SetCommitlogReadMode);
    }

    #[test]
    fn test_const_fn_to_i32() {
        // Verify that to_i32 is const and can be used in const context
        const CODE: i32 = RequestCode::SendMessage.to_i32();
        assert_eq!(CODE, 10);
    }

    #[test]
    fn test_repr_i32_size() {
        // Verify that RequestCode has the same size as i32
        use std::mem::size_of;
        assert_eq!(size_of::<RequestCode>(), size_of::<i32>());
    }
}
