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

/// Macro to define response code enums with automatic conversion implementations.
/// This reduces code duplication and makes maintenance easier.
macro_rules! define_response_code {
    (
        $(#[$enum_meta:meta])*
        pub enum $enum_name:ident {
            $(
                $(#[$variant_meta:meta])*
                $variant:ident = $value:expr
            ),* $(,)?
        },
        default = $default:ident
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
                    _ => $enum_name::$default,
                }
            }
        }

        impl $enum_name {
            /// Convert to i32 value
            #[inline]
            pub const fn to_i32(self) -> i32 {
                self as i32
            }

            /// Check if this is a success response
            #[inline]
            pub const fn is_success(&self) -> bool {
                matches!(self, Self::Success)
            }

            /// Check if this is an error response
            #[inline]
            pub const fn is_error(&self) -> bool {
                !self.is_success()
            }
        }
    };
}

define_response_code! {
    #[derive(Debug, PartialEq, Eq, Copy, Clone, Hash)]
    pub enum RemotingSysResponseCode {
        Success = 0,
        SystemError = 1,
        SystemBusy = 2,
        RequestCodeNotSupported = 3,
        TransactionFailed = 4,
        NoPermission = 16,
    },
    default = SystemError
}

define_response_code! {
    #[derive(Debug, PartialEq, Eq, Copy, Clone, Hash)]
    pub enum ResponseCode {
        Success = 0,
        SystemError = 1,
        SystemBusy = 2,
        RequestCodeNotSupported = 3,
        TransactionFailed = 4,
        FlushDiskTimeout = 10,
        SlaveNotAvailable = 11,
        FlushSlaveTimeout = 12,
        MessageIllegal = 13,
        ServiceNotAvailable = 14,
        VersionNotSupported = 15,
        NoPermission = 16,
        TopicNotExist = 17,
        TopicExistAlready = 18,
        PullNotFound = 19,
        PullRetryImmediately = 20,
        PullOffsetMoved = 21,
        QueryNotFound = 22,
        SubscriptionParseFailed = 23,
        SubscriptionNotExist = 24,
        SubscriptionNotLatest = 25,
        SubscriptionGroupNotExist = 26,
        FilterDataNotExist = 27,
        FilterDataNotLatest = 28,
        TransactionShouldCommit = 200,
        TransactionShouldRollback = 201,
        TransactionStateUnknow = 202,
        TransactionStateGroupWrong = 203,
        NoBuyerId = 204,
        NotInCurrentUnit = 205,
        ConsumerNotOnline = 206,
        ConsumeMsgTimeout = 207,
        NoMessage = 208,
        /*UpdateAndCreateAclConfigFailed = 209,
        DeleteAclConfigFailed = 210,
        UpdateGlobalWhiteAddrsConfigFailed = 211,*/
        PollingFull = 209,
        PollingTimeout = 210,
        BrokerNotExist = 211,
        BrokerDispatchNotComplete = 212,
        BroadcastConsumption = 213,
        FlowControl = 215,
        NotLeaderForQueue = 501,
        IllegalOperation = 604,
        RpcUnknown = -1000,
        RpcAddrIsNull = -1002,
        RpcSendToChannelFailed = -1004,
        RpcTimeOut = -1006,
        GoAway = 1500,
        ControllerFencedMasterEpoch = 2000,
        ControllerFencedSyncStateSetEpoch = 2001,
        ControllerInvalidMaster = 2002,
        ControllerInvalidReplicas = 2003,
        ControllerMasterNotAvailable = 2004,
        ControllerInvalidRequest = 2005,
        ControllerBrokerNotAlive = 2006,
        ControllerNotLeader = 2007,
        ControllerBrokerMetadataNotExist = 2008,
        ControllerInvalidCleanBrokerMetadata = 2009,
        ControllerBrokerNeedToBeRegistered = 2010,
        ControllerMasterStillExist = 2011,
        ControllerElectMasterFailed = 2012,
        ControllerAlterSyncStateSetFailed = 2013,
        ControllerBrokerIdInvalid = 2014,
    },
    default = SystemError
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_remoting_sys_response_code_to_i32() {
        assert_eq!(RemotingSysResponseCode::Success.to_i32(), 0);
        assert_eq!(RemotingSysResponseCode::SystemError.to_i32(), 1);
        assert_eq!(RemotingSysResponseCode::SystemBusy.to_i32(), 2);
        assert_eq!(RemotingSysResponseCode::RequestCodeNotSupported.to_i32(), 3);
        assert_eq!(RemotingSysResponseCode::TransactionFailed.to_i32(), 4);
        assert_eq!(RemotingSysResponseCode::NoPermission.to_i32(), 16);
    }

    #[test]
    fn remoting_sys_response_code_from_i32() {
        assert_eq!(RemotingSysResponseCode::from(0), RemotingSysResponseCode::Success);
        assert_eq!(RemotingSysResponseCode::from(1), RemotingSysResponseCode::SystemError);
        assert_eq!(RemotingSysResponseCode::from(2), RemotingSysResponseCode::SystemBusy);
        assert_eq!(
            RemotingSysResponseCode::from(3),
            RemotingSysResponseCode::RequestCodeNotSupported
        );
        assert_eq!(
            RemotingSysResponseCode::from(4),
            RemotingSysResponseCode::TransactionFailed
        );
        assert_eq!(RemotingSysResponseCode::from(16), RemotingSysResponseCode::NoPermission);
        assert_eq!(RemotingSysResponseCode::from(999), RemotingSysResponseCode::SystemError); // Edge case - unknown code defaults to SystemError
    }

    #[test]
    fn test_remoting_sys_response_code_is_success() {
        assert!(RemotingSysResponseCode::Success.is_success());
        assert!(!RemotingSysResponseCode::SystemError.is_success());
        assert!(!RemotingSysResponseCode::SystemBusy.is_success());
    }

    #[test]
    fn test_remoting_sys_response_code_is_error() {
        assert!(!RemotingSysResponseCode::Success.is_error());
        assert!(RemotingSysResponseCode::SystemError.is_error());
        assert!(RemotingSysResponseCode::SystemBusy.is_error());
        assert!(RemotingSysResponseCode::NoPermission.is_error());
    }

    #[test]
    fn test_response_code_to_i32() {
        assert_eq!(ResponseCode::Success.to_i32(), 0);
        assert_eq!(ResponseCode::SystemError.to_i32(), 1);
        assert_eq!(ResponseCode::FlushDiskTimeout.to_i32(), 10);
        assert_eq!(ResponseCode::RpcUnknown.to_i32(), -1000);
        assert_eq!(ResponseCode::ControllerFencedMasterEpoch.to_i32(), 2000);
    }

    #[test]
    fn response_code_from_i32() {
        assert_eq!(ResponseCode::from(0), ResponseCode::Success);
        assert_eq!(ResponseCode::from(1), ResponseCode::SystemError);
        assert_eq!(ResponseCode::from(2), ResponseCode::SystemBusy);
        assert_eq!(ResponseCode::from(3), ResponseCode::RequestCodeNotSupported);
        assert_eq!(ResponseCode::from(4), ResponseCode::TransactionFailed);
        assert_eq!(ResponseCode::from(10), ResponseCode::FlushDiskTimeout);
        assert_eq!(ResponseCode::from(11), ResponseCode::SlaveNotAvailable);
        assert_eq!(ResponseCode::from(12), ResponseCode::FlushSlaveTimeout);
        assert_eq!(ResponseCode::from(13), ResponseCode::MessageIllegal);
        assert_eq!(ResponseCode::from(14), ResponseCode::ServiceNotAvailable);
        assert_eq!(ResponseCode::from(15), ResponseCode::VersionNotSupported);
        assert_eq!(ResponseCode::from(16), ResponseCode::NoPermission);
        assert_eq!(ResponseCode::from(17), ResponseCode::TopicNotExist);
        assert_eq!(ResponseCode::from(18), ResponseCode::TopicExistAlready);
        assert_eq!(ResponseCode::from(19), ResponseCode::PullNotFound);
        assert_eq!(ResponseCode::from(20), ResponseCode::PullRetryImmediately);
        assert_eq!(ResponseCode::from(21), ResponseCode::PullOffsetMoved);
        assert_eq!(ResponseCode::from(22), ResponseCode::QueryNotFound);
        assert_eq!(ResponseCode::from(23), ResponseCode::SubscriptionParseFailed);
        assert_eq!(ResponseCode::from(24), ResponseCode::SubscriptionNotExist);
        assert_eq!(ResponseCode::from(25), ResponseCode::SubscriptionNotLatest);
        assert_eq!(ResponseCode::from(26), ResponseCode::SubscriptionGroupNotExist);
        assert_eq!(ResponseCode::from(27), ResponseCode::FilterDataNotExist);
        assert_eq!(ResponseCode::from(28), ResponseCode::FilterDataNotLatest);
        assert_eq!(ResponseCode::from(200), ResponseCode::TransactionShouldCommit);
        assert_eq!(ResponseCode::from(201), ResponseCode::TransactionShouldRollback);
        assert_eq!(ResponseCode::from(202), ResponseCode::TransactionStateUnknow);
        assert_eq!(ResponseCode::from(203), ResponseCode::TransactionStateGroupWrong);
        assert_eq!(ResponseCode::from(204), ResponseCode::NoBuyerId);
        assert_eq!(ResponseCode::from(205), ResponseCode::NotInCurrentUnit);
        assert_eq!(ResponseCode::from(206), ResponseCode::ConsumerNotOnline);
        assert_eq!(ResponseCode::from(207), ResponseCode::ConsumeMsgTimeout);
        assert_eq!(ResponseCode::from(208), ResponseCode::NoMessage);
        assert_eq!(ResponseCode::from(209), ResponseCode::PollingFull);
        assert_eq!(ResponseCode::from(210), ResponseCode::PollingTimeout);
        assert_eq!(ResponseCode::from(211), ResponseCode::BrokerNotExist);
        assert_eq!(ResponseCode::from(212), ResponseCode::BrokerDispatchNotComplete);
        assert_eq!(ResponseCode::from(213), ResponseCode::BroadcastConsumption);
        assert_eq!(ResponseCode::from(215), ResponseCode::FlowControl);
        assert_eq!(ResponseCode::from(501), ResponseCode::NotLeaderForQueue);
        assert_eq!(ResponseCode::from(604), ResponseCode::IllegalOperation);
        assert_eq!(ResponseCode::from(-1000), ResponseCode::RpcUnknown);
        assert_eq!(ResponseCode::from(-1002), ResponseCode::RpcAddrIsNull);
        assert_eq!(ResponseCode::from(-1004), ResponseCode::RpcSendToChannelFailed);
        assert_eq!(ResponseCode::from(-1006), ResponseCode::RpcTimeOut);
        assert_eq!(ResponseCode::from(1500), ResponseCode::GoAway);
        assert_eq!(ResponseCode::from(2000), ResponseCode::ControllerFencedMasterEpoch);
        assert_eq!(
            ResponseCode::from(2001),
            ResponseCode::ControllerFencedSyncStateSetEpoch
        );
        assert_eq!(ResponseCode::from(2002), ResponseCode::ControllerInvalidMaster);
        assert_eq!(ResponseCode::from(2003), ResponseCode::ControllerInvalidReplicas);
        assert_eq!(ResponseCode::from(2004), ResponseCode::ControllerMasterNotAvailable);
        assert_eq!(ResponseCode::from(2005), ResponseCode::ControllerInvalidRequest);
        assert_eq!(ResponseCode::from(2006), ResponseCode::ControllerBrokerNotAlive);
        assert_eq!(ResponseCode::from(2007), ResponseCode::ControllerNotLeader);
        assert_eq!(ResponseCode::from(2008), ResponseCode::ControllerBrokerMetadataNotExist);
        assert_eq!(
            ResponseCode::from(2009),
            ResponseCode::ControllerInvalidCleanBrokerMetadata
        );
        assert_eq!(
            ResponseCode::from(2010),
            ResponseCode::ControllerBrokerNeedToBeRegistered
        );
        assert_eq!(ResponseCode::from(2011), ResponseCode::ControllerMasterStillExist);
        assert_eq!(ResponseCode::from(2012), ResponseCode::ControllerElectMasterFailed);
        assert_eq!(
            ResponseCode::from(2013),
            ResponseCode::ControllerAlterSyncStateSetFailed
        );
        assert_eq!(ResponseCode::from(2014), ResponseCode::ControllerBrokerIdInvalid);
        assert_eq!(ResponseCode::from(9999), ResponseCode::SystemError); // Edge case - unknown
                                                                         // defaults to SystemError
    }

    #[test]
    fn test_response_code_is_success() {
        assert!(ResponseCode::Success.is_success());
        assert!(!ResponseCode::SystemError.is_success());
        assert!(!ResponseCode::FlushDiskTimeout.is_success());
        assert!(!ResponseCode::RpcUnknown.is_success());
    }

    #[test]
    fn test_response_code_is_error() {
        assert!(!ResponseCode::Success.is_error());
        assert!(ResponseCode::SystemError.is_error());
        assert!(ResponseCode::SystemBusy.is_error());
        assert!(ResponseCode::TopicNotExist.is_error());
        assert!(ResponseCode::RpcTimeOut.is_error());
    }

    #[test]
    fn test_response_code_round_trip() {
        let codes = vec![
            ResponseCode::Success,
            ResponseCode::SystemError,
            ResponseCode::FlushDiskTimeout,
            ResponseCode::NoPermission,
            ResponseCode::TransactionShouldCommit,
            ResponseCode::RpcUnknown,
            ResponseCode::ControllerNotLeader,
        ];

        for code in codes {
            let i32_val = code.to_i32();
            let converted_back = ResponseCode::from(i32_val);
            assert_eq!(code, converted_back, "Round trip failed for {:?}", code);
        }
    }

    #[test]
    fn test_response_code_from_trait() {
        let code: i32 = ResponseCode::Success.into();
        assert_eq!(code, 0);

        let code: i32 = ResponseCode::SystemError.into();
        assert_eq!(code, 1);

        let code: i32 = ResponseCode::RpcUnknown.into();
        assert_eq!(code, -1000);
    }

    #[test]
    fn test_response_code_const_fn() {
        const SUCCESS_CODE: i32 = ResponseCode::Success.to_i32();
        assert_eq!(SUCCESS_CODE, 0);

        const ERROR_CODE: i32 = ResponseCode::SystemError.to_i32();
        assert_eq!(ERROR_CODE, 1);
    }

    #[test]
    fn test_response_code_derive_traits() {
        // Test Debug
        let code = ResponseCode::Success;
        assert_eq!(format!("{:?}", code), "Success");

        // Test Clone and Copy
        let code1 = ResponseCode::SystemError;
        let code2 = code1;
        assert_eq!(code1, code2);

        // Test PartialEq and Eq
        assert_eq!(ResponseCode::Success, ResponseCode::Success);
        assert_ne!(ResponseCode::Success, ResponseCode::SystemError);

        // Test Hash
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(ResponseCode::Success);
        set.insert(ResponseCode::Success); // Duplicate
        set.insert(ResponseCode::SystemError);
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_response_code_repr_i32_size() {
        use std::mem::size_of;
        assert_eq!(size_of::<ResponseCode>(), size_of::<i32>());
        assert_eq!(size_of::<RemotingSysResponseCode>(), size_of::<i32>());
    }

    #[test]
    fn test_negative_response_codes() {
        // Test RPC error codes (negative values)
        assert_eq!(ResponseCode::from(-1000), ResponseCode::RpcUnknown);
        assert_eq!(ResponseCode::from(-1002), ResponseCode::RpcAddrIsNull);
        assert_eq!(ResponseCode::from(-1004), ResponseCode::RpcSendToChannelFailed);
        assert_eq!(ResponseCode::from(-1006), ResponseCode::RpcTimeOut);

        assert_eq!(ResponseCode::RpcUnknown.to_i32(), -1000);
        assert_eq!(ResponseCode::RpcAddrIsNull.to_i32(), -1002);
    }

    #[test]
    fn test_controller_response_codes() {
        // Test Controller error codes (2000-2014)
        assert_eq!(ResponseCode::from(2000), ResponseCode::ControllerFencedMasterEpoch);
        assert_eq!(ResponseCode::from(2007), ResponseCode::ControllerNotLeader);
        assert_eq!(ResponseCode::from(2014), ResponseCode::ControllerBrokerIdInvalid);

        assert_eq!(ResponseCode::ControllerFencedMasterEpoch.to_i32(), 2000);
        assert_eq!(ResponseCode::ControllerNotLeader.to_i32(), 2007);
    }
}
