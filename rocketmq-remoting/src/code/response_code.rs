/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#[derive(Debug, PartialEq, Copy, Clone)]
pub enum RemotingSysResponseCode {
    Success = 0,
    SystemError = 1,
    SystemBusy = 2,
    RequestCodeNotSupported = 3,
    TransactionFailed = 4,
    NoPermission = 16,
}

impl From<RemotingSysResponseCode> for i32 {
    fn from(value: RemotingSysResponseCode) -> Self {
        value as i32
    }
}

impl From<i32> for RemotingSysResponseCode {
    fn from(value: i32) -> Self {
        match value {
            0 => RemotingSysResponseCode::Success,
            1 => RemotingSysResponseCode::SystemError,
            2 => RemotingSysResponseCode::SystemBusy,
            3 => RemotingSysResponseCode::RequestCodeNotSupported,
            4 => RemotingSysResponseCode::TransactionFailed,
            16 => RemotingSysResponseCode::NoPermission,
            _ => RemotingSysResponseCode::SystemError,
        }
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
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
}

impl From<ResponseCode> for i32 {
    fn from(value: ResponseCode) -> Self {
        value as i32
    }
}

impl From<i32> for ResponseCode {
    fn from(value: i32) -> Self {
        match value {
            0 => ResponseCode::Success,
            1 => ResponseCode::SystemError,
            2 => ResponseCode::SystemBusy,
            3 => ResponseCode::RequestCodeNotSupported,
            4 => ResponseCode::TransactionFailed,
            10 => ResponseCode::FlushDiskTimeout,
            11 => ResponseCode::SlaveNotAvailable,
            12 => ResponseCode::FlushSlaveTimeout,
            13 => ResponseCode::MessageIllegal,
            14 => ResponseCode::ServiceNotAvailable,
            15 => ResponseCode::VersionNotSupported,
            16 => ResponseCode::NoPermission,
            17 => ResponseCode::TopicNotExist,
            18 => ResponseCode::TopicExistAlready,
            19 => ResponseCode::PullNotFound,
            20 => ResponseCode::PullRetryImmediately,
            21 => ResponseCode::PullOffsetMoved,
            22 => ResponseCode::QueryNotFound,
            23 => ResponseCode::SubscriptionParseFailed,
            24 => ResponseCode::SubscriptionNotExist,
            25 => ResponseCode::SubscriptionNotLatest,
            26 => ResponseCode::SubscriptionGroupNotExist,
            27 => ResponseCode::FilterDataNotExist,
            28 => ResponseCode::FilterDataNotLatest,
            200 => ResponseCode::TransactionShouldCommit,
            201 => ResponseCode::TransactionShouldRollback,
            202 => ResponseCode::TransactionStateUnknow,
            203 => ResponseCode::TransactionStateGroupWrong,
            204 => ResponseCode::NoBuyerId,
            205 => ResponseCode::NotInCurrentUnit,
            206 => ResponseCode::ConsumerNotOnline,
            207 => ResponseCode::ConsumeMsgTimeout,
            208 => ResponseCode::NoMessage,
            209 => ResponseCode::PollingFull,
            210 => ResponseCode::PollingTimeout,
            211 => ResponseCode::BrokerNotExist,
            212 => ResponseCode::BrokerDispatchNotComplete,
            213 => ResponseCode::BroadcastConsumption,
            215 => ResponseCode::FlowControl,
            501 => ResponseCode::NotLeaderForQueue,
            604 => ResponseCode::IllegalOperation,
            -1000 => ResponseCode::RpcUnknown,
            -1002 => ResponseCode::RpcAddrIsNull,
            -1004 => ResponseCode::RpcSendToChannelFailed,
            -1006 => ResponseCode::RpcTimeOut,
            1500 => ResponseCode::GoAway,
            2000 => ResponseCode::ControllerFencedMasterEpoch,
            2001 => ResponseCode::ControllerFencedSyncStateSetEpoch,
            2002 => ResponseCode::ControllerInvalidMaster,
            2003 => ResponseCode::ControllerInvalidReplicas,
            2004 => ResponseCode::ControllerMasterNotAvailable,
            2005 => ResponseCode::ControllerInvalidRequest,
            2006 => ResponseCode::ControllerBrokerNotAlive,
            2007 => ResponseCode::ControllerNotLeader,
            2008 => ResponseCode::ControllerBrokerMetadataNotExist,
            2009 => ResponseCode::ControllerInvalidCleanBrokerMetadata,
            2010 => ResponseCode::ControllerBrokerNeedToBeRegistered,
            2011 => ResponseCode::ControllerMasterStillExist,
            2012 => ResponseCode::ControllerElectMasterFailed,
            2013 => ResponseCode::ControllerAlterSyncStateSetFailed,
            2014 => ResponseCode::ControllerBrokerIdInvalid,
            _ => ResponseCode::SystemError,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remoting_sys_response_code_from_i32() {
        assert_eq!(
            RemotingSysResponseCode::from(0),
            RemotingSysResponseCode::Success
        );
        assert_eq!(
            RemotingSysResponseCode::from(1),
            RemotingSysResponseCode::SystemError
        );
        assert_eq!(
            RemotingSysResponseCode::from(2),
            RemotingSysResponseCode::SystemBusy
        );
        assert_eq!(
            RemotingSysResponseCode::from(3),
            RemotingSysResponseCode::RequestCodeNotSupported
        );
        assert_eq!(
            RemotingSysResponseCode::from(4),
            RemotingSysResponseCode::TransactionFailed
        );
        assert_eq!(
            RemotingSysResponseCode::from(999),
            RemotingSysResponseCode::SystemError
        ); // Edge case
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
        assert_eq!(
            ResponseCode::from(23),
            ResponseCode::SubscriptionParseFailed
        );
        assert_eq!(ResponseCode::from(24), ResponseCode::SubscriptionNotExist);
        assert_eq!(ResponseCode::from(25), ResponseCode::SubscriptionNotLatest);
        assert_eq!(
            ResponseCode::from(26),
            ResponseCode::SubscriptionGroupNotExist
        );
        assert_eq!(ResponseCode::from(27), ResponseCode::FilterDataNotExist);
        assert_eq!(ResponseCode::from(28), ResponseCode::FilterDataNotLatest);
        assert_eq!(
            ResponseCode::from(200),
            ResponseCode::TransactionShouldCommit
        );
        assert_eq!(
            ResponseCode::from(201),
            ResponseCode::TransactionShouldRollback
        );
        assert_eq!(
            ResponseCode::from(202),
            ResponseCode::TransactionStateUnknow
        );
        assert_eq!(
            ResponseCode::from(203),
            ResponseCode::TransactionStateGroupWrong
        );
        assert_eq!(ResponseCode::from(204), ResponseCode::NoBuyerId);
        assert_eq!(ResponseCode::from(205), ResponseCode::NotInCurrentUnit);
        assert_eq!(ResponseCode::from(206), ResponseCode::ConsumerNotOnline);
        assert_eq!(ResponseCode::from(207), ResponseCode::ConsumeMsgTimeout);
        assert_eq!(ResponseCode::from(208), ResponseCode::NoMessage);
        assert_eq!(ResponseCode::from(209), ResponseCode::PollingFull);
        assert_eq!(ResponseCode::from(210), ResponseCode::PollingTimeout);
        assert_eq!(ResponseCode::from(211), ResponseCode::BrokerNotExist);
        assert_eq!(
            ResponseCode::from(212),
            ResponseCode::BrokerDispatchNotComplete
        );
        assert_eq!(ResponseCode::from(213), ResponseCode::BroadcastConsumption);
        assert_eq!(ResponseCode::from(215), ResponseCode::FlowControl);
        assert_eq!(ResponseCode::from(501), ResponseCode::NotLeaderForQueue);
        assert_eq!(ResponseCode::from(604), ResponseCode::IllegalOperation);
        assert_eq!(ResponseCode::from(-1000), ResponseCode::RpcUnknown);
        assert_eq!(ResponseCode::from(-1002), ResponseCode::RpcAddrIsNull);
        assert_eq!(
            ResponseCode::from(-1004),
            ResponseCode::RpcSendToChannelFailed
        );
        assert_eq!(ResponseCode::from(-1006), ResponseCode::RpcTimeOut);
        assert_eq!(ResponseCode::from(1500), ResponseCode::GoAway);
        assert_eq!(
            ResponseCode::from(2000),
            ResponseCode::ControllerFencedMasterEpoch
        );
        assert_eq!(
            ResponseCode::from(2001),
            ResponseCode::ControllerFencedSyncStateSetEpoch
        );
        assert_eq!(
            ResponseCode::from(2002),
            ResponseCode::ControllerInvalidMaster
        );
        assert_eq!(
            ResponseCode::from(2003),
            ResponseCode::ControllerInvalidReplicas
        );
        assert_eq!(
            ResponseCode::from(2004),
            ResponseCode::ControllerMasterNotAvailable
        );
        assert_eq!(
            ResponseCode::from(2005),
            ResponseCode::ControllerInvalidRequest
        );
        assert_eq!(
            ResponseCode::from(2006),
            ResponseCode::ControllerBrokerNotAlive
        );
        assert_eq!(ResponseCode::from(2007), ResponseCode::ControllerNotLeader);
        assert_eq!(
            ResponseCode::from(2008),
            ResponseCode::ControllerBrokerMetadataNotExist
        );
        assert_eq!(
            ResponseCode::from(2009),
            ResponseCode::ControllerInvalidCleanBrokerMetadata
        );
        assert_eq!(
            ResponseCode::from(2010),
            ResponseCode::ControllerBrokerNeedToBeRegistered
        );
        assert_eq!(
            ResponseCode::from(2011),
            ResponseCode::ControllerMasterStillExist
        );
        assert_eq!(
            ResponseCode::from(2012),
            ResponseCode::ControllerElectMasterFailed
        );
        assert_eq!(
            ResponseCode::from(2013),
            ResponseCode::ControllerAlterSyncStateSetFailed
        );
        assert_eq!(
            ResponseCode::from(2014),
            ResponseCode::ControllerBrokerIdInvalid
        );
        assert_eq!(ResponseCode::from(9999), ResponseCode::SystemError); // Edge case
    }
}
