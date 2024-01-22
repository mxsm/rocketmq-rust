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
            _ => RemotingSysResponseCode::SystemError,
        }
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum ResponseCode {
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
/*impl Into<i32> for ResponseCode {
    fn into(self) -> i32 {
        match self {
            ResponseCode::FlushDiskTimeout => 10,
            ResponseCode::SlaveNotAvailable => 11,
            ResponseCode::FlushSlaveTimeout => 12,
            ResponseCode::MessageIllegal => 13,
            ResponseCode::ServiceNotAvailable => 14,
            ResponseCode::VersionNotSupported => 15,
            ResponseCode::NoPermission => 16,
            ResponseCode::TopicNotExist => 17,
            ResponseCode::TopicExistAlready => 18,
            ResponseCode::PullNotFound => 19,
            ResponseCode::PullRetryImmediately => 20,
            ResponseCode::PullOffsetMoved => 21,
            ResponseCode::QueryNotFound => 22,
            ResponseCode::SubscriptionParseFailed => 23,
            ResponseCode::SubscriptionNotExist => 24,
            ResponseCode::SubscriptionNotLatest => 25,
            ResponseCode::SubscriptionGroupNotExist => 26,
            ResponseCode::FilterDataNotExist => 27,
            ResponseCode::FilterDataNotLatest => 28,
            ResponseCode::TransactionShouldCommit => 200,
            ResponseCode::TransactionShouldRollback => 201,
            ResponseCode::TransactionStateUnknow => 202,
            ResponseCode::TransactionStateGroupWrong => 203,
            ResponseCode::NoBuyerId => 204,
            ResponseCode::NotInCurrentUnit => 205,
            ResponseCode::ConsumerNotOnline => 206,
            ResponseCode::ConsumeMsgTimeout => 207,
            ResponseCode::NoMessage => 208,
            ResponseCode::PollingFull => 209,
            ResponseCode::PollingTimeout => 210,
            ResponseCode::BrokerNotExist => 211,
            ResponseCode::BrokerDispatchNotComplete => 212,
            ResponseCode::BroadcastConsumption => 213,
            ResponseCode::FlowControl => 215,
            ResponseCode::NotLeaderForQueue => 501,
            ResponseCode::IllegalOperation => 604,
            ResponseCode::RpcUnknown => -1000,
            ResponseCode::RpcAddrIsNull => -1002,
            ResponseCode::RpcSendToChannelFailed => -1004,
            ResponseCode::RpcTimeOut => -1006,
            ResponseCode::GoAway => 1500,
            ResponseCode::ControllerFencedMasterEpoch => 2000,
            ResponseCode::ControllerFencedSyncStateSetEpoch => 2001,
            ResponseCode::ControllerInvalidMaster => 2002,
            ResponseCode::ControllerInvalidReplicas => 2003,
            ResponseCode::ControllerMasterNotAvailable => 2004,
            ResponseCode::ControllerInvalidRequest => 2005,
            ResponseCode::ControllerBrokerNotAlive => 2006,
            ResponseCode::ControllerNotLeader => 2007,
            ResponseCode::ControllerBrokerMetadataNotExist => 2008,
            ResponseCode::ControllerInvalidCleanBrokerMetadata => 2009,
            ResponseCode::ControllerBrokerNeedToBeRegistered => 2010,
            ResponseCode::ControllerMasterStillExist => 2011,
            ResponseCode::ControllerElectMasterFailed => 2012,
            ResponseCode::ControllerAlterSyncStateSetFailed => 2013,
            ResponseCode::ControllerBrokerIdInvalid => 2014,
        }
    }
}*/

impl From<ResponseCode> for i32 {
    fn from(value: ResponseCode) -> Self {
        value as i32
    }
}
