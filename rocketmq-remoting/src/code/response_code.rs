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
