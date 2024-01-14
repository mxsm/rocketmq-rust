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
#[derive(Debug, PartialEq, Clone, Copy, Hash)]
pub enum RequestCode {
    SendMessage = 10,
    PullMessage = 11,
    QueryMessage = 12,
    QueryBrokerOffset = 13,
    QueryConsumerOffset = 14,
    UpdateConsumerOffset = 15,
    UpdateAndCreateTopic = 17,
    GetAllTopicConfig = 21,
    GetTopicConfigList = 22,
    GetTopicNameList = 23,
    UpdateBrokerConfig = 25,
    GetBrokerConfig = 26,
    TriggerDeleteFiles = 27,
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
    GetClientConfig = 47,
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
    SuspendConsumer = 209,
    ResumeConsumer = 210,
    ResetConsumerOffsetInConsumer = 211,
    ResetConsumerOffsetInBroker = 212,
    AdjustConsumerThreadPool = 213,
    WhoConsumeTheMessage = 214,
    DeleteTopicInBroker = 215,
    DeleteTopicInNamesrv = 216,
    RegisterTopicInNamesrv = 217,
    GetKvlistByNamespace = 219,
    ResetConsumerClientOffset = 220,
    GetConsumerStatusFromClient = 221,
    InvokeBrokerToResetOffset = 222,
    InvokeBrokerToGetConsumerStatus = 223,
    QueryTopicConsumeByWho = 300,
    GetTopicsByCluster = 224,
    QueryTopicsByConsumer = 343,
    QuerySubscriptionByConsumer = 345,
    RegisterFilterServer = 301,
    RegisterMessageFilterClass = 302,
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
    LitePullMessage = 361,
    QueryAssignment = 400,
    SetMessageRequestMode = 401,
    GetAllMessageRequestMode = 402,
    UpdateAndCreateStaticTopic = 513,
    GetBrokerMemberGroup = 901,
    AddBroker = 902,
    RemoveBroker = 903,
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
}

impl From<RequestCode> for i32 {
    fn from(value: RequestCode) -> Self {
        value as i32
    }
}

impl RequestCode {
    pub fn to_i32(self) -> i32 {
        self.into()
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum ControllerRequestCode {
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
}
