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

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum AppendMessageStatus {
    #[default]
    PutOk,
    EndOfFile,
    MessageSizeExceeded,
    PropertiesSizeExceeded,
    UnknownError,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum PutMessageStatus {
    #[default]
    PutOk,
    FlushDiskTimeout,
    FlushSlaveTimeout,
    SlaveNotAvailable,
    ServiceNotAvailable,
    CreateMappedFileFailed,
    MessageIllegal,
    PropertiesSizeExceeded,
    OsPageCacheBusy,
    UnknownError,
    InSyncReplicasNotEnough,
    PutToRemoteBrokerFail,
    LmqConsumeQueueNumExceeded,
    WheelTimerFlowControl,
    WheelTimerMsgIllegal,
    WheelTimerNotEnable,
}

impl std::fmt::Display for PutMessageStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Default)]
pub enum GetMessageStatus {
    #[default]
    Found,
    NoMatchedMessage,
    MessageWasRemoving,
    OffsetFoundNull,
    OffsetOverflowBadly,
    OffsetOverflowOne,
    OffsetTooSmall,
    NoMatchedLogicQueue,
    NoMessageInQueue,
    OffsetReset,
}

impl std::fmt::Display for GetMessageStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
