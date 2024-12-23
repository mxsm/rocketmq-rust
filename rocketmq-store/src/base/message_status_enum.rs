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

impl std::fmt::Display for AppendMessageStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Compatible with Java enums
        match self {
            AppendMessageStatus::PutOk => write!(f, "PUT_OK"),
            AppendMessageStatus::EndOfFile => write!(f, "END_OF_FILE"),
            AppendMessageStatus::MessageSizeExceeded => write!(f, "MESSAGE_SIZE_EXCEEDED"),
            AppendMessageStatus::PropertiesSizeExceeded => write!(f, "PROPERTIES_SIZE_EXCEEDED"),
            AppendMessageStatus::UnknownError => write!(f, "UNKNOWN_ERROR"),
        }
    }
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

#[derive(Debug, Default, PartialEq, Clone, Copy)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_message_status_display_put_ok() {
        assert_eq!(AppendMessageStatus::PutOk.to_string(), "PUT_OK");
    }

    #[test]
    fn append_message_status_display_end_of_file() {
        assert_eq!(AppendMessageStatus::EndOfFile.to_string(), "END_OF_FILE");
    }

    #[test]
    fn append_message_status_display_message_size_exceeded() {
        assert_eq!(
            AppendMessageStatus::MessageSizeExceeded.to_string(),
            "MESSAGE_SIZE_EXCEEDED"
        );
    }

    #[test]
    fn append_message_status_display_properties_size_exceeded() {
        assert_eq!(
            AppendMessageStatus::PropertiesSizeExceeded.to_string(),
            "PROPERTIES_SIZE_EXCEEDED"
        );
    }

    #[test]
    fn append_message_status_display_unknown_error() {
        assert_eq!(
            AppendMessageStatus::UnknownError.to_string(),
            "UNKNOWN_ERROR"
        );
    }
}
