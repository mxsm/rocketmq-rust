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
        // Compatible with Java enums
        match self {
            PutMessageStatus::PutOk => write!(f, "PUT_OK"),
            PutMessageStatus::FlushDiskTimeout => write!(f, "FLUSH_DISK_TIMEOUT"),
            PutMessageStatus::FlushSlaveTimeout => write!(f, "FLUSH_SLAVE_TIMEOUT"),
            PutMessageStatus::SlaveNotAvailable => write!(f, "SLAVE_NOT_AVAILABLE"),
            PutMessageStatus::ServiceNotAvailable => write!(f, "SERVICE_NOT_AVAILABLE"),
            PutMessageStatus::CreateMappedFileFailed => write!(f, "CREATE_MAPPED_FILE_FAILED"),
            PutMessageStatus::MessageIllegal => write!(f, "MESSAGE_ILLEGAL"),
            PutMessageStatus::PropertiesSizeExceeded => write!(f, "PROPERTIES_SIZE_EXCEEDED"),
            PutMessageStatus::OsPageCacheBusy => write!(f, "OS_PAGE_CACHE_BUSY"),
            PutMessageStatus::UnknownError => write!(f, "UNKNOWN_ERROR"),
            PutMessageStatus::InSyncReplicasNotEnough => write!(f, "IN_SYNC_REPLICAS_NOT_ENOUGH"),
            PutMessageStatus::PutToRemoteBrokerFail => write!(f, "PUT_TO_REMOTE_BROKER_FAIL"),
            PutMessageStatus::LmqConsumeQueueNumExceeded => {
                write!(f, "LMQ_CONSUME_QUEUE_NUM_EXCEEDED")
            }
            PutMessageStatus::WheelTimerFlowControl => write!(f, "WHEEL_TIMER_FLOW_CONTROL"),
            PutMessageStatus::WheelTimerMsgIllegal => write!(f, "WHEEL_TIMER_MSG_ILLEGAL"),
            PutMessageStatus::WheelTimerNotEnable => write!(f, "WHEEL_TIMER_NOT_ENABLE"),
        }
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
        // Compatible with Java enums
        match self {
            GetMessageStatus::Found => write!(f, "FOUND"),
            GetMessageStatus::NoMatchedMessage => write!(f, "NO_MATCHED_MESSAGE"),
            GetMessageStatus::MessageWasRemoving => write!(f, "MESSAGE_WAS_REMOVING"),
            GetMessageStatus::OffsetFoundNull => write!(f, "OFFSET_FOUND_NULL"),
            GetMessageStatus::OffsetOverflowBadly => write!(f, "OFFSET_OVERFLOW_BADLY"),
            GetMessageStatus::OffsetOverflowOne => write!(f, "OFFSET_OVERFLOW_ONE"),
            GetMessageStatus::OffsetTooSmall => write!(f, "OFFSET_TOO_SMALL"),
            GetMessageStatus::NoMatchedLogicQueue => write!(f, "NO_MATCHED_LOGIC_QUEUE"),
            GetMessageStatus::NoMessageInQueue => write!(f, "NO_MESSAGE_IN_QUEUE"),
            GetMessageStatus::OffsetReset => write!(f, "OFFSET_RESET"),
        }
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
        assert_eq!(AppendMessageStatus::UnknownError.to_string(), "UNKNOWN_ERROR");
    }

    #[test]
    fn put_message_status_display_put_ok() {
        assert_eq!(PutMessageStatus::PutOk.to_string(), "PUT_OK");
    }

    #[test]
    fn get_message_status_display_found() {
        assert_eq!(GetMessageStatus::Found.to_string(), "FOUND");
    }
}
