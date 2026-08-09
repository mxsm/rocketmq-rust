// Copyright 2026 The RocketMQ Rust Authors
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

use crate::base::message_status_enum::PutMessageStatus;
use crate::timer::error::CorruptionReason;
use crate::timer::error::RetryClass;
use crate::timer::error::TimerWorkResult;

pub(crate) fn classify_delivery_status(status: PutMessageStatus) -> TimerWorkResult {
    match status {
        PutMessageStatus::PutOk
        | PutMessageStatus::FlushDiskTimeout
        | PutMessageStatus::FlushSlaveTimeout
        | PutMessageStatus::SlaveNotAvailable => TimerWorkResult::Complete,
        PutMessageStatus::ServiceNotAvailable
        | PutMessageStatus::CreateMappedFileFailed
        | PutMessageStatus::OsPageCacheBusy
        | PutMessageStatus::InSyncReplicasNotEnough
        | PutMessageStatus::PutToRemoteBrokerFail
        | PutMessageStatus::WheelTimerFlowControl => TimerWorkResult::Retry(RetryClass::DeliveryRejected),
        PutMessageStatus::MessageIllegal
        | PutMessageStatus::PropertiesSizeExceeded
        | PutMessageStatus::UnknownError
        | PutMessageStatus::LmqConsumeQueueNumExceeded
        | PutMessageStatus::WheelTimerMsgIllegal
        | PutMessageStatus::WheelTimerNotEnable => TimerWorkResult::Quarantine(CorruptionReason::UnsupportedRecord),
    }
}

pub(crate) fn delivery_shard(topic: &str, queue_id: i32, shard_count: usize) -> usize {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in topic.as_bytes().iter().chain(queue_id.to_be_bytes().iter()) {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    (hash as usize) % shard_count.max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timer_delivery_errors_have_one_explicit_policy() {
        assert_eq!(
            classify_delivery_status(PutMessageStatus::OsPageCacheBusy),
            TimerWorkResult::Retry(RetryClass::DeliveryRejected)
        );
        assert_eq!(
            classify_delivery_status(PutMessageStatus::MessageIllegal),
            TimerWorkResult::Quarantine(CorruptionReason::UnsupportedRecord)
        );
    }

    #[test]
    fn timer_same_fifo_group_always_uses_the_same_shard() {
        assert_eq!(delivery_shard("orders", 7, 8), delivery_shard("orders", 7, 8));
    }
}
