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

use crate::base::message_status_enum::PutMessageStatus;

pub use rocketmq_store_local::commit_log::append::AppendMessageResult;

#[derive(Default, Clone)]
pub struct PutMessageResult {
    put_message_status: PutMessageStatus,
    append_message_result: Option<AppendMessageResult>,
    remote_put: bool,
}

impl PutMessageResult {
    #[inline]
    pub fn new(
        put_message_status: PutMessageStatus,
        append_message_result: Option<AppendMessageResult>,
        remote_put: bool,
    ) -> Self {
        Self {
            put_message_status,
            append_message_result,
            remote_put,
        }
    }

    #[inline]
    pub fn new_append_result(
        put_message_status: PutMessageStatus,
        append_message_result: Option<AppendMessageResult>,
    ) -> Self {
        Self {
            put_message_status,
            append_message_result,
            remote_put: false,
        }
    }

    #[inline]
    pub fn new_default(put_message_status: PutMessageStatus) -> Self {
        Self {
            put_message_status,
            append_message_result: None,
            remote_put: false,
        }
    }

    #[inline]
    pub fn put_message_status(&self) -> PutMessageStatus {
        self.put_message_status
    }

    #[inline]
    pub fn append_message_result(&self) -> Option<&AppendMessageResult> {
        self.append_message_result.as_ref()
    }

    #[inline]
    pub fn remote_put(&self) -> bool {
        self.remote_put
    }

    #[inline]
    pub fn set_put_message_status(&mut self, put_message_status: PutMessageStatus) {
        self.put_message_status = put_message_status;
    }

    #[inline]
    pub fn set_append_message_result(&mut self, append_message_result: Option<AppendMessageResult>) {
        self.append_message_result = append_message_result;
    }

    #[inline]
    pub fn set_remote_put(&mut self, remote_put: bool) {
        self.remote_put = remote_put;
    }

    #[inline]
    pub fn is_ok(&self) -> bool {
        if self.remote_put {
            self.put_message_status == PutMessageStatus::PutOk
                || self.put_message_status == PutMessageStatus::FlushDiskTimeout
                || self.put_message_status == PutMessageStatus::FlushSlaveTimeout
                || self.put_message_status == PutMessageStatus::SlaveNotAvailable
        } else {
            self.append_message_result.is_some() && self.append_message_result.as_ref().unwrap().is_ok()
        }
    }
}

#[cfg(test)]
mod put_message_result_tests {
    use super::*;
    use crate::base::message_status_enum::AppendMessageStatus;
    use crate::base::message_status_enum::PutMessageStatus;

    fn create_append_message_result(status: AppendMessageStatus) -> AppendMessageResult {
        AppendMessageResult {
            status,
            wrote_offset: 100,
            wrote_bytes: 50,
            msg_id: None,
            msg_id_supplier: None,
            store_timestamp: 1609459200000,
            logics_offset: 10,
            page_cache_rt: 5,
            msg_num: 1,
        }
    }

    #[test]
    fn is_ok_with_remote_put_and_put_ok_status() {
        let result = PutMessageResult::new(PutMessageStatus::PutOk, None, true);
        assert!(result.is_ok());
    }

    #[test]
    fn is_ok_with_remote_put_and_flush_disk_timeout_status() {
        let result = PutMessageResult::new(PutMessageStatus::FlushDiskTimeout, None, true);
        assert!(result.is_ok());
    }

    #[test]
    fn is_ok_with_remote_put_and_flush_slave_timeout_status() {
        let result = PutMessageResult::new(PutMessageStatus::FlushSlaveTimeout, None, true);
        assert!(result.is_ok());
    }

    #[test]
    fn is_ok_with_remote_put_and_slave_not_available_status() {
        let result = PutMessageResult::new(PutMessageStatus::SlaveNotAvailable, None, true);
        assert!(result.is_ok());
    }

    #[test]
    fn is_ok_with_append_result_ok() {
        let append_result = Some(create_append_message_result(AppendMessageStatus::PutOk));
        let result = PutMessageResult::new(PutMessageStatus::PutOk, append_result, false);
        assert!(result.is_ok());
    }

    #[test]
    fn is_not_ok_with_remote_put_and_other_status() {
        let result = PutMessageResult::new(PutMessageStatus::CreateMappedFileFailed, None, true);
        assert!(!result.is_ok());
    }

    #[test]
    fn is_not_ok_without_append_result() {
        let result = PutMessageResult::new_default(PutMessageStatus::PutOk);
        assert!(!result.is_ok());
    }

    #[test]
    fn is_not_ok_with_append_result_not_ok() {
        let append_result = Some(create_append_message_result(AppendMessageStatus::EndOfFile));
        let result = PutMessageResult::new(PutMessageStatus::PutOk, append_result, false);
        assert!(!result.is_ok());
    }
}
