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

use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_rust::ArcMut;

use crate::consumer::pull_status::PullStatus;

/// Owned, runtime-neutral pull result used across crate boundaries.
pub type PullOutcome = rocketmq_model::result::PullOutcome<MessageExt>;

/// Failure converting an owned pull outcome back into the legacy shared-mutable result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PullOutcomeAdapterError {
    /// A non-empty owned batch cannot become `ArcMut` messages without introducing new unsafe
    /// aliasing.
    SharedMutableMessagesUnsupported { message_count: usize },
}

impl std::fmt::Display for PullOutcomeAdapterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SharedMutableMessagesUnsupported { message_count } => write!(
                f,
                "cannot convert {message_count} owned messages into the legacy shared-mutable PullResult"
            ),
        }
    }
}

impl std::error::Error for PullOutcomeAdapterError {}

pub struct PullResult {
    pub(crate) pull_status: PullStatus,
    pub(crate) next_begin_offset: u64,
    pub(crate) min_offset: u64,
    pub(crate) max_offset: u64,
    pub(crate) msg_found_list: Option<Vec<ArcMut<MessageExt>>>,
}

impl PullResult {
    pub fn new(
        pull_status: PullStatus,
        next_begin_offset: u64,
        min_offset: u64,
        max_offset: u64,
        msg_found_list: Option<Vec<ArcMut<MessageExt>>>,
    ) -> Self {
        Self {
            pull_status,
            next_begin_offset,
            min_offset,
            max_offset,
            msg_found_list,
        }
    }

    #[inline]
    pub fn pull_status(&self) -> &PullStatus {
        &self.pull_status
    }

    #[inline]
    pub fn next_begin_offset(&self) -> u64 {
        self.next_begin_offset
    }

    #[inline]
    pub fn min_offset(&self) -> u64 {
        self.min_offset
    }

    #[inline]
    pub fn max_offset(&self) -> u64 {
        self.max_offset
    }

    #[inline]
    pub fn msg_found_list(&self) -> Option<&Vec<ArcMut<MessageExt>>> {
        self.msg_found_list.as_ref()
    }

    #[inline]
    pub fn set_msg_found_list(&mut self, msg_found_list: Option<Vec<ArcMut<MessageExt>>>) {
        self.msg_found_list = msg_found_list;
    }
}

impl From<&PullResult> for PullOutcome {
    fn from(value: &PullResult) -> Self {
        let messages = value
            .msg_found_list
            .as_ref()
            .map(|messages| messages.iter().map(|message| (**message).clone()).collect());
        Self::new(
            value.pull_status,
            value.next_begin_offset,
            value.min_offset,
            value.max_offset,
            messages,
        )
    }
}

impl TryFrom<PullOutcome> for PullResult {
    type Error = PullOutcomeAdapterError;

    fn try_from(value: PullOutcome) -> Result<Self, Self::Error> {
        let pull_status = value.pull_status();
        let next_begin_offset = value.next_begin_offset();
        let min_offset = value.min_offset();
        let max_offset = value.max_offset();
        let messages = match value.into_messages() {
            None => None,
            Some(messages) if messages.is_empty() => Some(Vec::new()),
            Some(messages) => {
                return Err(PullOutcomeAdapterError::SharedMutableMessagesUnsupported {
                    message_count: messages.len(),
                });
            }
        };
        Ok(Self::new(
            pull_status,
            next_begin_offset,
            min_offset,
            max_offset,
            messages,
        ))
    }
}

impl std::fmt::Display for PullResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PullResult [pullStatus={}, nextBeginOffset={}, minOffset={}, maxOffset={}, msgFoundList={}]",
            self.pull_status,
            self.next_begin_offset,
            self.min_offset,
            self.max_offset,
            self.msg_found_list.as_ref().map_or(0, |v| v.len()),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pull_result_display_matches_java_to_string_shape() {
        let pull_result = PullResult::new(PullStatus::NoNewMsg, 12, 3, 45, None);

        assert_eq!(
            pull_result.to_string(),
            "PullResult [pullStatus=NO_NEW_MSG, nextBeginOffset=12, minOffset=3, maxOffset=45, msgFoundList=0]"
        );
    }
}
