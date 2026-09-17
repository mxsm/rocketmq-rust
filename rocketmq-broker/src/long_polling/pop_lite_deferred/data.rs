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

use std::num::NonZeroUsize;
use std::time::Duration;

use rocketmq_protocol::protocol::header::pop_lite_message_request_header::PopLiteMessageRequestHeader;

use crate::config::broker_config::BrokerConfig;

use super::deadline::PopLiteWaitDeadline;
use super::deadline::DEFAULT_POP_LITE_MAX_AGE;
use super::index::PopLiteIndexLease;
use super::index::PopLiteIndexLimits;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PopLiteDeferredPolicy {
    pub(crate) index_limits: PopLiteIndexLimits,
    pub(crate) max_age: Duration,
}

impl PopLiteDeferredPolicy {
    pub(crate) fn from_config(config: &BrokerConfig) -> Option<Self> {
        let max_entries = usize::try_from(config.max_pop_polling_size).unwrap_or(usize::MAX);
        Some(Self {
            index_limits: PopLiteIndexLimits::new(
                NonZeroUsize::new(max_entries)?,
                NonZeroUsize::new(config.pop_polling_map_size)?,
                NonZeroUsize::new(config.pop_polling_size)?,
            ),
            max_age: DEFAULT_POP_LITE_MAX_AGE,
        })
    }
}

/// Typed PopLite request ownership retained without a command or connection.
pub(crate) struct PopLiteRequestData {
    header: PopLiteMessageRequestHeader,
}

impl PopLiteRequestData {
    pub(crate) const fn new(header: PopLiteMessageRequestHeader) -> Self {
        Self { header }
    }

    pub(crate) const fn header(&self) -> &PopLiteMessageRequestHeader {
        &self.header
    }

    pub(crate) const fn client_id(&self) -> &cheetah_string::CheetahString {
        &self.header.client_id
    }

    pub(crate) fn try_estimated_dynamic_bytes(&self) -> Option<usize> {
        let mut total = [
            self.header.client_id.len(),
            self.header.consumer_group.len(),
            self.header.topic.len(),
        ]
        .into_iter()
        .try_fold(0usize, usize::checked_add)?;
        if let Some(attempt_id) = &self.header.attempt_id {
            total = total.checked_add(attempt_id.len())?;
        }
        if let Some(rpc) = &self.header.rpc {
            for value in [rpc.namespace.as_ref(), rpc.broker_name.as_ref()].into_iter().flatten() {
                total = total.checked_add(value.len())?;
            }
        }
        Some(total)
    }

    pub(crate) fn into_header(self) -> PopLiteMessageRequestHeader {
        self.header
    }
}

/// Affine PopLite business ownership moved from registry claim into resume.
#[must_use]
pub(crate) struct ResumePopLite {
    request: PopLiteRequestData,
    wait_deadline: PopLiteWaitDeadline,
    index_lease: Option<PopLiteIndexLease>,
}

impl ResumePopLite {
    pub(super) fn new(
        request: PopLiteRequestData,
        wait_deadline: PopLiteWaitDeadline,
        index_lease: PopLiteIndexLease,
    ) -> Self {
        Self {
            request,
            wait_deadline,
            index_lease: Some(index_lease),
        }
    }

    pub(crate) const fn request(&self) -> &PopLiteRequestData {
        &self.request
    }

    pub(crate) const fn wait_deadline(&self) -> PopLiteWaitDeadline {
        self.wait_deadline
    }

    pub(super) fn take_index_lease(&mut self) -> Option<PopLiteIndexLease> {
        self.index_lease.take()
    }

    pub(crate) fn into_request(mut self) -> PopLiteRequestData {
        drop(self.index_lease.take());
        self.request
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_protocol::protocol::header::pop_lite_message_request_header::PopLiteMessageRequestHeader;
    use rocketmq_protocol::rpc::rpc_request_header::RpcRequestHeader;

    use super::PopLiteRequestData;

    fn request_data(client_id: &str, group: &str, topic: &str) -> PopLiteRequestData {
        PopLiteRequestData::new(PopLiteMessageRequestHeader {
            client_id: client_id.into(),
            consumer_group: group.into(),
            topic: topic.into(),
            ..Default::default()
        })
    }

    #[test]
    fn pop_lite_request_data_counts_required_string_bytes() {
        let data = request_data("client", "group-a", "orders");

        assert_eq!(data.try_estimated_dynamic_bytes(), Some(6 + 7 + 6));
    }

    #[test]
    fn pop_lite_request_data_counts_attempt_and_rpc_fields_exactly() {
        let mut header = PopLiteMessageRequestHeader {
            client_id: "client".into(),
            consumer_group: "group".into(),
            topic: "topic".into(),
            attempt_id: Some("attempt".into()),
            rpc: Some(RpcRequestHeader::new(
                Some("namespace".into()),
                None,
                Some("broker-a".into()),
                None,
            )),
            ..Default::default()
        };

        let all_present = PopLiteRequestData::new(header.clone());
        assert_eq!(all_present.try_estimated_dynamic_bytes(), Some(6 + 5 + 5 + 7 + 9 + 8));

        header.rpc = Some(RpcRequestHeader::new(None, None, Some("broker-a".into()), None));
        let namespace_absent = PopLiteRequestData::new(header.clone());
        assert_eq!(namespace_absent.try_estimated_dynamic_bytes(), Some(6 + 5 + 5 + 7 + 8));

        header.rpc = Some(RpcRequestHeader::new(Some("namespace".into()), None, None, None));
        let broker_absent = PopLiteRequestData::new(header);
        assert_eq!(broker_absent.try_estimated_dynamic_bytes(), Some(6 + 5 + 5 + 7 + 9));
    }

    #[test]
    fn pop_lite_request_data_empty_strings_are_measurable() {
        let data = request_data("", "", "");

        assert_eq!(data.try_estimated_dynamic_bytes(), Some(0));
        assert!(data.try_estimated_dynamic_bytes().is_some());
    }
}
