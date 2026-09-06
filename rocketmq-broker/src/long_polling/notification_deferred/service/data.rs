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

use std::net::SocketAddr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_protocol::protocol::header::notification_request_header::NotificationRequestHeader;
use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_store::ArcMessageFilter;
use rocketmq_transport::api::RemotingRequest;
use rocketmq_transport::api::RequestId;
use rocketmq_transport::api::SessionId;

use super::NotificationDeferredPrepareFailure;
use crate::long_polling::notification_deferred::deadline::NotificationWaitDeadline;
use crate::long_polling::notification_deferred::index::NotificationIndexLease;
use crate::long_polling::notification_deferred::index::NotificationIndexReservation;
use crate::long_polling::notification_deferred::index::NotificationMatchCriteria;

pub(crate) struct NotificationRequestData {
    pub(super) header: NotificationRequestHeader,
    effective_peer: SocketAddr,
}

impl NotificationRequestData {
    pub(crate) const fn new(header: NotificationRequestHeader, effective_peer: SocketAddr) -> Self {
        Self { header, effective_peer }
    }

    #[must_use]
    pub(crate) const fn header(&self) -> &NotificationRequestHeader {
        &self.header
    }

    #[must_use]
    pub(crate) const fn effective_peer(&self) -> SocketAddr {
        self.effective_peer
    }

    #[must_use]
    pub(crate) const fn topic(&self) -> &CheetahString {
        &self.header.topic
    }

    #[must_use]
    pub(crate) const fn consumer_group(&self) -> &CheetahString {
        &self.header.consumer_group
    }

    #[must_use]
    pub(crate) const fn queue_id(&self) -> i32 {
        self.header.queue_id
    }

    pub(crate) fn into_parts(self) -> (NotificationRequestHeader, SocketAddr) {
        (self.header, self.effective_peer)
    }

    pub(super) fn estimated_dynamic_bytes(&self) -> Result<usize, NotificationDeferredPrepareFailure> {
        let mut bytes = self
            .header
            .topic
            .len()
            .checked_add(self.header.consumer_group.len())
            .ok_or(NotificationDeferredPrepareFailure::RetainedSizeOverflow)?;
        for value in [
            self.header.attempt_id.as_ref(),
            self.header.exp_type.as_ref(),
            self.header.exp.as_ref(),
            self.header.client_id.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            bytes = bytes
                .checked_add(value.len())
                .ok_or(NotificationDeferredPrepareFailure::RetainedSizeOverflow)?;
        }
        if let Some(rpc) = self
            .header
            .topic_request_header
            .as_ref()
            .and_then(|topic| topic.rpc.as_ref())
        {
            for value in [rpc.namespace.as_ref(), rpc.broker_name.as_ref()].into_iter().flatten() {
                bytes = bytes
                    .checked_add(value.len())
                    .ok_or(NotificationDeferredPrepareFailure::RetainedSizeOverflow)?;
            }
        }
        Ok(bytes)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct PreparedRequestProvenance {
    request_id: RequestId,
    session_id: SessionId,
}

impl PreparedRequestProvenance {
    pub(super) fn capture(request: &RemotingRequest) -> Self {
        Self {
            request_id: request.original_identity().request_id(),
            session_id: request.session().id(),
        }
    }

    pub(super) fn matches(self, request: &RemotingRequest) -> bool {
        self.request_id == request.original_identity().request_id() && self.session_id == request.session().id()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct NotificationRetainedEstimate {
    pub(super) resume_bytes: usize,
    pub(super) filter_bytes: usize,
    pub(super) metadata_bytes: usize,
}

impl NotificationRetainedEstimate {
    pub(crate) const fn new(resume_bytes: usize, filter_bytes: usize, metadata_bytes: usize) -> Self {
        Self {
            resume_bytes,
            filter_bytes,
            metadata_bytes,
        }
    }
}

#[must_use]
pub(crate) struct ResumeNotification {
    request: NotificationRequestData,
    criteria: Arc<NotificationMatchCriteria>,
    wait_deadline: NotificationWaitDeadline,
    index_lease: Option<NotificationIndexLease>,
}

impl ResumeNotification {
    pub(super) fn new(
        request: NotificationRequestData,
        criteria: Arc<NotificationMatchCriteria>,
        wait_deadline: NotificationWaitDeadline,
        index_lease: NotificationIndexLease,
    ) -> Self {
        Self {
            request,
            criteria,
            wait_deadline,
            index_lease: Some(index_lease),
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        request: NotificationRequestData,
        criteria: Arc<NotificationMatchCriteria>,
        wait_deadline: NotificationWaitDeadline,
    ) -> Self {
        Self {
            request,
            criteria,
            wait_deadline,
            index_lease: None,
        }
    }

    #[must_use]
    pub(crate) const fn request(&self) -> &NotificationRequestData {
        &self.request
    }

    #[must_use]
    pub(crate) fn subscription(&self) -> Option<&SubscriptionData> {
        self.criteria.subscription()
    }

    #[must_use]
    pub(crate) fn filter(&self) -> Option<&ArcMessageFilter> {
        self.criteria.filter()
    }

    #[must_use]
    pub(crate) const fn wait_deadline(&self) -> NotificationWaitDeadline {
        self.wait_deadline
    }

    pub(super) fn take_index_lease(&mut self) -> Option<NotificationIndexLease> {
        self.index_lease.take()
    }

    pub(crate) fn into_execution_parts(
        self,
    ) -> (
        NotificationRequestData,
        Option<SubscriptionData>,
        Option<ArcMessageFilter>,
    ) {
        let subscription = self.criteria.subscription().cloned();
        let filter = self.criteria.filter().cloned();
        (self.request, subscription, filter)
    }
}

#[must_use]
pub(crate) struct PreparedNotificationRegistration {
    pub(super) request: NotificationRequestData,
    pub(super) criteria: Arc<NotificationMatchCriteria>,
    pub(super) deadline: NotificationWaitDeadline,
    pub(super) reservation: NotificationIndexReservation,
    pub(super) permit: rocketmq_transport::api::DeferredWaitPermit,
    pub(super) provenance: Option<PreparedRequestProvenance>,
    pub(super) observation: PreparedObservation,
}

impl PreparedNotificationRegistration {
    #[must_use]
    pub(crate) const fn deadline(&self) -> NotificationWaitDeadline {
        self.deadline
    }

    #[must_use]
    pub(crate) const fn retained_bytes(&self) -> usize {
        self.permit.retained_bytes()
    }
}

pub(super) struct PreparedObservation {
    counter: Arc<AtomicUsize>,
}

impl PreparedObservation {
    pub(super) fn new(counter: Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::AcqRel);
        Self { counter }
    }
}

impl Drop for PreparedObservation {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::AcqRel);
    }
}

pub(super) struct CounterObservation {
    counter: Arc<AtomicUsize>,
    bytes: Option<(Arc<AtomicUsize>, usize)>,
}

impl CounterObservation {
    pub(super) fn new(counter: Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::AcqRel);
        Self { counter, bytes: None }
    }

    pub(super) fn new_with_bytes(counter: Arc<AtomicUsize>, bytes: Arc<AtomicUsize>, retained_bytes: usize) -> Self {
        counter.fetch_add(1, Ordering::AcqRel);
        bytes.fetch_add(retained_bytes, Ordering::AcqRel);
        Self {
            counter,
            bytes: Some((bytes, retained_bytes)),
        }
    }
}

impl Drop for CounterObservation {
    fn drop(&mut self) {
        if let Some((bytes, retained_bytes)) = self.bytes.take() {
            bytes.fetch_sub(retained_bytes, Ordering::AcqRel);
        }
        self.counter.fetch_sub(1, Ordering::AcqRel);
    }
}
