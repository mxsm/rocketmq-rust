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

//! Broker-private ownership seam for immediate Pull, Pop, Query, and View responses.

pub(crate) mod pop;

use std::fs::File;
use std::sync::Arc;

use bytes::Bytes;
use rocketmq_error::ErrorKind;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_store::FileRangeTransferHandle;
use rocketmq_store::SelectMappedBufferResult;
use rocketmq_transport::api::v1::command_from_error_with_factory_and_opaque;
use rocketmq_transport::api::v1::FileRegionLease;
use rocketmq_transport::api::v2::FileRegion;
use rocketmq_transport::api::v2::FileRegionSequence;
use rocketmq_transport::api::v2::HandlerOutcome;
use rocketmq_transport::api::v2::ResponsePlan;
use rocketmq_transport::api::v2::ResponsePlanError;

const MAX_RESPONSE_BODY_LEN: u64 = i32::MAX as u64 - 4;

/// The single affine body owner paired with a body-free Broker response head.
#[derive(Debug)]
pub(crate) enum BrokerResponseBodyOwner {
    Empty,
    Bytes(Bytes),
    Segments(Vec<Bytes>),
    FileRegions(FileRegionSequence),
}

/// A body-free response head and exactly one affine body owner.
pub(crate) struct BrokerResponseParts {
    head: RemotingCommand,
    body: BrokerResponseBodyOwner,
}

/// Typed failures while assembling a Broker-owned response body.
#[derive(Debug, thiserror::Error)]
pub(crate) enum BrokerResponseBuildError {
    #[error("invalid Broker response plan: {0}")]
    ResponsePlan(#[from] ResponsePlanError),
}

impl From<BrokerResponseBuildError> for RocketMQError {
    fn from(error: BrokerResponseBuildError) -> Self {
        Self::internal("broker-response-plan", error)
    }
}

/// Descriptor plus storage admission retained by one transport file region.
pub(crate) struct StoreFileRegionLease {
    handle: FileRangeTransferHandle,
}

impl StoreFileRegionLease {
    fn new(handle: FileRangeTransferHandle) -> Self {
        Self { handle }
    }
}

impl FileRegionLease for StoreFileRegionLease {
    fn file(&self) -> &File {
        self.handle.file()
    }
}

impl BrokerResponseParts {
    /// Splits a response command into the body-free head and affine byte owner required by the V2
    /// response contract. This prevents each leaf from open-coding body extraction or
    /// accidentally placing a body-bearing head in a [`ResponsePlan`].
    pub(crate) fn from_command(mut command: RemotingCommand) -> Result<Self, BrokerResponseBuildError> {
        match command.take_body() {
            Some(body) => Self::bytes(command, body),
            None => Self::command(command),
        }
    }

    pub(crate) fn command(head: RemotingCommand) -> Result<Self, BrokerResponseBuildError> {
        Self::new(head, BrokerResponseBodyOwner::Empty)
    }

    pub(crate) fn bytes(head: RemotingCommand, body: Bytes) -> Result<Self, BrokerResponseBuildError> {
        if body.is_empty() {
            return Self::command(head);
        }
        Self::new(head, BrokerResponseBodyOwner::Bytes(body))
    }

    pub(crate) fn segments(head: RemotingCommand, body_segments: Vec<Bytes>) -> Result<Self, BrokerResponseBuildError> {
        if body_segments.iter().all(Bytes::is_empty) {
            return Self::command(head);
        }
        Self::new(head, BrokerResponseBodyOwner::Segments(body_segments))
    }

    pub(crate) fn file_regions(
        head: RemotingCommand,
        regions: FileRegionSequence,
    ) -> Result<Self, BrokerResponseBuildError> {
        Self::new(head, BrokerResponseBodyOwner::FileRegions(regions))
    }

    fn new(head: RemotingCommand, body: BrokerResponseBodyOwner) -> Result<Self, BrokerResponseBuildError> {
        validate_head(&head)?;
        validate_body(&body)?;
        Ok(Self { head, body })
    }

    pub(crate) fn into_response_plan(self) -> RocketMQResult<ResponsePlan> {
        let result = match self.body {
            BrokerResponseBodyOwner::Empty => ResponsePlan::command(self.head),
            BrokerResponseBodyOwner::Bytes(body) => ResponsePlan::bytes(self.head, body),
            BrokerResponseBodyOwner::Segments(segments) => ResponsePlan::segments(self.head, segments),
            BrokerResponseBodyOwner::FileRegions(regions) => ResponsePlan::file_regions(self.head, regions),
        };
        result.map_err(|error| BrokerResponseBuildError::ResponsePlan(error).into())
    }

    pub(crate) fn into_handler_outcome(self) -> RocketMQResult<HandlerOutcome> {
        self.into_response_plan().map(HandlerOutcome::Reply)
    }

    #[cfg(test)]
    fn body(&self) -> &BrokerResponseBodyOwner {
        &self.body
    }
}

/// Converts an ordinary Broker leaf result into its immediate V2 outcome while preserving the
/// Broker-owned wire factory for typed request-header failures.
pub(crate) fn immediate_outcome_from_command_result(
    command_factory: &RemotingCommandFactory,
    result: RocketMQResult<Option<RemotingCommand>>,
    original_opaque: i32,
    missing_response: &'static str,
) -> RocketMQResult<HandlerOutcome> {
    let command = match result {
        Ok(Some(command)) => command,
        Ok(None) => return Err(RocketMQError::invariant_violated(missing_response)),
        Err(error) if error.kind() == ErrorKind::RequestHeaderError => {
            command_from_error_with_factory_and_opaque(command_factory, &error, original_opaque)
        }
        Err(error) => return Err(error),
    };
    BrokerResponseParts::from_command(command)?.into_handler_outcome()
}

fn validate_head(head: &RemotingCommand) -> Result<(), BrokerResponseBuildError> {
    if head.body().is_some() {
        return Err(ResponsePlanError::HeadHasBody.into());
    }
    if !head.is_response_type() {
        return Err(ResponsePlanError::RequestHead.into());
    }
    if head.is_oneway_rpc() {
        return Err(ResponsePlanError::OneWayHead.into());
    }
    Ok(())
}

fn validate_body(body: &BrokerResponseBodyOwner) -> Result<(), BrokerResponseBuildError> {
    match body {
        BrokerResponseBodyOwner::Empty => Ok(()),
        BrokerResponseBodyOwner::Bytes(body) => checked_body_len([body.len() as u64]).map(|_| ()),
        BrokerResponseBodyOwner::Segments(segments) => {
            checked_body_len(segments.iter().map(|segment| segment.len() as u64)).map(|_| ())
        }
        BrokerResponseBodyOwner::FileRegions(regions) => checked_body_len([regions.len()]).map(|_| ()),
    }
}

fn checked_body_len(lengths: impl IntoIterator<Item = u64>) -> Result<usize, BrokerResponseBuildError> {
    let mut body_len = 0_u64;
    for len in lengths {
        body_len = body_len.checked_add(len).ok_or(ResponsePlanError::BodyLengthOverflow)?;
    }
    if body_len > MAX_RESPONSE_BODY_LEN {
        return Err(ResponsePlanError::BodyTooLarge { actual: body_len }.into());
    }
    usize::try_from(body_len).map_err(|_| ResponsePlanError::BodyLengthNotRepresentable { actual: body_len }.into())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum StoreFileRegionStage {
    TransferHandle,
    Region,
    Sequence,
}

fn try_store_file_regions_with<Available, Lease>(
    selections: &[SelectMappedBufferResult],
    mut available: Available,
    mut lease: Lease,
) -> Option<FileRegionSequence>
where
    Available: FnMut(usize, StoreFileRegionStage) -> bool,
    Lease: FnMut(FileRangeTransferHandle) -> Arc<dyn FileRegionLease>,
{
    // File-region transfer is an optional optimization. Keep every selection untouched until the
    // complete sequence exists so any unavailable stage can fall back without exposing its error.
    if selections.is_empty() {
        return None;
    }

    let mut ranges = Vec::with_capacity(selections.len());
    for selection in selections {
        let range = selection.file_range()?;
        ranges.push(range);
    }

    let mut regions = Vec::with_capacity(ranges.len());
    for (index, range) in ranges.into_iter().enumerate() {
        let len = u64::try_from(range.len()).ok()?;
        if !available(index, StoreFileRegionStage::TransferHandle) {
            return None;
        }
        let handle = range.try_transfer_handle().ok()?;
        let lease = lease(handle);
        if !available(index, StoreFileRegionStage::Region) {
            return None;
        }
        let region = FileRegion::try_new(lease, range.position(), len).ok()?;
        regions.push(region);
    }
    if !available(regions.len(), StoreFileRegionStage::Sequence) {
        return None;
    }
    FileRegionSequence::try_new(regions).ok()
}

/// Consumes store selections into ordered owner-backed body-only byte segments.
pub(crate) fn store_body_segments(selections: Vec<SelectMappedBufferResult>) -> Vec<Bytes> {
    selections
        .into_iter()
        .map(SelectMappedBufferResult::into_owner_bytes)
        .collect()
}

/// Chooses all-file-regions or consumes every selection into ordered body segments.
pub(crate) fn store_response_parts(
    head: RemotingCommand,
    selections: Vec<SelectMappedBufferResult>,
) -> Result<BrokerResponseParts, BrokerResponseBuildError> {
    store_response_parts_with(
        head,
        selections,
        |_, _| true,
        |handle| Arc::new(StoreFileRegionLease::new(handle)),
    )
}

fn store_response_parts_with<Available, Lease>(
    head: RemotingCommand,
    selections: Vec<SelectMappedBufferResult>,
    available: Available,
    lease: Lease,
) -> Result<BrokerResponseParts, BrokerResponseBuildError>
where
    Available: FnMut(usize, StoreFileRegionStage) -> bool,
    Lease: FnMut(FileRangeTransferHandle) -> Arc<dyn FileRegionLease>,
{
    if let Some(regions) = try_store_file_regions_with(&selections, available, lease) {
        BrokerResponseParts::file_regions(head, regions)
    } else {
        BrokerResponseParts::segments(head, store_body_segments(selections))
    }
}

#[cfg(test)]
#[path = "response_plan/tests.rs"]
mod tests;
