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

//! Owned V2 response heads and body storage.

mod binding;
#[allow(
    dead_code,
    reason = "RSP-06 defines the private terminal compatibility seam wired by a later embedded adapter stage"
)]
mod materializer;

use std::fmt;

use bytes::Bytes;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;

use crate::file_region::FileRegionSequence;

pub(crate) use binding::BoundResponsePlan;
pub(crate) use binding::ResponseBindingError;
pub(crate) use materializer::LegacyLocalMaterializationError;
pub(crate) use materializer::LegacyMaterializationLimits;

const MAX_RESPONSE_BODY_LEN: u64 = i32::MAX as u64 - 4;

/// A validated response head and exactly one owned response body.
///
/// The head never carries a body. Instead, the plan owns either no body,
/// contiguous bytes, body-only segments, or a validated file-region sequence.
/// This keeps response metadata available to processors and later dispatch
/// stages without exposing response storage, an encoder, or file handles.
///
/// Instances are intentionally not [`Clone`]. Cloning could duplicate mutable
/// response ownership or file-region leases.
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::ResponsePlan;
///
/// fn cannot_clone(plan: &ResponsePlan) {
///     let _: ResponsePlan = plan.clone();
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::ResponsePlan;
///
/// fn cannot_construct_with_fields() {
///     let _ = ResponsePlan {
///         head: panic!(),
///         body: panic!(),
///         body_len: 0,
///         body_part_count: 0,
///     };
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::ResponsePlan;
///
/// fn cannot_read_the_body(plan: &ResponsePlan) {
///     let _ = plan.body();
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::ResponsePlan;
///
/// fn cannot_read_the_head(plan: &ResponsePlan) {
///     let _ = plan.head();
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::ResponsePlan;
///
/// fn cannot_read_contiguous_body_bytes(plan: &ResponsePlan) {
///     let _ = plan.bytes();
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::ResponsePlan;
///
/// fn cannot_read_body_segments(plan: &ResponsePlan) {
///     let _ = plan.segments();
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::ResponsePlan;
///
/// fn cannot_read_file_regions(plan: &ResponsePlan) {
///     let _ = plan.file_regions();
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::ResponseBody;
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::FileRegionLease;
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::ResponsePlan;
///
/// fn cannot_encode_an_arbitrary_complete_frame(plan: ResponsePlan) {
///     let _ = plan.into_bytes();
/// }
/// ```
///
/// ```compile_fail
/// use bytes::Bytes;
/// use rocketmq_transport::api::v2::ResponsePlan;
///
/// fn cannot_accept_a_pre_encoded_frame(frame: Bytes) {
///     let _ = ResponsePlan::encoded(frame);
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::ResponseBodyKind;
///
/// fn encoded_is_not_a_public_body_kind(kind: ResponseBodyKind) {
///     assert_eq!(kind, ResponseBodyKind::Encoded);
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::OriginalRequestIdentity;
/// use rocketmq_transport::api::v2::ResponsePlan;
///
/// fn cannot_bind_a_plan_outside_the_transport(plan: ResponsePlan, identity: OriginalRequestIdentity) {
///     let _ = plan.bind(identity);
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::BoundResponsePlan;
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::LegacyMaterializationLimits;
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::prelude::LegacyLocalMaterializationError;
/// ```
pub struct ResponsePlan {
    head: RemotingCommand,
    body: ResponseBody,
    body_len: usize,
    body_part_count: usize,
}

impl ResponsePlan {
    /// Creates a response with an empty body.
    ///
    /// # Errors
    ///
    /// Returns [`ResponsePlanError`] when `head` already owns a body, is a
    /// request command, or is marked one-way.
    pub fn command(head: RemotingCommand) -> Result<Self, ResponsePlanError> {
        Self::validate_head(&head)?;
        Ok(Self::new(head, ResponseBody::Empty, 0, 0))
    }

    /// Creates a response with one contiguous body allocation.
    ///
    /// Empty input is normalized to [`ResponseBodyKind::Empty`].
    ///
    /// # Errors
    ///
    /// Returns [`ResponsePlanError`] when the head is invalid or the body
    /// exceeds the protocol's absolute representable length.
    pub fn bytes(head: RemotingCommand, body: Bytes) -> Result<Self, ResponsePlanError> {
        Self::validate_head(&head)?;
        let body_len = checked_body_len([body.len() as u64])?;
        if body.is_empty() {
            return Ok(Self::new(head, ResponseBody::Empty, 0, 0));
        }
        Ok(Self::new(head, ResponseBody::Bytes(body), body_len, 1))
    }

    /// Moves a materialized response command into the owned plan representation.
    ///
    /// The body is detached before the head is validated, so a valid contiguous
    /// body retains its original `Bytes` allocation without cloning or decoding.
    ///
    /// # Errors
    ///
    /// Returns [`ResponsePlanError`] when the command is not a valid response
    /// head or its body exceeds the protocol's representable length.
    pub fn from_command(mut command: RemotingCommand) -> Result<Self, ResponsePlanError> {
        match command.take_body() {
            Some(body) => Self::bytes(command, body),
            None => Self::command(command),
        }
    }

    pub(crate) fn from_legacy_command(command: RemotingCommand) -> Result<Self, ResponsePlanError> {
        Self::from_command(command)
    }

    /// Creates a response from ordered body-only byte segments.
    ///
    /// Empty segments are discarded. The remaining segment bytes are accepted
    /// as body data without interpreting bytes that resemble a frame prefix.
    ///
    /// # Errors
    ///
    /// Returns [`ResponsePlanError`] when the head is invalid, segment lengths
    /// overflow, or the aggregate body exceeds the protocol's absolute limit.
    pub fn segments(head: RemotingCommand, body_segments: Vec<Bytes>) -> Result<Self, ResponsePlanError> {
        Self::validate_head(&head)?;
        let body_segments = body_segments
            .into_iter()
            .filter(|segment| !segment.is_empty())
            .collect::<Vec<_>>();
        let body_len = checked_body_len(body_segments.iter().map(|segment| segment.len() as u64))?;
        let body_part_count = body_segments.len();
        if body_segments.is_empty() {
            return Ok(Self::new(head, ResponseBody::Empty, 0, 0));
        }
        Ok(Self::new(
            head,
            ResponseBody::Segments(body_segments),
            body_len,
            body_part_count,
        ))
    }

    /// Creates a response with validated, leased external file regions.
    ///
    /// This constructor retains the supplied sequence by value and uses its
    /// cached aggregate length. It does not re-stat the source files.
    ///
    /// # Errors
    ///
    /// Returns [`ResponsePlanError`] when the head is invalid, the sequence
    /// length overflows, or the aggregate body exceeds the protocol's absolute
    /// limit.
    pub fn file_regions(head: RemotingCommand, regions: FileRegionSequence) -> Result<Self, ResponsePlanError> {
        Self::validate_head(&head)?;
        let body_len = checked_body_len([regions.len()])?;
        let body_part_count = regions.regions().len();
        Ok(Self::new(
            head,
            ResponseBody::FileRegions(regions),
            body_len,
            body_part_count,
        ))
    }

    /// Returns the response code from the validated head.
    #[must_use]
    pub fn response_code(&self) -> i32 {
        self.head.code()
    }

    /// Returns the storage category of the owned response body.
    #[must_use]
    pub const fn body_kind(&self) -> ResponseBodyKind {
        self.body.kind()
    }

    /// Returns the validated aggregate body length in bytes.
    #[must_use]
    pub const fn body_len(&self) -> usize {
        self.body_len
    }

    /// Returns the number of contiguous body allocations, segments, or file
    /// regions retained by this plan.
    ///
    /// Empty plans contain zero parts, contiguous byte plans contain one part,
    /// and segmented or file-region plans contain their respective number of
    /// retained body parts.
    #[must_use]
    pub const fn body_part_count(&self) -> usize {
        self.body_part_count
    }

    #[allow(
        dead_code,
        reason = "RSP-05 local delivery rebuilds this trusted wrapper before later dispatcher wiring"
    )]
    pub(crate) fn from_bound_parts(head: RemotingCommand, body: ResponseBody) -> Self {
        let (body_len, body_part_count) = body.metadata();
        Self::new(head, body, body_len, body_part_count)
    }

    #[allow(
        dead_code,
        reason = "DSP-03 body-free hook projection is consumed by the not-yet-wired private dispatcher"
    )]
    pub(crate) fn with_body_free_hook_head<T>(
        &mut self,
        apply: impl FnOnce(&mut RemotingCommand) -> rocketmq_error::RocketMQResult<T>,
    ) -> rocketmq_error::RocketMQResult<T> {
        debug_assert!(self.head.body().is_none());
        let result = apply(&mut self.head);
        if self.head.take_body().is_some() {
            return Err(rocketmq_error::RocketMQError::invariant_violated(
                "RPC hook attached a response body through the body-free V2 projection",
            ));
        }
        if self.head.is_oneway_rpc() {
            return Err(rocketmq_error::RocketMQError::invariant_violated(
                "RPC hook marked a V2 response plan head as one-way",
            ));
        }
        result
    }

    fn into_materialization_parts(self) -> (RemotingCommand, ResponseBody, usize, usize) {
        (self.head, self.body, self.body_len, self.body_part_count)
    }

    #[cfg(test)]
    pub(crate) const fn test_body(&self) -> &ResponseBody {
        &self.body
    }

    #[cfg(test)]
    pub(crate) const fn test_head(&self) -> &RemotingCommand {
        &self.head
    }

    fn new(head: RemotingCommand, body: ResponseBody, body_len: usize, body_part_count: usize) -> Self {
        Self {
            head,
            body,
            body_len,
            body_part_count,
        }
    }

    fn validate_head(head: &RemotingCommand) -> Result<(), ResponsePlanError> {
        if head.body().is_some() {
            return Err(ResponsePlanError::HeadHasBody);
        }
        if !head.is_response_type() {
            return Err(ResponsePlanError::RequestHead);
        }
        if head.is_oneway_rpc() {
            return Err(ResponsePlanError::OneWayHead);
        }
        Ok(())
    }
}

impl fmt::Debug for ResponsePlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResponsePlan")
            .field("response_code", &self.response_code())
            .field("body_kind", &self.body_kind())
            .field("body_len", &self.body_len())
            .field("body_part_count", &self.body_part_count())
            .finish()
    }
}

/// Metadata category for the one body owner in a [`ResponsePlan`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ResponseBodyKind {
    /// The plan has no body owner.
    Empty,
    /// The plan owns one contiguous [`Bytes`] value.
    Bytes,
    /// The plan owns ordered body-only [`Bytes`] segments.
    Segments,
    /// The plan owns an ordered sequence of validated file regions.
    FileRegions,
}

/// Internal storage owned by a [`ResponsePlan`].
///
/// This type remains crate-private so V2 processors can choose an explicit
/// constructor without recovering the stored bytes, file leases, or an
/// arbitrary encoded frame.
pub(crate) enum ResponseBody {
    Empty,
    Bytes(Bytes),
    Segments(Vec<Bytes>),
    FileRegions(FileRegionSequence),
}

impl ResponseBody {
    #[allow(dead_code, reason = "RSP-05 trusted local rewrapping preserves cached plan metadata")]
    fn metadata(&self) -> (usize, usize) {
        match self {
            Self::Empty => (0, 0),
            Self::Bytes(bytes) => (bytes.len(), 1),
            Self::Segments(segments) => (segments.iter().map(Bytes::len).sum(), segments.len()),
            Self::FileRegions(regions) => (regions.len() as usize, regions.regions().len()),
        }
    }

    const fn kind(&self) -> ResponseBodyKind {
        match self {
            Self::Empty => ResponseBodyKind::Empty,
            Self::Bytes(bytes) => {
                debug_assert!(!bytes.is_empty());
                ResponseBodyKind::Bytes
            }
            Self::Segments(segments) => {
                debug_assert!(!segments.is_empty());
                ResponseBodyKind::Segments
            }
            Self::FileRegions(regions) => {
                debug_assert!(!regions.is_empty());
                ResponseBodyKind::FileRegions
            }
        }
    }
}

/// Failure returned while constructing a [`ResponsePlan`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ResponsePlanError {
    /// The supplied response head already owns a body, including an empty one.
    #[error("response plan head must not already own a body")]
    HeadHasBody,
    /// The supplied head is a request command rather than a response command.
    #[error("response plan head must be a response command")]
    RequestHead,
    /// The supplied response head is marked one-way.
    #[error("response plan head must not be one-way")]
    OneWayHead,
    /// Adding body part lengths exceeded the protocol's length representation.
    #[error("response plan body length overflowed u64")]
    BodyLengthOverflow,
    /// The body exceeds the absolute RocketMQ frame body ceiling.
    #[error("response plan body length {actual} exceeds the maximum {MAX_RESPONSE_BODY_LEN}")]
    BodyTooLarge {
        /// Aggregate byte length supplied to the constructor.
        actual: u64,
    },
    /// The wire-representable body length does not fit this platform's address space.
    #[error("response plan body length {actual} is not representable as usize")]
    BodyLengthNotRepresentable {
        /// Aggregate byte length supplied to the constructor.
        actual: u64,
    },
}

fn checked_body_len<I>(lengths: I) -> Result<usize, ResponsePlanError>
where
    I: IntoIterator<Item = u64>,
{
    let mut body_len = 0_u64;
    for len in lengths {
        body_len = body_len.checked_add(len).ok_or(ResponsePlanError::BodyLengthOverflow)?;
    }
    if body_len > MAX_RESPONSE_BODY_LEN {
        return Err(ResponsePlanError::BodyTooLarge { actual: body_len });
    }
    usize::try_from(body_len).map_err(|_| ResponsePlanError::BodyLengthNotRepresentable { actual: body_len })
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use super::*;
    use crate::file_region::FileRegion;
    use crate::file_region::FileRegionLease;

    fn response_head() -> RemotingCommand {
        RemotingCommand::create_response_command_with_code(7)
    }

    fn assert_metadata(plan: &ResponsePlan, kind: ResponseBodyKind, body_len: usize, body_part_count: usize) {
        assert_eq!(plan.response_code(), 7);
        assert_eq!(plan.body_kind(), kind);
        assert_eq!(plan.body_len(), body_len);
        assert_eq!(plan.body_part_count(), body_part_count);
    }

    #[test]
    fn constructors_keep_exactly_one_normalized_body_owner() {
        let empty = ResponsePlan::command(response_head()).expect("valid empty response");
        assert_metadata(&empty, ResponseBodyKind::Empty, 0, 0);

        let bytes = ResponsePlan::bytes(response_head(), Bytes::from_static(b"bytes")).expect("valid bytes response");
        assert_metadata(&bytes, ResponseBodyKind::Bytes, 5, 1);

        let segments = ResponsePlan::segments(
            response_head(),
            vec![
                Bytes::new(),
                Bytes::from_static(b"left"),
                Bytes::new(),
                Bytes::from_static(b"right"),
            ],
        )
        .expect("valid segmented response");
        assert_metadata(&segments, ResponseBodyKind::Segments, 9, 2);
    }

    #[test]
    fn legacy_command_conversion_moves_the_original_bytes_owner() {
        let body = Bytes::from_static(b"legacy response body");
        let body_pointer = body.as_ptr();
        let plan =
            ResponsePlan::from_legacy_command(response_head().set_body(body)).expect("valid legacy response command");

        let ResponseBody::Bytes(moved) = plan.test_body() else {
            panic!("non-empty legacy body must remain contiguous bytes");
        };
        assert_eq!(moved.as_ptr(), body_pointer);
        assert_eq!(moved.as_ref(), b"legacy response body");
    }

    #[test]
    fn legacy_command_conversion_rejects_a_malformed_head_before_encoding() {
        let malformed = RemotingCommand::create_remoting_command(7).set_body(Bytes::from_static(b"body"));

        assert!(matches!(
            ResponsePlan::from_legacy_command(malformed),
            Err(ResponsePlanError::RequestHead)
        ));
    }

    #[test]
    fn segments_discard_empty_values_without_moving_non_empty_backing_storage() {
        let left = Bytes::from_static(b"left");
        let right = Bytes::from_static(b"right");
        let left_ptr = left.as_ptr();
        let right_ptr = right.as_ptr();

        let plan = ResponsePlan::segments(response_head(), vec![Bytes::new(), left, Bytes::new(), right])
            .expect("valid segmented response");

        let ResponseBody::Segments(segments) = &plan.body else {
            panic!("non-empty segments must retain their segmented body owner");
        };
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].as_ptr(), left_ptr);
        assert_eq!(segments[1].as_ptr(), right_ptr);
    }

    #[test]
    fn empty_bytes_and_segments_normalize_to_empty() {
        let bytes = ResponsePlan::bytes(response_head(), Bytes::new()).expect("empty bytes normalize");
        assert_metadata(&bytes, ResponseBodyKind::Empty, 0, 0);

        let segments = ResponsePlan::segments(response_head(), vec![Bytes::new(), Bytes::new()])
            .expect("empty segments normalize");
        assert_metadata(&segments, ResponseBodyKind::Empty, 0, 0);
    }

    #[test]
    fn segments_accept_payloads_that_resemble_frame_prefixes() {
        let plan = ResponsePlan::segments(response_head(), vec![Bytes::from_static(&[0, 0, 0, 2, 0x7f, 0xff])])
            .expect("body-only segments do not parse frame contents");

        assert_metadata(&plan, ResponseBodyKind::Segments, 6, 1);
    }

    #[test]
    fn constructors_reject_invalid_response_heads() {
        let head_with_empty_body = response_head().set_body(Bytes::new());
        assert!(matches!(
            ResponsePlan::command(head_with_empty_body),
            Err(ResponsePlanError::HeadHasBody)
        ));

        let request_head = RemotingCommand::create_remoting_command(7);
        assert!(matches!(
            ResponsePlan::command(request_head),
            Err(ResponsePlanError::RequestHead)
        ));

        let one_way_head = response_head().mark_oneway_rpc();
        assert!(matches!(
            ResponsePlan::command(one_way_head),
            Err(ResponsePlanError::OneWayHead)
        ));
    }

    fn file_region_sequence_with_len(len: u64) -> FileRegionSequence {
        let file = tempfile::tempfile().expect("temporary sparse file");
        file.set_len(len).expect("set sparse file length");
        let region = FileRegion::try_new(Arc::new(file), 0, len).expect("validated sparse region");
        FileRegionSequence::try_new(vec![region]).expect("validated sparse region sequence")
    }

    #[test]
    fn every_body_constructor_validates_its_response_head() {
        let body_head = response_head().set_body(Bytes::new());
        assert!(matches!(
            ResponsePlan::bytes(body_head, Bytes::from_static(b"body")),
            Err(ResponsePlanError::HeadHasBody)
        ));

        let request_head = RemotingCommand::create_remoting_command(7);
        assert!(matches!(
            ResponsePlan::segments(request_head, vec![Bytes::from_static(b"segment")]),
            Err(ResponsePlanError::RequestHead)
        ));

        let one_way_head = response_head().mark_oneway_rpc();
        assert!(matches!(
            ResponsePlan::file_regions(one_way_head, file_region_sequence_with_len(1)),
            Err(ResponsePlanError::OneWayHead)
        ));
    }

    #[test]
    fn checked_length_rejects_overflow_and_the_absolute_protocol_ceiling() {
        assert_eq!(
            checked_body_len([MAX_RESPONSE_BODY_LEN]).expect("exact limit"),
            usize::try_from(MAX_RESPONSE_BODY_LEN).expect("protocol ceiling fits usize")
        );
        assert_eq!(
            checked_body_len([MAX_RESPONSE_BODY_LEN + 1]),
            Err(ResponsePlanError::BodyTooLarge {
                actual: MAX_RESPONSE_BODY_LEN + 1,
            })
        );
        assert_eq!(
            checked_body_len([u64::MAX, 1]),
            Err(ResponsePlanError::BodyLengthOverflow)
        );
    }

    #[test]
    fn file_region_constructor_enforces_the_absolute_body_ceiling_without_writing_the_sparse_file() {
        let body_len = MAX_RESPONSE_BODY_LEN + 1;
        let regions = file_region_sequence_with_len(body_len);

        assert!(matches!(
            ResponsePlan::file_regions(response_head(), regions),
            Err(ResponsePlanError::BodyTooLarge { actual }) if actual == body_len
        ));
    }

    #[test]
    fn debug_uses_metadata_without_head_body_or_file_details() {
        let mut head = response_head().set_remark("response-remark-secret");
        head.add_ext_field("response-ext-key-secret", "response-ext-value-secret");
        let bytes =
            ResponsePlan::bytes(head, Bytes::from_static(b"response-body-secret")).expect("valid bytes response");
        let debug = format!("{bytes:?}");

        assert!(debug.contains("ResponsePlan"));
        assert!(debug.contains("body_kind: Bytes"));
        assert!(!debug.contains("response-remark-secret"));
        assert!(!debug.contains("response-ext-key-secret"));
        assert!(!debug.contains("response-ext-value-secret"));
        assert!(!debug.contains("response-body-secret"));
        assert!(!debug.contains("head:"));
        assert!(!debug.contains("body:"));
    }

    struct CountingLease {
        file: File,
        drops: Arc<AtomicUsize>,
        file_accesses: Arc<AtomicUsize>,
    }

    impl Drop for CountingLease {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl FileRegionLease for CountingLease {
        fn file(&self) -> &File {
            self.file_accesses.fetch_add(1, Ordering::SeqCst);
            &self.file
        }
    }

    #[test]
    fn file_regions_retain_the_validated_lease_without_restatting_or_extra_clones() {
        let drops = Arc::new(AtomicUsize::new(0));
        let file_accesses = Arc::new(AtomicUsize::new(0));
        let mut file = tempfile::tempfile().expect("temporary file");
        file.write_all(b"leased body").expect("write leased body");
        let lease = Arc::new(CountingLease {
            file,
            drops: drops.clone(),
            file_accesses: file_accesses.clone(),
        });
        let region = FileRegion::try_new(lease.clone(), 0, 11).expect("validated region");
        assert_eq!(file_accesses.load(Ordering::SeqCst), 1);
        let regions = FileRegionSequence::try_new(vec![region]).expect("validated region sequence");

        assert_eq!(Arc::strong_count(&lease), 2);
        let mut plan = ResponsePlan::file_regions(response_head(), regions).expect("valid file-region response");
        assert_eq!(file_accesses.load(Ordering::SeqCst), 1);
        assert_metadata(&plan, ResponseBodyKind::FileRegions, 11, 1);
        assert_eq!(Arc::strong_count(&lease), 2);
        plan.with_body_free_hook_head(|head| {
            assert!(head.body().is_none());
            head.set_remark_mut("hook-observed");
            Ok(())
        })
        .expect("body-free hook projection");
        assert_eq!(plan.response_code(), 7);
        assert_eq!(plan.body_len(), 11);
        assert_eq!(file_accesses.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        let debug = format!("{plan:?}");
        assert!(!debug.contains("FileRegion {"));
        assert!(!debug.contains("offset"));
        assert!(!debug.contains("lease"));
        assert_eq!(file_accesses.load(Ordering::SeqCst), 1);

        drop(plan);
        assert_eq!(Arc::strong_count(&lease), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(lease);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}
