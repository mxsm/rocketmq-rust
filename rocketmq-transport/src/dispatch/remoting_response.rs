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

//! Validated remoting responses with affine body ownership.

mod binding;

use std::fmt;

use bytes::Bytes;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;

use crate::contract::TransportContractViolation;
use crate::file_region::FileRegionSequence;

pub(crate) use binding::BoundResponse;

const MAX_RESPONSE_BODY_LEN: u64 = i32::MAX as u64 - 4;

/// A validated response head and exactly one owned response body.
///
/// The head never carries a body. Instead, the response owns either no body,
/// contiguous bytes, body-only segments, or a validated file-region sequence.
/// This keeps response metadata available to processors and later dispatch
/// stages without exposing response storage, an encoder, or file handles.
///
/// Instances are intentionally not [`Clone`]. Cloning could duplicate mutable
/// response ownership or file-region leases.
///
/// ```compile_fail
/// use rocketmq_transport::api::RemotingResponse;
///
/// fn cannot_clone(response: &RemotingResponse) {
///     let _: RemotingResponse = response.clone();
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::RemotingResponse;
///
/// fn cannot_construct_with_fields() {
///     let _ = RemotingResponse {
///         head: panic!(),
///         body: panic!(),
///         body_len: 0,
///         body_part_count: 0,
///     };
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::RemotingResponse;
///
/// fn cannot_read_the_body(response: &RemotingResponse) {
///     let _ = response.body();
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::RemotingResponse;
///
/// fn cannot_read_the_head(response: &RemotingResponse) {
///     let _ = response.head();
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::RemotingResponse;
///
/// fn cannot_read_contiguous_body_bytes(response: &RemotingResponse) {
///     let _ = response.bytes();
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::RemotingResponse;
///
/// fn cannot_read_body_segments(response: &RemotingResponse) {
///     let _ = response.segments();
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::RemotingResponse;
///
/// fn cannot_read_file_regions(response: &RemotingResponse) {
///     let _ = response.file_regions();
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::ResponseBody;
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::RemotingResponse;
///
/// fn cannot_encode_an_arbitrary_complete_frame(response: RemotingResponse) {
///     let _ = response.into_bytes();
/// }
/// ```
///
/// ```compile_fail
/// use bytes::Bytes;
/// use rocketmq_transport::api::RemotingResponse;
///
/// fn cannot_accept_a_pre_encoded_frame(frame: Bytes) {
///     let _ = RemotingResponse::encoded(frame);
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::ResponseBodyKind;
///
/// fn encoded_is_not_a_public_body_kind(kind: ResponseBodyKind) {
///     assert_eq!(kind, ResponseBodyKind::Encoded);
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::OriginalRequestIdentity;
/// use rocketmq_transport::api::RemotingResponse;
///
/// fn cannot_bind_a_response_outside_the_transport(response: RemotingResponse, identity: OriginalRequestIdentity) {
///     let _ = response.bind(identity);
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::BoundResponse;
/// ```
///
pub struct RemotingResponse {
    head: RemotingCommand,
    body: ResponseBody,
    body_len: usize,
    body_part_count: usize,
}

impl RemotingResponse {
    /// Creates an empty response from a response code.
    ///
    /// Unlike [`Self::command`], this constructor is infallible because it
    /// creates the response head itself: the head is response-typed, is not
    /// one-way, and has no attached body.
    #[must_use]
    pub fn empty_response(code: i32) -> Self {
        Self::new(
            RemotingCommand::create_response_command_with_code(code),
            ResponseBody::Empty,
            0,
            0,
        )
    }

    /// Creates a response with an empty body.
    ///
    /// # Errors
    ///
    /// Returns [`TransportContractViolation`] when `head` already owns a body, is a
    /// request command, or is marked one-way.
    pub fn command(head: RemotingCommand) -> Result<Self, TransportContractViolation> {
        Self::validate_head(&head)?;
        Ok(Self::new(head, ResponseBody::Empty, 0, 0))
    }

    /// Creates a response with one contiguous body allocation.
    ///
    /// Empty input is normalized to [`ResponseBodyKind::Empty`].
    ///
    /// # Errors
    ///
    /// Returns [`TransportContractViolation`] when the head is invalid or the body
    /// exceeds the protocol's absolute representable length.
    pub fn bytes(head: RemotingCommand, body: Bytes) -> Result<Self, TransportContractViolation> {
        Self::validate_head(&head)?;
        let body_len = checked_body_len([body.len() as u64])?;
        if body.is_empty() {
            return Ok(Self::new(head, ResponseBody::Empty, 0, 0));
        }
        Ok(Self::new(head, ResponseBody::Bytes(body), body_len, 1))
    }

    /// Moves a materialized response command into the owned response representation.
    ///
    /// The body is detached before the head is validated, so a valid contiguous
    /// body retains its original `Bytes` allocation without cloning or decoding.
    ///
    /// # Errors
    ///
    /// Returns [`TransportContractViolation`] when the command is not a valid response
    /// head or its body exceeds the protocol's representable length.
    pub fn from_command(mut command: RemotingCommand) -> Result<Self, TransportContractViolation> {
        match command.take_body() {
            Some(body) => Self::bytes(command, body),
            None => Self::command(command),
        }
    }

    /// Creates a response from ordered body-only byte segments.
    ///
    /// Empty segments are discarded. The remaining segment bytes are accepted
    /// as body data without interpreting bytes that resemble a frame prefix.
    ///
    /// # Errors
    ///
    /// Returns [`TransportContractViolation`] when the head is invalid, segment lengths
    /// overflow, or the aggregate body exceeds the protocol's absolute limit.
    pub fn segments(head: RemotingCommand, body_segments: Vec<Bytes>) -> Result<Self, TransportContractViolation> {
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
    /// Returns [`TransportContractViolation`] when the head is invalid, the sequence
    /// length overflows, or the aggregate body exceeds the protocol's absolute
    /// limit.
    pub fn file_regions(
        head: RemotingCommand,
        regions: FileRegionSequence,
    ) -> Result<Self, TransportContractViolation> {
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
    /// regions retained by this response.
    ///
    /// Empty responses contain zero parts, contiguous byte responses contain one part,
    /// and segmented or file-region responses contain their respective number of
    /// retained body parts.
    #[must_use]
    pub const fn body_part_count(&self) -> usize {
        self.body_part_count
    }

    /// Converts this affine response into the zero-encoding representation used by
    /// an in-process  consumer.
    ///
    /// The response head and every body owner move without cloning,
    /// concatenating segments, reading file regions, or constructing a
    /// transport channel.
    pub fn into_embedded_response(self) -> EmbeddedResponse {
        let body = match self.body {
            ResponseBody::Empty => EmbeddedResponseBody::Empty,
            ResponseBody::Bytes(body) => EmbeddedResponseBody::Bytes(body),
            ResponseBody::Segments(segments) => EmbeddedResponseBody::Segments(segments),
            ResponseBody::FileRegions(regions) => EmbeddedResponseBody::FileRegions(regions),
        };
        EmbeddedResponse { head: self.head, body }
    }

    pub(crate) fn from_bound_parts(head: RemotingCommand, body: ResponseBody) -> Self {
        let (body_len, body_part_count) = body.metadata();
        Self::new(head, body, body_len, body_part_count)
    }

    pub(crate) fn with_body_free_hook_head<T>(
        &mut self,
        apply: impl FnOnce(&mut RemotingCommand) -> rocketmq_error::RocketMQResult<T>,
    ) -> rocketmq_error::RocketMQResult<T> {
        debug_assert!(self.head.body().is_none());
        let result = apply(&mut self.head);
        if self.head.take_body().is_some() {
            return Err(rocketmq_error::RocketMQError::invariant_violated(
                "RPC hook attached a response body through the body-free projection",
            ));
        }
        if self.head.is_oneway_rpc() {
            return Err(rocketmq_error::RocketMQError::invariant_violated(
                "RPC hook marked a remoting response head as one-way",
            ));
        }
        result
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

    fn validate_head(head: &RemotingCommand) -> Result<(), TransportContractViolation> {
        if head.body().is_some() {
            return Err(TransportContractViolation::ResponseHeadHasBody);
        }
        if !head.is_response_type() {
            return Err(TransportContractViolation::ResponseRequestHead);
        }
        if head.is_oneway_rpc() {
            return Err(TransportContractViolation::ResponseOneWayHead);
        }
        Ok(())
    }
}

impl fmt::Debug for RemotingResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RemotingResponse")
            .field("response_code", &self.response_code())
            .field("body_kind", &self.body_kind())
            .field("body_len", &self.body_len())
            .field("body_part_count", &self.body_part_count())
            .finish()
    }
}

/// Affine response delivered to an in-process  consumer without wire
/// encoding or legacy command materialization.
#[must_use]
pub struct EmbeddedResponse {
    head: RemotingCommand,
    body: EmbeddedResponseBody,
}

impl EmbeddedResponse {
    /// Returns the validated, body-free response head.
    #[must_use]
    pub const fn head(&self) -> &RemotingCommand {
        &self.head
    }

    /// Returns the response code from the validated head.
    #[must_use]
    pub fn response_code(&self) -> i32 {
        self.head.code()
    }

    /// Returns the owned body without flattening segments or reading files.
    pub const fn body(&self) -> &EmbeddedResponseBody {
        &self.body
    }

    /// Moves the body-free head and exact body owner to the local consumer.
    pub fn into_parts(self) -> (RemotingCommand, EmbeddedResponseBody) {
        (self.head, self.body)
    }
}

impl fmt::Debug for EmbeddedResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EmbeddedResponse")
            .field("response_code", &self.response_code())
            .field("body_kind", &self.body.kind())
            .finish()
    }
}

/// Exact body owner transferred to an in-process  consumer.
#[must_use]
pub enum EmbeddedResponseBody {
    /// The response has no body.
    Empty,
    /// One contiguous body allocation.
    Bytes(Bytes),
    /// Ordered body-only segments, preserved without concatenation.
    Segments(Vec<Bytes>),
    /// Validated file regions with their storage leases intact.
    FileRegions(FileRegionSequence),
}

impl EmbeddedResponseBody {
    /// Returns the stable storage category of this body owner.
    #[must_use]
    pub const fn kind(&self) -> ResponseBodyKind {
        match self {
            Self::Empty => ResponseBodyKind::Empty,
            Self::Bytes(_) => ResponseBodyKind::Bytes,
            Self::Segments(_) => ResponseBodyKind::Segments,
            Self::FileRegions(_) => ResponseBodyKind::FileRegions,
        }
    }
}

impl fmt::Debug for EmbeddedResponseBody {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("EmbeddedResponseBody")
            .field(&self.kind())
            .finish()
    }
}

/// Metadata category for the one body owner in a [`RemotingResponse`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ResponseBodyKind {
    /// The response has no body owner.
    Empty,
    /// The response owns one contiguous [`Bytes`] value.
    Bytes,
    /// The response owns ordered body-only [`Bytes`] segments.
    Segments,
    /// The response owns an ordered sequence of validated file regions.
    FileRegions,
}

impl ResponseBodyKind {
    /// Returns the stable low-cardinality remoting-response label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::Bytes => "bytes",
            Self::Segments => "segments",
            Self::FileRegions => "file_regions",
        }
    }
}

/// Internal storage owned by a [`RemotingResponse`].
///
/// This type remains crate-private so  processors can choose an explicit
/// constructor without recovering the stored bytes, file leases, or an
/// arbitrary encoded frame.
pub(crate) enum ResponseBody {
    Empty,
    Bytes(Bytes),
    Segments(Vec<Bytes>),
    FileRegions(FileRegionSequence),
}

impl ResponseBody {
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

fn checked_body_len<I>(lengths: I) -> Result<usize, TransportContractViolation>
where
    I: IntoIterator<Item = u64>,
{
    let mut body_len = 0_u64;
    for len in lengths {
        body_len = body_len
            .checked_add(len)
            .ok_or(TransportContractViolation::ResponseBodyLengthOverflow)?;
    }
    if body_len > MAX_RESPONSE_BODY_LEN {
        return Err(TransportContractViolation::ResponseBodyTooLarge {
            actual: body_len,
            maximum: MAX_RESPONSE_BODY_LEN,
        });
    }
    usize::try_from(body_len)
        .map_err(|_| TransportContractViolation::ResponseBodyLengthNotRepresentable { actual: body_len })
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

    #[test]
    fn embedded_response_moves_bytes_and_segments_without_materializing_a_command_body() {
        let bytes = Bytes::from_static(b"embedded-bytes");
        let bytes_ptr = bytes.as_ptr();
        let response = RemotingResponse::bytes(response_head(), bytes)
            .expect("valid bytes response")
            .into_embedded_response();
        assert_eq!(response.response_code(), 7);
        assert!(response.head().body().is_none());
        let (_, EmbeddedResponseBody::Bytes(body)) = response.into_parts() else {
            panic!("bytes response must preserve its body owner")
        };
        assert_eq!(body.as_ptr(), bytes_ptr);

        let segments = vec![Bytes::from_static(b"first"), Bytes::from_static(b"second")];
        let vector_ptr = segments.as_ptr();
        let segment_ptrs = segments.iter().map(|segment| segment.as_ptr()).collect::<Vec<_>>();
        let response = RemotingResponse::segments(response_head(), segments)
            .expect("valid segmented response")
            .into_embedded_response();
        assert_eq!(response.body().kind(), ResponseBodyKind::Segments);
        let (_, EmbeddedResponseBody::Segments(segments)) = response.into_parts() else {
            panic!("segmented response must preserve its body owners")
        };
        assert_eq!(segments.as_ptr(), vector_ptr);
        assert_eq!(
            segments.iter().map(|segment| segment.as_ptr()).collect::<Vec<_>>(),
            segment_ptrs
        );
    }

    fn assert_metadata(response: &RemotingResponse, kind: ResponseBodyKind, body_len: usize, body_part_count: usize) {
        assert_eq!(response.response_code(), 7);
        assert_eq!(response.body_kind(), kind);
        assert_eq!(response.body_len(), body_len);
        assert_eq!(response.body_part_count(), body_part_count);
    }

    #[test]
    fn constructors_keep_exactly_one_normalized_body_owner() {
        let infallible_empty = RemotingResponse::empty_response(7);
        assert_metadata(&infallible_empty, ResponseBodyKind::Empty, 0, 0);

        let empty = RemotingResponse::command(response_head()).expect("valid empty response");
        assert_metadata(&empty, ResponseBodyKind::Empty, 0, 0);

        let bytes =
            RemotingResponse::bytes(response_head(), Bytes::from_static(b"bytes")).expect("valid bytes response");
        assert_metadata(&bytes, ResponseBodyKind::Bytes, 5, 1);

        let segments = RemotingResponse::segments(
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
    fn command_conversion_moves_the_original_bytes_owner() {
        let body = Bytes::from_static(b"response body");
        let body_pointer = body.as_ptr();
        let response = RemotingResponse::from_command(response_head().set_body(body)).expect("valid response command");

        let ResponseBody::Bytes(moved) = response.test_body() else {
            panic!("non-empty body must remain contiguous bytes");
        };
        assert_eq!(moved.as_ptr(), body_pointer);
        assert_eq!(moved.as_ref(), b"response body");
    }

    #[test]
    fn command_conversion_rejects_a_malformed_head_before_encoding() {
        let malformed = RemotingCommand::create_remoting_command(7).set_body(Bytes::from_static(b"body"));

        assert!(matches!(
            RemotingResponse::from_command(malformed),
            Err(TransportContractViolation::ResponseRequestHead)
        ));
    }

    #[test]
    fn segments_discard_empty_values_without_moving_non_empty_backing_storage() {
        let left = Bytes::from_static(b"left");
        let right = Bytes::from_static(b"right");
        let left_ptr = left.as_ptr();
        let right_ptr = right.as_ptr();

        let response = RemotingResponse::segments(response_head(), vec![Bytes::new(), left, Bytes::new(), right])
            .expect("valid segmented response");

        let ResponseBody::Segments(segments) = &response.body else {
            panic!("non-empty segments must retain their segmented body owner");
        };
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].as_ptr(), left_ptr);
        assert_eq!(segments[1].as_ptr(), right_ptr);
    }

    #[test]
    fn empty_bytes_and_segments_normalize_to_empty() {
        let bytes = RemotingResponse::bytes(response_head(), Bytes::new()).expect("empty bytes normalize");
        assert_metadata(&bytes, ResponseBodyKind::Empty, 0, 0);

        let segments = RemotingResponse::segments(response_head(), vec![Bytes::new(), Bytes::new()])
            .expect("empty segments normalize");
        assert_metadata(&segments, ResponseBodyKind::Empty, 0, 0);
    }

    #[test]
    fn segments_accept_payloads_that_resemble_frame_prefixes() {
        let response = RemotingResponse::segments(response_head(), vec![Bytes::from_static(&[0, 0, 0, 2, 0x7f, 0xff])])
            .expect("body-only segments do not parse frame contents");

        assert_metadata(&response, ResponseBodyKind::Segments, 6, 1);
    }

    #[test]
    fn constructors_reject_invalid_response_heads() {
        let head_with_empty_body = response_head().set_body(Bytes::new());
        assert!(matches!(
            RemotingResponse::command(head_with_empty_body),
            Err(TransportContractViolation::ResponseHeadHasBody)
        ));

        let request_head = RemotingCommand::create_remoting_command(7);
        assert!(matches!(
            RemotingResponse::command(request_head),
            Err(TransportContractViolation::ResponseRequestHead)
        ));

        let one_way_head = response_head().mark_oneway_rpc();
        assert!(matches!(
            RemotingResponse::command(one_way_head),
            Err(TransportContractViolation::ResponseOneWayHead)
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
            RemotingResponse::bytes(body_head, Bytes::from_static(b"body")),
            Err(TransportContractViolation::ResponseHeadHasBody)
        ));

        let request_head = RemotingCommand::create_remoting_command(7);
        assert!(matches!(
            RemotingResponse::segments(request_head, vec![Bytes::from_static(b"segment")]),
            Err(TransportContractViolation::ResponseRequestHead)
        ));

        let one_way_head = response_head().mark_oneway_rpc();
        assert!(matches!(
            RemotingResponse::file_regions(one_way_head, file_region_sequence_with_len(1)),
            Err(TransportContractViolation::ResponseOneWayHead)
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
            Err(TransportContractViolation::ResponseBodyTooLarge {
                actual: MAX_RESPONSE_BODY_LEN + 1,
                maximum: MAX_RESPONSE_BODY_LEN,
            })
        );
        assert_eq!(
            checked_body_len([u64::MAX, 1]),
            Err(TransportContractViolation::ResponseBodyLengthOverflow)
        );
    }

    #[test]
    fn file_region_constructor_enforces_the_absolute_body_ceiling_without_writing_the_sparse_file() {
        let body_len = MAX_RESPONSE_BODY_LEN + 1;
        let regions = file_region_sequence_with_len(body_len);

        assert!(matches!(
            RemotingResponse::file_regions(response_head(), regions),
            Err(TransportContractViolation::ResponseBodyTooLarge { actual, maximum })
                if actual == body_len && maximum == MAX_RESPONSE_BODY_LEN
        ));
    }

    #[test]
    fn debug_uses_metadata_without_head_body_or_file_details() {
        let mut head = response_head().set_remark("response-remark-secret");
        head.add_ext_field("response-ext-key-secret", "response-ext-value-secret");
        let bytes =
            RemotingResponse::bytes(head, Bytes::from_static(b"response-body-secret")).expect("valid bytes response");
        let debug = format!("{bytes:?}");

        assert!(debug.contains("RemotingResponse"));
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
        let mut response =
            RemotingResponse::file_regions(response_head(), regions).expect("valid file-region response");
        assert_eq!(file_accesses.load(Ordering::SeqCst), 1);
        assert_metadata(&response, ResponseBodyKind::FileRegions, 11, 1);
        assert_eq!(Arc::strong_count(&lease), 2);
        response
            .with_body_free_hook_head(|head| {
                assert!(head.body().is_none());
                head.set_remark_mut("hook-observed");
                Ok(())
            })
            .expect("body-free hook projection");
        assert_eq!(response.response_code(), 7);
        assert_eq!(response.body_len(), 11);
        assert_eq!(file_accesses.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        let debug = format!("{response:?}");
        assert!(!debug.contains("FileRegion {"));
        assert!(!debug.contains("offset"));
        assert!(!debug.contains("lease"));
        assert_eq!(file_accesses.load(Ordering::SeqCst), 1);

        drop(response);
        assert_eq!(Arc::strong_count(&lease), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(lease);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}
