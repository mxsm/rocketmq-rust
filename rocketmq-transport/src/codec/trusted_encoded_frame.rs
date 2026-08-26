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

//! Sealed validation for complete response frames used by compatibility code.

use std::fmt;

use bytes::Bytes;
use bytes::BytesMut;
use rocketmq_error::RocketMQError;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::SerializeType;

use super::remoting_command_codec::FrameLimits;
use crate::dispatch::OriginalRequestIdentity;

const FRAME_ENVELOPE_LEN: usize = 8;
const HEADER_LEN_MASK: u32 = 0x00ff_ffff;

#[allow(
    dead_code,
    reason = "RSP-02 seals trusted frame ownership before the legacy adapter consumes it"
)]
enum EncodedFrameStorage {
    Contiguous(Bytes),
    Segmented(Vec<Bytes>),
}

/// A complete encoded response whose envelope, header, and request identity were validated.
///
/// This capability is intentionally crate-private and non-`Clone`. Construction consumes the
/// untrusted storage so a rejected frame cannot be recovered from the validator for unchecked
/// transport use.
#[must_use]
#[allow(
    dead_code,
    reason = "RSP-02 seals trusted frame ownership before the legacy adapter consumes it"
)]
pub(crate) struct TrustedEncodedFrame {
    storage: EncodedFrameStorage,
    metadata: ValidatedFrameMetadata,
}

#[allow(
    dead_code,
    reason = "RSP-02 seals trusted frame metadata before the legacy adapter consumes it"
)]
impl TrustedEncodedFrame {
    pub(crate) fn try_from_bytes(
        bytes: Bytes,
        limits: FrameLimits,
        original: OriginalRequestIdentity,
    ) -> Result<Self, TrustedEncodedFrameError> {
        let metadata = validate_frame(std::slice::from_ref(&bytes), limits, original)?;
        Ok(Self {
            storage: EncodedFrameStorage::Contiguous(bytes),
            metadata,
        })
    }

    pub(crate) fn try_from_segments(
        segments: Vec<Bytes>,
        limits: FrameLimits,
        original: OriginalRequestIdentity,
    ) -> Result<Self, TrustedEncodedFrameError> {
        let metadata = validate_frame(&segments, limits, original)?;
        Ok(Self {
            storage: EncodedFrameStorage::Segmented(segments),
            metadata,
        })
    }

    #[must_use]
    pub(crate) const fn metadata(&self) -> &ValidatedFrameMetadata {
        &self.metadata
    }
}

impl fmt::Debug for TrustedEncodedFrame {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TrustedEncodedFrame")
            .field("metadata", &self.metadata)
            .finish()
    }
}

/// Non-sensitive facts retained after the decoded response header is dropped.
#[derive(Eq, PartialEq)]
#[allow(
    dead_code,
    reason = "RSP-02 seals trusted frame metadata before the legacy adapter consumes it"
)]
pub(crate) struct ValidatedFrameMetadata {
    encoded_len: usize,
    header_len: usize,
    body_len: usize,
    response_code: i32,
    opaque: i32,
    serialize_type: SerializeType,
}

impl fmt::Debug for ValidatedFrameMetadata {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ValidatedFrameMetadata")
            .field("encoded_len", &self.encoded_len)
            .field("header_len", &self.header_len)
            .field("body_len", &self.body_len)
            .field("response_code", &self.response_code)
            .field("serialize_type", &self.serialize_type)
            .finish()
    }
}

#[allow(
    dead_code,
    reason = "RSP-02 seals trusted frame metadata before the legacy adapter consumes it"
)]
impl ValidatedFrameMetadata {
    #[must_use]
    pub(crate) const fn encoded_len(&self) -> usize {
        self.encoded_len
    }

    #[must_use]
    pub(crate) const fn header_len(&self) -> usize {
        self.header_len
    }

    #[must_use]
    pub(crate) const fn body_len(&self) -> usize {
        self.body_len
    }

    #[must_use]
    pub(crate) const fn response_code(&self) -> i32 {
        self.response_code
    }

    #[must_use]
    pub(crate) const fn opaque(&self) -> i32 {
        self.opaque
    }

    #[must_use]
    pub(crate) const fn serialize_type(&self) -> SerializeType {
        self.serialize_type
    }
}

/// Failure returned when untrusted complete-frame storage cannot be sealed.
#[derive(thiserror::Error)]
#[allow(
    dead_code,
    reason = "RSP-02 seals trusted frame validation before the legacy adapter consumes it"
)]
pub(crate) enum TrustedEncodedFrameError {
    #[error("one-way original requests cannot have encoded responses")]
    OriginalRequestOneWay,

    #[error("encoded response frame failed envelope or limit validation")]
    FrameValidation {
        #[source]
        source: RocketMQError,
    },

    #[error("encoded response frame uses unsupported serialization type {code}")]
    UnsupportedSerializationType { code: u8 },

    #[error("encoded response {serialize_type:?} header failed to decode")]
    HeaderDecode {
        serialize_type: SerializeType,
        #[source]
        source: RocketMQError,
    },

    #[error("encoded response {serialize_type:?} header did not produce a command")]
    MissingCommand { serialize_type: SerializeType },

    #[error("encoded response frame contains a request-shaped command")]
    RequestCommand,

    #[error("encoded response frame is marked one-way")]
    ResponseOneWay,

    #[error("encoded response opaque does not match original request opaque")]
    OpaqueMismatch { expected: i32, actual: i32 },
}

impl fmt::Debug for TrustedEncodedFrameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OriginalRequestOneWay => formatter.write_str("OriginalRequestOneWay"),
            Self::FrameValidation { .. } => formatter.write_str("FrameValidation"),
            Self::UnsupportedSerializationType { code } => formatter
                .debug_struct("UnsupportedSerializationType")
                .field("code", code)
                .finish(),
            Self::HeaderDecode { serialize_type, .. } => formatter
                .debug_struct("HeaderDecode")
                .field("serialize_type", serialize_type)
                .finish(),
            Self::MissingCommand { serialize_type } => formatter
                .debug_struct("MissingCommand")
                .field("serialize_type", serialize_type)
                .finish(),
            Self::RequestCommand => formatter.write_str("RequestCommand"),
            Self::ResponseOneWay => formatter.write_str("ResponseOneWay"),
            Self::OpaqueMismatch { .. } => formatter.write_str("OpaqueMismatch"),
        }
    }
}

fn validate_frame(
    segments: &[Bytes],
    limits: FrameLimits,
    original: OriginalRequestIdentity,
) -> Result<ValidatedFrameMetadata, TrustedEncodedFrameError> {
    if original.is_one_way() {
        return Err(TrustedEncodedFrameError::OriginalRequestOneWay);
    }

    let encoded_len = limits
        .validate_frame_segments(segments)
        .map_err(|source| TrustedEncodedFrameError::FrameValidation { source })?;

    let mut envelope = [0_u8; FRAME_ENVELOPE_LEN];
    if !copy_segment_range(segments, 0, &mut envelope) {
        return Err(incomplete_validated_frame());
    }
    let header_marker = u32::from_be_bytes([envelope[4], envelope[5], envelope[6], envelope[7]]);
    let header_len = (header_marker & HEADER_LEN_MASK) as usize;
    let serialize_code = (header_marker >> 24) as u8;
    let Some(serialize_type) = SerializeType::value_of(serialize_code) else {
        return Err(TrustedEncodedFrameError::UnsupportedSerializationType { code: serialize_code });
    };

    let mut header = BytesMut::with_capacity(header_len);
    header.resize(header_len, 0);
    if !copy_segment_range(segments, FRAME_ENVELOPE_LEN, &mut header) {
        return Err(incomplete_validated_frame());
    }
    let command = RemotingCommand::header_decode(&mut header, header_len, serialize_type)
        .map_err(|source| TrustedEncodedFrameError::HeaderDecode { serialize_type, source })?
        .ok_or(TrustedEncodedFrameError::MissingCommand { serialize_type })?;

    if !command.is_response_type() {
        return Err(TrustedEncodedFrameError::RequestCommand);
    }
    if command.is_oneway_rpc() {
        return Err(TrustedEncodedFrameError::ResponseOneWay);
    }
    let expected = original.original_opaque();
    let actual = command.opaque();
    if actual != expected {
        return Err(TrustedEncodedFrameError::OpaqueMismatch { expected, actual });
    }

    let body_len = encoded_len
        .checked_sub(FRAME_ENVELOPE_LEN)
        .and_then(|payload_len| payload_len.checked_sub(header_len))
        .ok_or_else(incomplete_validated_frame)?;
    Ok(ValidatedFrameMetadata {
        encoded_len,
        header_len,
        body_len,
        response_code: command.code(),
        opaque: actual,
        serialize_type,
    })
}

fn incomplete_validated_frame() -> TrustedEncodedFrameError {
    TrustedEncodedFrameError::FrameValidation {
        source: rocketmq_error::SerializationError::decode_failed(
            "remoting-command",
            "validated encoded frame storage ended unexpectedly",
        )
        .into(),
    }
}

fn copy_segment_range(segments: &[Bytes], mut offset: usize, destination: &mut [u8]) -> bool {
    let mut copied = 0;
    for segment in segments {
        if offset >= segment.len() {
            offset -= segment.len();
            continue;
        }

        let available = segment.len() - offset;
        let count = available.min(destination.len() - copied);
        destination[copied..copied + count].copy_from_slice(&segment[offset..offset + count]);
        copied += count;
        offset = 0;
        if copied == destination.len() {
            return true;
        }
    }
    copied == destination.len()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::error::Error as _;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use bytes::Bytes;
    use rocketmq_protocol::protocol::encoded_frame::EncodedFrame;
    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    use rocketmq_protocol::protocol::SerializeType;

    use super::*;

    const OPAQUE: i32 = 0x1234_5678;

    struct DropSpy {
        bytes: Vec<u8>,
        drops: Arc<AtomicUsize>,
    }

    impl AsRef<[u8]> for DropSpy {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }

    impl Drop for DropSpy {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn owned_bytes(bytes: Vec<u8>, drops: Arc<AtomicUsize>) -> Bytes {
        Bytes::from_owner(DropSpy { bytes, drops })
    }

    fn assert_drop_counts(counts: &[Arc<AtomicUsize>], expected: usize) {
        for count in counts {
            assert_eq!(count.load(Ordering::SeqCst), expected);
        }
    }

    fn assert_redacted(output: &str, secrets: &[&str], opaques: &[i32]) {
        for secret in secrets {
            assert!(
                !output.contains(secret),
                "output leaked secret marker {secret:?}: {output}"
            );
        }
        for opaque in opaques {
            let opaque = opaque.to_string();
            assert!(
                !output.contains(&opaque),
                "output leaked wire opaque {opaque}: {output}"
            );
        }
    }

    fn original_identity(opaque: i32, one_way: bool) -> OriginalRequestIdentity {
        let mut request = RemotingCommand::create_remoting_command(17).set_opaque(opaque);
        if one_way {
            request = request.mark_oneway_rpc();
        }
        OriginalRequestIdentity::capture(41, &AtomicU64::new(1), &request)
            .expect("test request identity should allocate")
    }

    fn response(code: i32, opaque: i32, serialize_type: SerializeType, body: Bytes) -> RemotingCommand {
        let response = RemotingCommand::create_remoting_command(code)
            .set_opaque(opaque)
            .set_serialize_type(serialize_type)
            .mark_response_type();
        if body.is_empty() {
            response
        } else {
            response.set_body(body)
        }
    }

    fn encode(command: RemotingCommand) -> Bytes {
        EncodedFrame::from_command(command)
            .expect("test command should encode")
            .into_bytes()
    }

    fn header_len(frame: &Bytes) -> usize {
        (u32::from_be_bytes(frame[4..8].try_into().expect("encoded frame marker")) & HEADER_LEN_MASK) as usize
    }

    fn assert_frame_validation_error(result: Result<TrustedEncodedFrame, TrustedEncodedFrameError>) {
        assert!(matches!(result, Err(TrustedEncodedFrameError::FrameValidation { .. })));
    }

    #[test]
    fn json_contiguous_frame_is_valid_and_preserves_backing_storage() {
        let frame = encode(
            response(205, OPAQUE, SerializeType::JSON, Bytes::from_static(b"json-body")).set_remark("json-header"),
        );
        let original_pointer = frame.as_ptr();
        let original_len = frame.len();

        let trusted =
            TrustedEncodedFrame::try_from_bytes(frame, FrameLimits::default(), original_identity(OPAQUE, false))
                .expect("valid JSON response should be sealed");

        assert_eq!(trusted.metadata().encoded_len(), original_len);
        assert_eq!(trusted.metadata().response_code(), 205);
        assert_eq!(trusted.metadata().opaque(), OPAQUE);
        assert_eq!(trusted.metadata().serialize_type(), SerializeType::JSON);
        match trusted.storage {
            EncodedFrameStorage::Contiguous(stored) => {
                assert_eq!(stored.as_ptr(), original_pointer);
                assert_eq!(stored.len(), original_len);
            }
            EncodedFrameStorage::Segmented(_) => panic!("contiguous input changed storage kind"),
        }
    }

    #[test]
    fn rocketmq_binary_segments_cross_every_prefix_marker_and_header_boundary() {
        let frame = encode(response(
            206,
            OPAQUE,
            SerializeType::ROCKETMQ,
            Bytes::from_static(b"binary-body"),
        ));
        let segments = (0..frame.len())
            .map(|index| frame.slice(index..index + 1))
            .collect::<Vec<_>>();

        let trusted =
            TrustedEncodedFrame::try_from_segments(segments, FrameLimits::default(), original_identity(OPAQUE, false))
                .expect("byte-split RocketMQ response should be sealed");

        assert_eq!(trusted.metadata().response_code(), 206);
        assert_eq!(trusted.metadata().serialize_type(), SerializeType::ROCKETMQ);
        assert_eq!(trusted.metadata().body_len(), b"binary-body".len());
    }

    #[test]
    fn empty_body_is_valid() {
        let frame = encode(response(207, OPAQUE, SerializeType::JSON, Bytes::new()));
        let trusted =
            TrustedEncodedFrame::try_from_bytes(frame, FrameLimits::default(), original_identity(OPAQUE, false))
                .expect("empty response body should be valid");

        assert_eq!(trusted.metadata().body_len(), 0);
    }

    #[test]
    fn segmented_storage_preserves_empty_segments_order_and_backing_identity() {
        let frame = encode(response(
            208,
            OPAQUE,
            SerializeType::JSON,
            Bytes::from_static(b"segmented-body"),
        ));
        let segments = vec![
            Bytes::new(),
            frame.slice(..3),
            Bytes::new(),
            frame.slice(3..11),
            Bytes::new(),
            frame.slice(11..),
            Bytes::new(),
        ];
        let vector_pointer = segments.as_ptr();
        let identities = segments
            .iter()
            .map(|segment| (segment.as_ptr(), segment.len()))
            .collect::<Vec<_>>();

        let trusted =
            TrustedEncodedFrame::try_from_segments(segments, FrameLimits::default(), original_identity(OPAQUE, false))
                .expect("segmented response should be sealed");

        match trusted.storage {
            EncodedFrameStorage::Segmented(stored) => {
                assert_eq!(stored.as_ptr(), vector_pointer);
                assert_eq!(stored.len(), identities.len());
                assert_eq!(
                    stored
                        .iter()
                        .map(|segment| (segment.as_ptr(), segment.len()))
                        .collect::<Vec<_>>(),
                    identities
                );
                assert_eq!(
                    stored.iter().map(Bytes::len).collect::<Vec<_>>(),
                    vec![0, 3, 0, 8, 0, frame.len() - 11, 0]
                );
            }
            EncodedFrameStorage::Contiguous(_) => panic!("segmented input changed storage kind"),
        }
    }

    #[test]
    fn contiguous_owner_lives_until_the_trusted_capability_is_dropped() {
        let frame = encode(response(224, OPAQUE, SerializeType::JSON, Bytes::new()));
        let drops = Arc::new(AtomicUsize::new(0));
        let owned = owned_bytes(frame.to_vec(), Arc::clone(&drops));

        let trusted =
            TrustedEncodedFrame::try_from_bytes(owned, FrameLimits::default(), original_identity(OPAQUE, false))
                .expect("owned contiguous response should be sealed");

        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(trusted);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn every_segment_owner_including_empty_positions_lives_until_capability_drop() {
        let frame = encode(response(
            225,
            OPAQUE,
            SerializeType::ROCKETMQ,
            Bytes::from_static(b"owned-segments"),
        ));
        let parts = vec![
            Vec::new(),
            frame[..3].to_vec(),
            Vec::new(),
            frame[3..11].to_vec(),
            Vec::new(),
            frame[11..].to_vec(),
            Vec::new(),
        ];
        let drop_counts = (0..parts.len())
            .map(|_| Arc::new(AtomicUsize::new(0)))
            .collect::<Vec<_>>();
        let segments = parts
            .into_iter()
            .zip(&drop_counts)
            .map(|(part, drops)| owned_bytes(part, Arc::clone(drops)))
            .collect::<Vec<_>>();
        let identities = segments
            .iter()
            .map(|segment| (segment.as_ptr(), segment.len()))
            .collect::<Vec<_>>();

        let trusted =
            TrustedEncodedFrame::try_from_segments(segments, FrameLimits::default(), original_identity(OPAQUE, false))
                .expect("owned segmented response should be sealed");

        assert_drop_counts(&drop_counts, 0);
        match &trusted.storage {
            EncodedFrameStorage::Segmented(stored) => {
                assert_eq!(
                    stored
                        .iter()
                        .map(|segment| (segment.as_ptr(), segment.len()))
                        .collect::<Vec<_>>(),
                    identities
                );
                assert_eq!(
                    stored.iter().map(Bytes::len).collect::<Vec<_>>(),
                    vec![0, 3, 0, 8, 0, frame.len() - 11, 0]
                );
            }
            EncodedFrameStorage::Contiguous(_) => panic!("segmented input changed storage kind"),
        }
        assert_drop_counts(&drop_counts, 0);

        drop(trusted);
        assert_drop_counts(&drop_counts, 1);
    }

    #[test]
    fn failed_validation_consumes_storage_without_retaining_any_owner_in_the_error() {
        let frame = encode(response(226, OPAQUE, SerializeType::JSON, Bytes::new()));
        let mut trailing = frame.to_vec();
        trailing.push(0);

        let contiguous_drops = Arc::new(AtomicUsize::new(0));
        let contiguous_error = TrustedEncodedFrame::try_from_bytes(
            owned_bytes(trailing.clone(), Arc::clone(&contiguous_drops)),
            FrameLimits::default(),
            original_identity(OPAQUE, false),
        )
        .expect_err("trailing bytes must fail validation");
        assert_eq!(contiguous_drops.load(Ordering::SeqCst), 1);

        let parts = vec![
            Vec::new(),
            trailing[..2].to_vec(),
            Vec::new(),
            trailing[2..].to_vec(),
            Vec::new(),
        ];
        let segment_drop_counts = (0..parts.len())
            .map(|_| Arc::new(AtomicUsize::new(0)))
            .collect::<Vec<_>>();
        let segments = parts
            .into_iter()
            .zip(&segment_drop_counts)
            .map(|(part, drops)| owned_bytes(part, Arc::clone(drops)))
            .collect::<Vec<_>>();
        let segmented_error =
            TrustedEncodedFrame::try_from_segments(segments, FrameLimits::default(), original_identity(OPAQUE, false))
                .expect_err("segmented trailing bytes must fail validation");
        assert_drop_counts(&segment_drop_counts, 1);

        assert!(matches!(
            contiguous_error,
            TrustedEncodedFrameError::FrameValidation { .. }
        ));
        assert!(matches!(
            segmented_error,
            TrustedEncodedFrameError::FrameValidation { .. }
        ));
        assert_eq!(contiguous_drops.load(Ordering::SeqCst), 1);
        assert_drop_counts(&segment_drop_counts, 1);
    }

    #[test]
    fn body_bytes_that_look_like_complete_frames_are_accepted() {
        let nested = encode(response(209, 77, SerializeType::JSON, Bytes::from_static(b"nested")));
        let outer = encode(response(210, OPAQUE, SerializeType::JSON, nested.clone()));

        let trusted =
            TrustedEncodedFrame::try_from_bytes(outer, FrameLimits::default(), original_identity(OPAQUE, false))
                .expect("body content must not be scanned for nested frame prefixes");

        assert_eq!(trusted.metadata().body_len(), nested.len());
    }

    #[test]
    fn original_one_way_rejection_precedes_limits_or_storage_inspection() {
        let invalid_limits = FrameLimits {
            max_frame_bytes: 0,
            max_header_bytes: usize::MAX,
            max_body_bytes: 0,
            initial_read_bytes: 0,
        };

        assert!(matches!(
            TrustedEncodedFrame::try_from_bytes(Bytes::new(), invalid_limits, original_identity(OPAQUE, true),),
            Err(TrustedEncodedFrameError::OriginalRequestOneWay)
        ));
    }

    #[test]
    fn invalid_frame_limits_are_rejected() {
        let frame = encode(response(211, OPAQUE, SerializeType::JSON, Bytes::new()));
        let invalid_limits = FrameLimits {
            max_frame_bytes: 7,
            ..FrameLimits::default()
        };

        assert_frame_validation_error(TrustedEncodedFrame::try_from_bytes(
            frame,
            invalid_limits,
            original_identity(OPAQUE, false),
        ));
    }

    #[test]
    fn exact_frame_header_and_body_limits_are_accepted() {
        let body = Bytes::from_static(b"limit-body");
        let frame = encode(response(212, OPAQUE, SerializeType::JSON, body.clone()));
        let limits = FrameLimits::try_new(frame.len(), header_len(&frame), body.len(), 8)
            .expect("exact limits should form a valid profile");

        let _ = TrustedEncodedFrame::try_from_bytes(frame, limits, original_identity(OPAQUE, false))
            .expect("response at every exact limit should be valid");
    }

    #[test]
    fn frame_one_byte_over_limit_is_rejected() {
        let frame = encode(response(213, OPAQUE, SerializeType::JSON, Bytes::new()));
        let limits = FrameLimits::try_new(frame.len() - 1, header_len(&frame), 0, 8).expect("valid profile");

        assert_frame_validation_error(TrustedEncodedFrame::try_from_bytes(
            frame,
            limits,
            original_identity(OPAQUE, false),
        ));
    }

    #[test]
    fn header_one_byte_over_limit_is_rejected() {
        let frame = encode(response(214, OPAQUE, SerializeType::JSON, Bytes::new()));
        let header_limit = header_len(&frame) - 1;
        let limits = FrameLimits::try_new(frame.len(), header_limit, 0, 8).expect("valid profile");

        assert_frame_validation_error(TrustedEncodedFrame::try_from_bytes(
            frame,
            limits,
            original_identity(OPAQUE, false),
        ));
    }

    #[test]
    fn body_one_byte_over_limit_is_rejected() {
        let body = Bytes::from_static(b"body");
        let frame = encode(response(215, OPAQUE, SerializeType::JSON, body.clone()));
        let limits = FrameLimits::try_new(frame.len(), header_len(&frame), body.len() - 1, 8).expect("valid profile");

        assert_frame_validation_error(TrustedEncodedFrame::try_from_bytes(
            frame,
            limits,
            original_identity(OPAQUE, false),
        ));
    }

    #[test]
    fn frames_shorter_than_the_wire_envelope_are_rejected() {
        for len in 0..FRAME_ENVELOPE_LEN {
            assert_frame_validation_error(TrustedEncodedFrame::try_from_bytes(
                Bytes::from(vec![0; len]),
                FrameLimits::default(),
                original_identity(OPAQUE, false),
            ));
        }
    }

    #[test]
    fn negative_zero_and_too_small_announced_lengths_are_rejected() {
        for announced in [-1_i32, 0, 3] {
            let mut frame = Vec::with_capacity(FRAME_ENVELOPE_LEN);
            frame.extend_from_slice(&announced.to_be_bytes());
            frame.extend_from_slice(&0_u32.to_be_bytes());
            assert_frame_validation_error(TrustedEncodedFrame::try_from_bytes(
                Bytes::from(frame),
                FrameLimits::default(),
                original_identity(OPAQUE, false),
            ));
        }
    }

    #[test]
    fn truncated_and_trailing_frames_are_rejected() {
        let valid = encode(response(216, OPAQUE, SerializeType::JSON, Bytes::from_static(b"body")));
        let truncated = valid.slice(..valid.len() - 1);
        let mut trailing = valid.to_vec();
        trailing.push(0);

        assert_frame_validation_error(TrustedEncodedFrame::try_from_bytes(
            truncated,
            FrameLimits::default(),
            original_identity(OPAQUE, false),
        ));
        assert_frame_validation_error(TrustedEncodedFrame::try_from_bytes(
            Bytes::from(trailing),
            FrameLimits::default(),
            original_identity(OPAQUE, false),
        ));
    }

    #[test]
    fn two_concatenated_frames_are_rejected() {
        let frame = encode(response(217, OPAQUE, SerializeType::JSON, Bytes::new()));
        let mut concatenated = Vec::with_capacity(frame.len() * 2);
        concatenated.extend_from_slice(&frame);
        concatenated.extend_from_slice(&frame);

        assert_frame_validation_error(TrustedEncodedFrame::try_from_bytes(
            Bytes::from(concatenated),
            FrameLimits::default(),
            original_identity(OPAQUE, false),
        ));
    }

    #[test]
    fn header_length_outside_the_frame_envelope_is_rejected() {
        let mut frame = Vec::with_capacity(FRAME_ENVELOPE_LEN);
        frame.extend_from_slice(&4_i32.to_be_bytes());
        frame.extend_from_slice(&1_u32.to_be_bytes());

        assert_frame_validation_error(TrustedEncodedFrame::try_from_bytes(
            Bytes::from(frame),
            FrameLimits::default(),
            original_identity(OPAQUE, false),
        ));
    }

    #[test]
    fn unknown_serialization_marker_is_rejected() {
        let frame = encode(response(218, OPAQUE, SerializeType::JSON, Bytes::new()));
        let mut unknown = frame.to_vec();
        unknown[4] = 2;

        assert!(matches!(
            TrustedEncodedFrame::try_from_bytes(
                Bytes::from(unknown),
                FrameLimits::default(),
                original_identity(OPAQUE, false),
            ),
            Err(TrustedEncodedFrameError::UnsupportedSerializationType { code: 2 })
        ));
    }

    #[test]
    fn malformed_json_and_binary_headers_are_rejected() {
        for serialize_type in [SerializeType::JSON, SerializeType::ROCKETMQ] {
            let frame = encode(response(219, OPAQUE, serialize_type, Bytes::new()));
            let length = header_len(&frame);
            let mut malformed = frame.to_vec();
            malformed[FRAME_ENVELOPE_LEN..FRAME_ENVELOPE_LEN + length].fill(0xff);

            assert!(matches!(
                TrustedEncodedFrame::try_from_bytes(
                    Bytes::from(malformed),
                    FrameLimits::default(),
                    original_identity(OPAQUE, false),
                ),
                Err(TrustedEncodedFrameError::HeaderDecode {
                    serialize_type: actual,
                    ..
                }) if actual == serialize_type
            ));
        }
    }

    #[test]
    fn request_shaped_frame_is_rejected() {
        let frame = encode(
            RemotingCommand::create_remoting_command(220)
                .set_opaque(OPAQUE)
                .set_serialize_type(SerializeType::JSON),
        );

        assert!(matches!(
            TrustedEncodedFrame::try_from_bytes(frame, FrameLimits::default(), original_identity(OPAQUE, false),),
            Err(TrustedEncodedFrameError::RequestCommand)
        ));
    }

    #[test]
    fn one_way_response_frame_is_rejected() {
        let frame = encode(response(221, OPAQUE, SerializeType::JSON, Bytes::new()).mark_oneway_rpc());

        assert!(matches!(
            TrustedEncodedFrame::try_from_bytes(frame, FrameLimits::default(), original_identity(OPAQUE, false),),
            Err(TrustedEncodedFrameError::ResponseOneWay)
        ));
    }

    #[test]
    fn opaque_mismatch_is_rejected_without_rewriting() {
        let frame = encode(response(222, OPAQUE + 1, SerializeType::JSON, Bytes::new()));

        assert!(matches!(
            TrustedEncodedFrame::try_from_bytes(
                frame,
                FrameLimits::default(),
                original_identity(OPAQUE, false),
            ),
            Err(TrustedEncodedFrameError::OpaqueMismatch {
                expected: OPAQUE,
                actual,
            }) if actual == OPAQUE + 1
        ));
    }

    #[test]
    fn matching_negative_opaque_is_accepted_with_exact_signed_metadata() {
        const NEGATIVE_OPAQUE: i32 = -1_987_654_321;
        let frame = encode(response(227, NEGATIVE_OPAQUE, SerializeType::JSON, Bytes::new()));

        let trusted = TrustedEncodedFrame::try_from_bytes(
            frame,
            FrameLimits::default(),
            original_identity(NEGATIVE_OPAQUE, false),
        )
        .expect("matching negative opaque should preserve signed i32 identity");

        assert_eq!(trusted.metadata().opaque(), NEGATIVE_OPAQUE);
    }

    #[test]
    fn mismatching_negative_opaque_fails_closed_with_exact_signed_values() {
        const EXPECTED: i32 = -1_987_654_321;
        const ACTUAL: i32 = -1_987_654_320;
        let frame = encode(response(228, ACTUAL, SerializeType::ROCKETMQ, Bytes::new()));

        assert!(matches!(
            TrustedEncodedFrame::try_from_bytes(frame, FrameLimits::default(), original_identity(EXPECTED, false),),
            Err(TrustedEncodedFrameError::OpaqueMismatch {
                expected: EXPECTED,
                actual: ACTUAL,
            })
        ));
    }

    #[test]
    fn metadata_retains_exact_lengths_raw_code_opaque_and_serialization() {
        let body = Bytes::from_static(b"metadata-body");
        let frame = encode(response(-47, OPAQUE, SerializeType::ROCKETMQ, body.clone()));
        let expected_header_len = header_len(&frame);
        let expected_encoded_len = frame.len();
        let trusted =
            TrustedEncodedFrame::try_from_bytes(frame, FrameLimits::default(), original_identity(OPAQUE, false))
                .expect("metadata fixture should be valid");
        let metadata = trusted.metadata();

        assert_eq!(metadata.encoded_len(), expected_encoded_len);
        assert_eq!(metadata.header_len(), expected_header_len);
        assert_eq!(metadata.body_len(), body.len());
        assert_eq!(metadata.response_code(), -47);
        assert_eq!(metadata.opaque(), OPAQUE);
        assert_eq!(metadata.serialize_type(), SerializeType::ROCKETMQ);
    }

    #[test]
    fn capability_and_metadata_debug_redact_storage_header_secrets_and_wire_opaque() {
        const SENSITIVE_OPAQUE: i32 = -1_357_924_681;
        let secrets = [
            "secret-remark-material",
            "secret-ext-key-material",
            "secret-ext-value-material",
            "secret-body-frame-content",
        ];
        let frame = encode(
            response(
                229,
                SENSITIVE_OPAQUE,
                SerializeType::JSON,
                Bytes::from_static(b"secret-body-frame-content"),
            )
            .set_remark("secret-remark-material")
            .set_ext_fields(HashMap::from([(
                "secret-ext-key-material".into(),
                "secret-ext-value-material".into(),
            )])),
        );
        let trusted = TrustedEncodedFrame::try_from_bytes(
            frame,
            FrameLimits::default(),
            original_identity(SENSITIVE_OPAQUE, false),
        )
        .expect("debug fixture should be valid");

        let capability_debug = format!("{trusted:?}");
        let metadata_debug = format!("{:?}", trusted.metadata());
        assert!(capability_debug.contains("TrustedEncodedFrame"));
        assert!(capability_debug.contains("ValidatedFrameMetadata"));
        assert!(!capability_debug.contains("storage"));
        assert!(metadata_debug.contains("ValidatedFrameMetadata"));
        assert!(!metadata_debug.contains("opaque"));
        assert_redacted(&capability_debug, &secrets, &[SENSITIVE_OPAQUE]);
        assert_redacted(&metadata_debug, &secrets, &[SENSITIVE_OPAQUE]);
    }

    #[test]
    fn error_debug_and_display_are_redacted_while_typed_sources_remain_available() {
        const EXPECTED_OPAQUE: i32 = -1_246_813_579;
        const ACTUAL_OPAQUE: i32 = -1_246_813_578;
        let source_secrets = [
            "secret-frame-format",
            "secret-frame-source-detail",
            "secret-header-format",
            "secret-header-source-detail",
        ];

        let frame_error = TrustedEncodedFrameError::FrameValidation {
            source: rocketmq_error::SerializationError::decode_failed(
                "secret-frame-format",
                "secret-frame-source-detail",
            )
            .into(),
        };
        assert!(frame_error
            .source()
            .expect("frame validation should retain its typed source")
            .to_string()
            .contains("secret-frame-source-detail"));
        for output in [format!("{frame_error:?}"), frame_error.to_string()] {
            assert_redacted(&output, &source_secrets, &[EXPECTED_OPAQUE, ACTUAL_OPAQUE]);
        }

        let header_error = TrustedEncodedFrameError::HeaderDecode {
            serialize_type: SerializeType::JSON,
            source: rocketmq_error::SerializationError::decode_failed(
                "secret-header-format",
                "secret-header-source-detail",
            )
            .into(),
        };
        assert!(header_error
            .source()
            .expect("header decode should retain its typed source")
            .to_string()
            .contains("secret-header-source-detail"));
        for output in [format!("{header_error:?}"), header_error.to_string()] {
            assert!(output.contains("JSON"));
            assert_redacted(&output, &source_secrets, &[EXPECTED_OPAQUE, ACTUAL_OPAQUE]);
        }

        let opaque_error = TrustedEncodedFrameError::OpaqueMismatch {
            expected: EXPECTED_OPAQUE,
            actual: ACTUAL_OPAQUE,
        };
        for output in [format!("{opaque_error:?}"), opaque_error.to_string()] {
            assert!(output.contains("opaque") || output.contains("OpaqueMismatch"));
            assert_redacted(&output, &[], &[EXPECTED_OPAQUE, ACTUAL_OPAQUE]);
        }

        let frame_secrets = [
            "secret-error-remark",
            "secret-error-ext-key",
            "secret-error-ext-value",
            "secret-error-body-frame-content",
        ];
        let frame = encode(
            response(
                230,
                ACTUAL_OPAQUE,
                SerializeType::JSON,
                Bytes::from_static(b"secret-error-body-frame-content"),
            )
            .set_remark("secret-error-remark")
            .set_ext_fields(HashMap::from([(
                "secret-error-ext-key".into(),
                "secret-error-ext-value".into(),
            )])),
        );
        let validated_error = TrustedEncodedFrame::try_from_bytes(
            frame,
            FrameLimits::default(),
            original_identity(EXPECTED_OPAQUE, false),
        )
        .expect_err("opaque mismatch should fail closed");
        for output in [format!("{validated_error:?}"), validated_error.to_string()] {
            assert_redacted(&output, &frame_secrets, &[EXPECTED_OPAQUE, ACTUAL_OPAQUE]);
        }
    }
}
