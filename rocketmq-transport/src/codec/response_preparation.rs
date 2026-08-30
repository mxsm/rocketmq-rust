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

//! One-pass preparation of bound structured responses.

use rocketmq_error::SerializationError;

use super::remoting_command_codec::FrameLimits;
use crate::dispatch::BoundResponsePlan;
use crate::dispatch::RequestId;
use crate::dispatch::ResponseBody;
use crate::dispatch::ResponseBodyKind;
use crate::dispatch::ResponseError;
use crate::write_strategy::OutboundPayload;
use crate::write_strategy::PreparedStructuredResponseBody;
use crate::write_strategy::StructuredResponseFrame;

#[allow(
    dead_code,
    reason = "the later private response sink owns this prepared RSP-04 value"
)]
pub(crate) struct PreparedResponse {
    metadata: PreparedResponseMetadata,
    payload: OutboundPayload,
}

#[allow(
    dead_code,
    reason = "the later private response sink consumes prepared metadata and payload ownership"
)]
impl PreparedResponse {
    pub(crate) const fn metadata(&self) -> &PreparedResponseMetadata {
        &self.metadata
    }

    pub(crate) fn into_parts(self) -> (PreparedResponseMetadata, OutboundPayload) {
        (self.metadata, self.payload)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(
    dead_code,
    reason = "the later private response sink uses this RSP-04 admission and completion metadata"
)]
pub(crate) struct PreparedResponseMetadata {
    request_id: RequestId,
    response_code: i32,
    body_kind: ResponseBodyKind,
    body_len: usize,
    body_part_count: usize,
    encoded_len: usize,
    opaque_was_corrected: bool,
}

#[allow(
    dead_code,
    reason = "the later private response sink reads prepared response metadata"
)]
impl PreparedResponseMetadata {
    pub(crate) const fn request_id(self) -> RequestId {
        self.request_id
    }

    pub(crate) const fn response_code(self) -> i32 {
        self.response_code
    }

    pub(crate) const fn body_kind(self) -> ResponseBodyKind {
        self.body_kind
    }

    pub(crate) const fn body_len(self) -> usize {
        self.body_len
    }

    pub(crate) const fn body_part_count(self) -> usize {
        self.body_part_count
    }

    pub(crate) const fn encoded_len(self) -> usize {
        self.encoded_len
    }

    pub(crate) const fn opaque_was_corrected(self) -> bool {
        self.opaque_was_corrected
    }
}

#[allow(
    dead_code,
    reason = "the later private response sink invokes this RSP-04 preparation seam"
)]
pub(crate) fn prepare_response(
    bound: BoundResponsePlan,
    limits: FrameLimits,
) -> Result<PreparedResponse, ResponseError> {
    let opaque_was_corrected = bound.opaque_was_corrected();
    let (request_id, head, body) = bound.into_parts();
    let response_code = head.code();
    let (body_kind, body_len, body_part_count, body) = prepare_body(body).map_err(encode_error)?;
    let head = limits.encode_frame_head(head, body_len).map_err(encode_error)?;
    let encoded_len = head.encoded_len();
    let payload = match body {
        PreparedBody::Structured(body) => {
            StructuredResponseFrame::new(head, body).map(OutboundPayload::StructuredFrame)
        }
        PreparedBody::FileRegions(body) => Ok(OutboundPayload::FileFrame { head, body }),
    }
    .map_err(encode_error)?;

    Ok(PreparedResponse {
        metadata: PreparedResponseMetadata {
            request_id,
            response_code,
            body_kind,
            body_len,
            body_part_count,
            encoded_len,
            opaque_was_corrected,
        },
        payload,
    })
}

#[allow(
    dead_code,
    reason = "the later private response preparation seam computes body metadata before encoding"
)]
fn prepare_body(body: ResponseBody) -> rocketmq_error::RocketMQResult<(ResponseBodyKind, usize, usize, PreparedBody)> {
    match body {
        ResponseBody::Empty => {
            let body = PreparedStructuredResponseBody::empty()?;
            Ok((
                ResponseBodyKind::Empty,
                body.body_len(),
                0,
                PreparedBody::Structured(body),
            ))
        }
        ResponseBody::Bytes(bytes) => {
            let body = PreparedStructuredResponseBody::bytes(bytes)?;
            Ok((
                ResponseBodyKind::Bytes,
                body.body_len(),
                1,
                PreparedBody::Structured(body),
            ))
        }
        ResponseBody::Segments(segments) => {
            let body_part_count = segments.len();
            let body = PreparedStructuredResponseBody::segments(segments)?;
            Ok((
                ResponseBodyKind::Segments,
                body.body_len(),
                body_part_count,
                PreparedBody::Structured(body),
            ))
        }
        ResponseBody::FileRegions(regions) => {
            let body_len = usize::try_from(regions.len()).map_err(|_| {
                SerializationError::encode_failed(
                    "structured-response-frame",
                    "file-region response body length is not representable as usize",
                )
            })?;
            let body_part_count = regions.regions().len();
            Ok((
                ResponseBodyKind::FileRegions,
                body_len,
                body_part_count,
                PreparedBody::FileRegions(regions),
            ))
        }
    }
}

enum PreparedBody {
    Structured(PreparedStructuredResponseBody),
    FileRegions(crate::file_region::FileRegionSequence),
}

#[allow(
    dead_code,
    reason = "the later private response preparation seam retains typed encoding failures"
)]
fn encode_error(source: rocketmq_error::RocketMQError) -> ResponseError {
    ResponseError::Encode { source }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::Read;
    use std::io::Seek;
    use std::io::SeekFrom;
    use std::io::Write;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use bytes::Bytes;
    use cheetah_string::CheetahString;
    use rocketmq_protocol::protocol::command_custom_header::CommandCustomHeader;
    use rocketmq_protocol::protocol::encoded_frame::EncodedFrame;
    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    use rocketmq_protocol::protocol::SerializeType;

    use super::*;
    use crate::dispatch::OriginalRequestIdentity;
    use crate::dispatch::ResponsePlan;
    use crate::file_region::FileRegion;
    use crate::file_region::FileRegionLease;
    use crate::file_region::FileRegionSequence;

    fn bind(plan: ResponsePlan, owner_id: u64, original_opaque: i32) -> BoundResponsePlan {
        let sequence = AtomicU64::new(1);
        let request = RemotingCommand::create_remoting_command(41).set_opaque(original_opaque);
        let original = OriginalRequestIdentity::capture(owner_id, &sequence, &request)
            .expect("test request identity should be allocated");
        plan.bind(original).expect("ordinary requests should bind")
    }

    fn response_head(code: i32, opaque: i32, serialize_type: SerializeType) -> RemotingCommand {
        RemotingCommand::create_response_command_with_code(code)
            .set_opaque(opaque)
            .set_serialize_type(serialize_type)
            .set_remark("prepared-response")
    }

    fn structured_wire(payload: &OutboundPayload) -> Vec<u8> {
        let OutboundPayload::StructuredFrame(frame) = payload else {
            panic!("expected an in-memory structured frame");
        };
        frame
            .test_segments()
            .into_iter()
            .flat_map(|segment| segment.iter().copied())
            .collect()
    }

    fn complete_wire(payload: &OutboundPayload) -> Vec<u8> {
        match payload {
            OutboundPayload::StructuredFrame(_) => structured_wire(payload),
            OutboundPayload::FileFrame { head, body } => {
                let mut wire = head
                    .segments()
                    .into_iter()
                    .flat_map(|segment| segment.iter().copied())
                    .collect::<Vec<_>>();
                for region in body.regions() {
                    let mut file = region.lease().file().try_clone().expect("clone test file lease");
                    file.seek(SeekFrom::Start(region.offset()))
                        .expect("seek to file response region");
                    let mut bytes = vec![0; usize::try_from(region.len()).expect("test region length")];
                    file.read_exact(&mut bytes).expect("read complete file response region");
                    wire.extend_from_slice(&bytes);
                }
                assert_eq!(wire.len(), head.encoded_len());
                wire
            }
            _ => panic!("expected a structured response payload"),
        }
    }

    #[test]
    fn all_body_kinds_report_exact_metadata_and_retain_the_bound_identity() {
        let cases = [
            (
                ResponsePlan::command(response_head(71, -1, SerializeType::JSON)).expect("empty plan"),
                ResponseBodyKind::Empty,
                0,
                0,
            ),
            (
                ResponsePlan::bytes(response_head(72, -1, SerializeType::JSON), Bytes::from_static(b"bytes"))
                    .expect("bytes plan"),
                ResponseBodyKind::Bytes,
                5,
                1,
            ),
            (
                ResponsePlan::segments(
                    response_head(73, -1, SerializeType::JSON),
                    vec![Bytes::from_static(b"left"), Bytes::from_static(b"right")],
                )
                .expect("segments plan"),
                ResponseBodyKind::Segments,
                9,
                2,
            ),
        ];

        for (index, (plan, kind, body_len, body_part_count)) in cases.into_iter().enumerate() {
            let prepared = prepare_response(
                bind(plan, 100 + index as u64, 900 + index as i32),
                FrameLimits::default(),
            )
            .expect("response should prepare");
            let metadata = *prepared.metadata();
            assert_eq!(metadata.request_id().owner_id(), 100 + index as u64);
            assert_eq!(metadata.response_code(), 71 + index as i32);
            assert_eq!(metadata.body_kind(), kind);
            assert_eq!(metadata.body_len(), body_len);
            assert_eq!(metadata.body_part_count(), body_part_count);
            assert!(metadata.opaque_was_corrected());
            let copied = metadata;
            assert_eq!(copied, metadata, "metadata remains copyable");

            let (parts_metadata, payload) = prepared.into_parts();
            assert_eq!(parts_metadata, metadata);
            assert_eq!(payload.encoded_len(), metadata.encoded_len());
            assert_eq!(structured_wire(&payload).len(), metadata.encoded_len());
        }

        let mut file = tempfile::tempfile().expect("temporary file");
        file.write_all(b"file-body").expect("write file body");
        let sequence =
            FileRegionSequence::single(FileRegion::try_new(Arc::new(file), 0, 9).expect("valid file region"));
        let prepared = prepare_response(
            bind(
                ResponsePlan::file_regions(response_head(74, 903, SerializeType::JSON), sequence).expect("file plan"),
                103,
                903,
            ),
            FrameLimits::default(),
        )
        .expect("file response should prepare");
        let metadata = *prepared.metadata();
        assert_eq!(metadata.request_id().owner_id(), 103);
        assert_eq!(metadata.response_code(), 74);
        assert_eq!(metadata.body_kind(), ResponseBodyKind::FileRegions);
        assert_eq!(metadata.body_len(), 9);
        assert_eq!(metadata.body_part_count(), 1);
        assert!(!metadata.opaque_was_corrected());
        let (_, payload) = prepared.into_parts();
        assert!(matches!(payload, OutboundPayload::FileFrame { .. }));
        assert_eq!(payload.encoded_len(), metadata.encoded_len());
    }

    #[test]
    fn every_body_representation_matches_the_canonical_complete_frame() {
        for serialize_type in [SerializeType::JSON, SerializeType::ROCKETMQ] {
            let empty_head = response_head(81, -77, serialize_type);
            let empty_expected = EncodedFrame::from_command(empty_head.clone().set_opaque(377))
                .expect("canonical empty command should encode")
                .into_bytes();
            let empty = prepare_response(
                bind(ResponsePlan::command(empty_head).expect("empty plan"), 201, 377),
                FrameLimits::default(),
            )
            .expect("empty response should prepare");
            let empty_metadata = *empty.metadata();
            let (_, empty_payload) = empty.into_parts();
            assert_eq!(complete_wire(&empty_payload), empty_expected);
            assert_eq!(empty_payload.encoded_len(), empty_metadata.encoded_len());

            let canonical_body = Bytes::from_static(b"left-right");
            let expected = EncodedFrame::from_command(
                response_head(82, -77, serialize_type)
                    .set_opaque(377)
                    .set_body(canonical_body.clone()),
            )
            .expect("canonical body command should encode")
            .into_bytes();
            let bytes_plan = ResponsePlan::bytes(response_head(82, -77, serialize_type), canonical_body.clone())
                .expect("bytes plan");
            let segments_plan = ResponsePlan::segments(
                response_head(82, -77, serialize_type),
                vec![
                    Bytes::from_static(b"left"),
                    Bytes::from_static(b"-"),
                    Bytes::from_static(b"right"),
                ],
            )
            .expect("segments plan");

            let mut file = tempfile::tempfile().expect("temporary file");
            file.write_all(b"xxleft--rightyy").expect("write file fixture");
            let file: Arc<dyn FileRegionLease> = Arc::new(file);
            let file_regions = FileRegionSequence::try_new(vec![
                FileRegion::try_new(Arc::clone(&file), 2, 4).expect("left file region"),
                FileRegion::try_new(Arc::clone(&file), 7, 6).expect("separator and right file region"),
            ])
            .expect("ordered file regions");
            let file_plan = ResponsePlan::file_regions(response_head(82, -77, serialize_type), file_regions)
                .expect("file-region plan");

            for (owner, expected_kind, plan) in [
                (202, ResponseBodyKind::Bytes, bytes_plan),
                (203, ResponseBodyKind::Segments, segments_plan),
                (204, ResponseBodyKind::FileRegions, file_plan),
            ] {
                let prepared =
                    prepare_response(bind(plan, owner, 377), FrameLimits::default()).expect("response should prepare");
                let metadata = *prepared.metadata();
                let (_, payload) = prepared.into_parts();

                assert_eq!(metadata.body_kind(), expected_kind);
                assert_eq!(
                    complete_wire(&payload),
                    expected,
                    "{serialize_type:?} {expected_kind:?}"
                );
                assert_eq!(payload.encoded_len(), metadata.encoded_len());
            }
        }
    }

    #[test]
    fn preparation_moves_bytes_segment_buffers_and_the_segment_vector_allocation() {
        let bytes = Bytes::from_static(b"same bytes allocation");
        let bytes_pointer = bytes.as_ptr();
        let prepared = prepare_response(
            bind(
                ResponsePlan::bytes(response_head(82, 9, SerializeType::JSON), bytes).expect("bytes plan"),
                202,
                9,
            ),
            FrameLimits::default(),
        )
        .expect("bytes response should prepare");
        let (_, payload) = prepared.into_parts();
        let OutboundPayload::StructuredFrame(frame) = payload else {
            panic!("expected structured frame");
        };
        let wire_segments = frame.test_segments();
        assert_eq!(wire_segments.last().expect("body segment").as_ptr(), bytes_pointer);

        let first = Bytes::from_static(b"first");
        let second = Bytes::from_static(b"second");
        let first_pointer = first.as_ptr();
        let second_pointer = second.as_ptr();
        let mut input = Vec::with_capacity(4);
        input.push(first);
        input.push(second);
        let vector_pointer = input.as_ptr();
        let prepared = prepare_response(
            bind(
                ResponsePlan::segments(response_head(83, 10, SerializeType::JSON), input).expect("segments plan"),
                203,
                10,
            ),
            FrameLimits::default(),
        )
        .expect("segmented response should prepare");
        let (_, payload) = prepared.into_parts();
        let OutboundPayload::StructuredFrame(frame) = payload else {
            panic!("expected structured frame");
        };
        let segments = frame.test_body_segments().expect("segmented body storage");
        assert_eq!(segments.as_ptr(), vector_pointer);
        assert_eq!(segments[0].as_ptr(), first_pointer);
        assert_eq!(segments[1].as_ptr(), second_pointer);
        assert_eq!(segments[0].as_ref(), b"first");
        assert_eq!(segments[1].as_ref(), b"second");
    }

    struct CountingHeader {
        encodes: Arc<AtomicUsize>,
    }

    impl CommandCustomHeader for CountingHeader {
        fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
            self.encodes.fetch_add(1, Ordering::SeqCst);
            Some(HashMap::from([(
                CheetahString::from_static_str("counted"),
                CheetahString::from_static_str("once"),
            )]))
        }
    }

    fn counting_plan(encodes: Arc<AtomicUsize>, body: Bytes, serialize_type: SerializeType) -> ResponsePlan {
        ResponsePlan::bytes(
            RemotingCommand::create_response_command_with_code(84)
                .set_opaque(11)
                .set_serialize_type(serialize_type)
                .set_command_custom_header(CountingHeader { encodes }),
            body,
        )
        .expect("counting response plan")
    }

    #[test]
    fn encoder_runs_exactly_once_and_limit_failures_never_retry() {
        for serialize_type in [SerializeType::JSON, SerializeType::ROCKETMQ] {
            let success_count = Arc::new(AtomicUsize::new(0));
            prepare_response(
                bind(
                    counting_plan(Arc::clone(&success_count), Bytes::from_static(b"body"), serialize_type),
                    204,
                    11,
                ),
                FrameLimits::default(),
            )
            .expect("response should prepare");
            assert_eq!(success_count.load(Ordering::SeqCst), 1, "{serialize_type:?}");
        }

        let lower_bound_count = Arc::new(AtomicUsize::new(0));
        let limits = FrameLimits {
            max_body_bytes: 3,
            ..FrameLimits::default()
        };
        let Err(error) = prepare_response(
            bind(
                counting_plan(
                    Arc::clone(&lower_bound_count),
                    Bytes::from_static(b"body"),
                    SerializeType::JSON,
                ),
                205,
                11,
            ),
            limits,
        ) else {
            panic!("known body overflow should fail before serialization");
        };
        assert!(matches!(error, ResponseError::Encode { .. }));
        assert_eq!(lower_bound_count.load(Ordering::SeqCst), 0);

        let post_encode_count = Arc::new(AtomicUsize::new(0));
        let limits = FrameLimits {
            max_header_bytes: 0,
            ..FrameLimits::default()
        };
        let Err(error) = prepare_response(
            bind(
                counting_plan(
                    Arc::clone(&post_encode_count),
                    Bytes::from_static(b"body"),
                    SerializeType::JSON,
                ),
                206,
                11,
            ),
            limits,
        ) else {
            panic!("exact header overflow should fail after one serialization");
        };
        assert!(matches!(error, ResponseError::Encode { .. }));
        assert_eq!(post_encode_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn exact_configured_limits_pass_and_one_byte_smaller_limits_fail_at_the_expected_stage() {
        let body = Bytes::from_static(b"limit-body");
        let baseline = prepare_response(
            bind(
                ResponsePlan::bytes(response_head(85, 12, SerializeType::JSON), body.clone()).expect("baseline plan"),
                207,
                12,
            ),
            FrameLimits::default(),
        )
        .expect("baseline response should prepare");
        let metadata = *baseline.metadata();
        let (_, payload) = baseline.into_parts();
        let OutboundPayload::StructuredFrame(frame) = payload else {
            panic!("expected structured frame");
        };
        let header_len = frame.test_segments()[1].len();
        let exact = FrameLimits {
            max_frame_bytes: metadata.encoded_len(),
            max_header_bytes: header_len,
            max_body_bytes: body.len(),
            initial_read_bytes: 8,
        };
        let prepared = prepare_response(
            bind(
                ResponsePlan::bytes(response_head(85, 12, SerializeType::JSON), body.clone()).expect("exact plan"),
                208,
                12,
            ),
            exact,
        )
        .expect("exact limits should pass");
        assert_eq!(prepared.metadata().encoded_len(), metadata.encoded_len());

        for limits in [
            FrameLimits {
                max_frame_bytes: metadata.encoded_len() - 1,
                ..exact
            },
            FrameLimits {
                max_header_bytes: header_len - 1,
                ..exact
            },
            FrameLimits {
                max_body_bytes: body.len() - 1,
                ..exact
            },
        ] {
            let Err(error) = prepare_response(
                bind(
                    ResponsePlan::bytes(response_head(85, 12, SerializeType::JSON), body.clone())
                        .expect("one-over plan"),
                    209,
                    12,
                ),
                limits,
            ) else {
                panic!("one-byte excess should fail");
            };
            assert!(matches!(error, ResponseError::Encode { .. }));
        }
    }

    #[test]
    fn frame_profiles_accept_exact_protocol_ceilings_and_reject_one_over() {
        assert!(FrameLimits::try_new(i32::MAX as usize + 4, 0x00ff_ffff, i32::MAX as usize - 4, 8).is_ok());
        assert!(FrameLimits::try_new(i32::MAX as usize + 5, 0x00ff_ffff, i32::MAX as usize - 4, 8).is_err());
        assert!(FrameLimits::try_new(i32::MAX as usize + 4, 0x0100_0000, i32::MAX as usize - 4, 8).is_err());
    }

    #[test]
    fn preparation_accepts_the_exact_24_bit_header_and_rejects_one_byte_over() {
        const MAX_HEADER_BYTES: usize = 0x00ff_ffff;
        let base = response_head(88, 15, SerializeType::JSON).set_remark("");
        let base_head =
            rocketmq_protocol::protocol::encoded_frame::EncodedFrameHead::from_command_and_body_len(base.clone(), 0)
                .expect("base response head");
        let base_header_len = base_head.segments()[1].len();
        let exact_remark_len = MAX_HEADER_BYTES - base_header_len;
        let limits = FrameLimits {
            max_frame_bytes: i32::MAX as usize + 4,
            max_header_bytes: MAX_HEADER_BYTES,
            max_body_bytes: 0,
            initial_read_bytes: 8,
        };

        let exact = response_head(88, 15, SerializeType::JSON).set_remark("a".repeat(exact_remark_len));
        let prepared = prepare_response(
            bind(ResponsePlan::command(exact).expect("exact header plan"), 212, 15),
            limits,
        )
        .expect("24-bit header ceiling should encode");
        let (_, payload) = prepared.into_parts();
        let OutboundPayload::StructuredFrame(frame) = payload else {
            panic!("expected structured frame");
        };
        assert_eq!(frame.test_segments()[1].len(), MAX_HEADER_BYTES);
        drop(frame);

        let over = response_head(88, 15, SerializeType::JSON).set_remark("a".repeat(exact_remark_len + 1));
        let Err(error) = prepare_response(
            bind(ResponsePlan::command(over).expect("one-over header plan"), 213, 15),
            limits,
        ) else {
            panic!("header above the 24-bit field must fail");
        };
        assert!(matches!(error, ResponseError::Encode { .. }));
    }

    #[test]
    fn preparation_accepts_the_exact_signed_frame_length_and_rejects_one_byte_over() {
        let head = response_head(89, 16, SerializeType::JSON).set_remark("");
        let base_head =
            rocketmq_protocol::protocol::encoded_frame::EncodedFrameHead::from_command_and_body_len(head.clone(), 0)
                .expect("base response head");
        let header_len = base_head.segments()[1].len();
        let exact_body_len = i32::MAX as usize - 4 - header_len;
        let file = Arc::new(tempfile::tempfile().expect("temporary sparse file"));
        file.set_len((exact_body_len + 1) as u64)
            .expect("extend sparse file to signed boundary");
        let limits = FrameLimits {
            max_frame_bytes: i32::MAX as usize + 4,
            max_header_bytes: 0x00ff_ffff,
            max_body_bytes: i32::MAX as usize - 4,
            initial_read_bytes: 8,
        };

        let exact_region = FileRegion::try_new(file.clone(), 0, exact_body_len as u64).expect("exact file region");
        let prepared = prepare_response(
            bind(
                ResponsePlan::file_regions(head.clone(), FileRegionSequence::single(exact_region))
                    .expect("exact signed frame plan"),
                214,
                16,
            ),
            limits,
        )
        .expect("signed i32 payload ceiling should encode");
        assert_eq!(prepared.metadata().encoded_len(), i32::MAX as usize + 4);
        drop(prepared);

        let over_region = FileRegion::try_new(file, 0, (exact_body_len + 1) as u64).expect("one-over file region");
        let Err(error) = prepare_response(
            bind(
                ResponsePlan::file_regions(head, FileRegionSequence::single(over_region))
                    .expect("one-over signed frame plan"),
                215,
                16,
            ),
            limits,
        ) else {
            panic!("frame above the signed i32 payload limit must fail");
        };
        assert!(matches!(error, ResponseError::Encode { .. }));
    }

    struct CountingLease {
        file: File,
        file_accesses: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
    }

    impl FileRegionLease for CountingLease {
        fn file(&self) -> &File {
            self.file_accesses.fetch_add(1, Ordering::SeqCst);
            &self.file
        }
    }

    impl Drop for CountingLease {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn counting_file_sequence(
        file_accesses: &Arc<AtomicUsize>,
        drops: &Arc<AtomicUsize>,
    ) -> (Arc<CountingLease>, FileRegionSequence) {
        let mut file = tempfile::tempfile().expect("temporary file");
        file.write_all(b"leased-body").expect("write leased body");
        let lease = Arc::new(CountingLease {
            file,
            file_accesses: Arc::clone(file_accesses),
            drops: Arc::clone(drops),
        });
        let first = FileRegion::try_new(lease.clone(), 0, 6).expect("first region");
        let second = FileRegion::try_new(lease.clone(), 6, 5).expect("second region");
        let sequence = FileRegionSequence::try_new(vec![first, second]).expect("region sequence");
        (lease, sequence)
    }

    #[test]
    fn file_preparation_uses_cached_metadata_without_clone_or_restat_and_releases_on_drop() {
        let file_accesses = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let (lease, sequence) = counting_file_sequence(&file_accesses, &drops);
        assert_eq!(file_accesses.load(Ordering::SeqCst), 2);
        assert_eq!(Arc::strong_count(&lease), 3);
        let prepared = prepare_response(
            bind(
                ResponsePlan::file_regions(response_head(86, 13, SerializeType::JSON), sequence)
                    .expect("file response plan"),
                210,
                13,
            ),
            FrameLimits::default(),
        )
        .expect("file response should prepare");
        assert_eq!(file_accesses.load(Ordering::SeqCst), 2);
        assert_eq!(Arc::strong_count(&lease), 3, "preparation must not clone file leases");
        drop(prepared);
        assert_eq!(Arc::strong_count(&lease), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(lease);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn file_preparation_failure_releases_the_moved_lease_without_restatting() {
        let file_accesses = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let (lease, sequence) = counting_file_sequence(&file_accesses, &drops);
        let Err(error) = prepare_response(
            bind(
                ResponsePlan::file_regions(response_head(87, 14, SerializeType::JSON), sequence)
                    .expect("file response plan"),
                211,
                14,
            ),
            FrameLimits {
                max_body_bytes: 10,
                ..FrameLimits::default()
            },
        ) else {
            panic!("body lower bound should reject preparation");
        };
        assert!(matches!(error, ResponseError::Encode { .. }));
        assert_eq!(file_accesses.load(Ordering::SeqCst), 2);
        assert_eq!(Arc::strong_count(&lease), 1);
        drop(lease);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}
