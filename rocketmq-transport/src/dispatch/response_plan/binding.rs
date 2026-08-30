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

//! Private ownership boundary between a response plan and ingress identity.

use rocketmq_protocol::protocol::remoting_command::RemotingCommand;

use super::ResponseBody;
use super::ResponsePlan;
use crate::dispatch::OriginalRequestIdentity;
use crate::dispatch::RequestId;

/// A response plan that is bound to immutable ingress identity.
///
/// This capability is intentionally crate-private. It is the only value that
/// the response transport uses to recover the response head and body.
pub(crate) struct BoundResponsePlan {
    request_id: RequestId,
    plan: ResponsePlan,
    opaque_was_corrected: bool,
}

impl BoundResponsePlan {
    /// Returns the ingress-assigned process-local request identity.
    pub(crate) const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Returns whether binding replaced a processor-provided wire opaque.
    #[allow(
        dead_code,
        reason = "the later private dispatch path records this low-cardinality correction fact"
    )]
    pub(crate) const fn opaque_was_corrected(&self) -> bool {
        self.opaque_was_corrected
    }

    /// Consumes the binding capability at the private encoder seam.
    #[allow(dead_code, reason = "the later private response encoder owns the response parts")]
    pub(crate) fn into_parts(self) -> (RequestId, RemotingCommand, ResponseBody) {
        (self.request_id, self.plan.head, self.plan.body)
    }
}

/// Binding failed before a response became eligible for encoding.
#[allow(
    dead_code,
    reason = "the later private dispatcher handles the one-way response omission"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum ResponseBindingError {
    /// The captured inbound request was one-way and cannot produce a response.
    #[error("a one-way request cannot produce a bound response plan")]
    OneWayRequest,
}

impl ResponsePlan {
    /// Binds this owned plan to the immutable identity captured at ingress.
    ///
    /// The one-way decision precedes every plan mutation. Successful binding
    /// overwrites only the raw opaque and ORs in the response-type flag, so
    /// processor-provided response metadata and body ownership stay intact.
    #[allow(
        dead_code,
        reason = "the later private dispatcher creates this response binding before encoding"
    )]
    pub(crate) fn bind(mut self, original: OriginalRequestIdentity) -> Result<BoundResponsePlan, ResponseBindingError> {
        if original.is_one_way() {
            return Err(ResponseBindingError::OneWayRequest);
        }

        let opaque_was_corrected = self.head.opaque() != original.original_opaque();
        self.head.set_opaque_mut(original.original_opaque());
        self.head.mark_response_type_ref();

        Ok(BoundResponsePlan {
            request_id: original.request_id(),
            plan: self,
            opaque_was_corrected,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use bytes::Bytes;
    use cheetah_string::CheetahString;
    use rocketmq_protocol::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    use rocketmq_protocol::protocol::LanguageCode;
    use rocketmq_protocol::protocol::SerializeType;

    use super::*;
    use crate::file_region::FileRegion;
    use crate::file_region::FileRegionLease;
    use crate::file_region::FileRegionSequence;

    fn response_head(opaque: i32) -> RemotingCommand {
        RemotingCommand::create_response_command_with_code(71).set_opaque(opaque)
    }

    fn original_identity(owner: u64, opaque: i32) -> OriginalRequestIdentity {
        let sequence = AtomicU64::new(1);
        let request = RemotingCommand::create_remoting_command(31).set_opaque(opaque);
        OriginalRequestIdentity::capture(owner, &sequence, &request).expect("test identity should be allocated")
    }

    fn one_way_original_identity(opaque: i32) -> OriginalRequestIdentity {
        let sequence = AtomicU64::new(1);
        let request = RemotingCommand::create_remoting_command(31)
            .set_opaque(opaque)
            .mark_oneway_rpc();
        OriginalRequestIdentity::capture(31, &sequence, &request).expect("test identity should be allocated")
    }

    #[test]
    fn binding_uses_the_captured_request_id_and_opaque_for_matched_mismatched_and_negative_values() {
        let matched_original = original_identity(7, 41);
        let matched = ResponsePlan::command(response_head(41))
            .expect("valid response plan")
            .bind(matched_original)
            .expect("non-one-way requests bind");
        assert_eq!(matched.request_id(), matched_original.request_id());
        assert!(!matched.opaque_was_corrected());
        let (_, matched_head, ResponseBody::Empty) = matched.into_parts() else {
            panic!("empty response plan must retain its empty body");
        };
        assert_eq!(matched_head.opaque(), 41);

        let mismatched_original = original_identity(8, 42);
        let mismatched = ResponsePlan::command(response_head(99))
            .expect("valid response plan")
            .bind(mismatched_original)
            .expect("non-one-way requests bind");
        assert_eq!(mismatched.request_id(), mismatched_original.request_id());
        assert!(mismatched.opaque_was_corrected());
        let (_, mismatched_head, _) = mismatched.into_parts();
        assert_eq!(mismatched_head.opaque(), 42);

        let negative_original = original_identity(9, -1_234_567);
        let negative = ResponsePlan::command(response_head(99))
            .expect("valid response plan")
            .bind(negative_original)
            .expect("non-one-way requests bind");
        assert!(negative.opaque_was_corrected());
        let (_, negative_head, _) = negative.into_parts();
        assert_eq!(negative_head.opaque(), -1_234_567);
    }

    #[test]
    fn binding_rejects_captured_one_way_requests_before_plan_canonicalization() {
        let plan = ResponsePlan::command(response_head(-91)).expect("valid response plan");

        assert!(matches!(
            plan.bind(one_way_original_identity(123)),
            Err(ResponseBindingError::OneWayRequest)
        ));
    }

    #[test]
    fn binding_only_sets_the_response_type_bit_and_does_not_report_a_type_correction_as_opaque_correction() {
        const RESPONSE_TYPE_BIT: i32 = 1;
        let preserved_flags = i32::MIN | (1 << 9) | (1 << 24);
        let original = original_identity(10, 53);
        let mut plan = ResponsePlan::command(response_head(53).set_flag(preserved_flags | RESPONSE_TYPE_BIT))
            .expect("valid response plan");

        // Only this private test can imitate a malformed post-construction
        // plan, which exercises the binding layer's defense-in-depth bit set.
        plan.head = plan.head.set_flag(preserved_flags);
        let bound = plan.bind(original).expect("non-one-way requests bind");

        assert!(!bound.opaque_was_corrected());
        let (_, head, _) = bound.into_parts();
        assert_eq!(head.flag(), preserved_flags | RESPONSE_TYPE_BIT);
        assert!(head.is_response_type());
    }

    #[test]
    fn binding_preserves_response_metadata_and_typed_custom_headers() {
        let typed_header = SendMessageResponseHeader::new(
            CheetahString::from("message-id"),
            17,
            91,
            Some(CheetahString::from("transaction-id")),
            Some(CheetahString::from("batch-id")),
            None,
        );
        let mut head = response_head(77)
            .set_code(-19)
            .set_language(LanguageCode::CPP)
            .set_version(-31)
            .set_serialize_type(SerializeType::JSON)
            .set_remark("preserved remark")
            .set_command_custom_header(typed_header);
        head.add_ext_field("extension-key", "extension-value");
        let original = original_identity(11, 88);

        let bound = ResponsePlan::command(head)
            .expect("valid response plan")
            .bind(original)
            .expect("non-one-way requests bind");
        let (request_id, head, ResponseBody::Empty) = bound.into_parts() else {
            panic!("empty response plan must retain its empty body");
        };

        assert_eq!(request_id, original.request_id());
        assert_eq!(head.opaque(), 88);
        assert_eq!(head.code(), -19);
        assert_eq!(head.remark().map(CheetahString::as_str), Some("preserved remark"));
        assert_eq!(head.language(), LanguageCode::CPP);
        assert_eq!(head.version(), -31);
        assert_eq!(head.serialize_type(), SerializeType::JSON);
        assert_eq!(
            head.ext_fields()
                .and_then(|fields| fields.get("extension-key"))
                .map(CheetahString::as_str),
            Some("extension-value")
        );
        let header = head
            .read_custom_header_ref::<SendMessageResponseHeader>()
            .expect("binding must preserve the typed custom header");
        assert_eq!(header.msg_id().as_str(), "message-id");
        assert_eq!(header.queue_id(), 17);
        assert_eq!(header.queue_offset(), 91);
    }

    #[test]
    fn captured_identity_wins_after_a_processor_mutates_its_command() {
        let sequence = AtomicU64::new(1);
        let ingress = RemotingCommand::create_remoting_command(31).set_opaque(17);
        let original = OriginalRequestIdentity::capture(12, &sequence, &ingress).expect("identity should be allocated");
        let processor_mutated = ingress.set_opaque(-999).set_code(987).mark_response_type();

        let bound = ResponsePlan::command(processor_mutated)
            .expect("processor created a valid response-shaped plan")
            .bind(original)
            .expect("non-one-way requests bind");
        let (request_id, head, _) = bound.into_parts();

        assert_eq!(request_id, original.request_id());
        assert_eq!(head.opaque(), 17);
        assert_eq!(head.code(), 987);
    }

    #[test]
    fn matching_opaque_values_do_not_substitute_a_distinct_request_identity() {
        let first_original = original_identity(13, 66);
        let second_original = original_identity(14, 66);
        let first = ResponsePlan::command(response_head(66))
            .expect("valid response plan")
            .bind(first_original)
            .expect("non-one-way requests bind");
        let second = ResponsePlan::command(response_head(66))
            .expect("valid response plan")
            .bind(second_original)
            .expect("non-one-way requests bind");

        assert!(!first.opaque_was_corrected());
        assert!(!second.opaque_was_corrected());
        assert_ne!(first.request_id(), second.request_id());
        assert_eq!(first.request_id(), first_original.request_id());
        assert_eq!(second.request_id(), second_original.request_id());
    }

    #[test]
    fn binding_moves_bytes_and_segments_without_reallocating_or_reordering() {
        let bytes = Bytes::from_static(b"bound bytes");
        let bytes_pointer = bytes.as_ptr();
        let bound_bytes = ResponsePlan::bytes(response_head(1), bytes)
            .expect("valid bytes response")
            .bind(original_identity(15, 2))
            .expect("non-one-way requests bind");
        let (_, head, ResponseBody::Bytes(bytes)) = bound_bytes.into_parts() else {
            panic!("bytes response plan must retain its bytes owner");
        };
        assert_eq!(head.opaque(), 2);
        assert_eq!(bytes.as_ptr(), bytes_pointer);
        assert_eq!(bytes.as_ref(), b"bound bytes");

        let first = Bytes::from_static(b"first");
        let second = Bytes::from_static(b"second");
        let first_pointer = first.as_ptr();
        let second_pointer = second.as_ptr();
        let plan = ResponsePlan::segments(response_head(3), vec![first, second]).expect("valid segments response");
        let (segments_pointer, segments_capacity) = match &plan.body {
            ResponseBody::Segments(segments) => (segments.as_ptr(), segments.capacity()),
            _ => panic!("segments response plan must retain its segments owner"),
        };
        let bound_segments = plan.bind(original_identity(16, 4)).expect("non-one-way requests bind");
        let (_, head, ResponseBody::Segments(segments)) = bound_segments.into_parts() else {
            panic!("segments response plan must retain its segments owner");
        };
        assert_eq!(head.opaque(), 4);
        assert_eq!(segments.as_ptr(), segments_pointer);
        assert_eq!(segments.capacity(), segments_capacity);
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].as_ptr(), first_pointer);
        assert_eq!(segments[1].as_ptr(), second_pointer);
        assert_eq!(segments[0].as_ref(), b"first");
        assert_eq!(segments[1].as_ref(), b"second");
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
    fn binding_moves_file_regions_without_restatting_reallocating_or_shortening_the_lease_lifetime() {
        let drops = Arc::new(AtomicUsize::new(0));
        let file_accesses = Arc::new(AtomicUsize::new(0));
        let mut file = tempfile::tempfile().expect("temporary file");
        file.write_all(b"file regions").expect("write test body");
        let lease = Arc::new(CountingLease {
            file,
            drops: Arc::clone(&drops),
            file_accesses: Arc::clone(&file_accesses),
        });
        let first = FileRegion::try_new(lease.clone(), 7, 5).expect("valid first file region");
        let second = FileRegion::try_new(lease.clone(), 0, 7).expect("valid second file region");
        assert_eq!(file_accesses.load(Ordering::SeqCst), 2);
        assert_eq!(Arc::strong_count(&lease), 3);

        let mut input = Vec::with_capacity(4);
        input.push(first);
        input.push(second);
        let input_pointer = input.as_ptr();
        let input_capacity = input.capacity();
        let regions = FileRegionSequence::try_new(input).expect("valid file region sequence");
        assert_eq!(regions.regions().as_ptr(), input_pointer);
        assert_eq!(regions.regions_capacity(), input_capacity);
        let regions_pointer = regions.regions().as_ptr();

        let bound = ResponsePlan::file_regions(response_head(5), regions)
            .expect("valid file-region response")
            .bind(original_identity(17, 6))
            .expect("non-one-way requests bind");
        assert_eq!(file_accesses.load(Ordering::SeqCst), 2);
        assert_eq!(Arc::strong_count(&lease), 3);

        let (_, head, ResponseBody::FileRegions(regions)) = bound.into_parts() else {
            panic!("file-region response plan must retain its file-region owner");
        };
        assert_eq!(head.opaque(), 6);
        assert_eq!(regions.regions().as_ptr(), regions_pointer);
        assert_eq!(regions.regions_capacity(), input_capacity);
        assert_eq!(regions.regions().len(), 2);
        assert_eq!(regions.regions()[0].offset(), 7);
        assert_eq!(regions.regions()[0].len(), 5);
        assert_eq!(regions.regions()[1].offset(), 0);
        assert_eq!(regions.regions()[1].len(), 7);
        assert_eq!(file_accesses.load(Ordering::SeqCst), 2);
        assert_eq!(Arc::strong_count(&lease), 3);

        drop(regions);
        assert_eq!(Arc::strong_count(&lease), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(lease);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}
