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

//! Durable identity, ledger, replay, and namespace primitives for mapped-file retirement.

#[allow(
    dead_code,
    reason = "M3 stages Wave-B activation behind the external fleet-fencing evidence gate"
)]
pub(crate) mod activation;

#[allow(
    dead_code,
    reason = "M3.2 stages the pure bootstrap protocol before managed Store activation wiring"
)]
pub(crate) mod bootstrap;

#[allow(
    dead_code,
    reason = "M3.2 stages the pure compaction protocol before managed Store activation wiring"
)]
pub(crate) mod compaction;

#[allow(
    dead_code,
    reason = "M3 stages the byte-exact ledger codec before the durable writer consumes it"
)]
pub(crate) mod codec;

#[allow(
    dead_code,
    reason = "M3 stages validated identity values before the durable ledger consumes them"
)]
pub(crate) mod identity;
#[allow(
    dead_code,
    reason = "M3.2 stages handle-relative ledger I/O before managed Store wiring"
)]
pub(crate) mod io;
pub(crate) mod platform;

#[allow(
    dead_code,
    reason = "M3.2 stages pure replay decisions before namespace reconciliation wiring"
)]
pub(crate) mod replay;

#[allow(
    dead_code,
    reason = "M3 stages the strong retirement registry before managed queue handoff wiring"
)]
pub(crate) mod registry;

#[allow(
    dead_code,
    reason = "M3 stages the immutable sidecar codec before the durable writer consumes it"
)]
pub(crate) mod sidecar;

#[allow(
    dead_code,
    reason = "M3 stages the bounded retirement core before Wave-B Store activation"
)]
pub(crate) mod service;

#[allow(
    dead_code,
    reason = "M3.2 stages replay-validated state before namespace reconciliation wiring"
)]
pub(crate) mod state;

#[allow(
    dead_code,
    reason = "M3.2 stages the durable ledger writer before managed Store wiring"
)]
pub(crate) mod writer;

pub(crate) fn fuzz_decode_lifecycle(input: &[u8]) {
    fuzz_decode_sidecars(input);
    let (expected_sequence, expected_generation) = frame_identity(input).unwrap_or((1, 0));
    fuzz_decode_log(input, expected_sequence, expected_generation);

    if let Some(prefixed_frame) = input.get(16..) {
        let expected_sequence = read_u64(input, 0).unwrap_or(1);
        let expected_generation = read_u64(input, 8).unwrap_or(0);
        fuzz_decode_sidecars(prefixed_frame);
        fuzz_decode_log(prefixed_frame, expected_sequence, expected_generation);
    }
}

fn fuzz_decode_sidecars(input: &[u8]) {
    let _ = sidecar::decode_store_meta(input);
    let _ = sidecar::decode_enabled_marker_slot(input, 0);
    let _ = sidecar::decode_enabled_marker_slot(input, 1);
    let _ = sidecar::decode_enabled_marker_file(input);
    let _ = sidecar::decode_snapshot(input);
    let _ = codec::decode_acknowledgement_slot(input);
    let _ = codec::decode_acknowledgement_file(input);
    let _ = codec::decode_commit_seal(input);
}

fn fuzz_decode_log(input: &[u8], mut expected_sequence: u64, expected_generation: u64) {
    let mut remaining = input;
    loop {
        let Ok(codec::DecodeOutcome::Frame(frame)) =
            codec::decode_next_frame(remaining, expected_sequence, expected_generation)
        else {
            return;
        };
        let _ = frame.decode_record();
        let encoded_len = frame.encoded_len();
        let Ok(next_sequence) = frame.next_sequence() else {
            return;
        };
        let Some(next) = remaining.get(encoded_len..) else {
            return;
        };
        if next.len() == remaining.len() {
            return;
        }
        remaining = next;
        expected_sequence = next_sequence;
    }
}

fn frame_identity(input: &[u8]) -> Option<(u64, u64)> {
    Some((read_u64(input, 20)?, read_u64(input, 28)?))
}

fn read_u64(input: &[u8], offset: usize) -> Option<u64> {
    let bytes: [u8; 8] = input.get(offset..offset.checked_add(8)?)?.try_into().ok()?;
    Some(u64::from_le_bytes(bytes))
}

#[cfg(test)]
mod fixture_corpus;

#[cfg(test)]
mod fuzz_tests {
    use super::fuzz_decode_lifecycle;

    const COMPLETED_FRAME: &[u8] = include_bytes!("../../tests/fixtures/mapped_file_lifecycle/completed.frame.bin");

    #[test]
    fn lifecycle_fuzz_entrypoint_is_total_for_valid_truncated_and_prefixed_frames() {
        fuzz_decode_lifecycle(COMPLETED_FRAME);
        for end in 0..COMPLETED_FRAME.len() {
            fuzz_decode_lifecycle(&COMPLETED_FRAME[..end]);
        }

        let mut prefixed = Vec::with_capacity(16 + COMPLETED_FRAME.len());
        prefixed.extend_from_slice(&100_u64.to_le_bytes());
        prefixed.extend_from_slice(&2_u64.to_le_bytes());
        prefixed.extend_from_slice(COMPLETED_FRAME);
        fuzz_decode_lifecycle(&prefixed);
    }
}
