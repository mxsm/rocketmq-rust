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

mod build;
mod tests;

#[derive(Debug, Clone, Copy)]
enum FixtureValidation {
    StoreMeta,
    MarkerSlot {
        physical_slot: u8,
    },
    MarkerFile,
    AcknowledgementSlot,
    AcknowledgementFile,
    AcknowledgementFileWithoutAuthoritative,
    CommitSeal,
    Snapshot,
    TailEvidence {
        length: usize,
        crc32: u32,
    },
    LedgerFrame {
        sequence: u64,
        generation: u64,
    },
    InvalidMarker,
    InvalidAcknowledgement,
    InvalidAcknowledgementFile,
    InvalidCommitSeal,
    InvalidSnapshot,
    InvalidLedgerFrame {
        sequence: u64,
        generation: u64,
    },
    InvalidTypedLedgerFrame {
        sequence: u64,
        generation: u64,
    },
    LedgerFrameStream {
        first_sequence: u64,
        generation: u64,
    },
    InvalidLedgerFrameStream {
        first_sequence: u64,
        generation: u64,
    },
    SequenceOverflowStream {
        generation: u64,
    },
    LedgerFrameThenPartialSeal {
        sequence: u64,
        generation: u64,
    },
    AcknowledgementReconstructionBundle {
        sequence: u64,
        generation: u64,
    },
    InvalidAcknowledgedUnitBinding {
        sequence: u64,
        generation: u64,
    },
    AcknowledgedLogBundle {
        first_sequence: u64,
        final_sequence: u64,
        generation: u64,
    },
    AcknowledgedSuffixLossBundle {
        first_sequence: u64,
        generation: u64,
    },
    SealedUnitsWithUnsealedFinal {
        first_sequence: u64,
        generation: u64,
        sealed_units: usize,
    },
    TruncatedLedgerFrames {
        sequence: u64,
        generation: u64,
    },
    TruncatedSealedUnit {
        sequence: u64,
        generation: u64,
        frame_length: usize,
    },
}

#[derive(Debug)]
struct Fixture {
    name: String,
    bytes: Vec<u8>,
    validation: FixtureValidation,
}

impl Fixture {
    fn new(name: impl Into<String>, bytes: impl Into<Vec<u8>>, validation: FixtureValidation) -> Self {
        Self {
            name: name.into(),
            bytes: bytes.into(),
            validation,
        }
    }
}
