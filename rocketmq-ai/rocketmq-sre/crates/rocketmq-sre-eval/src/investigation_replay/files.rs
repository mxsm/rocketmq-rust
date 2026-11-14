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

use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::Path,
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use super::*;

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum InvestigationReplayFileRejection {
    InputUnavailable,
    InputTooLarge,
    InvalidJson,
    Replay(InvestigationReplayRejection),
    OutputUnavailable,
}

impl std::fmt::Display for InvestigationReplayFileRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Investigation replay file operation was rejected")
    }
}
impl std::fmt::Debug for InvestigationReplayFileRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::error::Error for InvestigationReplayFileRejection {}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileReport {
    replay: InvestigationReplayPair,
    adjudication: AdjudicatedReplayPair,
}

/// Runs bounded, local JSON inputs and saves a report without Evidence content.
/// `annotations` is separate caller-supplied input, never an answer generator.
/// This has no network, SQL, provider or execution capability.
///
/// # Errors
/// Rejects unreadable/oversized JSON, invalid replay/annotations and output I/O.
/// Existing output paths are rejected, including input aliases. Source paths,
/// payloads and reviewer notes are absent from error formatting.
pub fn replay_files(
    case_path: &Path,
    annotations_path: Option<&Path>,
    output_path: &Path,
) -> Result<(), InvestigationReplayFileRejection> {
    use InvestigationReplayFileRejection as Error;
    let case: InvestigationReplayCase = read_json(case_path, 16 * 1024 * 1024)?;
    let annotations: Vec<SuppliedReplayAdjudication> = annotations_path
        .map(|path| read_json(path, 32 * 1024))
        .transpose()?
        .unwrap_or_default();
    let replay = replay_pair(&case).map_err(Error::Replay)?;
    let adjudication = adjudicate_pair(&replay, &annotations).map_err(Error::Replay)?;
    let report = FileReport { replay, adjudication };
    validation::json_len(&report, 32 * 1024 * 1024).map_err(Error::Replay)?;
    let encoded = serde_json::to_vec_pretty(&report).map_err(|_| Error::OutputUnavailable)?;
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output_path)
        .map_err(|_| Error::OutputUnavailable)?;
    output.write_all(&encoded).map_err(|_| Error::OutputUnavailable)
}

fn read_json<T: DeserializeOwned>(path: &Path, maximum: u64) -> Result<T, InvestigationReplayFileRejection> {
    use InvestigationReplayFileRejection as Error;
    let mut bytes = vec![];
    File::open(path)
        .map_err(|_| Error::InputUnavailable)?
        .take(maximum + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| Error::InputUnavailable)?;
    if bytes.len() as u64 > maximum {
        return Err(Error::InputTooLarge);
    }
    serde_json::from_slice(&bytes).map_err(|_| Error::InvalidJson)
}
