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

use std::fs::File;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use super::model::*;
use super::*;

#[path = "files/unique_json.rs"]
mod unique_json;

pub(super) fn read_fragments(inputs: &[PathBuf]) -> Result<Report, Rejection> {
    if inputs.is_empty() || inputs.len() > MAX_FRAGMENTS {
        return Err(Rejection::Arguments);
    }
    let mut report = Report {
        schema_version: REPORT_SCHEMA,
        run_id: String::new(),
        production_qualified: false,
        runtime_admission_authority: false,
        physical_truth_validated: false,
        coverage: Vec::new(),
        cases: Vec::new(),
    };
    let mut total = 0usize;
    for input in inputs {
        if !input.is_absolute() {
            return Err(Rejection::Arguments);
        }
        let file = File::open(input).map_err(|_| Rejection::InputUnavailable)?;
        if !file.metadata().map_err(|_| Rejection::InputUnavailable)?.is_file() {
            return Err(Rejection::InputUnavailable);
        }
        let mut bytes = Vec::new();
        file.take(MAX_FRAGMENT_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| Rejection::InputUnavailable)?;
        if bytes.len() as u64 > MAX_FRAGMENT_BYTES {
            return Err(Rejection::LimitExceeded);
        }
        total = total.checked_add(bytes.len()).ok_or(Rejection::LimitExceeded)?;
        if total > MAX_TOTAL_BYTES {
            return Err(Rejection::LimitExceeded);
        }
        let value = unique_json::parse(&bytes)?;
        let fragment: Fragment = serde_json::from_value(value).map_err(|_| Rejection::InvalidRecord)?;
        if fragment.schema_version != FRAGMENT_SCHEMA
            || fragment.production_qualified
            || fragment.runtime_admission_authority
            || fragment.cases.is_empty()
            || (!report.run_id.is_empty() && report.run_id != fragment.run_id)
        {
            return Err(Rejection::InvalidRecord);
        }
        if report
            .cases
            .len()
            .checked_add(fragment.cases.len())
            .ok_or(Rejection::LimitExceeded)?
            > MAX_CASES
        {
            return Err(Rejection::LimitExceeded);
        }
        report.run_id = fragment.run_id;
        report.cases.extend(fragment.cases);
    }
    Ok(report)
}

pub(super) fn write_new(output: &Path, report: &Report) -> Result<(), Rejection> {
    if !output.is_absolute() {
        return Err(Rejection::Arguments);
    }
    let mut bytes = serde_json::to_vec_pretty(report).map_err(|_| Rejection::InvalidRecord)?;
    bytes.push(b'\n');
    if bytes.len() as u64 > MAX_FRAGMENT_BYTES {
        return Err(Rejection::LimitExceeded);
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                Rejection::OutputExists
            } else {
                Rejection::OutputUnavailable
            }
        })?;
    // A failed new output is retained for diagnosis; no old input/report is removed.
    file.write_all(&bytes).map_err(|_| Rejection::OutputUnavailable)?;
    file.sync_all().map_err(|_| Rejection::OutputUnavailable)
}
