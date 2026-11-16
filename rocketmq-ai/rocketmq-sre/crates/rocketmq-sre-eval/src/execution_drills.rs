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

//! Offline consistency checking of maintained engineering drill fragments.
//!
//! This module is private to the report binary. It has no target transport,
//! signer, admission or model interface. Successful parsing cannot establish
//! that an asserted physical observation actually occurred.

#[path = "execution_drills/files.rs"]
mod files;
#[path = "execution_drills/model.rs"]
mod model;
#[path = "execution_drills/validation.rs"]
mod validation;

#[cfg(test)]
#[path = "execution_drills/tests.rs"]
mod tests;

use std::path::Path;
use std::path::PathBuf;

pub(super) use model::Rejection;

const MAX_FRAGMENT_BYTES: u64 = 1024 * 1024;
const MAX_TOTAL_BYTES: usize = 8 * 1024 * 1024;
const MAX_FRAGMENTS: usize = 32;
const MAX_CASES: usize = 128;

/// Validates immutable fragments and exclusively creates a bounded report.
/// Neither the inputs nor an existing output are overwritten.
pub(super) fn combine_files(inputs: &[PathBuf], output: &Path) -> Result<usize, Rejection> {
    let mut report = files::read_fragments(inputs)?;
    validation::validate(&report)?;
    report.coverage = validation::coverage(&report.cases);
    let count = report.cases.len();
    files::write_new(output, &report)?;
    Ok(count)
}
