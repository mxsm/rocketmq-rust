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

#[path = "../execution_drills.rs"]
mod execution_drills;

use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    match run() {
        Ok(cases) => {
            println!("EXECUTION_DRILL_RECORD_CONSISTENT cases={cases} physical_truth_validated=false authority=false");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("execution_drill_report_failed: {}", error.code());
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<usize, execution_drills::Rejection> {
    let mut inputs = Vec::new();
    let mut output = None;
    let mut arguments = std::env::args_os().skip(1);
    while let Some(argument) = arguments.next() {
        let value = arguments.next().ok_or(execution_drills::Rejection::Arguments)?;
        match argument.to_str() {
            Some("--input") => inputs.push(PathBuf::from(value)),
            Some("--output") if output.is_none() => output = Some(PathBuf::from(value)),
            _ => return Err(execution_drills::Rejection::Arguments),
        }
    }
    execution_drills::combine_files(&inputs, &output.ok_or(execution_drills::Rejection::Arguments)?)
}
