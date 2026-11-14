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

use rocketmq_sre_eval::investigation_replay::replay_files;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args_os().skip(1);
    let mut case = None;
    let mut annotations = None;
    let mut output = None;
    while let Some(argument) = arguments.next() {
        let target = match argument.to_str() {
            Some("--case") => &mut case,
            Some("--annotations") => &mut annotations,
            Some("--output") => &mut output,
            _ => return Err(
                "Usage: investigation-replay --case CASE.json [--annotations ANNOTATIONS.json] --output REPORT.json"
                    .into(),
            ),
        };
        if target.is_some() {
            return Err("Duplicate replay argument".into());
        }
        *target = Some(PathBuf::from(arguments.next().ok_or("Missing replay argument")?));
    }
    let case = case.ok_or("Missing --case")?;
    let output = output.ok_or("Missing --output")?;
    replay_files(&case, annotations.as_deref(), &output)?;
    Ok(())
}
