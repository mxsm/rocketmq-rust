// Copyright 2023 The RocketMQ Rust Authors
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

use std::env;
use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;

use rocketmq_sre_eval::phase1_shadow::ProviderMode;
use rocketmq_sre_eval::phase1_shadow::ShadowEvalError;
use rocketmq_sre_eval::phase1_shadow::ShadowHarness;

struct Arguments {
    manifest: PathBuf,
    fixtures_root: PathBuf,
    provider_mode: ProviderMode,
    compact: bool,
    help: bool,
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{}: {error}", error.code());
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), ShadowEvalError> {
    let arguments = parse_arguments()?;
    if arguments.help {
        println!(
            "phase01-shadow-eval [--manifest PATH] [--fixtures-root PATH] [--provider mock|rules-only|outage] \
             [--compact]"
        );
        return Ok(());
    }
    let harness = ShadowHarness::load(&arguments.manifest, &arguments.fixtures_root)?;
    let cluster_id = harness.manifest().cluster_id;
    let summary = harness.run(arguments.provider_mode, cluster_id)?;
    if !summary.passed {
        return Err(ShadowEvalError::InvalidSynthesis(
            "shadow suite completed without satisfying the mutation-zero invariant".to_owned(),
        ));
    }
    let output = if arguments.compact {
        serde_json::to_string(&summary)
    } else {
        serde_json::to_string_pretty(&summary)
    }
    .map_err(|error| ShadowEvalError::InvalidSynthesis(error.to_string()))?;
    println!("{output}");
    Ok(())
}

fn parse_arguments() -> Result<Arguments, ShadowEvalError> {
    let mut manifest = PathBuf::from("tests/fixtures/e2e/wave-a-manifest.v1.yaml");
    let mut fixtures_root = PathBuf::from("tests/fixtures");
    let mut provider_mode = env::var("ROCKETMQ_SRE_SHADOW_PROVIDER_MODE")
        .ok()
        .map_or(Ok(ProviderMode::Mock), |value| ProviderMode::from_str(&value))?;
    let mut compact = false;
    let mut help = false;
    let mut arguments = env::args().skip(1);
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--manifest" => {
                manifest = PathBuf::from(required_value(&mut arguments, "--manifest")?);
            }
            "--fixtures-root" => {
                fixtures_root = PathBuf::from(required_value(&mut arguments, "--fixtures-root")?);
            }
            "--provider" => {
                provider_mode = ProviderMode::from_str(&required_value(&mut arguments, "--provider")?)?;
            }
            "--compact" => compact = true,
            "--help" | "-h" => help = true,
            other => {
                return Err(ShadowEvalError::InvalidManifest(format!("unknown argument `{other}`")));
            }
        }
    }
    Ok(Arguments {
        manifest,
        fixtures_root,
        provider_mode,
        compact,
        help,
    })
}

fn required_value(arguments: &mut impl Iterator<Item = String>, option: &str) -> Result<String, ShadowEvalError> {
    arguments
        .next()
        .ok_or_else(|| ShadowEvalError::InvalidManifest(format!("option `{option}` requires a value")))
}
