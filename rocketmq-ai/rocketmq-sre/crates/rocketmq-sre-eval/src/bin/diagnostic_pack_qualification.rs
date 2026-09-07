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
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::process::ExitCode;

use rocketmq_sre_eval::EvalError;
use rocketmq_sre_eval::EvalOutcome;
use rocketmq_sre_eval::EvalRejection;
use rocketmq_sre_eval::diagnostic_qualification::LiveQualificationConfig;
use rocketmq_sre_eval::diagnostic_qualification::run_live_qualification;
use rocketmq_sre_eval::diagnostic_qualification::write_generated_manifest;

enum QualificationCommandError {
    Eval(EvalError),
    Rejected(EvalRejection),
    InvalidArguments,
    Io(std::io::Error),
    Json(serde_json::Error),
}

impl QualificationCommandError {
    const fn code(&self) -> &'static str {
        match self {
            Self::Eval(source) => source.code(),
            Self::Rejected(rejection) => rejection.code(),
            Self::InvalidArguments => "invalid_qualification_manifest",
            Self::Io(source) => {
                let _ = source;
                "source_unavailable"
            }
            Self::Json(source) => {
                let _ = source;
                "invalid_qualification_manifest"
            }
        }
    }
}

impl From<EvalError> for QualificationCommandError {
    fn from(source: EvalError) -> Self {
        Self::Eval(source)
    }
}

impl From<std::io::Error> for QualificationCommandError {
    fn from(source: std::io::Error) -> Self {
        Self::Io(source)
    }
}

impl From<serde_json::Error> for QualificationCommandError {
    fn from(source: serde_json::Error) -> Self {
        Self::Json(source)
    }
}

const DEFAULT_TENANT: &str = "00000000-0000-4000-9000-000000008929";

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("diagnostic_qualification_failed: {}", error.code());
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), QualificationCommandError> {
    let mut arguments = env::args().skip(1);
    match arguments.next().as_deref() {
        Some("export-manifest") => {
            let path = arguments.next().ok_or(QualificationCommandError::InvalidArguments)?;
            if arguments.next().is_some() {
                return Err(QualificationCommandError::InvalidArguments);
            }
            match write_generated_manifest(Path::new(&path))? {
                EvalOutcome::Completed(()) => {}
                EvalOutcome::Rejected(rejection) => {
                    return Err(QualificationCommandError::Rejected(rejection));
                }
            }
            println!("DIAGNOSTIC_QUALIFICATION_MANIFEST_WRITTEN path={path}");
            Ok(())
        }
        Some("run") => {
            let output = required_output_path(arguments.next())?;
            if arguments.next().is_some() {
                return Err(QualificationCommandError::InvalidArguments);
            }
            let config = config_from_env()?;
            let report = match run_live_qualification(&config).await? {
                EvalOutcome::Completed(report) => report,
                EvalOutcome::Rejected(rejection) => {
                    return Err(QualificationCommandError::Rejected(rejection));
                }
            };
            write_report(&output, &report)?;
            println!(
                "DIAGNOSTIC_PACK_QUALIFICATION_OK packs={} scenarios={} pack_scenarios={} \
                 model_network_calls={} target_mutation_calls={} execution_records={} report={}",
                report.pack_count,
                report.scenario_count,
                report.pack_scenario_count,
                report.model_provider_network_calls,
                report.target_mutation_calls,
                report.execution_records,
                output.display()
            );
            Ok(())
        }
        Some("--help" | "-h") | None => {
            println!(
                "diagnostic-pack-qualification export-manifest <PATH>\n\
                 diagnostic-pack-qualification run <MACHINE_LOCAL_REPORT_PATH>"
            );
            Ok(())
        }
        Some(command) => {
            let _ = command;
            Err(QualificationCommandError::InvalidArguments)
        }
    }
}

fn config_from_env() -> Result<LiveQualificationConfig, QualificationCommandError> {
    Ok(LiveQualificationConfig {
        public_url: optional_env("ROCKETMQ_SRE_QUALIFICATION_PUBLIC_URL", "http://127.0.0.1:8090"),
        connector_url: optional_env("ROCKETMQ_SRE_QUALIFICATION_CONNECTOR_URL", "http://127.0.0.1:8093"),
        database_url: required_env("DATABASE_URL")?,
        token: required_env("ROCKETMQ_SRE_QUALIFICATION_TOKEN")?,
        tenant_id: parse_id("ROCKETMQ_SRE_QUALIFICATION_TENANT_ID", DEFAULT_TENANT)?,
        revision: required_env("ROCKETMQ_SRE_QUALIFICATION_REVISION")?,
        environment: optional_env("ROCKETMQ_SRE_QUALIFICATION_ENVIRONMENT", "docker-postgresql-local"),
    })
}

fn required_env(name: &'static str) -> Result<String, QualificationCommandError> {
    env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or(QualificationCommandError::InvalidArguments)
}

fn optional_env(name: &'static str, fallback: &'static str) -> String {
    env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| fallback.to_owned())
}

fn parse_id<T>(name: &'static str, fallback: &'static str) -> Result<T, QualificationCommandError>
where
    T: std::str::FromStr,
{
    optional_env(name, fallback)
        .parse()
        .map_err(|_| QualificationCommandError::InvalidArguments)
}

fn required_output_path(value: Option<String>) -> Result<PathBuf, QualificationCommandError> {
    let path = PathBuf::from(value.ok_or(QualificationCommandError::InvalidArguments)?);
    if !path.is_absolute() {
        return Err(QualificationCommandError::InvalidArguments);
    }
    Ok(path)
}

fn write_report(
    path: &Path,
    report: &rocketmq_sre_eval::diagnostic_qualification::DiagnosticQualificationReport,
) -> Result<(), QualificationCommandError> {
    let parent = path.parent().ok_or(QualificationCommandError::InvalidArguments)?;
    fs::create_dir_all(parent)?;
    let mut encoded = serde_json::to_vec_pretty(report)?;
    encoded.push(b'\n');
    fs::write(path, encoded)?;
    Ok(())
}
