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

use rocketmq_sre_eval::diagnostic_qualification::DiagnosticQualificationError;
use rocketmq_sre_eval::diagnostic_qualification::LiveQualificationConfig;
use rocketmq_sre_eval::diagnostic_qualification::run_live_qualification;
use rocketmq_sre_eval::diagnostic_qualification::write_generated_manifest;

const DEFAULT_TENANT: &str = "00000000-0000-4000-9000-000000008929";

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{}: {error}", error.code());
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), DiagnosticQualificationError> {
    let mut arguments = env::args().skip(1);
    match arguments.next().as_deref() {
        Some("export-manifest") => {
            let path = arguments.next().ok_or_else(|| {
                DiagnosticQualificationError::InvalidManifest(
                    "export-manifest requires an explicit output path".to_owned(),
                )
            })?;
            if arguments.next().is_some() {
                return Err(DiagnosticQualificationError::InvalidManifest(
                    "export-manifest accepts only one output path".to_owned(),
                ));
            }
            write_generated_manifest(Path::new(&path))?;
            println!("DIAGNOSTIC_QUALIFICATION_MANIFEST_WRITTEN path={path}");
            Ok(())
        }
        Some("run") => {
            let output = required_output_path(arguments.next())?;
            if arguments.next().is_some() {
                return Err(DiagnosticQualificationError::InvalidManifest(
                    "run accepts only one report output path".to_owned(),
                ));
            }
            let config = config_from_env()?;
            let report = run_live_qualification(&config).await?;
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
        Some(command) => Err(DiagnosticQualificationError::InvalidManifest(format!(
            "unknown command `{command}`"
        ))),
    }
}

fn config_from_env() -> Result<LiveQualificationConfig, DiagnosticQualificationError> {
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

fn required_env(name: &'static str) -> Result<String, DiagnosticQualificationError> {
    env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| DiagnosticQualificationError::InvalidManifest(format!("{name} must be configured")))
}

fn optional_env(name: &'static str, fallback: &'static str) -> String {
    env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| fallback.to_owned())
}

fn parse_id<T>(name: &'static str, fallback: &'static str) -> Result<T, DiagnosticQualificationError>
where
    T: std::str::FromStr,
{
    optional_env(name, fallback)
        .parse()
        .map_err(|_| DiagnosticQualificationError::InvalidManifest(format!("{name} must be a UUID")))
}

fn required_output_path(value: Option<String>) -> Result<PathBuf, DiagnosticQualificationError> {
    let path = PathBuf::from(value.ok_or_else(|| {
        DiagnosticQualificationError::InvalidManifest("run requires a machine-local report path".to_owned())
    })?);
    if !path.is_absolute() {
        return Err(DiagnosticQualificationError::InvalidManifest(
            "qualification report path must be absolute and outside the repository".to_owned(),
        ));
    }
    Ok(path)
}

fn write_report(
    path: &Path,
    report: &rocketmq_sre_eval::diagnostic_qualification::DiagnosticQualificationReport,
) -> Result<(), DiagnosticQualificationError> {
    let parent = path.parent().ok_or_else(|| {
        DiagnosticQualificationError::InvalidManifest("qualification report path has no parent".to_owned())
    })?;
    fs::create_dir_all(parent).map_err(|source| DiagnosticQualificationError::Io {
        path: parent.to_path_buf(),
        source,
    })?;
    let mut encoded = serde_json::to_vec_pretty(report)?;
    encoded.push(b'\n');
    fs::write(path, encoded).map_err(|source| DiagnosticQualificationError::Io {
        path: path.to_path_buf(),
        source,
    })
}
