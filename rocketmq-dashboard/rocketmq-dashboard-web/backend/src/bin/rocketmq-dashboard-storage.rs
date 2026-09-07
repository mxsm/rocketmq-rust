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

use rocketmq_dashboard_web_backend::config::StorageConfig;
use rocketmq_dashboard_web_backend::persistence::DashboardPersistence;
use rocketmq_dashboard_web_backend::persistence::storage_operations;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use std::env;
use std::error::Error as StdError;
use std::path::PathBuf;
use std::process::ExitCode;

type CliSource = Box<dyn StdError + Send + Sync + 'static>;

#[derive(Debug, thiserror::Error)]
enum StorageCliError {
    #[error("{message}")]
    Usage { code: &'static str, message: &'static str },
    #[error("{message}")]
    Source {
        code: &'static str,
        message: &'static str,
        #[source]
        source: CliSource,
    },
    #[error("{message}")]
    Failure { code: &'static str, message: &'static str },
}

impl StorageCliError {
    fn source<E>(code: &'static str, message: &'static str, source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::Source {
            code,
            message,
            source: Box::new(source),
        }
    }

    const fn code(&self) -> &'static str {
        match self {
            Self::Usage { code, .. } | Self::Source { code, .. } | Self::Failure { code, .. } => code,
        }
    }

    const fn message(&self) -> &'static str {
        match self {
            Self::Usage { message, .. } | Self::Source { message, .. } | Self::Failure { message, .. } => message,
        }
    }
}

fn main() -> ExitCode {
    match run_process() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{}: {}", error.code(), error.message());
            ExitCode::FAILURE
        }
    }
}

fn run_process() -> Result<(), StorageCliError> {
    let command = Command::parse(env::args().skip(1))?;
    let owner = RuntimeOwner::plan(RuntimeConfig::server_default("rocketmq-dashboard-storage"))
        .map_err(|source| {
            StorageCliError::source(
                "RUNTIME_PLAN_FAILED",
                "Storage runtime configuration is invalid",
                source,
            )
        })?
        .build()
        .map_err(|source| {
            StorageCliError::source("RUNTIME_START_FAILED", "Storage runtime could not be started", source)
        })?;
    let result = owner.block_on(run(command, owner.root_context().component("storage-operations")));
    let shutdown = owner
        .shutdown_runtime_blocking()
        .map_err(|source| StorageCliError::source("RUNTIME_SHUTDOWN_FAILED", "Storage runtime cleanup failed", source))
        .and_then(require_healthy_shutdown);
    result?;
    shutdown?;
    Ok(())
}

fn require_healthy_shutdown(report: rocketmq_runtime::ShutdownReport) -> Result<(), StorageCliError> {
    if report.is_healthy() {
        Ok(())
    } else {
        report.log_if_unhealthy();
        Err(StorageCliError::Failure {
            code: "RUNTIME_SHUTDOWN_INCOMPLETE",
            message: "Storage runtime cleanup was incomplete",
        })
    }
}

async fn run(command: Command, context: rocketmq_runtime::ChildServiceContext) -> Result<(), StorageCliError> {
    match command {
        Command::Verify { input } => {
            let data = storage_operations::read_verified_backup(&input, None).map_err(storage_error)?;
            println!(
                "backup verified: backend={} formatVersion={}",
                data.manifest.backend.as_str(),
                data.manifest.format_version
            );
            Ok(())
        }
        Command::Status { json } => {
            let config = StorageConfig::from_env().map_err(|source| {
                StorageCliError::source("STORAGE_CONFIG_INVALID", "Storage configuration is invalid", source)
            })?;
            let persistence = DashboardPersistence::initialize(&config, context)
                .await
                .map_err(storage_error)?;
            let health = persistence.storage_health().await;
            if json {
                println!(
                    "{}",
                    serde_json::to_string(&health).map_err(|source| {
                        StorageCliError::source("STATUS_ENCODE_FAILED", "Storage status could not be encoded", source)
                    })?
                );
            } else {
                println!(
                    "backend={} mode={:?} status={:?} schemaVersion={:?}",
                    health.backend.as_str(),
                    health.mode,
                    health.status,
                    health.schema_version
                );
            }
            Ok(())
        }
        Command::Backup { output } => {
            let config = StorageConfig::from_env().map_err(|source| {
                StorageCliError::source("STORAGE_CONFIG_INVALID", "Storage configuration is invalid", source)
            })?;
            let persistence = DashboardPersistence::initialize(&config, context)
                .await
                .map_err(storage_error)?;
            let data = storage_operations::snapshot(&persistence)
                .await
                .map_err(storage_error)?;
            storage_operations::write_backup(&output, &data).map_err(storage_error)?;
            println!(
                "backup created: backend={} records={}",
                data.manifest.backend.as_str(),
                total_records(data.manifest.counts)
            );
            Ok(())
        }
        Command::Restore {
            input,
            confirm_empty_target,
        } => {
            if !confirm_empty_target {
                return Err(StorageCliError::Usage {
                    code: "RESTORE_CONFIRMATION_REQUIRED",
                    message: "Restore requires --confirm-empty-target",
                });
            }
            let config = StorageConfig::from_env().map_err(|source| {
                StorageCliError::source("STORAGE_CONFIG_INVALID", "Storage configuration is invalid", source)
            })?;
            let data = storage_operations::read_verified_backup(&input, Some(config.backend)).map_err(storage_error)?;
            if config.backend == rocketmq_dashboard_web_backend::model::StorageBackend::File {
                storage_operations::restore_file_target(&data, &config, context)
                    .await
                    .map_err(storage_error)?;
            } else {
                let persistence = DashboardPersistence::initialize(&config, context)
                    .await
                    .map_err(storage_error)?;
                storage_operations::restore(&persistence, &data)
                    .await
                    .map_err(storage_error)?;
            }
            println!(
                "backup restored: backend={} records={}",
                data.manifest.backend.as_str(),
                total_records(data.manifest.counts)
            );
            Ok(())
        }
    }
}

fn total_records(counts: storage_operations::BackupCounts) -> u64 {
    counts.environments + counts.endpoints + counts.monitors + counts.history + counts.sessions + counts.audit
}

fn storage_error(source: rocketmq_dashboard_web_backend::persistence::error::PersistenceError) -> StorageCliError {
    StorageCliError::source("STORAGE_OPERATION_FAILED", "Storage operation failed", source)
}

#[derive(Debug)]
enum Command {
    Status { json: bool },
    Backup { output: PathBuf },
    Verify { input: PathBuf },
    Restore { input: PathBuf, confirm_empty_target: bool },
}

impl Command {
    fn parse(arguments: impl Iterator<Item = String>) -> Result<Self, StorageCliError> {
        let values = arguments.collect::<Vec<_>>();
        let Some(command) = values.first().map(String::as_str) else {
            return Err(usage_error());
        };
        match command {
            "status" if values.len() == 1 => Ok(Self::Status { json: false }),
            "status" if values.len() == 2 && values[1] == "--json" => Ok(Self::Status { json: true }),
            "backup" if values.len() == 3 => Ok(Self::Backup {
                output: required_path(&values, "--output")?,
            }),
            "verify" if values.len() == 3 => Ok(Self::Verify {
                input: required_path(&values, "--input")?,
            }),
            "restore" if values.len() == 4 && values.iter().any(|value| value == "--confirm-empty-target") => {
                Ok(Self::Restore {
                    input: required_path(&values, "--input")?,
                    confirm_empty_target: true,
                })
            }
            _ => Err(usage_error()),
        }
    }
}

fn required_path(values: &[String], option: &str) -> Result<PathBuf, StorageCliError> {
    let Some(index) = values.iter().position(|value| value == option) else {
        return Err(usage_error());
    };
    let value = values
        .get(index + 1)
        .filter(|value| !value.starts_with('-'))
        .ok_or_else(usage_error)?;
    if values.iter().filter(|item| item.as_str() == option).count() != 1 {
        return Err(usage_error());
    }
    Ok(PathBuf::from(value))
}

const fn usage_error() -> StorageCliError {
    StorageCliError::Usage {
        code: "INVALID_ARGUMENTS",
        message: usage(),
    }
}

const fn usage() -> &'static str {
    "usage: rocketmq-dashboard-storage status [--json] | backup --output <new-dir> | verify --input <dir> | restore --input <dir> --confirm-empty-target"
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn parser_rejects_database_url_arguments_without_echoing_values() {
        let result = Command::parse(
            ["backup", "--output", "backup", "--database-url", "sqlite://secret"]
                .into_iter()
                .map(str::to_owned),
        );
        let error = result.expect_err("unexpected arguments fail");
        assert_eq!(error.code(), "INVALID_ARGUMENTS");
        assert!(!error.to_string().contains("sqlite://secret"));
    }

    #[test]
    fn unhealthy_shutdown_report_becomes_a_fixed_cli_error() {
        let mut report = rocketmq_runtime::ShutdownReport::new("sensitive-component-name", Duration::ZERO);
        report.timed_out = 1;

        let error = require_healthy_shutdown(report).expect_err("an unhealthy report must fail the process");

        assert_eq!(error.code(), "RUNTIME_SHUTDOWN_INCOMPLETE");
        assert_eq!(error.message(), "Storage runtime cleanup was incomplete");
        assert!(!error.to_string().contains("sensitive-component-name"));
    }
}
