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
use std::path::PathBuf;

fn main() -> anyhow::Result<()> {
    let command = Command::parse(env::args().skip(1))?;
    let owner = RuntimeOwner::new(RuntimeConfig::server_default("rocketmq-dashboard-storage"))?;
    let result = owner.block_on(run(command, owner.root_context().component("storage-operations")));
    let shutdown = owner.shutdown_runtime_blocking();
    result?;
    shutdown?;
    Ok(())
}

async fn run(command: Command, context: rocketmq_runtime::ChildServiceContext) -> anyhow::Result<()> {
    match command {
        Command::Verify { input } => {
            let data = storage_operations::read_verified_backup(&input, None).map_err(cli_error)?;
            println!(
                "backup verified: backend={} formatVersion={}",
                data.manifest.backend.as_str(),
                data.manifest.format_version
            );
            Ok(())
        }
        Command::Status { json } => {
            let config = StorageConfig::from_env().map_err(|_| anyhow::anyhow!("storage configuration is invalid"))?;
            let persistence = DashboardPersistence::initialize(&config, context)
                .await
                .map_err(cli_error)?;
            let health = persistence.storage_health().await;
            if json {
                println!(
                    "{}",
                    serde_json::to_string(&health).map_err(|_| anyhow::anyhow!("cannot encode status"))?
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
            let config = StorageConfig::from_env().map_err(|_| anyhow::anyhow!("storage configuration is invalid"))?;
            let persistence = DashboardPersistence::initialize(&config, context)
                .await
                .map_err(cli_error)?;
            let data = storage_operations::snapshot(&persistence).await.map_err(cli_error)?;
            storage_operations::write_backup(&output, &data).map_err(cli_error)?;
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
                return Err(anyhow::anyhow!("restore requires --confirm-empty-target"));
            }
            let config = StorageConfig::from_env().map_err(|_| anyhow::anyhow!("storage configuration is invalid"))?;
            let data = storage_operations::read_verified_backup(&input, Some(config.backend)).map_err(cli_error)?;
            if config.backend == rocketmq_dashboard_web_backend::model::StorageBackend::File {
                storage_operations::restore_file_target(&data, &config, context)
                    .await
                    .map_err(cli_error)?;
            } else {
                let persistence = DashboardPersistence::initialize(&config, context)
                    .await
                    .map_err(cli_error)?;
                storage_operations::restore(&persistence, &data)
                    .await
                    .map_err(cli_error)?;
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

fn cli_error(error: rocketmq_dashboard_web_backend::persistence::error::PersistenceError) -> anyhow::Error {
    anyhow::anyhow!("storage operation failed: {}", error.stable_code())
}

enum Command {
    Status { json: bool },
    Backup { output: PathBuf },
    Verify { input: PathBuf },
    Restore { input: PathBuf, confirm_empty_target: bool },
}

impl Command {
    fn parse(arguments: impl Iterator<Item = String>) -> anyhow::Result<Self> {
        let values = arguments.collect::<Vec<_>>();
        let Some(command) = values.first().map(String::as_str) else {
            return Err(anyhow::anyhow!(usage()));
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
            _ => Err(anyhow::anyhow!(usage())),
        }
    }
}

fn required_path(values: &[String], option: &str) -> anyhow::Result<PathBuf> {
    let Some(index) = values.iter().position(|value| value == option) else {
        return Err(anyhow::anyhow!(usage()));
    };
    let value = values
        .get(index + 1)
        .filter(|value| !value.starts_with('-'))
        .ok_or_else(|| anyhow::anyhow!(usage()))?;
    if values.iter().filter(|item| item.as_str() == option).count() != 1 {
        return Err(anyhow::anyhow!(usage()));
    }
    Ok(PathBuf::from(value))
}

const fn usage() -> &'static str {
    "usage: rocketmq-dashboard-storage status [--json] | backup --output <new-dir> | verify --input <dir> | restore --input <dir> --confirm-empty-target"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_rejects_database_url_arguments() {
        let result = Command::parse(
            ["backup", "--output", "backup", "--database-url", "sqlite://secret"]
                .into_iter()
                .map(str::to_owned),
        );
        assert!(result.is_err());
    }
}
