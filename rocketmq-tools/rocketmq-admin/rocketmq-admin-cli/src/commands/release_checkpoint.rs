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

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Args;
use clap::Subcommand;
use rocketmq_admin_core::client_adapter::ClientRuntime;
use rocketmq_admin_core::core::release_checkpoint::ReleaseCheckpointSetBuilder;
use rocketmq_admin_core::core::release_checkpoint::ValidatedMaintenanceCapabilities;
use rocketmq_admin_core::core::release_checkpoint::decode_checkpoint_set;
use rocketmq_admin_core::core::release_checkpoint::encode_checkpoint_set;
use rocketmq_admin_core::core::release_checkpoint::verify_checkpoint_set_restore;
use rocketmq_admin_core::core::security::AdminCredentials;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_protocol::protocol::body::release_checkpoint::ControllerReleaseSnapshotManifest;
use rocketmq_protocol::protocol::body::release_checkpoint::MaintenanceCapabilitiesResponse;
use rocketmq_protocol::protocol::body::release_checkpoint::ReleaseCheckpointRestoreVerification;
use rocketmq_protocol::protocol::body::release_checkpoint::StoreReleaseCheckpointManifest;
use tokio::io::AsyncWriteExt;

use crate::commands::CommandExecute;

#[derive(Subcommand)]
pub enum ReleaseCheckpointCommands {
    /// Validate and display an authenticated capabilities response.
    Capabilities(CapabilitiesCommand),
    /// Bind Controller and Store manifests into one fenced checkpoint set.
    CreateSet(CreateSetCommand),
    /// Verify the complete set barrier, members, offsets, and integrity metadata.
    VerifySet(VerifySetCommand),
    /// Verify one complete restore proof for every set member.
    RestoreVerify(RestoreVerifyCommand),
}

impl CommandExecute for ReleaseCheckpointCommands {
    async fn execute(
        &self,
        _credentials: Option<AdminCredentials>,
        _client_runtime: Arc<ClientRuntime>,
    ) -> RocketMQResult<()> {
        match self {
            Self::Capabilities(command) => command.execute().await,
            Self::CreateSet(command) => command.execute().await,
            Self::VerifySet(command) => command.execute().await,
            Self::RestoreVerify(command) => command.execute().await,
        }
    }
}

#[derive(Clone, Debug, Args)]
pub struct CapabilitiesCommand {
    /// JSON response returned by MaintenanceGetCapabilities.
    #[arg(long, value_name = "FILE")]
    input: PathBuf,
}

impl CapabilitiesCommand {
    async fn execute(&self) -> RocketMQResult<()> {
        let response: MaintenanceCapabilitiesResponse = read_json(&self.input, "maintenance capabilities").await?;
        let capabilities = ValidatedMaintenanceCapabilities::try_from_response(response)
            .map_err(|error| RocketMQError::validation_failed("maintenanceCapabilities", error.to_string()))?;
        println!(
            "{}",
            serde_json::to_string_pretty(capabilities.response())
                .map_err(|error| RocketMQError::internal("encode maintenance capabilities", error))?
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Args)]
pub struct CreateSetCommand {
    #[arg(long, value_name = "FILE")]
    controller_manifest: PathBuf,
    #[arg(long, value_name = "FILE", required = true)]
    store_manifest: Vec<PathBuf>,
    #[arg(long)]
    release_id: String,
    #[arg(long)]
    policy_version: u64,
    #[arg(long)]
    fencing_token: u64,
    #[arg(long)]
    max_store_members: u32,
    #[arg(long, value_name = "FILE")]
    output: PathBuf,
}

impl CreateSetCommand {
    async fn execute(&self) -> RocketMQResult<()> {
        let controller: ControllerReleaseSnapshotManifest =
            read_json(&self.controller_manifest, "Controller checkpoint manifest").await?;
        let mut stores = Vec::with_capacity(self.store_manifest.len());
        for path in &self.store_manifest {
            stores.push(read_json::<StoreReleaseCheckpointManifest>(path, "Store checkpoint manifest").await?);
        }
        let manifest = ReleaseCheckpointSetBuilder::new(
            self.release_id.clone(),
            self.policy_version,
            self.fencing_token,
            self.max_store_members,
        )
        .and_then(|builder| {
            builder.build(
                controller,
                stores,
                rocketmq_runtime::common::time_utils::current_millis(),
            )
        })
        .map_err(|error| RocketMQError::validation_failed("checkpointSet", error.to_string()))?;
        let bytes = encode_checkpoint_set(&manifest)
            .map_err(|error| RocketMQError::validation_failed("checkpointSet", error.to_string()))?;
        write_new_file(&self.output, &bytes).await?;
        println!("{}", self.output.display());
        Ok(())
    }
}

#[derive(Clone, Debug, Args)]
pub struct VerifySetCommand {
    #[arg(long, value_name = "FILE")]
    manifest: PathBuf,
}

impl VerifySetCommand {
    async fn execute(&self) -> RocketMQResult<()> {
        let bytes = read_file(&self.manifest, "checkpoint set").await?;
        decode_checkpoint_set(&bytes)
            .map_err(|error| RocketMQError::validation_failed("checkpointSet", error.to_string()))?;
        println!("checkpoint set verified: {}", self.manifest.display());
        Ok(())
    }
}

#[derive(Clone, Debug, Args)]
pub struct RestoreVerifyCommand {
    #[arg(long, value_name = "FILE")]
    manifest: PathBuf,
    #[arg(long, value_name = "FILE", required = true)]
    proof: Vec<PathBuf>,
}

impl RestoreVerifyCommand {
    async fn execute(&self) -> RocketMQResult<()> {
        let manifest_bytes = read_file(&self.manifest, "checkpoint set").await?;
        let manifest = decode_checkpoint_set(&manifest_bytes)
            .map_err(|error| RocketMQError::validation_failed("checkpointSet", error.to_string()))?;
        let mut proofs = Vec::with_capacity(self.proof.len());
        for path in &self.proof {
            proofs.push(read_json::<ReleaseCheckpointRestoreVerification>(path, "restore proof").await?);
        }
        verify_checkpoint_set_restore(&manifest, &proofs)
            .map_err(|error| RocketMQError::validation_failed("restoreProofs", error.to_string()))?;
        println!("checkpoint restore proofs verified: {}", self.manifest.display());
        Ok(())
    }
}

async fn read_json<T>(path: &Path, artifact: &'static str) -> RocketMQResult<T>
where
    T: serde::de::DeserializeOwned,
{
    let bytes = read_file(path, artifact).await?;
    serde_json::from_slice(&bytes)
        .map_err(|error| RocketMQError::request_body_invalid(artifact, format!("{}: {error}", path.display())))
}

async fn read_file(path: &Path, artifact: &'static str) -> RocketMQResult<Vec<u8>> {
    tokio::fs::read(path)
        .await
        .map_err(|error| RocketMQError::storage_read_failed(path.display().to_string(), format!("{artifact}: {error}")))
}

async fn write_new_file(path: &Path, bytes: &[u8]) -> RocketMQResult<()> {
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await
        .map_err(|error| RocketMQError::storage_write_failed(path.display().to_string(), error.to_string()))?;
    file.write_all(bytes)
        .await
        .map_err(|error| RocketMQError::storage_write_failed(path.display().to_string(), error.to_string()))?;
    file.write_all(b"\n")
        .await
        .map_err(|error| RocketMQError::storage_write_failed(path.display().to_string(), error.to_string()))?;
    file.sync_all()
        .await
        .map_err(|error| RocketMQError::storage_write_failed(path.display().to_string(), error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct TestCli {
        #[command(subcommand)]
        command: ReleaseCheckpointCommands,
    }

    #[test]
    fn release_checkpoint_cli_exposes_all_four_production_commands() {
        assert!(TestCli::try_parse_from(["test", "capabilities", "--input", "capabilities.json"]).is_ok());
        assert!(TestCli::try_parse_from(["test", "verify-set", "--manifest", "set.json"]).is_ok());
        assert!(
            TestCli::try_parse_from([
                "test",
                "restore-verify",
                "--manifest",
                "set.json",
                "--proof",
                "controller-proof.json"
            ])
            .is_ok()
        );
    }
}
