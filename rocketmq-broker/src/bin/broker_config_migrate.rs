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

use std::fs;
use std::path::Path;
use std::path::PathBuf;

use anyhow::bail;
use anyhow::Context;
use anyhow::Result;
use clap::Parser;
use rocketmq_broker::config::raw::RawBrokerConfig;
use rocketmq_broker::config::validated::ValidatedBrokerConfig;

#[derive(Debug, Parser)]
#[command(
    name = "broker_config_migrate",
    about = "Checks broker files against the canonical sectioned configuration schema"
)]
struct Arguments {
    /// Validate without modifying any configuration file.
    #[arg(long)]
    check: bool,

    /// Configuration files or directories. Repository broker examples are used when omitted.
    #[arg(value_name = "PATH")]
    paths: Vec<PathBuf>,
}

fn main() -> Result<()> {
    let arguments = Arguments::parse();
    if !arguments.check {
        bail!("only the non-mutating --check operation is supported");
    }

    let paths = if arguments.paths.is_empty() {
        repository_config_paths()?
    } else {
        collect_config_paths(&arguments.paths)?
    };
    if paths.is_empty() {
        bail!("no broker TOML configuration files were found");
    }

    for path in &paths {
        let raw = RawBrokerConfig::load(path)
            .with_context(|| format!("{} does not match the canonical raw schema", path.display()))?;
        ValidatedBrokerConfig::try_from(raw)
            .with_context(|| format!("{} does not produce a valid runtime configuration", path.display()))?;
        println!("valid {}", path.display());
    }
    println!("checked {} broker configuration file(s)", paths.len());
    Ok(())
}

fn repository_config_paths() -> Result<Vec<PathBuf>> {
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .context("rocketmq-broker must be inside the workspace")?
        .to_path_buf();
    collect_config_paths(&[
        workspace.join("distribution").join("config").join("broker"),
        workspace.join("docker").join("smoke-config").join("broker.toml"),
    ])
}

fn collect_config_paths(inputs: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let mut configs = Vec::new();
    for input in inputs {
        collect_path(input, &mut configs)?;
    }
    configs.sort();
    configs.dedup();
    Ok(configs)
}

fn collect_path(path: &Path, configs: &mut Vec<PathBuf>) -> Result<()> {
    if path.is_file() {
        if path.extension().is_some_and(|extension| extension == "toml") {
            configs.push(path.to_path_buf());
        }
        return Ok(());
    }
    if !path.is_dir() {
        bail!("configuration path does not exist: {}", path.display());
    }

    for entry in fs::read_dir(path).with_context(|| format!("failed to list {}", path.display()))? {
        let entry = entry.with_context(|| format!("failed to read an entry under {}", path.display()))?;
        collect_path(&entry.path(), configs)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::collect_config_paths;

    #[test]
    fn collection_is_recursive_sorted_and_toml_only() {
        let directory = tempdir().expect("temporary directory should be created");
        let nested = directory.path().join("nested");
        let root_config = directory.path().join("b.toml");
        let nested_config = nested.join("a.toml");
        fs::create_dir(&nested).expect("nested directory should be created");
        fs::write(&root_config, "").expect("fixture should be written");
        fs::write(directory.path().join("ignored.conf"), "").expect("fixture should be written");
        fs::write(&nested_config, "").expect("fixture should be written");

        let configs = collect_config_paths(&[directory.path().to_path_buf(), root_config.clone()])
            .expect("configuration paths should collect");

        assert_eq!(configs, [root_config, nested_config]);
    }
}
