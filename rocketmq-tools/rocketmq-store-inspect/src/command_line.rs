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

use std::path::PathBuf;

use clap::Parser;
use clap::Subcommand;

#[derive(Parser, Debug)]
#[command(author = "mxsm", version = "0.2.0", about = "RocketMQ CLI(Rust)")]
pub struct RootCli {
    #[arg(long, global = true, help = "Include controlled diagnostic fields in error output")]
    pub verbose: bool,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    #[command(
        arg_required_else_help = true,
        author = "mxsm",
        version = "0.2.0",
        about = "read message log file"
    )]
    ReadMessageLog {
        #[arg(
            short,
            long,
            value_name = "FILE",
            default_missing_value = "None",
            help = "message log file path"
        )]
        config: Option<PathBuf>,

        #[arg(
            short = 'f',
            long,
            value_name = "FROM",
            default_missing_value = "None",
            help = "The number of data started to be read, default to read from the beginning. start from 0"
        )]
        from: Option<u32>,

        #[arg(
            short = 't',
            long,
            value_name = "TO",
            default_missing_value = "None",
            help = "The position of the data for ending the reading, defaults to reading until the end of the file."
        )]
        to: Option<u32>,
    },
    #[command(
        name = "consolidate-multipath",
        arg_required_else_help = true,
        about = "offline consolidation of CommitLog segments into a new single root"
    )]
    ConsolidateMultipath {
        #[arg(long = "source-root", required = true)]
        source_roots: Vec<PathBuf>,
        #[arg(long)]
        target: PathBuf,
        #[arg(long)]
        mapped_file_size: u64,
        #[arg(long)]
        store_root: PathBuf,
    },
    #[command(
        name = "downgrade-preflight",
        arg_required_else_help = true,
        about = "fail-closed offline compatibility check before starting an older Broker"
    )]
    DowngradePreflight {
        #[arg(long)]
        target_version: String,
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        output: Option<PathBuf>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_offline_upgrade_commands() {
        let cli = RootCli::try_parse_from([
            "rocketmq-cli-rust",
            "downgrade-preflight",
            "--target-version",
            "0.9.0",
            "--config",
            "broker.toml",
        ])
        .expect("parse preflight");
        assert!(matches!(cli.command, Commands::DowngradePreflight { .. }));

        let cli = RootCli::try_parse_from([
            "rocketmq-cli-rust",
            "consolidate-multipath",
            "--source-root",
            "a",
            "--target",
            "target",
            "--mapped-file-size",
            "1024",
            "--store-root",
            "store",
        ])
        .expect("parse consolidation");
        assert!(matches!(cli.command, Commands::ConsolidateMultipath { .. }));
    }

    #[test]
    fn parses_verbose_in_any_argument_position() {
        let before = RootCli::try_parse_from([
            "rocketmq-cli-rust",
            "--verbose",
            "downgrade-preflight",
            "--target-version",
            "0.9.0",
            "--config",
            "broker.toml",
        ])
        .expect("verbose before subcommand");
        let after = RootCli::try_parse_from([
            "rocketmq-cli-rust",
            "downgrade-preflight",
            "--target-version",
            "0.9.0",
            "--config",
            "broker.toml",
            "--verbose",
        ])
        .expect("verbose after subcommand");

        assert!(before.verbose);
        assert!(after.verbose);
    }
}
