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

use clap::CommandFactory;
use clap::Parser;
use clap_complete::generate;
use clap_complete::shells::Bash;
use clap_complete::shells::Fish;
use clap_complete::shells::Zsh;

use crate::commands::CommandExecute;
use crate::commands::Commands;

#[derive(Parser)]
#[command(name = "rocketmq-admin-cli-rust")]
#[command(about = "Rocketmq Rust admin commands", long_about = None, author="mxsm")]
pub struct RocketMQCli {
    /// Generate shell completion script
    #[arg(
        long = "generate-completion",
        value_name = "SHELL",
        help = "Generate shell completion script (bash, zsh, fish)"
    )]
    completion: Option<String>,

    #[command(subcommand)]
    commands: Option<Commands>,
}

impl RocketMQCli {
    pub async fn handle(&self) {
        // Handle completion generation
        if let Some(shell) = &self.completion {
            let mut cmd = RocketMQCli::command();
            let bin_name = "rocketmq-admin-cli-rust";

            match shell.to_lowercase().as_str() {
                "bash" => {
                    generate(Bash, &mut cmd, bin_name, &mut std::io::stdout());
                }
                "zsh" => {
                    generate(Zsh, &mut cmd, bin_name, &mut std::io::stdout());
                }
                "fish" => {
                    generate(Fish, &mut cmd, bin_name, &mut std::io::stdout());
                }
                _ => {
                    eprintln!("Unsupported shell: {}", shell);
                    eprintln!("Supported shells: bash, zsh, fish");
                    std::process::exit(1);
                }
            }
            return;
        }

        // Handle regular commands
        if let Some(ref commands) = self.commands {
            if let Err(e) = commands.execute(None).await {
                eprintln!("Error: {e}");
            }
        } else {
            eprintln!("No command specified. Use --help for usage information.");
        }
    }
}
