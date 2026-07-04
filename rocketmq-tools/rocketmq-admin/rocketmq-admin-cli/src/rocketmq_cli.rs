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

use std::ffi::OsString;

use clap::CommandFactory;
use clap::Parser;
use clap_complete::generate;
use clap_complete::shells::Bash;
use clap_complete::shells::Fish;
use clap_complete::shells::Zsh;
use rocketmq_error::CliErrorView;
use rocketmq_error::RocketMQError;

use crate::commands::CommandExecute;
use crate::commands::Commands;

#[derive(Parser)]
#[command(name = "rocketmq-admin-cli")]
#[command(about = "Rocketmq Rust admin commands", long_about = None, author="mxsm")]
pub struct RocketMQCli {
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
    pub fn parse_from_java_compatible_args() -> Self {
        Self::parse_from(normalize_java_compatible_args(std::env::args_os()))
    }

    pub async fn handle(&self) -> i32 {
        if let Some(shell) = &self.completion {
            let mut cmd = RocketMQCli::command();
            let bin_name = "rocketmq-admin-cli";

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
                    return render_cli_error(&RocketMQError::validation_failed(
                        "generate-completion",
                        format!("unsupported shell '{shell}', supported shells: bash, zsh, fish"),
                    ));
                }
            }
            return 0;
        }

        if let Some(ref commands) = self.commands {
            if let Err(e) = commands.execute(None).await {
                return render_cli_error(&e);
            }
            0
        } else {
            render_cli_error(&RocketMQError::validation_failed(
                "command",
                "command must be specified; use --help for usage information",
            ))
        }
    }
}

fn render_cli_error(error: &RocketMQError) -> i32 {
    let view = CliErrorView::from_error(error);
    eprintln!("{}", view.render_stderr());
    view.exit_code().as_i32()
}

fn normalize_java_compatible_args<I>(args: I) -> Vec<OsString>
where
    I: IntoIterator<Item = OsString>,
{
    args.into_iter()
        .map(|arg| {
            if arg == "-bn" {
                OsString::from("--brokerName")
            } else {
                arg
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::RocketMQCli;
    use super::normalize_java_compatible_args;
    use clap::Parser;
    use rocketmq_error::CliExitCode;
    use std::ffi::OsString;

    #[test]
    fn normalizes_java_multi_character_short_broker_name_option() {
        let args = normalize_java_compatible_args([
            OsString::from("rocketmq-admin-cli"),
            OsString::from("controller"),
            OsString::from("electMaster"),
            OsString::from("-bn"),
            OsString::from("broker-a"),
        ]);

        assert_eq!(args[3], OsString::from("--brokerName"));
    }

    #[tokio::test]
    async fn handle_returns_usage_exit_for_missing_command() {
        let cli = RocketMQCli::try_parse_from(["rocketmq-admin-cli"]).unwrap();

        assert_eq!(cli.handle().await, CliExitCode::USAGE.as_i32());
    }

    #[tokio::test]
    async fn handle_returns_usage_exit_for_unsupported_completion_shell() {
        let cli = RocketMQCli::try_parse_from(["rocketmq-admin-cli", "--generate-completion", "powershell"]).unwrap();

        assert_eq!(cli.handle().await, CliExitCode::USAGE.as_i32());
    }
}
