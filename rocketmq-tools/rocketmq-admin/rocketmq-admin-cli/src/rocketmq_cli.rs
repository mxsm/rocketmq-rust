use clap::CommandFactory;
use clap::Parser;
use clap_complete::generate;
use clap_complete::shells::Bash;
use clap_complete::shells::Fish;
use clap_complete::shells::Zsh;
use rocketmq_admin_core::commands::CommandExecute;
use rocketmq_admin_core::commands::Commands;

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
    pub async fn handle(&self) {
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
                    eprintln!("Unsupported shell: {}", shell);
                    eprintln!("Supported shells: bash, zsh, fish");
                    std::process::exit(1);
                }
            }
            return;
        }

        if let Some(ref commands) = self.commands {
            if let Err(e) = commands.execute(None).await {
                eprintln!("Error: {e}");
            }
        } else {
            eprintln!("No command specified. Use --help for usage information.");
        }
    }
}
