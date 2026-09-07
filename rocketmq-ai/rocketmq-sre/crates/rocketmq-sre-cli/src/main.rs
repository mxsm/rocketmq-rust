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

use std::io;
use std::io::Write;
use std::process::ExitCode;

use rocketmq_sre_cli::CliFailure;
use rocketmq_sre_cli::Command;
use rocketmq_sre_cli::USAGE;
use rocketmq_sre_cli::execute;
use rocketmq_sre_cli::parse_process_args;
use rocketmq_sre_cli::render;
enum MainFailure {
    Cli(CliFailure),
    Output(io::Error),
}

impl std::fmt::Display for MainFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.public_message())
    }
}

impl std::fmt::Debug for MainFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("MainFailure")
    }
}

impl MainFailure {
    fn public_message(&self) -> &'static str {
        match self {
            Self::Cli(failure) => failure.public_message(),
            Self::Output(source) => {
                let _kind = source.kind();
                "failed to write CLI output"
            }
        }
    }

    const fn is_usage(&self) -> bool {
        matches!(self, Self::Cli(failure) if failure.is_usage())
    }
}

impl From<CliFailure> for MainFailure {
    fn from(failure: CliFailure) -> Self {
        Self::Cli(failure)
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            let mut stderr = io::stderr().lock();
            let _ = writeln!(stderr, "rocketmq-sre: {}", error.public_message());
            if error.is_usage() {
                let _ = writeln!(stderr, "\n{USAGE}");
                ExitCode::from(2)
            } else {
                ExitCode::FAILURE
            }
        }
    }
}

async fn run() -> Result<(), MainFailure> {
    let invocation = parse_process_args(std::env::args_os().skip(1))?;
    if invocation.command == Command::Help {
        return write_stdout(USAGE).map_err(MainFailure::Output);
    }
    let value = execute(&invocation).await?;
    let output = render(&invocation, &value)?;
    write_stdout(&output).map_err(MainFailure::Output)
}

fn write_stdout(value: &str) -> io::Result<()> {
    let mut stdout = io::stdout().lock();
    stdout.write_all(value.as_bytes())?;
    if !value.ends_with('\n') {
        stdout.write_all(b"\n")?;
    }
    stdout.flush()
}
