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

use clap::Parser;
use rocketmq_error::fields;
use rocketmq_error::CliErrorView;
use rocketmq_error::CliExitCode;
use rocketmq_error::CliVerbosity;
use rocketmq_error::Error;
use rocketmq_error::ErrorContext;
use rocketmq_error::RocketMQError;
use rocketmq_error::STORAGE_WRITE_FAILED;
use rocketmq_store_inspect::command_line::Commands;
use rocketmq_store_inspect::command_line::RootCli;
use rocketmq_store_inspect::content_show::print_content;
use rocketmq_store_inspect::downgrade_preflight::run_preflight;
use rocketmq_store_inspect::downgrade_preflight::DowngradePreflightRequest;
use rocketmq_store_inspect::multipath_consolidate::consolidate_multipath;
use rocketmq_store_inspect::multipath_consolidate::ConsolidationRequest;

fn print_release_version_if_requested(component: &str) -> bool {
    let arguments: Vec<_> = std::env::args_os().skip(1).collect();
    let version = std::ffi::OsStr::new("--version");
    let verbose = std::ffi::OsStr::new("--verbose");
    let requested = (arguments.len() == 1 && arguments[0].as_os_str() == version)
        || (arguments.len() == 2
            && arguments.iter().any(|argument| argument.as_os_str() == version)
            && arguments.iter().any(|argument| argument.as_os_str() == verbose));
    if !requested {
        return false;
    }
    println!("{component}");
    println!("version={}", env!("CARGO_PKG_VERSION"));
    if arguments.len() == 2 {
        println!(
            "artifact_id={}",
            option_env!("ROCKETMQ_RELEASE_ARTIFACT_ID").unwrap_or("development")
        );
        println!(
            "requested_features={}",
            option_env!("ROCKETMQ_RELEASE_REQUESTED_FEATURES").unwrap_or("default")
        );
        println!(
            "effective_features={}",
            option_env!("ROCKETMQ_RELEASE_EFFECTIVE_FEATURES").unwrap_or("default")
        );
    }
    true
}

fn main() {
    if print_release_version_if_requested("rocketmq-store-inspect") {
        return;
    }
    let exit_code = run().exit_code();
    if exit_code != 0 {
        std::process::exit(exit_code);
    }
}

fn run() -> ProcessOutcome {
    let verbosity = verbosity_requested();
    let cli = match RootCli::try_parse() {
        Ok(cli) => cli,
        Err(error)
            if matches!(
                error.kind(),
                clap::error::ErrorKind::DisplayHelp
                    | clap::error::ErrorKind::DisplayVersion
                    | clap::error::ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
            ) =>
        {
            print!("{error}");
            return ProcessOutcome::Completed;
        }
        Err(_) => {
            return render_error(
                &rocketmq_error::RocketMQError::validation_failed("command-line", "invalid command-line arguments"),
                verbosity,
            );
        }
    };
    let verbosity = if cli.verbose {
        CliVerbosity::Verbose
    } else {
        CliVerbosity::Default
    };
    match cli.command {
        Commands::ReadMessageLog { config, from, to } => {
            if let Err(error) = print_content(from, to, config) {
                return render_error(&error, verbosity);
            }
        }
        Commands::ConsolidateMultipath {
            source_roots,
            target,
            mapped_file_size,
            store_root,
        } => {
            let request =
                ConsolidationRequest::new(source_roots, target.clone(), mapped_file_size).with_store_root(store_root);
            match consolidate_multipath(&request) {
                Ok(report) => {
                    if let Err(error) = print_json(&report) {
                        return render_error(&error, verbosity);
                    }
                }
                Err(error) => {
                    return render_error(&storage_write_error("consolidate_multipath", error), verbosity);
                }
            }
        }
        Commands::DowngradePreflight {
            target_version,
            config,
            output,
        } => match run_preflight(&DowngradePreflightRequest::new(target_version, config)) {
            Ok(report) => {
                let body = match serde_json::to_string_pretty(&report) {
                    Ok(body) => format!("{body}\n"),
                    Err(error) => {
                        return render_error(
                            &rocketmq_error::RocketMQError::internal("serialize downgrade preflight report", error),
                            verbosity,
                        );
                    }
                };
                if let Some(path) = output {
                    if let Err(error) = std::fs::write(&path, body) {
                        return render_error(&storage_write_error("write_preflight_report", error), verbosity);
                    }
                } else {
                    print!("{body}");
                }
                if !report.allowed {
                    return ProcessOutcome::PolicyRefused;
                }
            }
            Err(error) => return render_error(&error, verbosity),
        },
    }
    ProcessOutcome::Completed
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProcessOutcome {
    Completed,
    Error(CliExitCode),
    PolicyRefused,
}

impl ProcessOutcome {
    const fn exit_code(self) -> i32 {
        match self {
            Self::Completed => 0,
            Self::Error(exit_code) => exit_code.as_i32(),
            // The JSON report is the complete operator-facing explanation for
            // this expected compatibility-policy outcome.
            Self::PolicyRefused => 2,
        }
    }
}

fn verbosity_requested() -> CliVerbosity {
    if std::env::args_os().any(|argument| argument == "--verbose") {
        CliVerbosity::Verbose
    } else {
        CliVerbosity::Default
    }
}

fn print_json(value: &impl serde::Serialize) -> Result<(), rocketmq_error::RocketMQError> {
    let body = serde_json::to_string_pretty(value)
        .map_err(|error| rocketmq_error::RocketMQError::internal("serialize store inspection report", error))?;
    println!("{body}");
    Ok(())
}

fn storage_write_error(operation: &'static str, source: std::io::Error) -> RocketMQError {
    let context = ErrorContext::new()
        .with_text(fields::STORE_OPERATION, operation)
        .with_text(fields::STORE_COMPONENT, "store-inspect")
        .with_secret_presence(fields::STORE_DETAIL_PRESENT)
        .with_secret_presence(fields::SOURCE_PRESENT);
    RocketMQError::Shared(std::sync::Arc::new(
        Error::caused_by(&STORAGE_WRITE_FAILED, source).with_context(context),
    ))
}

fn render_error(error: &rocketmq_error::RocketMQError, verbosity: CliVerbosity) -> ProcessOutcome {
    let output = CliErrorView::from_error(error).output(verbosity);
    eprintln!("{}", output.stderr());
    ProcessOutcome::Error(output.exit_code())
}

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;

    use super::*;

    #[test]
    fn storage_write_bridge_retains_typed_source_and_safe_cli_projection() {
        let source = std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "C:/secret/report.json password=plain-text",
        );
        let error = storage_write_error("write_preflight_report", source);
        let RocketMQError::Shared(shared) = &error else {
            panic!("storage write bridge must use the canonical shared carrier");
        };
        assert!(StdError::source(shared.as_ref())
            .and_then(|source| source.downcast_ref::<std::io::Error>())
            .is_some());

        for verbosity in [CliVerbosity::Default, CliVerbosity::Verbose] {
            let output = CliErrorView::from_error(&error).output(verbosity);
            assert_eq!(output.exit_code(), CliExitCode::SOFTWARE);
            assert!(!output.stderr().contains("secret"));
            assert!(!output.stderr().contains("plain-text"));
            assert!(!output.stderr().contains("report.json"));
        }
    }

    #[test]
    fn policy_refusal_is_a_distinct_non_error_outcome() {
        assert_eq!(ProcessOutcome::Completed.exit_code(), 0);
        assert_eq!(ProcessOutcome::PolicyRefused.exit_code(), 2);
        assert_eq!(ProcessOutcome::Error(CliExitCode::DATA).exit_code(), 65);
    }
}
