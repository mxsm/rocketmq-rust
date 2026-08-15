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
use rocketmq_error::CliErrorView;
use rocketmq_store_inspect::command_line::Commands;
use rocketmq_store_inspect::command_line::RootCli;
use rocketmq_store_inspect::content_show::print_content;
use rocketmq_store_inspect::downgrade_preflight::run_preflight;
use rocketmq_store_inspect::downgrade_preflight::DowngradePreflightRequest;
use rocketmq_store_inspect::multipath_consolidate::consolidate_multipath;
use rocketmq_store_inspect::multipath_consolidate::ConsolidationRequest;

fn main() {
    let exit_code = run();
    if exit_code != 0 {
        std::process::exit(exit_code);
    }
}

fn run() -> i32 {
    let cli = RootCli::parse();
    match cli.command {
        Commands::ReadMessageLog { config, from, to } => {
            if let Err(error) = print_content(from, to, config) {
                let view = CliErrorView::from_error(&error);
                eprintln!("{}", view.render_stderr());
                return view.exit_code().as_i32();
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
                        return render_error(&error);
                    }
                }
                Err(error) => {
                    return render_error(&rocketmq_error::RocketMQError::storage_write_failed(
                        target.display().to_string(),
                        error.to_string(),
                    ));
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
                        return render_error(&rocketmq_error::RocketMQError::internal(
                            "serialize downgrade preflight report",
                            error,
                        ));
                    }
                };
                if let Some(path) = output {
                    if let Err(error) = std::fs::write(&path, body) {
                        return render_error(&rocketmq_error::RocketMQError::storage_write_failed(
                            path.display().to_string(),
                            error.to_string(),
                        ));
                    }
                } else {
                    print!("{body}");
                }
                if !report.allowed {
                    return 2;
                }
            }
            Err(error) => return render_error(&error),
        },
    }
    0
}

fn print_json(value: &impl serde::Serialize) -> Result<(), rocketmq_error::RocketMQError> {
    let body = serde_json::to_string_pretty(value)
        .map_err(|error| rocketmq_error::RocketMQError::internal("serialize store inspection report", error))?;
    println!("{body}");
    Ok(())
}

fn render_error(error: &rocketmq_error::RocketMQError) -> i32 {
    let view = CliErrorView::from_error(error);
    eprintln!("{}", view.render_stderr());
    view.exit_code().as_i32()
}
