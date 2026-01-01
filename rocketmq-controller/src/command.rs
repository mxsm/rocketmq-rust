// Copyright 2025-2026 The RocketMQ Rust Authors
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

#[derive(Parser, Debug)]
#[command(
    author = "mxsm",
    version = "0.8.0",
    about = "RocketMQ Controller Server (Rust)",
    long_about = "RocketMQ Controller Server implemented in Rust.\nResponsible for broker metadata management and \
                  leader election."
)]
pub struct Args {
    /// Controller configuration file path
    ///
    /// Example:
    ///   --config-file ./conf/controller.toml
    #[arg(short, long, value_name = "FILE")]
    pub config_file: Option<PathBuf>,

    /// Print important configuration items and exit
    #[arg(short = 'p', long = "print-config-item")]
    pub print_config_item: bool,

    /// NameServer address list
    ///
    /// Format:
    ///   ip:port;ip:port
    ///
    /// Example:
    ///   192.168.0.1:9876;192.168.0.2:9876
    #[arg(short, long, value_name = "ADDR", default_value = "127.0.0.1:9876")]
    pub namesrv_addr: String,

    /// Print all configuration items and exit
    #[arg(long)]
    pub print_all_config_item: bool,
}
