/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::path::PathBuf;

use clap::Parser;
use rocketmq_common::{
    common::namesrv::namesrv_config::NamesrvConfig, EnvUtils::EnvUtils, ParseConfigFile,
};
use rocketmq_namesrv::bootstrap::Builder;
use rocketmq_remoting::server::config::ServerConfig;
use rocketmq_rust::rocketmq;
use tracing::info;

#[rocketmq::main]
async fn main() -> anyhow::Result<()> {
    rocketmq_common::log::init_logger();
    let args = Args::parse();
    let home = EnvUtils::get_rocketmq_home();

    info!("Rocketmq(Rust) home: {}", home);
    info!(
        "Rocketmq name server(Rust) running on: {}:{}",
        args.ip, args.port
    );
    let config_file = PathBuf::from(home).join("conf").join("namesrv.toml");
    let namesrv_config = ParseConfigFile::parse_config_file::<NamesrvConfig>(config_file.clone())?;
    Builder::new()
        .set_name_server_config(namesrv_config)
        .set_server_config(ServerConfig {
            listen_port: args.port,
            bind_address: args.ip,
        })
        .build()
        .boot()
        .await;

    Ok(())
}

#[derive(Parser, Debug)]
#[command(
    author = "mxsm",
    version = "0.1.0",
    about = "RocketMQ Name server(Rust)"
)]
struct Args {
    /// rocketmq name server port
    #[arg(
        short,
        long,
        value_name = "PORT",
        default_missing_value = "9876",
        default_value = "9876",
        required = false
    )]
    port: u32,

    /// rocketmq name server ip
    #[arg(
        short,
        long,
        value_name = "IP",
        default_value = "0.0.0.0",
        required = false
    )]
    ip: String,
    /// rocketmq name server config file
    #[arg(short, long, value_name = "FILE", default_missing_value = "None")]
    config: Option<PathBuf>,
}
