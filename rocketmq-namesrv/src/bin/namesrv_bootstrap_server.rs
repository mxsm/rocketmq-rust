//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.

use std::path::PathBuf;
use std::process::exit;

use clap::Parser;
use rocketmq_common::common::mq_version::CURRENT_VERSION;
use rocketmq_common::common::namesrv::namesrv_config::NamesrvConfig;
use rocketmq_common::common::server::config::ServerConfig;
use rocketmq_common::EnvUtils::EnvUtils;
use rocketmq_common::ParseConfigFile;
use rocketmq_error::Result;
use rocketmq_namesrv::bootstrap::Builder;
use rocketmq_remoting::protocol::remoting_command;
use rocketmq_rust::rocketmq;
use tracing::info;
use tracing::warn;

const LOGO: &str = r#"
      _____            _        _   __  __  ____         _____           _     _   _                         _____
     |  __ \          | |      | | |  \/  |/ __ \       |  __ \         | |   | \ | |                       / ____|
     | |__) |___   ___| | _____| |_| \  / | |  | |______| |__) |   _ ___| |_  |  \| | __ _ _ __ ___   ___  | (___   ___ _ ____   _____ _ __
     |  _  // _ \ / __| |/ / _ \ __| |\/| | |  | |______|  _  / | | / __| __| | . ` |/ _` | '_ ` _ \ / _ \  \___ \ / _ \ '__\ \ / / _ \ '__|
     | | \ \ (_) | (__|   <  __/ |_| |  | | |__| |      | | \ \ |_| \__ \ |_  | |\  | (_| | | | | | |  __/  ____) |  __/ |   \ V /  __/ |
     |_|  \_\___/ \___|_|\_\___|\__|_|  |_|\___\_\      |_|  \_\__,_|___/\__| |_| \_|\__,_|_| |_| |_|\___| |_____/ \___|_|    \_/ \___|_|
    "#;

#[rocketmq::main]
async fn main() -> Result<()> {
    // Initialize the logger
    rocketmq_common::log::init_logger_with_level(rocketmq_common::log::Level::INFO)?;
    println!("            {}", LOGO);
    EnvUtils::put_property(
        remoting_command::REMOTING_VERSION_KEY,
        (CURRENT_VERSION as u32).to_string(),
    );

    let (namesrv_config, port, ip) = parse_config_file()?;

    Builder::new()
        .set_name_server_config(namesrv_config)
        .set_server_config(ServerConfig {
            listen_port: port,
            bind_address: ip,
        })
        .build()
        .boot()
        .await?;

    Ok(())
}

fn parse_config_file() -> Result<(NamesrvConfig, u32, String)> {
    // parse command line arguments
    let args = Args::parse();

    let home = EnvUtils::get_rocketmq_home();
    info!("Rocketmq(Rust) home: {}", home);

    let config_file = if let Some(config) = args.config_file {
        if config.exists() && config.is_file() {
            Some(config)
        } else {
            eprintln!("Config file not found: {config:?}");
            exit(1);
        }
    } else {
        None
    };

    let namesrv_config = if let Some(config_file) = config_file {
        let config = ParseConfigFile::parse_config_file::<NamesrvConfig>(config_file)?;
        info!("Parsed namesrv config: {:?}", config);
        config
    } else {
        warn!("Config file not found, using default");
        NamesrvConfig::default()
    };

    info!(
        "Rocketmq name server(Rust) running on: {}:{}",
        args.ip, args.port
    );
    Ok((namesrv_config, args.port, args.ip))
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

    /// Name server config properties file
    #[arg(
        short,
        long,
        value_name = "CONFIG FILE",
        default_missing_value = "None"
    )]
    config_file: Option<PathBuf>,
}
