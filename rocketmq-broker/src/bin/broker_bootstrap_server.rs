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

use anyhow::bail;
use clap::Parser;
use rocketmq_broker::command::Args;
use rocketmq_broker::Builder;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::mq_version::CURRENT_VERSION;
use rocketmq_common::EnvUtils::EnvUtils;
use rocketmq_common::ParseConfigFile;
use rocketmq_error::Result;
use rocketmq_remoting::protocol::remoting_command;
use rocketmq_rust::rocketmq;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use tracing::info;

#[rocketmq::main]
async fn main() -> Result<()> {
    // init logger
    rocketmq_common::log::init_logger_with_level(rocketmq_common::log::Level::INFO)?;
    const LOGO: &str = r#"
      _____            _        _   __  __  ____         _____           _     ____            _
     |  __ \          | |      | | |  \/  |/ __ \       |  __ \         | |   |  _ \          | |
     | |__) |___   ___| | _____| |_| \  / | |  | |______| |__) |   _ ___| |_  | |_) |_ __ ___ | | _____ _ __
     |  _  // _ \ / __| |/ / _ \ __| |\/| | |  | |______|  _  / | | / __| __| |  _ <| '__/ _ \| |/ / _ \ '__|
     | | \ \ (_) | (__|   <  __/ |_| |  | | |__| |      | | \ \ |_| \__ \ |_  | |_) | | | (_) |   <  __/ |
     |_|  \_\___/ \___|_|\_\___|\__|_|  |_|\___\_\      |_|  \_\__,_|___/\__| |____/|_|  \___/|_|\_\___|_|
    "#;
    info!("         {}", LOGO);
    EnvUtils::put_property(
        remoting_command::REMOTING_VERSION_KEY,
        (CURRENT_VERSION as u32).to_string(),
    );

    let (broker_config, message_store_config) = parse_config_file()?;
    // boot strap broker
    Builder::new()
        .set_broker_config(broker_config)
        .set_message_store_config(message_store_config)
        .build()
        .boot()
        .await;
    Ok(())
}

fn parse_config_file() -> Result<(BrokerConfig, MessageStoreConfig)> {
    let args = Args::parse();
    let home = EnvUtils::get_rocketmq_home();
    info!("Rocketmq(Rust) home: {}", home);
    let config = if let Some(ref config_file) = args.config_file {
        let config_file = PathBuf::from(config_file);
        info!("Using config file: {:?}", config_file);
        if !config_file.exists() || !config_file.is_file() {
            bail!(
                "Config file does not exist or is not a file: {:?}",
                config_file
            );
        }
        Ok((
            ParseConfigFile::parse_config_file::<BrokerConfig>(config_file.clone())?,
            ParseConfigFile::parse_config_file::<MessageStoreConfig>(config_file)?,
        ))
    } else {
        let config_file = PathBuf::from(home.as_str())
            .join("conf")
            .join("broker.toml");
        info!("Using config file: {:?}", config_file);
        if !config_file.exists() || !config_file.is_file() {
            return Ok((Default::default(), Default::default()));
        }
        Ok((
            ParseConfigFile::parse_config_file::<BrokerConfig>(config_file.clone())?,
            ParseConfigFile::parse_config_file::<MessageStoreConfig>(config_file)?,
        ))
    };
    config
}
