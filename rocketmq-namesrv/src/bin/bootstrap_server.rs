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

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use clap::Parser;
use namesrv::{
    processor::{default_request_processor::DefaultRequestProcessor, ClientRequestProcessor},
    KVConfigManager, RouteInfoManager,
};
use rocketmq_common::common::namesrv::namesrv_config::NamesrvConfig;
use rocketmq_remoting::{
    code::request_code::RequestCode,
    runtime::{processor::RequestProcessor, server},
};
use tokio::net::TcpListener;
use tracing::info;

#[rocketmq::main]
async fn main() -> anyhow::Result<()> {
    rocketmq_common::log::init_logger();
    let args = Args::parse();
    let home = std::env::var("ROCKETMQ_HOME").unwrap_or(
        std::env::current_dir()
            .unwrap()
            .into_os_string()
            .to_string_lossy()
            .to_string(),
    );
    info!("rocketmq home: {}", home);
    info!(
        "Rocketmq name server(Rust) running on {}:{}",
        args.ip, args.port
    );
    let listener = TcpListener::bind(&format!("{}:{}", args.ip, args.port)).await?;
    let config = NamesrvConfig::new();
    let route_info_manager = RouteInfoManager::new_with_config(config.clone());
    let kvconfig_manager = KVConfigManager::new(config.clone());
    let (processor_table, default_request_processor) =
        init_processors(route_info_manager, config, kvconfig_manager);
    //run server
    server::run(
        listener,
        tokio::signal::ctrl_c(),
        default_request_processor,
        processor_table,
    )
    .await;

    Ok(())
}

fn init_processors(
    route_info_manager: RouteInfoManager,
    namesrv_config: NamesrvConfig,
    kvconfig_manager: KVConfigManager,
) -> (
    HashMap<i32, Box<dyn RequestProcessor + Send + Sync + 'static>>,
    DefaultRequestProcessor,
) {
    let route_info_manager_inner = Arc::new(parking_lot::RwLock::new(route_info_manager));
    let kvconfig_manager_inner = Arc::new(parking_lot::RwLock::new(kvconfig_manager));
    let mut processors: HashMap<i32, Box<dyn RequestProcessor + Send + Sync + 'static>> =
        HashMap::new();
    processors.insert(
        RequestCode::GetRouteinfoByTopic.to_i32(),
        Box::new(ClientRequestProcessor::new(
            route_info_manager_inner.clone(),
            namesrv_config,
            kvconfig_manager_inner.clone(),
        )),
    );
    (processors, DefaultRequestProcessor::new())
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
        default_value = "127.0.0.1",
        required = false
    )]
    ip: String,
    /// rocketmq name server config file
    #[arg(short, long, value_name = "FILE", default_missing_value = "None")]
    config: Option<PathBuf>,
}
