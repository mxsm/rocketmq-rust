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

use std::{collections::HashMap, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use clap::Parser;
use rocketmq_common::{
    common::{mix_all::ROCKETMQ_HOME_ENV, namesrv::namesrv_config::NamesrvConfig},
    ScheduledExecutorService,
};
use rocketmq_namesrv::{
    parse_command_and_config_file,
    processor::{default_request_processor::DefaultRequestProcessor, ClientRequestProcessor},
    KVConfigManager, RouteInfoManager,
};
use rocketmq_remoting::{
    code::request_code::RequestCode,
    runtime::{processor::RequestProcessor, server},
};
use rocketmq_rust::rocketmq;
use tokio::{net::TcpListener, sync::broadcast, task::JoinHandle};
use tracing::info;

#[rocketmq::main]
async fn main() -> anyhow::Result<()> {
    rocketmq_common::log::init_logger();
    let args = Args::parse();
    let home = std::env::var(ROCKETMQ_HOME_ENV).unwrap_or_else(|_| {
        let rocketmq_home_dir = std::env::current_dir()
            .unwrap()
            .into_os_string()
            .to_string_lossy()
            .to_string();
        std::env::set_var(ROCKETMQ_HOME_ENV, rocketmq_home_dir.clone());
        rocketmq_home_dir
    });

    info!("rocketmq home: {}", home);
    info!(
        "Rocketmq name server(Rust) running on {}:{}",
        args.ip, args.port
    );

    //bind local host and port, start tcp listen
    let listener = TcpListener::bind(&format!("{}:{}", args.ip, args.port)).await?;
    let config_file = PathBuf::from(home).join("conf").join("namesrv.conf");
    let config = parse_command_and_config_file(config_file)?;
    let (notify_conn_disconnect, _) = broadcast::channel::<SocketAddr>(100);
    let receiver = notify_conn_disconnect.subscribe();
    let route_info_manager = RouteInfoManager::new_with_config(config.clone());
    let kvconfig_manager = KVConfigManager::new(config.clone());
    let (processor_table, default_request_processor, scheduled_executor_service, _handle) =
        init_processors(route_info_manager, config, kvconfig_manager, receiver);
    //run server
    server::run(
        listener,
        tokio::signal::ctrl_c(),
        default_request_processor,
        processor_table,
        Some(notify_conn_disconnect),
    )
    .await;
    scheduled_executor_service.shutdown();
    Ok(())
}

type InitProcessorsReturn = (
    HashMap<i32, Box<dyn RequestProcessor + Send + Sync + 'static>>,
    DefaultRequestProcessor,
    ScheduledExecutorService,
    JoinHandle<()>,
);

fn init_processors(
    route_info_manager: RouteInfoManager,
    namesrv_config: NamesrvConfig,
    kvconfig_manager: KVConfigManager,
    receiver: broadcast::Receiver<SocketAddr>,
) -> InitProcessorsReturn {
    let route_info_manager_inner = Arc::new(parking_lot::RwLock::new(route_info_manager));
    let handle = RouteInfoManager::start(route_info_manager_inner.clone(), receiver);
    let kvconfig_manager_inner = Arc::new(parking_lot::RwLock::new(kvconfig_manager));
    let mut processors: HashMap<i32, Box<dyn RequestProcessor + Send + Sync + 'static>> =
        HashMap::new();
    processors.insert(
        RequestCode::GetRouteinfoByTopic.to_i32(),
        Box::new(ClientRequestProcessor::new(
            route_info_manager_inner.clone(),
            namesrv_config.clone(),
            kvconfig_manager_inner.clone(),
        )),
    );
    let scheduled_executor_service = ScheduledExecutorService::new_with_config(
        1,
        Some("Namesrv-"),
        Duration::from_secs(60),
        10000,
    );
    let arc = route_info_manager_inner.clone();
    scheduled_executor_service.schedule_at_fixed_rate(
        move || arc.write().scan_not_active_broker(),
        Some(Duration::from_secs(5)),
        Duration::from_millis(namesrv_config.scan_not_active_broker_interval),
    );
    (
        processors,
        DefaultRequestProcessor::new_with(route_info_manager_inner, kvconfig_manager_inner),
        scheduled_executor_service,
        handle,
    )
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
