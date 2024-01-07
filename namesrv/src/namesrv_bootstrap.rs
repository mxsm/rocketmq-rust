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

use std::{sync::Arc, time::Duration};

use config::Config;
use rocketmq_common::{common::namesrv::namesrv_config::NamesrvConfig, TokioExecutorService};
use rocketmq_remoting::runtime::{
    remoting_service::RemotingService, server::tokio_remoting_server::TokioRemotingServer,
};
use tracing::info;

use crate::{
    processor::default_request_processor::DefaultRequestProcessor,
    route::route_info_manager::RouteInfoManager,
};

pub async fn boot() -> anyhow::Result<()> {
    //let _ = parse_command_and_config_file();
    let config = NamesrvConfig::new();

    let manager = Arc::new(parking_lot::RwLock::new(RouteInfoManager::new()));
    let server = TokioRemotingServer::new(
        9876,
        "127.0.0.1",
        TokioExecutorService::new_with_config(
            config.default_thread_pool_nums as usize,
            "RemotingExecutorThread-",
            Duration::from_secs(60),
            config.default_thread_pool_queue_capacity as usize,
        ),
        Box::new(DefaultRequestProcessor::new_with_route_info_manager(
            manager.clone(),
        )),
    );
    info!("Starting rocketmq name server (Rust)");
    info!("Rocketmq name server(Rust) running on 127.0.0.1:9876");
    server.start();
    Ok(())
}

fn parse_command_and_config_file() -> anyhow::Result<(), anyhow::Error> {
    let namesrv_config = Config::builder()
        .add_source(config::File::with_name(
            format!(
                "namesrv{}resource{}namesrv.toml",
                std::path::MAIN_SEPARATOR,
                std::path::MAIN_SEPARATOR
            )
            .as_str(),
        ))
        .build()
        .unwrap();
    let result = namesrv_config.try_deserialize::<NamesrvConfig>().unwrap();
    info!("namesrv config: {:?}", result);
    Ok(())
}
