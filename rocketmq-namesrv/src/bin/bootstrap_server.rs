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

use std::{collections::HashMap, process::exit};

use clap::Parser;
use namesrv::processor::default_request_processor::DefaultRequestProcessor;
use rocketmq_remoting::runtime::server;
use tokio::net::TcpListener;
use tracing::{error, info};

#[rocketmq::main]
async fn main() -> anyhow::Result<()> {
    rocketmq_common::log::init_logger();
    let home = std::env::var("ROCKETMQ_HOME");
    if home.is_err() {
        error!(
            "Please set the ROCKETMQ_HOME variable in your environment to match the location of \
             the RocketMQ installation"
        );
        exit(0);
    }
    let args = Args::parse();
    info!(
        "Rocketmq name server(Rust) running on {}:{}",
        args.ip, args.port
    );
    let listener = TcpListener::bind(&format!("{}:{}", args.ip, args.port)).await?;
    let default_request_processor = DefaultRequestProcessor::new();
    //run server
    server::run(
        listener,
        tokio::signal::ctrl_c(),
        default_request_processor,
        HashMap::new(),
    )
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
        default_value = "127.0.0.1",
        required = false
    )]
    ip: String,
    /* /// rocketmq name server config file
    #[arg(short, long, value_name = "FILE")]
    config: Option<PathBuf>,*/
}
