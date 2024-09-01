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

#[derive(Parser, Debug)]
#[command(
    author = "mxsm",
    version = "0.2.0",
    about = "RocketMQ Broker Server(Rust)"
)]
pub struct Args {
    /// Broker config properties file
    #[arg(short, long, value_name = "FILE", default_missing_value = "None")]
    pub config_file: Option<PathBuf>,

    /// Print important config item
    #[arg(short = 'm', long, required = false)]
    pub print_important_config: bool,

    /// Name remoting_server address list, eg: '192.168.0.1:9876;192.168.0.2:9876'
    #[arg(
        short,
        long,
        value_name = "IP",
        default_value = "127.0.0.1:9876",
        required = false
    )]
    pub namesrv_addr: String,

    ///Print all config item
    #[arg(short, long, required = false)]
    pub print_config_item: bool,
}
