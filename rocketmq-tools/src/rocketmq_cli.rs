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
use clap::Parser;

use crate::commands::CommandExecute;
use crate::commands::Commands;

#[derive(Parser)]
#[command(name = "rocketmq-admin-cli-rust")]
#[command(about = "Rocketmq Rust admin commands", long_about = None, author="mxsm")]
pub struct RocketMQCli {
    #[command(subcommand)]
    commands: Commands,
}

impl RocketMQCli {
    pub async fn handle(&self) {
        if let Err(e) = self.commands.execute(None).await {
            eprintln!("Error: {e}");
        }
    }
}
