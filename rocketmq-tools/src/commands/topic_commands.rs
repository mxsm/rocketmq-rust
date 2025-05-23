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
mod allocate_mq_sub_command;

use std::sync::Arc;

use clap::Subcommand;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;

#[derive(Subcommand)]
pub enum TopicCommands {
    #[command(
        name = "allocateMQ",
        about = "Allocate memory space for each topic",
        long_about = r#"Allocate memory space for each topic, which is used to allocate the memory
space of the topic when the topic is created. The default value is 1. If you want to allocate
more memory space, you can use this command to allocate it."#
    )]
    AllocateMQ(allocate_mq_sub_command::AllocateMQSubCommand),
}

impl CommandExecute for TopicCommands {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        match self {
            TopicCommands::AllocateMQ(cmd) => cmd.execute(rpc_hook).await,
        }
    }
}
