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
use std::sync::Arc;

use cheetah_string::CheetahString;
use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::CommandExecute;
use crate::commands::CommonArgs;

#[derive(Debug, Clone, Parser)]
pub struct GetNamesrvConfigCommand {
    #[command(flatten)]
    common: CommonArgs,
}

impl GetNamesrvConfigCommand {
    fn parse_server_list(&self) -> Option<Vec<CheetahString>> {
        self.common.namesrv_addr.as_ref().and_then(|servers| {
            if servers.trim().is_empty() {
                None
            } else {
                let server_array = servers
                    .trim()
                    .split(';')
                    .filter(|s| !s.trim().is_empty())
                    .map(|s| s.trim().to_string().into())
                    .collect::<Vec<CheetahString>>();

                if server_array.is_empty() {
                    None
                } else {
                    Some(server_array)
                }
            }
        })
    }
}

impl CommandExecute for GetNamesrvConfigCommand {
    async fn execute(&self, _rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        if self.common.namesrv_addr.is_none() {
            eprintln!("Please set the namesrvAddr parameter");
            return Ok(());
        }
        let mut admin = DefaultMQAdminExt::new();
        admin.client_config_mut().instance_name = get_current_millis().to_string().into();

        let server_list = self.parse_server_list();
        if let Some(server_list) = server_list {
            admin.start().await?;
            let _ = admin.get_name_server_config(server_list).await;
        } else {
            eprintln!("Please set the namesrvAddr parameter");
            return Ok(());
        }

        unimplemented!("GetNamesrvConfigCommand is not implemented yet");
    }
}
