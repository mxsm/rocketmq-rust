// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::common::mq_version::RocketMqVersion;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::runtime::RPCHook;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
pub struct ProducerConnectionSubCommand {
    #[arg(short = 'g', long = "producerGroup", required = true, help = "producer group name")]
    producer_group: String,

    #[arg(short = 't', long = "topic", required = true, help = "topic name")]
    topic: String,
}

impl CommandExecute for ProducerConnectionSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> rocketmq_error::RocketMQResult<()> {
        let mut default_mq_admin_ext = if let Some(rpc_hook) = rpc_hook {
            DefaultMQAdminExt::with_rpc_hook(rpc_hook)
        } else {
            DefaultMQAdminExt::new()
        };

        default_mq_admin_ext
            .client_config_mut()
            .set_instance_name(get_current_millis().to_string().into());

        default_mq_admin_ext.start().await?;

        let group = self.producer_group.trim();
        let topic = self.topic.trim();

        let pc = default_mq_admin_ext
            .examine_producer_connection_info(group.into(), topic.into())
            .await?;

        let mut connections: Vec<_> = pc.connection_set().iter().collect();
        connections.sort_by_key(|a| a.get_client_id());
        for (idx, conn) in connections.iter().enumerate() {
            let version_desc = RocketMqVersion::from_ordinal(conn.get_version() as u32).name();
            println!(
                "{:04}  {:<32} {:<22} {:<8} {}",
                idx + 1,
                conn.get_client_id(),
                conn.get_client_addr(),
                conn.get_language(),
                version_desc
            );
        }

        default_mq_admin_ext.shutdown().await;
        Ok(())
    }
}
