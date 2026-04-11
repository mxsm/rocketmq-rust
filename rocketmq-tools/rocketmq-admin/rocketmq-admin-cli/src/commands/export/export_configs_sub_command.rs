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

use std::collections::HashMap;
use std::sync::Arc;

use cheetah_string::CheetahString;
use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;
use crate::commands::command_util::CommandUtil;
use rocketmq_admin_core::admin::default_mq_admin_ext::DefaultMQAdminExt;

const DEFAULT_FILE_PATH: &str = "/tmp/rocketmq/export";

const NEED_BROKER_PROPERTY_KEYS: &[&str] = &[
    "brokerClusterName",
    "brokerId",
    "brokerName",
    "brokerRole",
    "fileReservedTime",
    "filterServerNums",
    "flushDiskType",
    "maxMessageSize",
    "messageDelayLevel",
    "msgTraceTopicName",
    "slaveReadEnable",
    "traceOn",
    "traceTopicEnable",
    "useTLS",
    "autoCreateTopicEnable",
    "autoCreateSubscriptionGroup",
];

#[derive(Debug, Clone, Parser)]
pub struct ExportConfigsSubCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(
        short = 'c',
        long = "clusterName",
        required = true,
        help = "choose a cluster to export"
    )]
    cluster_name: String,

    #[arg(
        short = 'f',
        long = "filePath",
        required = false,
        default_value = DEFAULT_FILE_PATH,
        help = "export configs.json path | default /tmp/rocketmq/export"
    )]
    file_path: String,
}

impl ExportConfigsSubCommand {
    fn need_broker_properties(properties: &HashMap<CheetahString, CheetahString>) -> HashMap<String, String> {
        let mut filtered = HashMap::new();
        for &key in NEED_BROKER_PROPERTY_KEYS {
            if let Some(value) = properties.get(&CheetahString::from(key)) {
                filtered.insert(key.to_string(), value.to_string());
            }
        }
        filtered
    }
}

impl CommandExecute for ExportConfigsSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let cluster_name = self.cluster_name.trim();
        let file_path = self.file_path.trim();

        let mut admin_ext = if let Some(ref hook) = rpc_hook {
            DefaultMQAdminExt::with_rpc_hook(hook.clone())
        } else {
            DefaultMQAdminExt::new()
        };
        admin_ext
            .client_config_mut()
            .set_instance_name(current_millis().to_string().into());

        if let Some(addr) = &self.common_args.namesrv_addr {
            admin_ext.set_namesrv_addr(addr.trim());
        }

        admin_ext.start().await.map_err(|e| {
            RocketMQError::Internal(format!("ExportConfigsSubCommand: Failed to start MQAdminExt: {}", e))
        })?;

        let result = self.do_execute(&admin_ext, cluster_name, file_path).await;

        admin_ext.shutdown().await;

        result
    }
}

impl ExportConfigsSubCommand {
    async fn do_execute(
        &self,
        admin_ext: &DefaultMQAdminExt,
        cluster_name: &str,
        file_path: &str,
    ) -> RocketMQResult<()> {
        let mut result = serde_json::Map::new();

        // name servers
        let name_server_address_list = admin_ext.get_name_server_address_list().await;
        let name_servers: Vec<String> = name_server_address_list.iter().map(|addr| addr.to_string()).collect();

        // broker
        let mut master_broker_size: i64 = 0;
        let mut slave_broker_size: i64 = 0;
        let mut broker_configs: HashMap<String, HashMap<String, String>> = HashMap::new();

        let cluster_info = admin_ext.examine_broker_cluster_info().await?;
        let master_and_slave_map = CommandUtil::fetch_master_and_slave_distinguish(&cluster_info, cluster_name)?;

        for (master_addr, slave_addrs) in &master_and_slave_map {
            if master_addr.as_str() == CommandUtil::NO_MASTER_PLACEHOLDER {
                slave_broker_size += slave_addrs.len() as i64;
                continue;
            }

            let master_properties = admin_ext.get_broker_config(master_addr.clone()).await.map_err(|e| {
                RocketMQError::Internal(format!(
                    "ExportConfigsSubCommand: Failed to get broker config for {}: {}",
                    master_addr, e
                ))
            })?;

            master_broker_size += 1;
            slave_broker_size += slave_addrs.len() as i64;

            let broker_name = master_properties
                .get(&CheetahString::from("brokerName"))
                .map(|v| v.to_string())
                .unwrap_or_else(|| master_addr.to_string());

            let filtered_properties = Self::need_broker_properties(&master_properties);
            broker_configs.insert(broker_name, filtered_properties);
        }

        // cluster scale
        let mut cluster_scale_map = serde_json::Map::new();
        cluster_scale_map.insert(
            "namesrvSize".to_string(),
            serde_json::Value::Number(serde_json::Number::from(name_servers.len() as i64)),
        );
        cluster_scale_map.insert(
            "masterBrokerSize".to_string(),
            serde_json::Value::Number(serde_json::Number::from(master_broker_size)),
        );
        cluster_scale_map.insert(
            "slaveBrokerSize".to_string(),
            serde_json::Value::Number(serde_json::Number::from(slave_broker_size)),
        );

        result.insert(
            "brokerConfigs".to_string(),
            serde_json::to_value(&broker_configs).map_err(|e| {
                RocketMQError::Internal(format!(
                    "ExportConfigsSubCommand: Failed to serialize broker configs: {}",
                    e
                ))
            })?,
        );
        result.insert("clusterScale".to_string(), serde_json::Value::Object(cluster_scale_map));

        let path = format!("{}/configs.json", file_path);
        let json_content = serde_json::to_string_pretty(&result).map_err(|e| {
            RocketMQError::Internal(format!(
                "ExportConfigsSubCommand: Failed to serialize export result: {}",
                e
            ))
        })?;

        rocketmq_common::FileUtils::string_to_file(&json_content, &path)?;
        println!("export {} success", path);

        Ok(())
    }
}
