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

use clap::ArgGroup;
use clap::Parser;
use rocketmq_common::utils::util_all::time_millis_to_human_string2;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;
use serde_json::Value;

use crate::commands::CommandExecute;
use rocketmq_admin_core::core::broker::BrokerConfigSectionTarget;
use rocketmq_admin_core::core::broker::BrokerService;
use rocketmq_admin_core::core::broker::ColdDataFlowCtrInfoQueryRequest;
use rocketmq_admin_core::core::broker::ColdDataFlowCtrInfoSection;

#[derive(Debug, Clone, Parser)]
#[command(group(ArgGroup::new("target")
    .required(true)
    .args(&["broker_addr", "cluster_name"]))
)]
pub struct GetColdDataFlowCtrInfoSubCommand {
    #[arg(short = 'b', long = "brokerAddr", required = false, help = "get from which broker")]
    broker_addr: Option<String>,

    #[arg(short = 'c', long = "clusterName", required = false, help = "get from which cluster")]
    cluster_name: Option<String>,
}

impl GetColdDataFlowCtrInfoSubCommand {
    fn request(&self) -> RocketMQResult<ColdDataFlowCtrInfoQueryRequest> {
        ColdDataFlowCtrInfoQueryRequest::try_new(self.broker_addr.clone(), self.cluster_name.clone())
    }

    fn print_section(section: ColdDataFlowCtrInfoSection) -> RocketMQResult<()> {
        print!(" {}", section_prefix(&section.target));

        if section.raw_info.is_empty() {
            println!("Broker[{}] has no cold ctr table !", section.broker_addr);
            return Ok(());
        }

        let mut json_value: Value = serde_json::from_str(section.raw_info.as_str()).map_err(|e| {
            RocketMQError::Internal(format!(
                "GetColdDataFlowCtrInfoSubCommand: Failed to parse JSON response from broker {}: {}",
                section.broker_addr, e
            ))
        })?;

        if let Some(runtime_table) = json_value.get_mut("runtimeTable") {
            if let Some(table_obj) = runtime_table.as_object_mut() {
                for (_key, entry) in table_obj.iter_mut() {
                    if let Some(entry_obj) = entry.as_object_mut() {
                        if let Some(last_cold_read_time) = entry_obj.remove("lastColdReadTimeMills") {
                            let millis = match &last_cold_read_time {
                                Value::Number(n) => n.as_i64().unwrap_or(0),
                                Value::String(s) => s.parse::<i64>().unwrap_or(0),
                                _ => 0,
                            };
                            entry_obj.insert(
                                "lastColdReadTimeFormat".to_string(),
                                Value::String(time_millis_to_human_string2(millis)),
                            );
                        }

                        if let Some(create_time) = entry_obj.remove("createTimeMills") {
                            let millis = match &create_time {
                                Value::Number(n) => n.as_i64().unwrap_or(0),
                                Value::String(s) => s.parse::<i64>().unwrap_or(0),
                                _ => 0,
                            };
                            entry_obj.insert(
                                "createTimeFormat".to_string(),
                                Value::String(time_millis_to_human_string2(millis)),
                            );
                        }
                    }
                }
            }
        }

        let format_str = serde_json::to_string_pretty(&json_value).map_err(|e| {
            RocketMQError::Internal(format!("GetColdDataFlowCtrInfoSubCommand: Failed to format JSON: {e}"))
        })?;
        println!("{format_str}");
        Ok(())
    }
}

impl CommandExecute for GetColdDataFlowCtrInfoSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let result =
            BrokerService::query_cold_data_flow_ctr_info_by_request_with_rpc_hook(self.request()?, rpc_hook).await?;
        for section in result.sections {
            Self::print_section(section)?;
        }
        Ok(())
    }
}

fn section_prefix(target: &BrokerConfigSectionTarget) -> String {
    match target {
        BrokerConfigSectionTarget::Broker(addr) => format!("============{}============\n", addr),
        BrokerConfigSectionTarget::Master(master_addr) => format!("============Master: {}============\n", master_addr),
        BrokerConfigSectionTarget::Slave {
            master_addr,
            slave_addr,
        } => format!(
            "============My Master: {}=====Slave: {}============\n",
            master_addr, slave_addr
        ),
        BrokerConfigSectionTarget::NoMaster => "============NO_MASTER============\n".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn parses_get_cold_data_flow_ctr_info_request() {
        let command =
            GetColdDataFlowCtrInfoSubCommand::try_parse_from(["getColdDataFlowCtrInfo", "-b", " 127.0.0.1:10911 "])
                .unwrap();
        let request = command.request().unwrap();

        assert!(matches!(
            request.target(),
            rocketmq_admin_core::core::broker::BrokerTarget::BrokerAddr(addr)
                if addr.as_str() == "127.0.0.1:10911"
        ));
    }
}
