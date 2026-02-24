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
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::command_util::CommandUtil;
use crate::commands::CommandExecute;

const ROCKSDB_TIMELINE: &str = "ROCKSDB_TIMELINE";
const FILE_TIME_WHEEL: &str = "FILE_TIME_WHEEL";

#[derive(Debug, Clone, Parser)]
#[command(group(ArgGroup::new("target")
    .required(true)
    .args(&["broker_addr", "cluster_name"]))
)]
pub struct SwitchTimerEngineSubCommand {
    #[arg(short = 'b', long = "brokerAddr", required = false, help = "update which broker")]
    broker_addr: Option<String>,

    #[arg(short = 'c', long = "clusterName", required = false, help = "update which cluster")]
    cluster_name: Option<String>,

    #[arg(
        short = 'e',
        long = "engineType",
        required = true,
        help = "R/F, R for rocksdb timeline engine, F for file time wheel engine"
    )]
    engine_type: String,
}

impl CommandExecute for SwitchTimerEngineSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let mut default_mqadmin_ext = if let Some(rpc_hook) = rpc_hook {
            DefaultMQAdminExt::with_rpc_hook(rpc_hook)
        } else {
            DefaultMQAdminExt::new()
        };
        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(get_current_millis().to_string().into());

        let engine_type = self.engine_type.trim().to_string();
        if engine_type.is_empty()
            || (engine_type != MessageConst::TIMER_ENGINE_ROCKSDB_TIMELINE
                && engine_type != MessageConst::TIMER_ENGINE_FILE_TIME_WHEEL)
        {
            println!("switchTimerEngine engineType must be R or F");
            return Ok(());
        }

        let engine_name = if engine_type == MessageConst::TIMER_ENGINE_ROCKSDB_TIMELINE {
            ROCKSDB_TIMELINE
        } else {
            FILE_TIME_WHEEL
        };

        MQAdminExt::start(&mut default_mqadmin_ext).await.map_err(|e| {
            RocketMQError::Internal(format!(
                "SwitchTimerEngineSubCommand: Failed to start MQAdminExt: {}",
                e
            ))
        })?;

        let operation_result = switch_timer_engine(&default_mqadmin_ext, &engine_type, engine_name, self).await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}

async fn switch_timer_engine(
    default_mqadmin_ext: &DefaultMQAdminExt,
    engine_type: &str,
    engine_name: &str,
    command: &SwitchTimerEngineSubCommand,
) -> RocketMQResult<()> {
    if let Some(ref broker_addr) = command.broker_addr {
        let broker_addr = broker_addr.trim();
        default_mqadmin_ext
            .switch_timer_engine(broker_addr.into(), engine_type.into())
            .await?;
        println!("switchTimerEngine to {} success, {}", engine_name, broker_addr);
    } else if let Some(ref cluster_name) = command.cluster_name {
        let cluster_name = cluster_name.trim();
        let cluster_info = default_mqadmin_ext.examine_broker_cluster_info().await?;
        let master_set = CommandUtil::fetch_master_addr_by_cluster_name(&cluster_info, cluster_name)?;
        for broker_addr in master_set {
            match default_mqadmin_ext
                .switch_timer_engine(broker_addr.clone(), engine_type.into())
                .await
            {
                Ok(()) => {
                    println!("switchTimerEngine to {} success, {}", engine_name, broker_addr);
                }
                Err(e) => {
                    eprintln!("switchTimerEngine to {} failed, {}: {}", engine_name, broker_addr, e);
                }
            }
        }
    }
    Ok(())
}
