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
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::core::broker::BrokerOperationResult;
use rocketmq_admin_core::core::broker::BrokerService;
use rocketmq_admin_core::core::broker::SwitchTimerEngineRequest;

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

impl SwitchTimerEngineSubCommand {
    fn request(&self) -> RocketMQResult<SwitchTimerEngineRequest> {
        SwitchTimerEngineRequest::try_new(
            self.broker_addr.clone(),
            self.cluster_name.clone(),
            self.engine_type.clone(),
        )
    }
}

impl CommandExecute for SwitchTimerEngineSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let request = match self.request() {
            Ok(request) => request,
            Err(error) if error.to_string().contains("engineType") => {
                println!("switchTimerEngine engineType must be R or F");
                return Ok(());
            }
            Err(error) => return Err(error),
        };

        let engine_name = request.engine_name().clone();
        let result = BrokerService::switch_timer_engine_by_request_with_rpc_hook(request, rpc_hook).await?;
        print_switch_timer_engine_result(engine_name.as_str(), &result);
        Ok(())
    }
}

fn print_switch_timer_engine_result(engine_name: &str, result: &BrokerOperationResult) {
    for broker_addr in &result.broker_addrs {
        println!("switchTimerEngine to {} success, {}", engine_name, broker_addr);
    }
    for failure in &result.failures {
        eprintln!(
            "switchTimerEngine to {} failed, {}: {}",
            engine_name, failure.broker_addr, failure.error
        );
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use rocketmq_admin_core::core::broker::BrokerTarget;

    use super::*;

    #[test]
    fn parses_switch_timer_engine_request() {
        let cmd =
            SwitchTimerEngineSubCommand::try_parse_from(["switchTimerEngine", "-c", " DefaultCluster ", "-e", "R"])
                .unwrap();
        let request = cmd.request().unwrap();

        assert!(matches!(
            request.target(),
            BrokerTarget::ClusterName(cluster) if cluster.as_str() == "DefaultCluster"
        ));
        assert_eq!(request.engine_type().as_str(), "R");
        assert_eq!(request.engine_name().as_str(), "ROCKSDB_TIMELINE");
    }

    #[test]
    fn rejects_invalid_switch_timer_engine_type() {
        let cmd =
            SwitchTimerEngineSubCommand::try_parse_from(["switchTimerEngine", "-b", "127.0.0.1:10911", "-e", "bad"])
                .unwrap();

        assert!(cmd.request().is_err());
    }
}
