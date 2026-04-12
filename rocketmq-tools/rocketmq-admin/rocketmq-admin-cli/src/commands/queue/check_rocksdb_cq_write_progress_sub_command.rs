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
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::check_rocksdb_cqwrite_progress_response_body::CheckStatus;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::core::queue::CheckRocksdbCqWriteProgressRequest;
use rocketmq_admin_core::core::queue::CheckRocksdbCqWriteProgressResult;
use rocketmq_admin_core::core::queue::QueueService;

#[derive(Debug, Clone, Parser)]
pub struct CheckRocksdbCqWriteProgressSubCommand {
    #[arg(short = 'c', long = "cluster", required = true, help = "Cluster name")]
    cluster_name: String,

    #[arg(short = 'n', long = "nameserverAddr", required = true, help = "nameserver address")]
    namesrv_addr: String,

    #[arg(short = 't', long = "topic", help = "topic name")]
    topic: Option<String>,

    #[arg(long = "checkFrom", help = "check from time")]
    check_from: Option<i64>,
}

impl CheckRocksdbCqWriteProgressSubCommand {
    fn request(&self) -> RocketMQResult<CheckRocksdbCqWriteProgressRequest> {
        CheckRocksdbCqWriteProgressRequest::try_new(
            self.cluster_name.clone(),
            self.namesrv_addr.clone(),
            self.topic.clone(),
            self.check_from,
        )
    }
}

impl CommandExecute for CheckRocksdbCqWriteProgressSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let result =
            QueueService::check_rocksdb_cq_write_progress_by_request_with_rpc_hook(self.request()?, rpc_hook).await?;
        print_check_rocksdb_cq_write_progress_result(&result);
        Ok(())
    }
}

fn print_check_rocksdb_cq_write_progress_result(result: &CheckRocksdbCqWriteProgressResult) {
    if !result.cluster_found {
        println!("clusterAddrTable is empty");
        return;
    }

    for entry in &result.entries {
        let check_status = entry.result.get_check_status();
        let check_result = entry.result.check_result.as_deref().unwrap_or("");
        if check_status == CheckStatus::CheckError {
            println!(
                "{} check error, please check log... errInfo: {}",
                entry.broker_name, check_result
            );
        } else {
            println!(
                "{} check doing, please wait and get the result from log...",
                entry.broker_name
            );
        }
    }

    for failure in &result.failures {
        println!("{} check error: {}", failure.broker_name, failure.error);
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn parses_check_rocksdb_cq_write_progress_request() {
        let cmd = CheckRocksdbCqWriteProgressSubCommand::try_parse_from([
            "checkRocksdbCqWriteProgress",
            "-c",
            " DefaultCluster ",
            "-n",
            " 127.0.0.1:9876 ",
            "-t",
            " TestTopic ",
            "--checkFrom",
            "1024",
        ])
        .unwrap();
        let request = cmd.request().unwrap();

        assert_eq!(request.cluster_name().as_str(), "DefaultCluster");
        assert_eq!(request.namesrv_addr(), "127.0.0.1:9876");
        assert_eq!(request.topic().as_str(), "TestTopic");
        assert_eq!(request.check_store_time(), 1024);
    }
}
