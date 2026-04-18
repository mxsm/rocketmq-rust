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
use rocketmq_admin_core::core::message::DumpCompactionLogRequest;
use rocketmq_admin_core::core::message::MessageService;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
pub struct DumpCompactionLogSubCommand {
    #[arg(short = 'f', long = "file", required = false, help = "to dump file name")]
    file: Option<String>,
}

impl DumpCompactionLogSubCommand {
    fn request(&self) -> DumpCompactionLogRequest {
        DumpCompactionLogRequest::try_new(self.file.clone())
    }
}

impl CommandExecute for DumpCompactionLogSubCommand {
    async fn execute(&self, _rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let result = MessageService::dump_compaction_log_by_request(&self.request())?;
        if result.missing_file_name {
            println!("miss dump log file name");
            return Ok(());
        }

        for message in result.messages {
            println!("{}", message);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dump_compaction_log_sub_command_builds_core_request() {
        let command =
            DumpCompactionLogSubCommand::try_parse_from(["dumpCompactionLog", "-f", " ./compact.log "]).unwrap();

        let request = command.request();

        assert_eq!(request.file().unwrap().to_string_lossy(), "./compact.log");
    }
}
