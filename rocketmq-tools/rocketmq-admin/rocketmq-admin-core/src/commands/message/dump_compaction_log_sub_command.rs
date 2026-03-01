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

use std::fs;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use clap::Parser;
use rocketmq_common::common::message::message_decoder;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
pub struct DumpCompactionLogSubCommand {
    #[arg(short = 'f', long = "file", required = false, help = "to dump file name")]
    file: Option<String>,
}

impl CommandExecute for DumpCompactionLogSubCommand {
    async fn execute(&self, _rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        if let Some(file_name) = &self.file {
            let file_path = Path::new(file_name);

            if !file_path.exists() {
                return Err(RocketMQError::Internal(format!("file {} not exist.", file_name)));
            }

            if file_path.is_dir() {
                return Err(RocketMQError::Internal(format!("file {} is a directory.", file_name)));
            }

            let data = fs::read(file_name)
                .map_err(|e| RocketMQError::Internal(format!("Failed to read file {}: {}", file_name, e)))?;

            let file_size = data.len();
            let mut buf = Bytes::from(data);
            let mut current = 0usize;

            while current < file_size {
                if buf.len() < 4 {
                    break;
                }

                let size = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);

                if size <= 0 || size as usize > file_size {
                    break;
                }

                if buf.len() < size as usize {
                    break;
                }

                let mut msg_bytes = buf.split_to(size as usize);

                match message_decoder::decode(&mut msg_bytes, false, false, false, false, false) {
                    Some(message_ext) => {
                        current += size as usize;
                        println!("{}", message_ext);
                    }
                    None => {
                        break;
                    }
                }
            }
        } else {
            println!("miss dump log file name");
        }

        Ok(())
    }
}
