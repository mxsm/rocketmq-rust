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
use std::path::PathBuf;

use bytes::Buf;
use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_decoder;
use rocketmq_common::common::message::MessageConst;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_store::log_file::mapped_file::default_mapped_file_impl::DefaultMappedFile;
use rocketmq_store::log_file::mapped_file::MappedFile;
use tabled::Table;
use tabled::Tabled;

pub fn print_content(from: Option<u32>, to: Option<u32>, path: Option<PathBuf>) -> RocketMQResult<()> {
    let path =
        path.ok_or_else(|| RocketMQError::validation_failed("config", "message log file path must be provided"))?;
    let from = from.unwrap_or_default();
    let to = to.unwrap_or(u32::MAX);
    if from > to {
        return Err(RocketMQError::validation_failed(
            "range",
            format!("from ({from}) must be less than or equal to to ({to})"),
        ));
    }

    let path_display = path.to_string_lossy().to_string();
    let file_metadata = fs::metadata(&path)
        .map_err(|error| RocketMQError::storage_read_failed(path_display.clone(), error.to_string()))?;
    println!("file size: {}B", file_metadata.len());
    let mapped_file = DefaultMappedFile::new(CheetahString::from(path_display), file_metadata.len());
    // read message number
    let mut counter = 0;
    let mut current_pos = 0usize;
    let mut table = vec![];
    loop {
        if counter >= to {
            break;
        }
        let Some(mut size_bytes) = mapped_file.get_bytes(current_pos, 4) else {
            break;
        };
        let size = size_bytes.get_i32();
        if size <= 0 {
            break;
        }
        counter += 1;
        if counter < from {
            current_pos += size as usize;
            continue;
        }
        let Some(mut msg_bytes) = mapped_file.get_bytes(current_pos, size as usize) else {
            break;
        };
        current_pos += size as usize;
        let message = message_decoder::decode(&mut msg_bytes, true, false, false, false, true);
        //parse message bytes and print it
        match message {
            None => {}
            Some(value) => {
                table.push(MessagePrint {
                    message_id: value.msg_id,
                    client_message_id: value
                        .message
                        .property(&CheetahString::from_static_str(
                            MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
                        ))
                        .map(Into::into)
                        .unwrap_or_else(CheetahString::new),
                });
            }
        }
    }
    println!("{}", Table::new(table));
    Ok(())
}

#[derive(Tabled)]
struct MessagePrint {
    message_id: CheetahString,
    client_message_id: CheetahString,
}

#[cfg(test)]
mod tests {
    use super::print_content;
    use rocketmq_error::CliExitCode;

    #[test]
    fn print_content_requires_path() {
        let error = print_content(None, None, None).unwrap_err();

        assert_eq!(error.spec().cli.exit_code, CliExitCode::USAGE);
        assert_eq!(error.spec().code.as_str(), "ILLEGAL_ARGUMENT");
    }

    #[test]
    fn print_content_rejects_invalid_range() {
        let error = print_content(Some(2), Some(1), Some("missing.log".into())).unwrap_err();

        assert_eq!(error.spec().cli.exit_code, CliExitCode::USAGE);
        assert_eq!(error.spec().code.as_str(), "ILLEGAL_ARGUMENT");
    }
}
