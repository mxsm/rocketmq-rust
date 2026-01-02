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
use rocketmq_store::log_file::mapped_file::default_mapped_file_impl::DefaultMappedFile;
use rocketmq_store::log_file::mapped_file::MappedFile;
use tabled::Table;
use tabled::Tabled;

pub fn print_content(from: Option<u32>, to: Option<u32>, path: Option<PathBuf>) {
    if path.is_none() {
        eprintln!("File path is none");
        return;
    }
    let path_buf = path.unwrap().into_os_string();
    let file_metadata = fs::metadata(path_buf.clone()).unwrap();
    println!("file size: {}B", file_metadata.len());
    let mapped_file = DefaultMappedFile::new(
        CheetahString::from(path_buf.to_string_lossy().to_string()),
        file_metadata.len(),
    );
    // read message number
    let mut counter = 0;
    let form = from.unwrap_or_default();
    let to = to.unwrap_or(u32::MAX);
    let mut current_pos = 0usize;
    let mut table = vec![];
    loop {
        if counter >= to {
            break;
        }
        let bytes = mapped_file.get_bytes(current_pos, 4);
        if bytes.is_none() {
            break;
        }
        let mut size_bytes = bytes.unwrap();
        let size = size_bytes.get_i32();
        if size <= 0 {
            break;
        }
        counter += 1;
        if counter < form {
            current_pos += size as usize;
            continue;
        }
        let mut msg_bytes = mapped_file.get_bytes(current_pos, size as usize);
        current_pos += size as usize;
        if msg_bytes.is_none() {
            break;
        }
        let message = message_decoder::decode(msg_bytes.as_mut().unwrap(), true, false, false, false, true);
        //parse message bytes and print it
        match message {
            None => {}
            Some(value) => {
                table.push(MessagePrint {
                    message_id: value.msg_id,
                    client_message_id: value
                        .message
                        .get_property(&CheetahString::from_static_str(
                            MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
                        ))
                        .unwrap_or(CheetahString::empty()),
                });
            }
        }
    }
    println!("{}", Table::new(table));
}

#[derive(Tabled)]
struct MessagePrint {
    message_id: CheetahString,
    client_message_id: CheetahString,
}
