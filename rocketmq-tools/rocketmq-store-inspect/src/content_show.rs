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

use std::borrow::Cow;
use std::fs;
use std::path::PathBuf;

use bytes::Buf;
use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_store::inspection::decode_commit_log_record;
use rocketmq_store::inspection::CommitLogRecord;
use rocketmq_store::inspection::CommitLogRecordBodyMode;
use rocketmq_store::inspection::CommitLogRecordChecksum;
use rocketmq_store::inspection::CommitLogRecordOutcome;
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
        let Some(msg_bytes) = mapped_file.get_bytes(current_pos, size as usize) else {
            break;
        };
        current_pos += size as usize;
        if let Ok(CommitLogRecordOutcome::Message(record)) =
            decode_commit_log_record(&msg_bytes, CommitLogRecordBodyMode::Skip, &InspectionChecksum)
        {
            table.push(message_print(&record));
        }
    }
    println!("{}", Table::new(table));
    Ok(())
}

const UNIQUE_CLIENT_MESSAGE_ID: &[u8] = b"UNIQ_KEY";

struct InspectionChecksum;

impl CommitLogRecordChecksum for InspectionChecksum {
    fn checksum(&self, _bytes: &[u8]) -> u32 {
        0
    }
}

fn message_print(record: &CommitLogRecord) -> MessagePrint {
    MessagePrint {
        message_id: CheetahString::from_string(build_message_id(&record.store_host, record.physical_offset)),
        client_message_id: property_value(record.properties.as_ref(), UNIQUE_CLIENT_MESSAGE_ID)
            .map(cheetah_from_utf8_lossy)
            .unwrap_or_default(),
    }
}

fn build_message_id(store_host: &Bytes, physical_offset: i64) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";

    let mut encoded = String::with_capacity((store_host.len() + 8) * 2);
    for byte in store_host.iter().chain(physical_offset.to_be_bytes().iter()) {
        encoded.push(HEX[usize::from(*byte >> 4)] as char);
        encoded.push(HEX[usize::from(*byte & 0x0F)] as char);
    }
    encoded
}

fn property_value<'a>(properties: &'a [u8], name: &[u8]) -> Option<&'a [u8]> {
    properties.split(|byte| *byte == 2).find_map(|entry| {
        let separator = entry.iter().position(|byte| *byte == 1)?;
        (entry[..separator] == *name).then_some(&entry[separator + 1..])
    })
}

fn cheetah_from_utf8_lossy(bytes: &[u8]) -> CheetahString {
    match String::from_utf8_lossy(bytes) {
        Cow::Borrowed(value) => CheetahString::from_slice(value),
        Cow::Owned(value) => CheetahString::from_string_owned(value),
    }
}

#[derive(Tabled)]
struct MessagePrint {
    message_id: CheetahString,
    client_message_id: CheetahString,
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use rocketmq_error::CliExitCode;

    use super::build_message_id;
    use super::print_content;
    use super::property_value;

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

    #[test]
    fn canonical_inspection_helpers_preserve_legacy_ids_and_properties() {
        let host = Bytes::from_static(&[127, 0, 0, 1, 0, 0, 42, 159]);
        assert_eq!(build_message_id(&host, 13), "7F00000100002A9F000000000000000D");

        let properties = b"TAGS\x01TagA\x02UNIQ_KEY\x01client-1\x02";
        assert_eq!(property_value(properties, b"UNIQ_KEY"), Some(&b"client-1"[..]));
    }
}
