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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;
use rocketmq_store_local::commit_log::append::AppendMessageResult;
use rocketmq_store_local::commit_log::append::AppendMessageStatus;
use rocketmq_store_local::commit_log::append::CompactionAppendMsgCallback;
use rocketmq_store_local::commit_log::append::PutMessageContext;
use rocketmq_store_local::config::FlushDiskType;

#[test]
fn append_message_status_preserves_default_and_java_vocabulary() {
    assert_eq!(AppendMessageStatus::PutOk, AppendMessageStatus::default());
    assert_eq!("PUT_OK", AppendMessageStatus::PutOk.to_string());
    assert_eq!("END_OF_FILE", AppendMessageStatus::EndOfFile.to_string());
    assert_eq!(
        "MESSAGE_SIZE_EXCEEDED",
        AppendMessageStatus::MessageSizeExceeded.to_string()
    );
    assert_eq!(
        "PROPERTIES_SIZE_EXCEEDED",
        AppendMessageStatus::PropertiesSizeExceeded.to_string()
    );
    assert_eq!("UNKNOWN_ERROR", AppendMessageStatus::UnknownError.to_string());
}

#[test]
fn append_message_result_preserves_fields_default_display_and_supplier() {
    let default = AppendMessageResult::default();
    assert_eq!(AppendMessageStatus::UnknownError, default.status);
    assert_eq!(0, default.wrote_offset);
    assert_eq!(0, default.wrote_bytes);
    assert_eq!(None, default.msg_id);
    assert!(default.msg_id_supplier.is_none());
    assert_eq!(0, default.store_timestamp);
    assert_eq!(0, default.logics_offset);
    assert_eq!(0, default.page_cache_rt);
    assert_eq!(1, default.msg_num);
    assert!(!default.is_ok());

    let calls = Arc::new(AtomicUsize::new(0));
    let supplier_calls = Arc::clone(&calls);
    let result = AppendMessageResult {
        status: AppendMessageStatus::PutOk,
        wrote_offset: 41,
        wrote_bytes: 13,
        msg_id: Some("eager-id".to_string()),
        msg_id_supplier: Some(Arc::new(move || {
            supplier_calls.fetch_add(1, Ordering::Relaxed);
            "lazy-id".to_string()
        })),
        store_timestamp: 123,
        logics_offset: 7,
        page_cache_rt: 9,
        msg_num: 2,
    };
    assert!(result.is_ok());
    assert_eq!(Some("lazy-id".to_string()), result.get_message_id());
    assert_eq!(1, calls.load(Ordering::Relaxed));
    assert_eq!(
        "AppendMessageResult [status=PutOk, wrote_offset=41, wrote_bytes=13, msg_id=Some(\"eager-id\"), \
         store_timestamp=123, logics_offset=7, page_cache_rt=9, msg_num=2]",
        result.to_string()
    );

    let direct = AppendMessageResult {
        msg_id: Some("direct-id".to_string()),
        ..AppendMessageResult::default()
    };
    assert_eq!(Some("direct-id".to_string()), direct.get_message_id());
}

#[test]
fn put_message_context_preserves_owned_key_and_slice_accessors() {
    let mut context = PutMessageContext::new("topic-queue".to_string());
    assert_eq!("topic-queue", context.get_topic_queue_table_key());
    assert_eq!(&[] as &[i64], context.get_phy_pos());
    assert_eq!(0, context.get_batch_size());

    context.set_phy_pos(vec![3, 5]);
    context.get_phy_pos_mut()[1] = 8;
    context.set_batch_size(2);
    context.set_topic_queue_table_key("other-queue".to_string());

    assert_eq!("other-queue", context.get_topic_queue_table_key());
    assert_eq!(&[3, 8], context.get_phy_pos());
    assert_eq!(2, context.get_batch_size());
}

struct CopyCompactionCallback;

impl CompactionAppendMsgCallback for CopyCompactionCallback {
    fn do_append(
        &self,
        bb_dest: &mut Bytes,
        file_from_offset: i64,
        max_blank: i32,
        bb_src: &mut Bytes,
    ) -> AppendMessageResult {
        *bb_dest = bb_src.clone();
        AppendMessageResult {
            status: AppendMessageStatus::PutOk,
            wrote_offset: file_from_offset,
            wrote_bytes: max_blank,
            ..AppendMessageResult::default()
        }
    }
}

#[test]
fn compaction_callback_preserves_object_safe_slice_contract() {
    let callback: &dyn CompactionAppendMsgCallback = &CopyCompactionCallback;
    let mut destination = Bytes::new();
    let mut source = Bytes::from_static(b"record");

    let result = callback.do_append(&mut destination, 64, 6, &mut source);

    assert_eq!(Bytes::from_static(b"record"), destination);
    assert_eq!(64, result.wrote_offset);
    assert_eq!(6, result.wrote_bytes);
}

#[test]
fn flush_disk_type_preserves_default_vocabulary_and_deserialization() {
    assert_eq!(FlushDiskType::AsyncFlush, FlushDiskType::default());
    assert_eq!("SYNC_FLUSH", FlushDiskType::SyncFlush.get_flush_disk_type());
    assert_eq!("ASYNC_FLUSH", FlushDiskType::AsyncFlush.get_flush_disk_type());

    for value in ["SYNC_FLUSH", "SyncFlush"] {
        assert_eq!(
            FlushDiskType::SyncFlush,
            serde_json::from_str::<FlushDiskType>(&format!("\"{value}\"")).expect("valid sync flush vocabulary")
        );
    }
    for value in ["ASYNC_FLUSH", "AsyncFlush"] {
        assert_eq!(
            FlushDiskType::AsyncFlush,
            serde_json::from_str::<FlushDiskType>(&format!("\"{value}\"")).expect("valid async flush vocabulary")
        );
    }
    for value in ["INVALID", "sync_flush", ""] {
        assert!(serde_json::from_str::<FlushDiskType>(&format!("\"{value}\"")).is_err());
    }

    let copied = FlushDiskType::SyncFlush;
    let cloned = copied;
    assert_eq!(copied, cloned);
    assert_eq!("SyncFlush", format!("{copied:?}"));
}
