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

#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use rocketmq_rust_fuzz::corpus_bytes;
use rocketmq_store_local::commit_log::record::is_blank_message;
use rocketmq_store_local::commit_log::record::CommitLogFrameCursor;
use rocketmq_store_local::commit_log::record::CommitLogFrameSource;
use rocketmq_store_local::consume_queue::record::ConsumeQueueRecord;
use rocketmq_store_local::index::codec::IndexEntry;
use rocketmq_store_local::index::codec::IndexHeaderRecord;
use rocketmq_store_local::index::codec::IndexSlot;

#[derive(Clone)]
struct CorpusSource(Bytes);

impl CommitLogFrameSource for CorpusSource {
    fn source_len(&self) -> usize {
        self.0.len()
    }

    fn read(&self, offset: usize, len: usize) -> Option<Bytes> {
        let end = offset.checked_add(len)?;
        self.0.get(offset..end).map(Bytes::copy_from_slice)
    }
}

fuzz_target!(|input: &[u8]| {
    if input.len() > 1024 * 1024 {
        return;
    }
    let input = corpus_bytes(input);
    let bytes = Bytes::copy_from_slice(input.as_ref());
    let mut frames = CommitLogFrameCursor::new(CorpusSource(bytes.clone()));
    while let Some((frame, _, _)) = frames.next_message() {
        let _ = is_blank_message(&frame);
    }

    let _ = ConsumeQueueRecord::decode(bytes.as_ref());
    let _ = IndexHeaderRecord::decode(bytes.as_ref());
    let _ = IndexSlot::decode(bytes.as_ref());
    let _ = IndexEntry::decode(bytes.as_ref());
});
