// Copyright 2026 The RocketMQ Rust Authors
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

//! Group commit of the `native` engine.
//!
//! A checkpoint records how far the Timer CQ is indexed, how much of each file holds those
//! sources, how far each lane delivered, and the lowest CommitLog offset a pending timer or an
//! unindexed source still needs. Nothing on the indexing path is synced; a crash costs the
//! replay of the sources after the last checkpoint, and delivers again at most what the lanes
//! delivered since it.

use rocketmq_store_local::timer::native_format::NativeCheckpoint;

use super::NativeEngineError;
use super::NativeTimerEngine;

impl NativeTimerEngine {
    /// Makes every timer and recall of the sources before the frontier durable, and records
    /// them in a checkpoint along with the lane watermarks and the CommitLog fence. Returns the
    /// checkpoint written.
    ///
    /// `source_fence` is the CommitLog offset of the first source the engine has not indexed,
    /// or the end of the CommitLog when it has indexed every source. The checkpoint's fence is
    /// the lowest of it and of the offsets the hour buckets point at: CommitLog cleanup must
    /// keep every file from there on.
    ///
    /// # Errors
    ///
    /// Returns a storage error when the files cannot be synced or the checkpoint written; the
    /// last checkpoint then stays the one to recover from.
    pub(crate) fn checkpoint(&self, source_fence: i64, now_ms: i64) -> Result<NativeCheckpoint, NativeEngineError> {
        let _commit = self.commit.lock();
        // Indexing holds the state lock, so the frontier and the written lengths taken together
        // describe the same sources: the checkpoint never keeps bytes of a source it would
        // also replay.
        let (source_cq_offset, mut lengths, lane_watermarks, commit_log_fence, retired) = {
            let state = self.state.lock();
            let fence = state
                .bucket_min_offsets
                .values()
                .copied()
                .chain(state.opened_fence)
                .fold(source_fence, i64::min);
            (
                state.source_frontier,
                self.storage.written_lengths(),
                state.watermarks.clone(),
                fence,
                state.retired.clone(),
            )
        };
        // A split hour's timers are in its minute runs, which this checkpoint names.
        lengths.buckets.retain(|bucket| !retired.contains(&bucket.bucket));
        self.storage.sync()?;
        let checkpoint = NativeCheckpoint {
            sequence: self.storage.last_checkpoint_sequence() + 1,
            written_ms: now_ms,
            source_cq_offset,
            commit_log_fence,
            cancel_log_length: lengths.cancel_log_length,
            lane_watermarks,
            buckets: lengths.buckets,
            payload_segments: lengths.payload_segments,
        };
        self.storage.write_checkpoint(&checkpoint)?;
        // No checkpoint names the retired hour buckets any more. One left behind by a failed
        // delete is deleted after the next checkpoint, or by recovery.
        let mut state = self.state.lock();
        for hour in retired {
            self.storage.delete_bucket(hour)?;
            state.retired.remove(&hour);
        }
        Ok(checkpoint)
    }
}
