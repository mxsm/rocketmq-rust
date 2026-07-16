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

//! Backend-neutral IndexService composition, lifecycle, query, and build control.

use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;

/// Number of attempts retained by the legacy index-file creation policy.
pub const MAX_TRY_INDEX_FILE_CREATE: usize = 3;

/// Canonical owner of one Store-facing IndexService adapter.
#[derive(Clone, Debug)]
pub struct IndexServiceRoot<A> {
    adapter: A,
}

impl<A> IndexServiceRoot<A> {
    pub fn new(adapter: A) -> Self {
        Self { adapter }
    }

    pub fn adapter(&self) -> &A {
        &self.adapter
    }

    pub fn adapter_mut(&mut self) -> &mut A {
        &mut self.adapter
    }

    pub fn into_adapter(self) -> A {
        self.adapter
    }
}

impl<A> Deref for IndexServiceRoot<A> {
    type Target = A;

    fn deref(&self) -> &Self::Target {
        self.adapter()
    }
}

impl<A> DerefMut for IndexServiceRoot<A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.adapter_mut()
    }
}

/// Backend-neutral file operations needed by IndexService algorithms.
pub trait IndexServiceFile {
    fn file_size(&self) -> usize;
    fn has_entries(&self) -> bool;
    fn is_write_full(&self) -> bool;
    fn begin_timestamp(&self) -> i64;
    fn end_timestamp(&self) -> i64;
    fn end_phy_offset(&self) -> i64;
    fn is_time_matched(&self, begin: i64, end: i64) -> bool;
    fn select_phy_offsets(&self, offsets: &mut Vec<i64>, key: &str, max_num: usize, begin: i64, end: i64);
}

impl<F: IndexServiceFile> IndexServiceFile for Arc<F> {
    fn file_size(&self) -> usize {
        (**self).file_size()
    }

    fn has_entries(&self) -> bool {
        (**self).has_entries()
    }

    fn is_write_full(&self) -> bool {
        (**self).is_write_full()
    }

    fn begin_timestamp(&self) -> i64 {
        (**self).begin_timestamp()
    }

    fn end_timestamp(&self) -> i64 {
        (**self).end_timestamp()
    }

    fn end_phy_offset(&self) -> i64 {
        (**self).end_phy_offset()
    }

    fn is_time_matched(&self, begin: i64, end: i64) -> bool {
        (**self).is_time_matched(begin, end)
    }

    fn select_phy_offsets(&self, offsets: &mut Vec<i64>, key: &str, max_num: usize, begin: i64, end: i64) {
        (**self).select_phy_offsets(offsets, key, max_num, begin, end);
    }
}

/// Canonical result returned by an IndexService query.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QueryOffsetResult {
    phy_offsets: Vec<i64>,
    index_last_update_timestamp: i64,
    index_last_update_phyoffset: i64,
}

impl QueryOffsetResult {
    pub fn new(phy_offsets: Vec<i64>, index_last_update_timestamp: i64, index_last_update_phyoffset: i64) -> Self {
        Self {
            phy_offsets,
            index_last_update_timestamp,
            index_last_update_phyoffset,
        }
    }

    #[inline]
    pub fn get_phy_offsets(&self) -> &Vec<i64> {
        &self.phy_offsets
    }

    #[inline]
    pub fn get_phy_offsets_mut(&mut self) -> &mut Vec<i64> {
        &mut self.phy_offsets
    }

    #[inline]
    pub const fn get_index_last_update_timestamp(&self) -> i64 {
        self.index_last_update_timestamp
    }

    #[inline]
    pub const fn get_index_last_update_phyoffset(&self) -> i64 {
        self.index_last_update_phyoffset
    }
}

/// Queries newest-to-oldest files while preserving the legacy stop conditions and metadata.
pub fn query_index_files<F: IndexServiceFile>(
    files: &[F],
    key: &str,
    max_num: usize,
    begin: i64,
    end: i64,
) -> QueryOffsetResult {
    if max_num == 0 {
        return QueryOffsetResult::default();
    }

    let mut offsets = Vec::with_capacity(max_num);
    let mut last_update_timestamp = 0;
    let mut last_update_phy_offset = 0;
    for (reverse_index, file) in files.iter().rev().enumerate() {
        if reverse_index == 0 {
            last_update_timestamp = file.end_timestamp();
            last_update_phy_offset = file.end_phy_offset();
        }
        if file.is_time_matched(begin, end) {
            file.select_phy_offsets(&mut offsets, key, max_num, begin, end);
        }
        if file.begin_timestamp() < begin || offsets.len() >= max_num {
            break;
        }
    }
    QueryOffsetResult::new(offsets, last_update_timestamp, last_update_phy_offset)
}

/// Returns the legacy aggregate IndexFile size.
pub fn total_index_file_size<F: IndexServiceFile>(files: &[F]) -> u64 {
    files.first().map_or(0, |file| (file.file_size() * files.len()) as u64)
}

/// Returns the newest non-empty file's dispatch progress.
pub fn max_index_dispatch_offset<F: IndexServiceFile>(files: &[F]) -> Option<i64> {
    files
        .iter()
        .rev()
        .find(|file| file.has_entries())
        .map(IndexServiceFile::end_phy_offset)
}

/// Applies shutdown to every file before clearing the owned list.
pub fn shutdown_index_files<F, Shutdown>(files: &mut Vec<F>, mut shutdown: Shutdown)
where
    Shutdown: FnMut(&F),
{
    for file in files.iter() {
        shutdown(file);
    }
    files.clear();
}

/// Applies destroy to every file before clearing the owned list.
pub fn destroy_index_files<F, Destroy>(files: &mut Vec<F>, mut destroy: Destroy)
where
    Destroy: FnMut(&F),
{
    for file in files.iter() {
        destroy(file);
    }
    files.clear();
}

/// Returns how many oldest files are expired while always retaining the newest file.
pub fn expired_index_file_count<F: IndexServiceFile>(files: &[F], offset: u64) -> usize {
    if files.is_empty() || files[0].end_phy_offset() as u64 >= offset {
        return 0;
    }
    files
        .iter()
        .take(files.len() - 1)
        .take_while(|file| (file.end_phy_offset() as u64) < offset)
        .count()
}

/// Selects the safe offset represented by retained files and a persisted checkpoint.
pub fn restore_index_safe_offset<F: IndexServiceFile>(
    files: &[F],
    persisted_safe_offset: u64,
    removed_unsafe_file: bool,
) -> u64 {
    let loaded_safe_offset = files
        .iter()
        .rev()
        .find(|file| file.has_entries())
        .map(|file| file.end_phy_offset().max(0) as u64)
        .unwrap_or(0);
    if removed_unsafe_file || loaded_safe_offset == 0 {
        loaded_safe_offset
    } else {
        persisted_safe_offset.max(loaded_safe_offset)
    }
}

/// Returns whether an unclean restart must discard a file newer than the durable checkpoint.
pub const fn should_remove_unsafe_index_file(last_exit_ok: bool, end_timestamp: i64, checkpoint: i64) -> bool {
    !last_exit_ok && end_timestamp > checkpoint
}

/// Reuse-or-create decision for the current tail file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexFileCreationSeed<F> {
    pub reusable: Option<F>,
    pub previous_full: Option<F>,
    pub previous_end_phy_offset: i64,
    pub previous_end_timestamp: i64,
}

/// Plans tail reuse or creation without holding a Store write lock.
pub fn plan_last_index_file<F>(files: &[F]) -> IndexFileCreationSeed<F>
where
    F: Clone + IndexServiceFile,
{
    let Some(last) = files.last() else {
        return IndexFileCreationSeed {
            reusable: None,
            previous_full: None,
            previous_end_phy_offset: 0,
            previous_end_timestamp: 0,
        };
    };
    if !last.is_write_full() {
        return IndexFileCreationSeed {
            reusable: Some(last.clone()),
            previous_full: None,
            previous_end_phy_offset: 0,
            previous_end_timestamp: 0,
        };
    }
    IndexFileCreationSeed {
        reusable: None,
        previous_full: Some(last.clone()),
        previous_end_phy_offset: last.end_phy_offset(),
        previous_end_timestamp: last.end_timestamp(),
    }
}

/// Retries index-file creation and delegates the legacy wait/log side effect after each failure.
pub fn retry_index_file_create<F, Create, Wait>(
    max_attempts: usize,
    mut create: Create,
    mut wait_after_failure: Wait,
) -> Option<F>
where
    Create: FnMut() -> Option<F>,
    Wait: FnMut(usize, usize),
{
    for attempt in 1..=max_attempts {
        if let Some(file) = create() {
            return Some(file);
        }
        wait_after_failure(attempt, max_attempts);
    }
    None
}

/// Retries one key against newly created tail files until it is written or creation fails.
pub fn drive_index_service_put<F, Put, Next>(mut file: F, mut put: Put, mut next: Next) -> Option<F>
where
    Put: FnMut(&F) -> bool,
    Next: FnMut(&F) -> Option<F>,
{
    loop {
        if put(&file) {
            return Some(file);
        }
        file = next(&file)?;
    }
}

/// Checkpoint timestamp written after a full IndexFile flush.
pub fn index_flush_checkpoint_timestamp<F: IndexServiceFile>(file: &F) -> Option<u64> {
    file.is_write_full()
        .then(|| file.end_timestamp() as u64)
        .filter(|timestamp| *timestamp > 0)
}

/// Preflight action for one CommitLog-to-index request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexBuildPreflight {
    Build,
    AdvanceSafeOffset(Option<i64>),
    SkipOldOffset,
}

/// Selects rollback/empty/old-offset handling before creating an IndexFile.
pub fn plan_index_build(
    rollback: bool,
    has_indexable_keys: bool,
    commit_log_offset: i64,
    message_size: i32,
    max_dispatch_offset: Option<i64>,
) -> IndexBuildPreflight {
    if rollback || !has_indexable_keys {
        return IndexBuildPreflight::AdvanceSafeOffset(index_safe_offset(commit_log_offset, message_size));
    }
    if max_dispatch_offset.is_some_and(|end_offset| commit_log_offset < end_offset) {
        return IndexBuildPreflight::SkipOldOffset;
    }
    IndexBuildPreflight::Build
}

/// Returns the durable exclusive offset represented by one valid dispatch request.
pub fn index_safe_offset(commit_log_offset: i64, message_size: i32) -> Option<i64> {
    (commit_log_offset >= 0 && message_size > 0).then(|| commit_log_offset.saturating_add(i64::from(message_size)))
}

/// Kind of key visited by the build driver.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexBuildKeyKind {
    Unique,
    Normal,
    Tag,
}

/// Result of visiting every indexable key in legacy order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexBuildKeysOutcome {
    Completed,
    Aborted(IndexBuildKeyKind),
    TagFailed,
}

impl IndexBuildKeysOutcome {
    pub const fn advances_safe_offset(self) -> bool {
        matches!(self, Self::Completed | Self::TagFailed)
    }
}

/// Visits unique, normal, and tag keys in the exact legacy order.
pub fn drive_index_build_keys<'a, Visit>(
    unique_key: Option<&'a str>,
    keys: &'a str,
    tags: Option<&'a str>,
    key_separator: &str,
    unique_type: &'a str,
    tag_type: &'a str,
    mut visit: Visit,
) -> IndexBuildKeysOutcome
where
    Visit: FnMut(IndexBuildKeyKind, &'a str, Option<&'a str>) -> bool,
{
    if let Some(unique_key) = unique_key {
        if !visit(IndexBuildKeyKind::Unique, unique_key, Some(unique_type)) {
            return IndexBuildKeysOutcome::Aborted(IndexBuildKeyKind::Unique);
        }
    }
    for key in keys.split(key_separator).filter(|key| !key.is_empty()) {
        if !visit(IndexBuildKeyKind::Normal, key, None) {
            return IndexBuildKeysOutcome::Aborted(IndexBuildKeyKind::Normal);
        }
    }
    if let Some(tags) = tags {
        if !visit(IndexBuildKeyKind::Tag, tags, Some(tag_type)) {
            return IndexBuildKeysOutcome::TagFailed;
        }
    }
    IndexBuildKeysOutcome::Completed
}

/// Returns whether a request contains any unique, normal, or tag key.
pub fn has_indexable_keys(unique_key: Option<&str>, keys: &str, tags: Option<&str>, key_separator: &str) -> bool {
    unique_key.is_some() || keys.split(key_separator).any(|key| !key.is_empty()) || tags.is_some()
}

/// Builds `topic#key` or `topic#indexType#key` into a reusable buffer.
pub fn build_index_key_into<'a>(buffer: &'a mut String, topic: &str, key: &str, index_type: Option<&str>) -> &'a str {
    buffer.clear();
    let required_len = index_key_len(topic, key, index_type);
    if buffer.capacity() < required_len {
        buffer.reserve(required_len - buffer.capacity());
    }
    buffer.push_str(topic);
    buffer.push('#');
    if let Some(index_type) = index_type {
        buffer.push_str(index_type);
        buffer.push('#');
    }
    buffer.push_str(key);
    buffer.as_str()
}

/// Allocates one default `topic#key` index key.
pub fn build_index_key(topic: &str, key: &str) -> String {
    let mut buffer = String::with_capacity(index_key_len(topic, key, None));
    build_index_key_into(&mut buffer, topic, key, None);
    buffer
}

/// Allocates one typed `topic#indexType#key` index key.
pub fn build_index_key_with_type(topic: &str, key: &str, index_type: &str) -> String {
    let mut buffer = String::with_capacity(index_key_len(topic, key, Some(index_type)));
    build_index_key_into(&mut buffer, topic, key, Some(index_type));
    buffer
}

/// Exact capacity required by one rendered index key.
pub fn index_key_len(topic: &str, key: &str, index_type: Option<&str>) -> usize {
    topic.len() + key.len() + 1 + index_type.map_or(0, |index_type| index_type.len() + 1)
}

/// Maximum reusable buffer capacity needed by all keys in one dispatch request.
pub fn index_build_key_capacity(
    topic: &str,
    unique_key: Option<&str>,
    keys: &str,
    tags: Option<&str>,
    key_separator: &str,
    unique_type: &str,
    tag_type: &str,
) -> usize {
    let normal_key_len = keys.split(key_separator).map(str::len).max().unwrap_or_default();
    let unique_key_len = unique_key.map_or(0, |key| key.len() + unique_type.len() + 1);
    let tag_key_len = tags.map_or(0, |key| key.len() + tag_type.len() + 1);
    topic.len() + normal_key_len.max(unique_key_len).max(tag_key_len) + 1
}
