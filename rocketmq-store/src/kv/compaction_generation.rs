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

use std::collections::BTreeMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;

#[cfg(windows)]
use std::ffi::OsStr;
#[cfg(windows)]
use std::iter;
#[cfg(windows)]
use std::os::windows::ffi::OsStrExt;

const CURRENT_MAGIC: u32 = 0x4343_5547;
const GENERATION_MAGIC: u32 = 0x4347_454E;
const RECORDS_MAGIC: u32 = 0x4347_5253;
const FORMAT_VERSION: u16 = 1;
const CURRENT_SIZE: usize = 40;
const GENERATION_METADATA_SIZE: usize = 64;
const RECORDS_HEADER_SIZE: usize = 16;
const RECORD_HEADER_SIZE: usize = 40;
const NO_GENERATION: u64 = u64::MAX;
const NO_KEY: u16 = u16::MAX;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct CompactionQueueKey {
    pub(crate) topic: CheetahString,
    pub(crate) queue_id: i32,
}

impl CompactionQueueKey {
    pub(crate) fn new(topic: &CheetahString, queue_id: i32) -> Self {
        Self {
            topic: topic.clone(),
            queue_id,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum CompactionPayload {
    CommitLog,
    Generation { generation: u64, position: u64, size: i32 },
    Inline(Bytes),
}

#[derive(Clone, Debug)]
pub(crate) struct CompactionRecord {
    pub(crate) queue_offset: i64,
    pub(crate) batch_num: i32,
    pub(crate) key: Option<CheetahString>,
    pub(crate) source_physical_offset: i64,
    pub(crate) source_size: i32,
    pub(crate) payload: CompactionPayload,
}

impl CompactionRecord {
    pub(crate) fn physical_offset(&self) -> i64 {
        self.source_physical_offset
    }

    pub(crate) fn end_physical_offset(&self) -> i64 {
        self.source_physical_offset
            .saturating_add(i64::from(self.source_size.max(0)))
    }
}

pub(crate) type CompactionQueues = BTreeMap<CompactionQueueKey, BTreeMap<i64, CompactionRecord>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct GenerationMetadata {
    pub(crate) generation: u64,
    pub(crate) record_count: u64,
    pub(crate) min_queue_offset: i64,
    pub(crate) max_queue_offset: i64,
    pub(crate) min_physical_offset: i64,
    pub(crate) max_physical_offset: i64,
    pub(crate) content_crc: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CurrentPointer {
    pub(crate) current: u64,
    pub(crate) previous: Option<u64>,
    pub(crate) next_generation: u64,
}

#[derive(Debug)]
pub(crate) struct LoadedGeneration {
    pub(crate) metadata: GenerationMetadata,
    pub(crate) queues: CompactionQueues,
}

pub(crate) fn empty_metadata(generation: u64) -> GenerationMetadata {
    GenerationMetadata {
        generation,
        record_count: 0,
        min_queue_offset: 0,
        max_queue_offset: 0,
        min_physical_offset: 0,
        max_physical_offset: 0,
        content_crc: crc32(&encode_records_header(0)),
    }
}

pub(crate) fn generation_path(root: &Path, generation: u64) -> PathBuf {
    root.join("generations").join(format!("gen-{generation}"))
}

pub(crate) fn temporary_generation_path(root: &Path, generation: u64) -> PathBuf {
    root.join("generations").join(format!("gen-{generation}.tmp"))
}

pub(crate) async fn read_current(root: PathBuf) -> Result<Option<CurrentPointer>, RocketMQError> {
    crate::runtime::spawn_io("compaction-read-current", move || {
        let path = root.join("CURRENT");
        match std::fs::read(&path) {
            Ok(bytes) => decode_current(&bytes, &path).map(Some),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(read_error(&path, error)),
        }
    })
    .await?
}

pub(crate) async fn load_generation(root: PathBuf, generation: u64) -> Result<LoadedGeneration, RocketMQError> {
    if generation == 0 {
        return Ok(LoadedGeneration {
            metadata: empty_metadata(0),
            queues: CompactionQueues::new(),
        });
    }
    crate::runtime::spawn_io("compaction-load-generation", move || {
        let directory = generation_path(&root, generation);
        let metadata_path = directory.join("GENERATION");
        let records_path = directory.join("records");
        let metadata_bytes = std::fs::read(&metadata_path).map_err(|error| read_error(&metadata_path, error))?;
        let metadata = decode_generation_metadata(&metadata_bytes, &metadata_path)?;
        if metadata.generation != generation {
            return Err(corrupted(&metadata_path));
        }
        let records_bytes = std::fs::read(&records_path).map_err(|error| read_error(&records_path, error))?;
        if crc32(&records_bytes) != metadata.content_crc {
            return Err(corrupted(&records_path));
        }
        let queues = decode_records(&records_bytes, &records_path, generation)?;
        let actual = metadata_for(generation, &queues, &records_bytes);
        if actual != metadata {
            return Err(corrupted(&metadata_path));
        }
        Ok(LoadedGeneration { metadata, queues })
    })
    .await?
}

pub(crate) async fn read_generation_payload(
    root: PathBuf,
    generation: u64,
    position: u64,
    size: i32,
) -> Result<Bytes, RocketMQError> {
    crate::runtime::spawn_io("compaction-read-generation-payload", move || {
        if generation == 0 || size <= 0 {
            return Err(corrupted(&generation_path(&root, generation).join("records")));
        }
        let path = generation_path(&root, generation).join("records");
        let mut file = File::open(&path).map_err(|error| read_error(&path, error))?;
        file.seek(SeekFrom::Start(position))
            .map_err(|error| read_error(&path, error))?;
        let mut payload = vec![0_u8; size as usize];
        file.read_exact(&mut payload)
            .map_err(|error| read_error(&path, error))?;
        Ok(Bytes::from(payload))
    })
    .await?
}

pub(crate) async fn write_temporary_generation(
    root: PathBuf,
    generation: u64,
    queues: CompactionQueues,
) -> Result<GenerationMetadata, RocketMQError> {
    crate::runtime::spawn_io("compaction-build-generation", move || {
        let directory = temporary_generation_path(&root, generation);
        remove_path_if_exists(&directory)?;
        std::fs::create_dir_all(&directory).map_err(|error| write_error(&directory, error))?;
        let records = encode_records(&queues, &directory.join("records"))?;
        let metadata = metadata_for(generation, &queues, &records);
        write_file(&directory.join("records"), &records)?;
        write_file(&directory.join("GENERATION"), &encode_generation_metadata(metadata))?;
        Ok(metadata)
    })
    .await?
}

pub(crate) async fn sync_temporary_generation(root: PathBuf, generation: u64) -> Result<(), RocketMQError> {
    crate::runtime::spawn_io("compaction-sync-generation", move || {
        let directory = temporary_generation_path(&root, generation);
        sync_file(&directory.join("records"))?;
        sync_file(&directory.join("GENERATION"))?;
        sync_directory(&directory)
    })
    .await?
}

pub(crate) async fn validate_temporary_generation(
    root: PathBuf,
    generation: u64,
    expected: GenerationMetadata,
) -> Result<(), RocketMQError> {
    crate::runtime::spawn_io("compaction-validate-generation", move || {
        let directory = temporary_generation_path(&root, generation);
        let metadata_path = directory.join("GENERATION");
        let records_path = directory.join("records");
        let metadata_bytes = std::fs::read(&metadata_path).map_err(|error| read_error(&metadata_path, error))?;
        let metadata = decode_generation_metadata(&metadata_bytes, &metadata_path)?;
        let records_bytes = std::fs::read(&records_path).map_err(|error| read_error(&records_path, error))?;
        let queues = decode_records(&records_bytes, &records_path, generation)?;
        let actual = metadata_for(generation, &queues, &records_bytes);
        if metadata != expected || actual != expected {
            return Err(corrupted(&metadata_path));
        }
        Ok(())
    })
    .await?
}

pub(crate) async fn rename_generation(root: PathBuf, generation: u64) -> Result<(), RocketMQError> {
    crate::runtime::spawn_io("compaction-rename-generation", move || {
        let source = temporary_generation_path(&root, generation);
        let destination = generation_path(&root, generation);
        rename_path(&source, &destination).map_err(|error| write_error(&destination, error))?;
        sync_directory(&root.join("generations"))
    })
    .await?
}

pub(crate) async fn write_current(root: PathBuf, pointer: CurrentPointer) -> Result<(), RocketMQError> {
    crate::runtime::spawn_io("compaction-write-current", move || {
        std::fs::create_dir_all(&root).map_err(|error| write_error(&root, error))?;
        let destination = root.join("CURRENT");
        let temporary = root.join("CURRENT.atomic.tmp");
        write_file(&temporary, &encode_current(pointer))?;
        sync_file(&temporary)?;
        replace_file(&temporary, &destination).map_err(|error| write_error(&destination, error))?;
        sync_directory(&root)
    })
    .await?
}

pub(crate) async fn delete_generation(root: PathBuf, generation: u64) -> Result<(), RocketMQError> {
    crate::runtime::spawn_io("compaction-delete-generation", move || {
        let path = generation_path(&root, generation);
        remove_path_if_exists(&path)?;
        sync_directory(&root.join("generations"))
    })
    .await?
}

pub(crate) async fn cleanup_orphans(root: PathBuf, current: u64, previous: Option<u64>) -> Result<(), RocketMQError> {
    crate::runtime::spawn_io("compaction-cleanup-orphans", move || {
        let generations = root.join("generations");
        let entries = match std::fs::read_dir(&generations) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(read_error(&generations, error)),
        };
        for entry in entries {
            let entry = entry.map_err(|error| read_error(&generations, error))?;
            let path = entry.path();
            let keep = parse_generation_id(&path)
                .is_some_and(|generation| generation == current || Some(generation) == previous);
            if !keep {
                remove_path_if_exists(&path)?;
            }
        }
        sync_directory(&generations)
    })
    .await?
}

fn metadata_for(generation: u64, queues: &CompactionQueues, records: &[u8]) -> GenerationMetadata {
    let mut count = 0_u64;
    let mut min_queue_offset = i64::MAX;
    let mut max_queue_offset = i64::MIN;
    let mut min_physical_offset = i64::MAX;
    let mut max_physical_offset = i64::MIN;
    for queue in queues.values() {
        for record in queue.values() {
            count = count.saturating_add(1);
            min_queue_offset = min_queue_offset.min(record.queue_offset);
            max_queue_offset = max_queue_offset.max(record.queue_offset);
            min_physical_offset = min_physical_offset.min(record.physical_offset());
            max_physical_offset = max_physical_offset.max(record.end_physical_offset());
        }
    }
    if count == 0 {
        min_queue_offset = 0;
        max_queue_offset = 0;
        min_physical_offset = 0;
        max_physical_offset = 0;
    }
    GenerationMetadata {
        generation,
        record_count: count,
        min_queue_offset,
        max_queue_offset,
        min_physical_offset,
        max_physical_offset,
        content_crc: crc32(records),
    }
}

fn encode_records(queues: &CompactionQueues, path: &Path) -> Result<Vec<u8>, RocketMQError> {
    let record_count = queues.values().map(BTreeMap::len).sum::<usize>();
    let mut bytes = BytesMut::with_capacity(RECORDS_HEADER_SIZE + record_count.saturating_mul(RECORD_HEADER_SIZE));
    bytes.put_slice(&encode_records_header(record_count as u64));
    for (queue_key, queue) in queues {
        let topic = queue_key.topic.as_bytes();
        let topic_len = u16::try_from(topic.len()).map_err(|_| {
            RocketMQError::storage_write_failed(path_to_string(path), "compaction topic exceeds u16 format limit")
        })?;
        for record in queue.values() {
            let key_bytes = record.key.as_ref().map(CheetahString::as_bytes);
            let key_len = match key_bytes {
                Some(key) => u16::try_from(key.len()).map_err(|_| {
                    RocketMQError::storage_write_failed(path_to_string(path), "compaction key exceeds u16 format limit")
                })?,
                None => NO_KEY,
            };
            let CompactionPayload::Inline(payload) = &record.payload else {
                return Err(RocketMQError::storage_write_failed(
                    path_to_string(path),
                    "compaction generation must materialize payload before persistence",
                ));
            };
            if record.source_physical_offset < 0
                || record.source_size <= 0
                || payload.len() != record.source_size as usize
            {
                return Err(RocketMQError::storage_write_failed(
                    path_to_string(path),
                    "compaction payload size does not match source record",
                ));
            }
            bytes.put_u16(topic_len);
            bytes.put_u16(key_len);
            bytes.put_i32(queue_key.queue_id);
            bytes.put_i64(record.queue_offset);
            bytes.put_i32(record.batch_num);
            bytes.put_i64(record.source_physical_offset);
            bytes.put_i32(record.source_size);
            bytes.put_u32(0);
            bytes.put_u32(0);
            bytes.put_slice(topic);
            if let Some(key) = key_bytes {
                bytes.put_slice(key);
            }
            bytes.put_slice(payload);
        }
    }
    Ok(bytes.to_vec())
}

fn decode_records(bytes: &[u8], path: &Path, generation: u64) -> Result<CompactionQueues, RocketMQError> {
    if bytes.len() < RECORDS_HEADER_SIZE {
        return Err(corrupted(path));
    }
    let mut cursor = Bytes::copy_from_slice(bytes);
    if cursor.get_u32() != RECORDS_MAGIC || cursor.get_u16() != FORMAT_VERSION {
        return Err(corrupted(path));
    }
    let _reserved = cursor.get_u16();
    let count = cursor.get_u64();
    let mut queues = CompactionQueues::new();
    for _ in 0..count {
        if cursor.remaining() < RECORD_HEADER_SIZE {
            return Err(corrupted(path));
        }
        let topic_len = cursor.get_u16() as usize;
        let raw_key_len = cursor.get_u16();
        let key_len = if raw_key_len == NO_KEY { 0 } else { raw_key_len as usize };
        let queue_id = cursor.get_i32();
        let queue_offset = cursor.get_i64();
        let batch_num = cursor.get_i32();
        let physical_offset = cursor.get_i64();
        let size = cursor.get_i32();
        let _reserved = cursor.get_u32();
        let _reserved = cursor.get_u32();
        let variable_size = topic_len.saturating_add(key_len).saturating_add(size.max(0) as usize);
        if queue_offset < 0 || batch_num <= 0 || physical_offset < 0 || size <= 0 || cursor.remaining() < variable_size
        {
            return Err(corrupted(path));
        }
        let topic = std::str::from_utf8(&cursor.copy_to_bytes(topic_len))
            .map_err(|_| corrupted(path))?
            .to_owned();
        let key = if raw_key_len == NO_KEY {
            None
        } else {
            Some(
                std::str::from_utf8(&cursor.copy_to_bytes(key_len))
                    .map_err(|_| corrupted(path))?
                    .to_owned(),
            )
        };
        let payload_position = bytes.len().saturating_sub(cursor.remaining()) as u64;
        cursor.advance(size as usize);
        let queue_key = CompactionQueueKey {
            topic: CheetahString::from_string(topic),
            queue_id,
        };
        let record = CompactionRecord {
            queue_offset,
            batch_num,
            key: key.map(CheetahString::from_string),
            source_physical_offset: physical_offset,
            source_size: size,
            payload: CompactionPayload::Generation {
                generation,
                position: payload_position,
                size,
            },
        };
        if queues
            .entry(queue_key)
            .or_default()
            .insert(queue_offset, record)
            .is_some()
        {
            return Err(corrupted(path));
        }
    }
    if cursor.has_remaining() {
        return Err(corrupted(path));
    }
    Ok(queues)
}

fn encode_records_header(record_count: u64) -> [u8; RECORDS_HEADER_SIZE] {
    let mut bytes = BytesMut::with_capacity(RECORDS_HEADER_SIZE);
    bytes.put_u32(RECORDS_MAGIC);
    bytes.put_u16(FORMAT_VERSION);
    bytes.put_u16(0);
    bytes.put_u64(record_count);
    bytes.as_ref().try_into().unwrap_or([0; RECORDS_HEADER_SIZE])
}

fn encode_generation_metadata(metadata: GenerationMetadata) -> [u8; GENERATION_METADATA_SIZE] {
    let mut bytes = BytesMut::with_capacity(GENERATION_METADATA_SIZE);
    bytes.put_u32(GENERATION_MAGIC);
    bytes.put_u16(FORMAT_VERSION);
    bytes.put_u16(0);
    bytes.put_u64(metadata.generation);
    bytes.put_u64(metadata.record_count);
    bytes.put_i64(metadata.min_queue_offset);
    bytes.put_i64(metadata.max_queue_offset);
    bytes.put_i64(metadata.min_physical_offset);
    bytes.put_i64(metadata.max_physical_offset);
    bytes.put_u32(metadata.content_crc);
    let checksum = crc32(&bytes);
    bytes.put_u32(checksum);
    bytes.as_ref().try_into().unwrap_or([0; GENERATION_METADATA_SIZE])
}

fn decode_generation_metadata(bytes: &[u8], path: &Path) -> Result<GenerationMetadata, RocketMQError> {
    if bytes.len() != GENERATION_METADATA_SIZE {
        return Err(corrupted(path));
    }
    let expected_crc = u32::from_be_bytes(bytes[GENERATION_METADATA_SIZE - 4..].try_into().unwrap_or_default());
    if crc32(&bytes[..GENERATION_METADATA_SIZE - 4]) != expected_crc {
        return Err(corrupted(path));
    }
    let mut cursor = Bytes::copy_from_slice(bytes);
    if cursor.get_u32() != GENERATION_MAGIC || cursor.get_u16() != FORMAT_VERSION {
        return Err(corrupted(path));
    }
    let _reserved = cursor.get_u16();
    let metadata = GenerationMetadata {
        generation: cursor.get_u64(),
        record_count: cursor.get_u64(),
        min_queue_offset: cursor.get_i64(),
        max_queue_offset: cursor.get_i64(),
        min_physical_offset: cursor.get_i64(),
        max_physical_offset: cursor.get_i64(),
        content_crc: cursor.get_u32(),
    };
    let valid_empty = metadata.record_count == 0
        && metadata.min_queue_offset == 0
        && metadata.max_queue_offset == 0
        && metadata.min_physical_offset == 0
        && metadata.max_physical_offset == 0;
    let valid_non_empty = metadata.record_count > 0
        && metadata.min_queue_offset <= metadata.max_queue_offset
        && metadata.min_physical_offset <= metadata.max_physical_offset;
    if !valid_empty && !valid_non_empty {
        return Err(corrupted(path));
    }
    Ok(metadata)
}

fn encode_current(pointer: CurrentPointer) -> [u8; CURRENT_SIZE] {
    let mut bytes = BytesMut::with_capacity(CURRENT_SIZE);
    bytes.put_u32(CURRENT_MAGIC);
    bytes.put_u16(FORMAT_VERSION);
    bytes.put_u16(0);
    bytes.put_u64(pointer.current);
    bytes.put_u64(pointer.previous.unwrap_or(NO_GENERATION));
    bytes.put_u64(pointer.next_generation);
    let checksum = crc32(&bytes);
    bytes.put_u32(checksum);
    bytes.put_u32(0);
    bytes.as_ref().try_into().unwrap_or([0; CURRENT_SIZE])
}

fn decode_current(bytes: &[u8], path: &Path) -> Result<CurrentPointer, RocketMQError> {
    if bytes.len() != CURRENT_SIZE {
        return Err(corrupted(path));
    }
    let expected_crc = u32::from_be_bytes(bytes[32..36].try_into().unwrap_or_default());
    if crc32(&bytes[..32]) != expected_crc {
        return Err(corrupted(path));
    }
    let mut cursor = Bytes::copy_from_slice(bytes);
    if cursor.get_u32() != CURRENT_MAGIC || cursor.get_u16() != FORMAT_VERSION {
        return Err(corrupted(path));
    }
    let _reserved = cursor.get_u16();
    let current = cursor.get_u64();
    let previous = match cursor.get_u64() {
        NO_GENERATION => None,
        generation => Some(generation),
    };
    let next_generation = cursor.get_u64();
    if next_generation <= current || previous == Some(current) {
        return Err(corrupted(path));
    }
    Ok(CurrentPointer {
        current,
        previous,
        next_generation,
    })
}

fn parse_generation_id(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    if name.ends_with(".tmp") {
        return None;
    }
    name.strip_prefix("gen-")?.parse().ok()
}

fn write_file(path: &Path, bytes: &[u8]) -> Result<(), RocketMQError> {
    let mut file = File::create(path).map_err(|error| write_error(path, error))?;
    file.write_all(bytes).map_err(|error| write_error(path, error))?;
    file.flush().map_err(|error| write_error(path, error))
}

fn sync_file(path: &Path) -> Result<(), RocketMQError> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| write_error(path, error))
}

fn remove_path_if_exists(path: &Path) -> Result<(), RocketMQError> {
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(write_error(path, error)),
    };
    if metadata.is_dir() {
        std::fs::remove_dir_all(path).map_err(|error| write_error(path, error))
    } else {
        std::fs::remove_file(path).map_err(|error| write_error(path, error))
    }
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<(), RocketMQError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| write_error(path, error))
}

#[cfg(windows)]
fn sync_directory(_path: &Path) -> Result<(), RocketMQError> {
    // MoveFileExW/ReplaceFileW with WRITE_THROUGH are the durable Windows metadata boundaries.
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn sync_directory(_path: &Path) -> Result<(), RocketMQError> {
    Ok(())
}

#[cfg(not(windows))]
fn rename_path(source: &Path, destination: &Path) -> std::io::Result<()> {
    std::fs::rename(source, destination)
}

#[cfg(windows)]
fn rename_path(source: &Path, destination: &Path) -> std::io::Result<()> {
    let destination = wide_path(destination);
    let source = wide_path(source);
    // SAFETY: Both UTF-16 buffers are NUL-terminated and remain alive for the duration of MoveFileExW.
    let renamed = unsafe {
        windows_sys::Win32::Storage::FileSystem::MoveFileExW(
            source.as_ptr(),
            destination.as_ptr(),
            windows_sys::Win32::Storage::FileSystem::MOVEFILE_WRITE_THROUGH,
        )
    };
    if renamed == 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(not(windows))]
fn replace_file(source: &Path, destination: &Path) -> std::io::Result<()> {
    std::fs::rename(source, destination)
}

#[cfg(windows)]
fn replace_file(source: &Path, destination: &Path) -> std::io::Result<()> {
    if !destination.exists() {
        return std::fs::rename(source, destination);
    }
    let destination = wide_path(destination);
    let source = wide_path(source);
    // SAFETY: Both buffers are NUL-terminated and remain alive for the call; optional pointers are
    // null.
    let replaced = unsafe {
        windows_sys::Win32::Storage::FileSystem::ReplaceFileW(
            destination.as_ptr(),
            source.as_ptr(),
            std::ptr::null(),
            windows_sys::Win32::Storage::FileSystem::REPLACEFILE_WRITE_THROUGH,
            std::ptr::null(),
            std::ptr::null(),
        )
    };
    if replaced == 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(windows)]
fn wide_path(path: &Path) -> Vec<u16> {
    OsStr::new(path).encode_wide().chain(iter::once(0)).collect()
}

fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn read_error(path: &Path, error: std::io::Error) -> RocketMQError {
    RocketMQError::storage_read_failed(path_to_string(path), io_reason(&error))
}

fn write_error(path: &Path, error: std::io::Error) -> RocketMQError {
    RocketMQError::storage_write_failed(path_to_string(path), io_reason(&error))
}

fn io_reason(error: &std::io::Error) -> String {
    format!("I/O error kind {:?}, OS code {:?}", error.kind(), error.raw_os_error())
}

fn corrupted(path: &Path) -> RocketMQError {
    RocketMQError::StorageCorrupted {
        path: path_to_string(path),
    }
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFF_u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_and_generation_metadata_reject_corruption() {
        let pointer = CurrentPointer {
            current: 7,
            previous: Some(6),
            next_generation: 8,
        };
        let encoded = encode_current(pointer);
        assert_eq!(decode_current(&encoded, Path::new("CURRENT")).unwrap(), pointer);
        let mut corrupt = encoded;
        corrupt[8] ^= 1;
        assert!(decode_current(&corrupt, Path::new("CURRENT")).is_err());

        let metadata = GenerationMetadata {
            generation: 7,
            record_count: 1,
            min_queue_offset: 3,
            max_queue_offset: 3,
            min_physical_offset: 10,
            max_physical_offset: 20,
            content_crc: 42,
        };
        let encoded = encode_generation_metadata(metadata);
        assert_eq!(
            decode_generation_metadata(&encoded, Path::new("GENERATION")).unwrap(),
            metadata
        );
    }
}
