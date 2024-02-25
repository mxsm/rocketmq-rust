use std::{
    fs::{File, OpenOptions},
    path::PathBuf,
    sync::atomic::{AtomicI32, AtomicI64},
};

use bytes::Bytes;
use memmap2::MmapMut;
use rocketmq_common::common::message::{
    message_batch::MessageExtBatch, message_single::MessageExtBrokerInner,
};

use crate::{
    base::{
        append_message_callback::AppendMessageCallback,
        compaction_append_msg_callback::CompactionAppendMsgCallback,
        message_result::AppendMessageResult, put_message_context::PutMessageContext,
        select_result::SelectMappedBufferResult, transient_store_pool::TransientStorePool,
    },
    config::flush_disk_type::FlushDiskType,
    log_file::mapped_file::MappedFile,
};

pub struct DefaultMappedFile {
    pub(crate) file: File,
    // pub(crate) file_channel: FileChannel,
    //pub(crate) write_buffer: Option<ByteBuffer>,
    pub(crate) mmapped_file: MmapMut,
    pub(crate) transient_store_pool: Option<TransientStorePool>,
    pub(crate) file_name: String,
    pub(crate) file_from_offset: i64,
    pub(crate) mapped_byte_buffer: Option<bytes::Bytes>,
    pub(crate) wrote_position: AtomicI32,
    pub(crate) committed_position: AtomicI32,
    pub(crate) flushed_position: AtomicI32,
    pub(crate) file_size: u64,
    pub(crate) store_timestamp: AtomicI64,
    pub(crate) first_create_in_queue: bool,
    pub(crate) last_flush_time: u64,
    // pub(crate) mapped_byte_buffer_wait_to_clean: Option<MappedByteBuffer>,
    pub(crate) swap_map_time: u64,
    pub(crate) mapped_byte_buffer_access_count_since_last_swap: AtomicI64,
    pub(crate) start_timestamp: u64,
    pub(crate) stop_timestamp: u64,
}

impl DefaultMappedFile {
    pub fn new(file_name: String, file_size: u64) -> Self {
        let file_from_offset = Self::get_file_from_offset(&file_name);
        let file = Self::build_file(&file_name, file_size);
        let mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
        Self {
            file,
            file_name,
            file_from_offset,
            mapped_byte_buffer: None,
            wrote_position: Default::default(),
            committed_position: Default::default(),
            flushed_position: Default::default(),
            file_size,
            store_timestamp: Default::default(),
            first_create_in_queue: false,
            last_flush_time: 0,
            swap_map_time: 0,
            mapped_byte_buffer_access_count_since_last_swap: Default::default(),
            start_timestamp: 0,
            transient_store_pool: None,
            stop_timestamp: 0,
            mmapped_file: mmap,
        }
    }

    fn get_file_from_offset(file_name: &String) -> i64 {
        let file_from_offset = PathBuf::from(file_name.to_owned())
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .parse::<i64>()
            .unwrap();
        file_from_offset
    }

    fn build_file(file_name: &String, file_size: u64) -> File {
        let path = PathBuf::from(file_name.clone());
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .unwrap();
        file.set_len(file_size)
            .unwrap_or_else(|_| panic!("failed to set file size: {}", file_name));
        file
    }

    pub fn new_with_transient_store_pool(
        file_name: String,
        file_size: u64,
        transient_store_pool: TransientStorePool,
    ) -> Self {
        let file_from_offset = Self::get_file_from_offset(&file_name);
        let file = Self::build_file(&file_name, file_size);
        let mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
        Self {
            file,
            file_name,
            file_from_offset,
            mapped_byte_buffer: None,
            wrote_position: Default::default(),
            committed_position: Default::default(),
            flushed_position: Default::default(),
            file_size,
            store_timestamp: Default::default(),
            first_create_in_queue: false,
            last_flush_time: 0,
            swap_map_time: 0,
            mapped_byte_buffer_access_count_since_last_swap: Default::default(),
            start_timestamp: 0,
            transient_store_pool: Some(transient_store_pool),
            stop_timestamp: 0,
            mmapped_file: mmap,
        }
    }
}

#[allow(unused_variables)]
impl MappedFile for DefaultMappedFile {
    fn get_file_name(&self) -> &str {
        todo!()
    }

    fn rename_to(&mut self, file_name: &str) -> bool {
        todo!()
    }

    fn get_file_size(&self) -> usize {
        todo!()
    }

    fn get_file_channel(&self) -> std::io::Result<&File> {
        todo!()
    }

    fn is_full(&self) -> bool {
        todo!()
    }

    fn is_available(&self) -> bool {
        todo!()
    }

    fn append_message(
        &mut self,
        message: &MessageExtBrokerInner,
        message_callback: &dyn AppendMessageCallback,
        put_message_context: &PutMessageContext,
    ) -> AppendMessageResult {
        todo!()
    }

    fn append_messages(
        &mut self,
        message: &MessageExtBatch,
        message_callback: &dyn AppendMessageCallback,
        put_message_context: &PutMessageContext,
    ) -> AppendMessageResult {
        todo!()
    }

    fn append_message_compaction(
        &mut self,
        byte_buffer_msg: &mut Bytes,
        cb: &dyn CompactionAppendMsgCallback,
    ) -> AppendMessageResult {
        todo!()
    }

    fn append_message_byte_array(&mut self, data: &[u8]) -> bool {
        todo!()
    }

    fn append_message_bytes(&mut self, data: &mut Bytes) -> bool {
        todo!()
    }

    fn append_message_offset_length(&mut self, data: &[u8], offset: usize, length: usize) -> bool {
        todo!()
    }

    fn get_file_from_offset(&self) -> i64 {
        todo!()
    }

    fn flush(&mut self, flush_least_pages: usize) -> usize {
        todo!()
    }

    fn commit(&mut self, commit_least_pages: usize) -> usize {
        todo!()
    }

    fn select_mapped_buffer_size(&self, pos: usize, size: usize) -> SelectMappedBufferResult {
        todo!()
    }

    fn select_mapped_buffer(&self, pos: usize) -> SelectMappedBufferResult {
        todo!()
    }

    fn get_mapped_byte_buffer(&self) -> Bytes {
        todo!()
    }

    fn slice_byte_buffer(&self) -> Bytes {
        todo!()
    }

    fn get_store_timestamp(&self) -> i64 {
        todo!()
    }

    fn get_last_modified_timestamp(&self) -> i64 {
        todo!()
    }

    fn get_data(&self, pos: usize, size: usize, byte_buffer: &mut Bytes) -> bool {
        todo!()
    }

    fn destroy(&self, interval_forcibly: i64) -> bool {
        todo!()
    }

    fn shutdown(&self, interval_forcibly: i64) {
        todo!()
    }

    fn release(&self) {
        todo!()
    }

    fn hold(&self) -> bool {
        todo!()
    }

    fn is_first_create_in_queue(&self) -> bool {
        todo!()
    }

    fn set_first_create_in_queue(&mut self, first_create_in_queue: bool) {
        todo!()
    }

    fn get_flushed_position(&self) -> usize {
        todo!()
    }

    fn set_flushed_position(&mut self, flushed_position: usize) {
        todo!()
    }

    fn get_wrote_position(&self) -> usize {
        todo!()
    }

    fn set_wrote_position(&mut self, wrote_position: usize) {
        todo!()
    }

    fn get_read_position(&self) -> usize {
        todo!()
    }

    fn set_committed_position(&mut self, committed_position: usize) {
        todo!()
    }

    fn mlock(&self) {
        todo!()
    }

    fn munlock(&self) {
        todo!()
    }

    fn warm_mapped_file(&self, flush_disk_type: FlushDiskType, pages: usize) {
        todo!()
    }

    fn swap_map(&self) -> bool {
        todo!()
    }

    fn clean_swaped_map(&self, force: bool) {
        todo!()
    }

    fn get_recent_swap_map_time(&self) -> i64 {
        todo!()
    }

    fn get_mapped_byte_buffer_access_count_since_last_swap(&self) -> i64 {
        todo!()
    }

    fn get_file(&self) -> &File {
        todo!()
    }

    fn rename_to_delete(&self) {
        todo!()
    }

    fn move_to_parent(&self) -> std::io::Result<()> {
        todo!()
    }

    fn get_last_flush_time(&self) -> i64 {
        todo!()
    }

    fn init(
        &mut self,
        file_name: &str,
        file_size: usize,
        transient_store_pool: &TransientStorePool,
    ) -> std::io::Result<()> {
        todo!()
    }

    fn iterator(&self, pos: usize) -> Box<dyn Iterator<Item = SelectMappedBufferResult>> {
        todo!()
    }

    fn is_loaded(&self, position: i64, size: usize) -> bool {
        todo!()
    }
}
