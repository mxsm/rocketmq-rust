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

//! The legacy local-file facade and the normalized Local composition remain
//! available through their public compatibility paths.
//!
//! ```
//! use rocketmq_store::message_store::local_file_message_store::LocalFileMessageStore;
//! use rocketmq_store_local::message_store::local_file_message_store::LocalStoreComposition;
//!
//! let _ = std::mem::size_of::<LocalFileMessageStore>();
//! let _ = std::mem::size_of::<LocalStoreComposition>();
//! ```

#![allow(dead_code)]
#![allow(unused_variables)]

pub mod base;
pub mod config;
pub mod consume_queue;
pub mod filter;
pub mod ha;
pub mod hook;
mod index;
pub mod inspection;
mod kv;
pub mod log_file;
pub(crate) mod message_encoder;
pub mod message_store;
pub mod platform;
pub mod pop;
pub mod queue;
#[cfg(feature = "rocksdb_store")]
pub mod rocksdb;
pub(crate) mod runtime;
pub mod stats;
pub mod store;
pub mod store_api_adapter;
pub mod store_error;
pub mod store_path_config_helper;
#[cfg(feature = "tieredstore")]
pub mod tieredstore;
pub mod timer;
pub mod transfer;
pub mod utils;

#[doc(hidden)]
pub mod bench_support {
    use std::io;
    use std::io::IoSlice;
    #[cfg(target_os = "linux")]
    use std::io::Write;
    #[cfg(target_os = "linux")]
    use std::os::fd::AsRawFd;
    #[cfg(target_os = "linux")]
    use std::os::fd::RawFd;
    use std::pin::Pin;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::task::Context;
    use std::task::Poll;
    use std::time::Duration;
    use std::time::Instant;

    use bytes::BufMut;
    use bytes::Bytes;
    use bytes::BytesMut;
    use cheetah_string::CheetahString;
    use dashmap::DashMap;
    use futures_util::future::join_all;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_common::common::config::TopicConfig;
    use rocketmq_runtime::BlockingExecutorSnapshot;
    use rocketmq_runtime::ShutdownReport;
    use serde::Serialize;
    use tokio::io::AsyncWrite;

    use crate::base::message_store::MessageStore;
    use crate::base::store_stats_service::StoreStatsService;
    use crate::config::message_store_config::MessageStoreConfig;
    use crate::ha::transfer_engine::bytes::BytesTransferEngine;
    #[cfg(target_os = "linux")]
    use crate::ha::transfer_engine::sendfile::SendfileTransferEngine;
    #[cfg(target_os = "linux")]
    use crate::ha::transfer_engine::sendfile::SendfileWriteTarget;
    use crate::ha::transfer_engine::vectored::VectoredTransferEngine;
    #[cfg(target_os = "linux")]
    use crate::ha::transfer_engine::TransferEngineKind;
    use crate::ha::transfer_engine::TransferStats;
    use crate::kv::compaction_service::CompactionService;
    use crate::kv::compaction_store::CompactionStore;
    use crate::message_store::local_file_shared_owner::new_legacy_shared_owner;
    #[cfg(feature = "rocksdb_store")]
    use crate::rocksdb::config::RocksDbConfig;
    #[cfg(feature = "rocksdb_store")]
    use crate::rocksdb::maintenance::RocksDbMaintenanceService;
    #[cfg(feature = "rocksdb_store")]
    use crate::rocksdb::store::RocksDbStore;
    use crate::timer::timer_message_store::TimerMessageStore;
    use crate::transfer::batch::TransferBatch;
    use crate::transfer::batch::TransferKind;
    use crate::transfer::segment::SegmentLease;
    use crate::transfer::segment::TransferCacheState;

    #[derive(Debug, Clone, Serialize)]
    pub struct StoreBlockingIoProbe {
        pub task_count: usize,
        pub elapsed_us: u128,
        pub max_active: usize,
        pub queue_wait_min_us: u128,
        pub queue_wait_p50_us: u128,
        pub queue_wait_p95_us: u128,
        pub queue_wait_p99_us: u128,
        pub queue_wait_max_us: u128,
        pub snapshot: BlockingExecutorSnapshot,
        pub shutdown_report: ShutdownReport,
        pub healthy: bool,
    }

    #[derive(Debug, Clone, Serialize)]
    pub struct StoreKvCompactionLifecycleProbe {
        pub compacted: bool,
        pub task_count_before_shutdown: usize,
        pub task_count_after_shutdown: usize,
        pub scheduled_runs: u64,
        pub scheduled_skips: u64,
        pub scheduled_overlaps: u64,
        pub scheduled_failures: u64,
        pub shutdown_elapsed_us: u128,
        pub shutdown_report: Option<ShutdownReport>,
        pub healthy: bool,
    }

    #[derive(Debug, Clone, Serialize)]
    pub struct StoreStatsServiceLifecycleProbe {
        pub task_count_before_shutdown: usize,
        pub task_count_after_shutdown: usize,
        pub scheduled_runs: u64,
        pub scheduled_skips: u64,
        pub scheduled_overlaps: u64,
        pub scheduled_failures: u64,
        pub snapshot_count: usize,
        pub shutdown_elapsed_us: u128,
        pub shutdown_report: Option<ShutdownReport>,
        pub healthy: bool,
    }

    #[derive(Debug, Clone, Serialize)]
    pub struct StoreTimerSchedulerLifecycleProbe {
        pub task_count_before_shutdown: usize,
        pub task_count_after_shutdown: usize,
        pub scheduled_runs: u64,
        pub scheduled_skips: u64,
        pub scheduled_overlaps: u64,
        pub scheduled_failures: u64,
        pub shutdown_elapsed_us: u128,
        pub shutdown_report: Option<ShutdownReport>,
        pub healthy: bool,
    }

    #[derive(Debug, Clone, Serialize)]
    pub struct StoreLocalFileScheduledLifecycleProbe {
        pub task_count_before_shutdown: usize,
        pub task_count_after_shutdown: usize,
        pub scheduled_runs: u64,
        pub scheduled_skips: u64,
        pub scheduled_overlaps: u64,
        pub scheduled_failures: u64,
        pub shutdown_elapsed_us: u128,
        pub healthy: bool,
    }

    #[derive(Debug, Clone, Serialize)]
    pub struct HaTransferBenchmarkReport {
        pub batch_count: usize,
        pub bytes_baseline: HaTransferEngineBenchmarkReport,
        pub vectored_optimized: HaTransferEngineBenchmarkReport,
        pub syscall_reduction_percent: usize,
        pub frames_match: bool,
        pub ack_offsets_match: bool,
    }

    #[derive(Debug, Clone, Serialize)]
    pub struct HaSendfileBenchmarkReport {
        pub batch_count: usize,
        pub vectored_baseline: HaTransferEngineBenchmarkReport,
        pub sendfile_optimized: HaTransferEngineBenchmarkReport,
        pub write_syscall_reduction_percent: usize,
        pub user_cpu_reduction_percent: usize,
        pub frames_match: bool,
        pub ack_offsets_match: bool,
    }

    #[derive(Debug, Clone, Serialize)]
    pub struct HaTransferEngineBenchmarkReport {
        pub engine: &'static str,
        pub batch_count: usize,
        pub frame_bytes: usize,
        pub body_bytes: usize,
        pub write_syscall_count: usize,
        pub sendfile_syscall_count: usize,
        pub fallback_bytes: usize,
        pub partial_write_count: usize,
        pub ack_offset: i64,
        pub ack_latency_nanos: u128,
        pub user_cpu_nanos: u128,
    }

    struct HaTransferEngineRun {
        report: HaTransferEngineBenchmarkReport,
        frame: Vec<u8>,
    }

    #[derive(Default)]
    struct HaBenchmarkWriter {
        written: Vec<u8>,
    }

    impl AsyncWrite for HaBenchmarkWriter {
        fn poll_write(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
            self.written.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_write_vectored(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            let mut written = 0;
            for buf in bufs {
                self.written.extend_from_slice(buf);
                written += buf.len();
            }
            Poll::Ready(Ok(written))
        }

        fn is_write_vectored(&self) -> bool {
            true
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[cfg(target_os = "linux")]
    struct HaBenchmarkFileWriter {
        file: std::fs::File,
    }

    #[cfg(target_os = "linux")]
    impl HaBenchmarkFileWriter {
        fn new(file: std::fs::File) -> Self {
            Self { file }
        }

        fn into_std(self) -> std::fs::File {
            self.file
        }
    }

    #[cfg(target_os = "linux")]
    impl SendfileWriteTarget for HaBenchmarkFileWriter {
        fn sendfile_out_fd(&self) -> RawFd {
            self.file.as_raw_fd()
        }
    }

    #[cfg(target_os = "linux")]
    impl AsyncWrite for HaBenchmarkFileWriter {
        fn poll_write(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
            Poll::Ready(self.file.write(buf))
        }

        fn poll_write_vectored(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(self.file.write_vectored(bufs))
        }

        fn is_write_vectored(&self) -> bool {
            true
        }

        fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(self.file.flush())
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(self.file.flush())
        }
    }

    pub async fn run_ha_bytes_vectored_benchmark_report(body_size: usize) -> HaTransferBenchmarkReport {
        let body = Bytes::from(vec![7; body_size]);
        let batch = ha_transfer_benchmark_batch(body);
        let bytes_baseline = run_ha_bytes_benchmark(&batch).await;
        let vectored_optimized = run_ha_vectored_benchmark(&batch).await;
        let baseline_syscalls = bytes_baseline.report.write_syscall_count;
        let optimized_syscalls = vectored_optimized.report.write_syscall_count;
        let syscall_reduction_percent = baseline_syscalls
            .saturating_sub(optimized_syscalls)
            .checked_mul(100)
            .and_then(|reduction| reduction.checked_div(baseline_syscalls))
            .unwrap_or(0);

        HaTransferBenchmarkReport {
            batch_count: bytes_baseline.report.batch_count,
            frames_match: bytes_baseline.frame == vectored_optimized.frame,
            ack_offsets_match: bytes_baseline.report.ack_offset == vectored_optimized.report.ack_offset,
            bytes_baseline: bytes_baseline.report,
            vectored_optimized: vectored_optimized.report,
            syscall_reduction_percent,
        }
    }

    async fn run_ha_bytes_benchmark(batch: &TransferBatch) -> HaTransferEngineRun {
        let cpu_started = process_user_cpu_nanos();
        let started = Instant::now();
        let mut engine = BytesTransferEngine::new(HaBenchmarkWriter::default());
        let stats = engine.send_batch(batch).await.expect("bytes HA benchmark transfer");
        let elapsed = started.elapsed();
        let user_cpu_nanos = elapsed_user_cpu_nanos(cpu_started, elapsed);
        let writer = engine.into_inner();
        let ack_offset = decode_ha_ack_offset(&writer.written);
        ha_transfer_engine_run(
            "bytes",
            stats,
            writer.written,
            ack_offset,
            elapsed.as_nanos(),
            user_cpu_nanos,
        )
    }

    async fn run_ha_vectored_benchmark(batch: &TransferBatch) -> HaTransferEngineRun {
        let cpu_started = process_user_cpu_nanos();
        let started = Instant::now();
        let mut engine = VectoredTransferEngine::new(HaBenchmarkWriter::default());
        let stats = engine.send_batch(batch).await.expect("vectored HA benchmark transfer");
        let elapsed = started.elapsed();
        let user_cpu_nanos = elapsed_user_cpu_nanos(cpu_started, elapsed);
        let writer = engine.into_inner();
        let ack_offset = decode_ha_ack_offset(&writer.written);
        ha_transfer_engine_run(
            "vectored",
            stats,
            writer.written,
            ack_offset,
            elapsed.as_nanos(),
            user_cpu_nanos,
        )
    }

    #[cfg(target_os = "linux")]
    pub async fn run_ha_vectored_sendfile_benchmark_report(body_size: usize) -> io::Result<HaSendfileBenchmarkReport> {
        const BATCH_COUNT: usize = 32;

        let body = Bytes::from(vec![7; body_size]);
        let (_input_guard, input_file) = ha_benchmark_input_file(&body)?;
        let vectored_baseline = run_ha_vectored_file_benchmark(body.clone(), BATCH_COUNT).await?;
        let sendfile_optimized = run_ha_sendfile_file_benchmark(input_file, body_size, BATCH_COUNT).await?;
        let baseline_syscalls = vectored_baseline.report.write_syscall_count;
        let optimized_syscalls =
            sendfile_optimized.report.write_syscall_count + sendfile_optimized.report.sendfile_syscall_count;
        let write_syscall_reduction_percent = percent_reduction(baseline_syscalls as u128, optimized_syscalls as u128);
        let user_cpu_reduction_percent = percent_reduction(
            vectored_baseline.report.user_cpu_nanos,
            sendfile_optimized.report.user_cpu_nanos,
        );

        Ok(HaSendfileBenchmarkReport {
            batch_count: BATCH_COUNT,
            frames_match: vectored_baseline.frame == sendfile_optimized.frame,
            ack_offsets_match: vectored_baseline.report.ack_offset == sendfile_optimized.report.ack_offset,
            vectored_baseline: vectored_baseline.report,
            sendfile_optimized: sendfile_optimized.report,
            write_syscall_reduction_percent,
            user_cpu_reduction_percent,
        })
    }

    #[cfg(not(target_os = "linux"))]
    pub async fn run_ha_vectored_sendfile_benchmark_report(_body_size: usize) -> io::Result<HaSendfileBenchmarkReport> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "sendfile benchmark report is only available on Linux",
        ))
    }

    #[cfg(target_os = "linux")]
    async fn run_ha_vectored_file_benchmark(body: Bytes, batch_count: usize) -> io::Result<HaTransferEngineRun> {
        let batch = ha_transfer_benchmark_batch(body);
        let mut engine = VectoredTransferEngine::new(HaBenchmarkWriter::default());
        let cpu_started = process_user_cpu_nanos();
        let started = Instant::now();
        let mut stats = empty_repeated_ha_stats(TransferEngineKind::Vectored, batch_count);
        for _ in 0..batch_count {
            let batch_stats = engine.send_batch(&batch).await.map_err(transfer_error_to_io)?;
            add_repeated_ha_stats(&mut stats, batch_stats);
        }
        let elapsed = started.elapsed();
        let user_cpu_nanos = elapsed_user_cpu_nanos(cpu_started, elapsed);
        let writer = engine.into_inner();
        let ack_offset = decode_repeated_ha_ack_offset(&writer.written, batch_count);

        Ok(ha_transfer_engine_run(
            "vectored",
            stats,
            writer.written,
            ack_offset,
            elapsed.as_nanos(),
            user_cpu_nanos,
        ))
    }

    #[cfg(target_os = "linux")]
    async fn run_ha_sendfile_file_benchmark(
        input_file: Arc<std::fs::File>,
        body_size: usize,
        batch_count: usize,
    ) -> io::Result<HaTransferEngineRun> {
        let batch = ha_transfer_file_benchmark_batch(input_file, body_size);
        let output = tempfile::NamedTempFile::new()?;
        let writer = HaBenchmarkFileWriter::new(output.reopen()?);
        let mut engine = SendfileTransferEngine::new(writer);
        let cpu_started = process_user_cpu_nanos();
        let started = Instant::now();
        let mut stats = empty_repeated_ha_stats(TransferEngineKind::Sendfile, batch_count);
        for _ in 0..batch_count {
            let batch_stats = engine.send_batch(&batch).await.map_err(transfer_error_to_io)?;
            add_repeated_ha_stats(&mut stats, batch_stats);
        }
        let elapsed = started.elapsed();
        let user_cpu_nanos = elapsed_user_cpu_nanos(cpu_started, elapsed);
        let (writer, _) = engine.into_parts();
        writer.into_std().sync_all()?;
        let frame = std::fs::read(output.path())?;
        let ack_offset = decode_repeated_ha_ack_offset(&frame, batch_count);

        Ok(ha_transfer_engine_run(
            "sendfile",
            stats,
            frame,
            ack_offset,
            elapsed.as_nanos(),
            user_cpu_nanos,
        ))
    }

    fn ha_transfer_engine_run(
        engine: &'static str,
        stats: TransferStats,
        frame: Vec<u8>,
        ack_offset: i64,
        ack_latency_nanos: u128,
        user_cpu_nanos: u128,
    ) -> HaTransferEngineRun {
        HaTransferEngineRun {
            report: HaTransferEngineBenchmarkReport {
                engine,
                batch_count: stats.frame_count,
                frame_bytes: stats.bytes_written,
                body_bytes: stats.body_bytes,
                write_syscall_count: stats.write_call_count,
                sendfile_syscall_count: stats.sendfile_call_count,
                fallback_bytes: stats.fallback_bytes,
                partial_write_count: stats.partial_write_count,
                ack_offset,
                ack_latency_nanos,
                user_cpu_nanos,
            },
            frame,
        }
    }

    #[cfg(target_os = "linux")]
    fn empty_repeated_ha_stats(engine: TransferEngineKind, frame_count: usize) -> TransferStats {
        TransferStats {
            engine,
            bytes_written: 0,
            body_bytes: 0,
            frame_count,
            write_call_count: 0,
            sendfile_call_count: 0,
            sendfile_bytes: 0,
            fallback_bytes: 0,
            partial_write_count: 0,
        }
    }

    #[cfg(target_os = "linux")]
    fn add_repeated_ha_stats(total: &mut TransferStats, next: TransferStats) {
        total.bytes_written += next.bytes_written;
        total.body_bytes += next.body_bytes;
        total.write_call_count += next.write_call_count;
        total.sendfile_call_count += next.sendfile_call_count;
        total.sendfile_bytes += next.sendfile_bytes;
        total.fallback_bytes += next.fallback_bytes;
        total.partial_write_count += next.partial_write_count;
    }

    fn ha_transfer_benchmark_batch(body: Bytes) -> TransferBatch {
        let start_offset = 16 * 1024;
        let body_size = body.len();
        TransferBatch {
            frame_header: ha_transfer_benchmark_header(start_offset, body_size),
            segments: vec![SegmentLease::from_bytes(start_offset, 0, body, TransferCacheState::Hot)],
            total_body_len: body_size,
            start_offset,
            next_offset: start_offset + body_size as i64,
            kind: TransferKind::Data,
        }
    }

    #[cfg(target_os = "linux")]
    fn ha_transfer_file_benchmark_batch(file: Arc<std::fs::File>, body_size: usize) -> TransferBatch {
        let start_offset = 16 * 1024;
        TransferBatch {
            frame_header: ha_transfer_benchmark_header(start_offset, body_size),
            segments: vec![SegmentLease::from_file_range(
                start_offset,
                start_offset as u64,
                0,
                body_size,
                file,
                TransferCacheState::Hot,
            )],
            total_body_len: body_size,
            start_offset,
            next_offset: start_offset + body_size as i64,
            kind: TransferKind::Data,
        }
    }

    fn ha_transfer_benchmark_header(offset: i64, body_size: usize) -> Bytes {
        let mut header = BytesMut::with_capacity(12);
        header.put_i64(offset);
        header.put_i32(i32::try_from(body_size).expect("HA benchmark body size fits i32"));
        header.freeze()
    }

    fn decode_ha_ack_offset(frame: &[u8]) -> i64 {
        assert!(frame.len() >= 12, "HA benchmark frame must include a header");
        let offset = i64::from_be_bytes(frame[0..8].try_into().expect("HA benchmark offset header"));
        let body_len = i32::from_be_bytes(frame[8..12].try_into().expect("HA benchmark body size header"));
        assert!(body_len >= 0, "HA benchmark body size must be non-negative");
        assert_eq!(frame.len() - 12, body_len as usize);
        offset + body_len as i64
    }

    fn decode_repeated_ha_ack_offset(frame: &[u8], batch_count: usize) -> i64 {
        let mut cursor = 0;
        let mut ack_offset = 0;

        for _ in 0..batch_count {
            assert!(
                frame.len() >= cursor + 12,
                "HA benchmark frame must include repeated headers"
            );
            let offset = i64::from_be_bytes(
                frame[cursor..cursor + 8]
                    .try_into()
                    .expect("HA benchmark repeated offset header"),
            );
            let body_len = i32::from_be_bytes(
                frame[cursor + 8..cursor + 12]
                    .try_into()
                    .expect("HA benchmark repeated body size header"),
            );
            assert!(body_len >= 0, "HA benchmark body size must be non-negative");
            cursor += 12 + body_len as usize;
            assert!(
                cursor <= frame.len(),
                "HA benchmark repeated frame must include body bytes: cursor={cursor}, frame_len={}",
                frame.len()
            );
            ack_offset = offset + body_len as i64;
        }

        assert_eq!(cursor, frame.len());
        ack_offset
    }

    #[cfg(target_os = "linux")]
    fn ha_benchmark_input_file(body: &[u8]) -> io::Result<(tempfile::NamedTempFile, Arc<std::fs::File>)> {
        let mut input = tempfile::NamedTempFile::new()?;
        input.write_all(body)?;
        input.as_file_mut().sync_all()?;
        let file = Arc::new(input.reopen()?);
        Ok((input, file))
    }

    fn percent_reduction(baseline: u128, optimized: u128) -> usize {
        baseline
            .saturating_sub(optimized)
            .checked_mul(100)
            .and_then(|reduction| reduction.checked_div(baseline))
            .and_then(|percent| usize::try_from(percent).ok())
            .unwrap_or(0)
    }

    #[cfg(target_os = "linux")]
    fn transfer_error_to_io(error: crate::transfer::error::TransferError) -> io::Error {
        match error {
            crate::transfer::error::TransferError::Io(error) => error,
            error => io::Error::other(format!("{error:?}")),
        }
    }

    #[cfg(unix)]
    fn process_user_cpu_nanos() -> Option<u128> {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
        let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
        if result != 0 {
            return None;
        }

        let usage = unsafe { usage.assume_init() };
        timeval_to_nanos(usage.ru_utime)
    }

    #[cfg(not(unix))]
    fn process_user_cpu_nanos() -> Option<u128> {
        None
    }

    #[cfg(unix)]
    fn timeval_to_nanos(timeval: libc::timeval) -> Option<u128> {
        let seconds = u128::try_from(timeval.tv_sec).ok()?;
        let micros = u128::try_from(timeval.tv_usec).ok()?;
        Some(seconds * 1_000_000_000 + micros * 1_000)
    }

    fn elapsed_user_cpu_nanos(started: Option<u128>, elapsed: Duration) -> u128 {
        match (started, process_user_cpu_nanos()) {
            (Some(started), Some(finished)) if finished >= started => finished - started,
            _ => elapsed.as_nanos(),
        }
    }

    pub const IO_URING_FLUSH_BENCHMARK_GROUP: &str = "io_uring/flush_semantics";

    pub const DEFAULT_IO_URING_FLUSH_BENCHMARK_WORKLOAD: IoUringFlushBenchmarkWorkload =
        IoUringFlushBenchmarkWorkload {
            file_size: 16 * 1024 * 1024,
            message_size: 4 * 1024,
            message_count: 64,
            flush_least_pages: 0,
        };

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
    pub enum IoUringFlushBenchmarkPath {
        DefaultMappedFileBaseline,
        IoUringExperimental,
    }

    impl IoUringFlushBenchmarkPath {
        pub const fn as_str(self) -> &'static str {
            match self {
                Self::DefaultMappedFileBaseline => "default_mapped_file_baseline",
                Self::IoUringExperimental => "io_uring_experimental",
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
    pub struct IoUringFlushBenchmarkWorkload {
        pub file_size: usize,
        pub message_size: usize,
        pub message_count: usize,
        pub flush_least_pages: i32,
    }

    impl IoUringFlushBenchmarkWorkload {
        pub const fn total_bytes(self) -> usize {
            self.message_size * self.message_count
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
    pub struct IoUringFlushBenchmarkCase {
        pub name: &'static str,
        pub path: IoUringFlushBenchmarkPath,
        pub workload: IoUringFlushBenchmarkWorkload,
    }

    pub fn io_uring_flush_benchmark_cases() -> [IoUringFlushBenchmarkCase; 2] {
        [
            IoUringFlushBenchmarkCase {
                name: "default_mapped_file_flush",
                path: IoUringFlushBenchmarkPath::DefaultMappedFileBaseline,
                workload: DEFAULT_IO_URING_FLUSH_BENCHMARK_WORKLOAD,
            },
            IoUringFlushBenchmarkCase {
                name: "io_uring_experimental_flush",
                path: IoUringFlushBenchmarkPath::IoUringExperimental,
                workload: DEFAULT_IO_URING_FLUSH_BENCHMARK_WORKLOAD,
            },
        ]
    }

    pub const PHASE5_PLATFORM_ACCEPTANCE_MIN_BENEFIT_PERCENT: f64 = 5.0;

    #[derive(Debug, Clone, Serialize)]
    pub struct Phase5PlatformDefaultPolicy {
        pub min_benefit_percent: f64,
        pub require_low_variance: bool,
        pub keep_unmeasured_disabled: bool,
        pub store_io_hint_enable_default: bool,
        pub store_lazy_mmap_enable_default: bool,
        pub rollout_rule: &'static str,
    }

    #[derive(Debug, Clone, Serialize)]
    pub struct Phase5CurrentPlatform {
        pub os: &'static str,
        pub arch: &'static str,
        pub io_hint_branch: &'static str,
        pub mmap_advice_supported: bool,
        pub file_prefetch_supported: bool,
        pub lazy_mmap_supported: bool,
        pub store_io_hint_enable_default: bool,
        pub store_lazy_mmap_enable_default: bool,
        pub effective_linux_recovery_fadvise_default: &'static str,
    }

    #[derive(Debug, Clone, Serialize)]
    pub struct Phase5PlatformAcceptanceScenario {
        pub id: &'static str,
        pub platform: &'static str,
        pub storage_medium: &'static str,
        pub page_cache_state: &'static str,
        pub optimization_paths: Vec<&'static str>,
        pub benchmark_scope: &'static str,
        pub measured_benefit_percent: Option<f64>,
        pub high_variance: bool,
        pub default_enabled: bool,
        pub conclusion: &'static str,
        pub not_applicable_reason: Option<&'static str>,
    }

    #[derive(Debug, Clone, Serialize)]
    pub struct Phase5PlatformOptimizationAcceptanceReport {
        pub default_policy: Phase5PlatformDefaultPolicy,
        pub current_platform: Phase5CurrentPlatform,
        pub recovery_correctness_commands: Vec<&'static str>,
        pub scenarios: Vec<Phase5PlatformAcceptanceScenario>,
    }

    pub fn phase5_platform_optimization_default_enabled(
        measured_benefit_percent: Option<f64>,
        high_variance: bool,
    ) -> bool {
        measured_benefit_percent
            .map(|benefit| benefit >= PHASE5_PLATFORM_ACCEPTANCE_MIN_BENEFIT_PERCENT && !high_variance)
            .unwrap_or(false)
    }

    pub fn phase5_platform_optimization_acceptance_report() -> Phase5PlatformOptimizationAcceptanceReport {
        let config = MessageStoreConfig::default();
        let capability = crate::platform::current_store_platform_capability();
        let default_policy = Phase5PlatformDefaultPolicy {
            min_benefit_percent: PHASE5_PLATFORM_ACCEPTANCE_MIN_BENEFIT_PERCENT,
            require_low_variance: true,
            keep_unmeasured_disabled: true,
            store_io_hint_enable_default: config.store_io_hint_enable,
            store_lazy_mmap_enable_default: config.store_lazy_mmap_enable,
            rollout_rule: "Enable per platform only after reproducible P50/P95/P99 recovery benchmarks show at least \
                           5% stable benefit and recovery correctness stays unchanged.",
        };
        let current_platform = Phase5CurrentPlatform {
            os: std::env::consts::OS,
            arch: std::env::consts::ARCH,
            io_hint_branch: capability.optimization.io_hint_branch.as_str(),
            mmap_advice_supported: capability.optimization.mmap_advice_supported,
            file_prefetch_supported: capability.optimization.file_prefetch_supported,
            lazy_mmap_supported: capability.optimization.lazy_mmap_supported,
            store_io_hint_enable_default: config.store_io_hint_enable,
            store_lazy_mmap_enable_default: config.store_lazy_mmap_enable,
            effective_linux_recovery_fadvise_default: config.effective_linux_recovery_fadvise().as_str(),
        };

        let scenario_specs = [
            (
                "linux_cold_boot_nvme",
                "linux",
                "nvme",
                "cold_boot",
                vec!["mmap_advice", "lazy_mmap"],
                "Run on Linux with cold page cache and NVMe-backed CommitLog files.",
                None,
                false,
                "No global benefit claim until target hardware benchmark artifact is attached.",
                None,
            ),
            (
                "linux_hot_page_cache",
                "linux",
                "nvme_or_ssd",
                "hot_page_cache",
                vec!["mmap_advice", "lazy_mmap"],
                "Run after priming page cache to detect low-benefit hot-cache behavior.",
                Some(0.0),
                false,
                "Hot page cache is expected to stay below the 5% gate; keep defaults disabled.",
                None,
            ),
            (
                "linux_general_ssd_cold_boot",
                "linux",
                "general_ssd",
                "cold_boot",
                vec!["mmap_advice", "lazy_mmap"],
                "Run on non-NVMe SSD to avoid projecting NVMe results to common SSD deployments.",
                None,
                false,
                "Requires separate SSD artifact; do not reuse NVMe conclusion.",
                None,
            ),
            (
                "windows_local_disk",
                "windows",
                "local_disk",
                "cold_or_warm_cache",
                vec!["prefetch_virtual_memory", "lazy_mmap"],
                "Run on Windows local disk because PrefetchVirtualMemory behavior is platform-specific.",
                None,
                false,
                "Windows benefit must be reported independently from Linux mmap advice.",
                None,
            ),
            (
                "unsupported_platform",
                "other",
                "not_applicable",
                "not_applicable",
                Vec::new(),
                "Unsupported platforms must record N/A instead of inheriting Linux or Windows results.",
                None,
                false,
                "No platform optimization path is enabled by default.",
                Some("No mmap advice, PrefetchVirtualMemory, or lazy mmap capability is advertised."),
            ),
        ];
        let scenarios = scenario_specs
            .into_iter()
            .map(
                |(
                    id,
                    platform,
                    storage_medium,
                    page_cache_state,
                    optimization_paths,
                    benchmark_scope,
                    measured_benefit_percent,
                    high_variance,
                    conclusion,
                    not_applicable_reason,
                )| Phase5PlatformAcceptanceScenario {
                    id,
                    platform,
                    storage_medium,
                    page_cache_state,
                    optimization_paths,
                    benchmark_scope,
                    measured_benefit_percent,
                    high_variance,
                    default_enabled: phase5_platform_optimization_default_enabled(
                        measured_benefit_percent,
                        high_variance,
                    ),
                    conclusion,
                    not_applicable_reason,
                },
            )
            .collect();

        Phase5PlatformOptimizationAcceptanceReport {
            default_policy,
            current_platform,
            recovery_correctness_commands: vec![
                "cargo test -p rocketmq-store lazy_mmap --lib",
                "cargo test -p rocketmq-store recovery_mmap --lib",
                "cargo test -p rocketmq-store recovery_file_prefetch --lib",
                "cargo test -p rocketmq-store --test commitlog_recovery_tests",
                "cargo clippy -p rocketmq-store --all-targets --all-features -- -D warnings",
            ],
            scenarios,
        }
    }

    #[cfg(feature = "rocksdb_store")]
    #[derive(Debug, Clone, Serialize)]
    pub struct StoreRocksDbMaintenanceLifecycleProbe {
        pub task_count_before_shutdown: usize,
        pub task_count_after_shutdown: usize,
        pub scheduled_runs: u64,
        pub scheduled_skips: u64,
        pub scheduled_overlaps: u64,
        pub scheduled_failures: u64,
        pub shutdown_elapsed_us: u128,
        pub shutdown_report: Option<ShutdownReport>,
        pub healthy: bool,
    }

    pub async fn run_store_blocking_io_probe(task_count: usize, work_duration: Duration) -> StoreBlockingIoProbe {
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let started_at = Instant::now();
        let runs = (0..task_count)
            .map(|_| {
                let active = Arc::clone(&active);
                let max_active = Arc::clone(&max_active);
                let submitted_at = Instant::now();
                crate::runtime::spawn_io("store blocking benchmark", move || {
                    let queue_wait_us = submitted_at.elapsed().as_micros();
                    let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                    max_active.fetch_max(current, Ordering::SeqCst);
                    std::thread::sleep(work_duration);
                    active.fetch_sub(1, Ordering::SeqCst);
                    queue_wait_us
                })
            })
            .collect::<Vec<_>>();

        let mut queue_waits = Vec::with_capacity(task_count);
        for result in join_all(runs).await {
            queue_waits.push(result.expect("store blocking benchmark task should complete"));
        }
        let elapsed_us = started_at.elapsed().as_micros();
        let snapshot = wait_for_store_blocking_idle().await;
        assert_eq!(snapshot.blocking_still_running, 0, "{snapshot:?}");
        assert!(snapshot.tasks.is_empty(), "{snapshot:?}");

        let mut shutdown_report = ShutdownReport::new("rocketmq-store.blocking", Duration::ZERO);
        shutdown_report.merge_blocking(snapshot.clone());
        let healthy = shutdown_report.is_healthy() && max_active.load(Ordering::SeqCst) <= snapshot.max_concurrency;

        StoreBlockingIoProbe {
            task_count,
            elapsed_us,
            max_active: max_active.load(Ordering::SeqCst),
            queue_wait_min_us: percentile_us(&queue_waits, 0),
            queue_wait_p50_us: percentile_us(&queue_waits, 50),
            queue_wait_p95_us: percentile_us(&queue_waits, 95),
            queue_wait_p99_us: percentile_us(&queue_waits, 99),
            queue_wait_max_us: percentile_us(&queue_waits, 100),
            snapshot,
            shutdown_report,
            healthy,
        }
    }

    async fn wait_for_store_blocking_idle() -> BlockingExecutorSnapshot {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        let mut snapshot = crate::runtime::blocking_snapshot().expect("store blocking snapshot should be available");

        while snapshot.blocking_still_running != 0 || !snapshot.tasks.is_empty() {
            if tokio::time::Instant::now() >= deadline {
                break;
            }

            tokio::time::sleep(Duration::from_millis(1)).await;
            snapshot = crate::runtime::blocking_snapshot().expect("store blocking snapshot should be available");
        }

        snapshot
    }

    pub async fn run_store_kv_compaction_lifecycle_probe() -> StoreKvCompactionLifecycleProbe {
        let compaction_store = Arc::new(CompactionStore::new());
        let topic = CheetahString::from_static_str("kv-compaction-lifecycle-topic");
        let key = CheetahString::from_static_str("same-key");
        compaction_store.put_message_with_key(&topic, 0, 0, 1, Some(key.clone()), Bytes::from_static(b"old-message"));
        compaction_store.put_message_with_key(&topic, 0, 1, 1, Some(key), Bytes::from_static(b"latest-message"));

        let mut service = CompactionService::new(compaction_store.clone(), 1);
        let _ = service.load(true, 0).await;
        compaction_store.finish_recovery(0);
        service.start();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        let compacted = loop {
            if compaction_store.message_count(&topic, 0) == 1 {
                break true;
            }
            if tokio::time::Instant::now() >= deadline {
                break false;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        };

        let mut snapshots = service.schedule_snapshot();
        for _ in 0..50 {
            if snapshots
                .iter()
                .any(|snapshot| snapshot.runs > 0 && snapshot.active_runs == 0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
            snapshots = service.schedule_snapshot();
        }
        let scheduled_runs = snapshots.iter().map(|snapshot| snapshot.runs).sum();
        let scheduled_skips = snapshots.iter().map(|snapshot| snapshot.skips).sum();
        let scheduled_overlaps = snapshots.iter().map(|snapshot| snapshot.overlaps).sum();
        let scheduled_failures = snapshots.iter().map(|snapshot| snapshot.failures).sum();
        let task_count_before_shutdown = service.task_count();
        let shutdown_started_at = Instant::now();
        let shutdown_report = service.shutdown_with_report().await;
        let shutdown_elapsed_us = shutdown_started_at.elapsed().as_micros();
        let task_count_after_shutdown = service.task_count();
        let shutdown_healthy = shutdown_report
            .as_ref()
            .map(ShutdownReport::is_healthy)
            .unwrap_or(false);
        let healthy = compacted
            && scheduled_runs > 0
            && scheduled_overlaps == 0
            && scheduled_failures == 0
            && task_count_before_shutdown > 0
            && task_count_after_shutdown == 0
            && shutdown_healthy;

        StoreKvCompactionLifecycleProbe {
            compacted,
            task_count_before_shutdown,
            task_count_after_shutdown,
            scheduled_runs,
            scheduled_skips,
            scheduled_overlaps,
            scheduled_failures,
            shutdown_elapsed_us,
            shutdown_report,
            healthy,
        }
    }

    pub async fn run_store_stats_service_lifecycle_probe() -> StoreStatsServiceLifecycleProbe {
        let service = Arc::new(StoreStatsService::new(None));
        service.start();

        let mut snapshots = service.schedule_snapshot();
        for _ in 0..50 {
            if snapshots
                .iter()
                .any(|snapshot| snapshot.runs > 0 && snapshot.active_runs == 0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
            snapshots = service.schedule_snapshot();
        }
        let scheduled_runs = snapshots.iter().map(|snapshot| snapshot.runs).sum();
        let scheduled_skips = snapshots.iter().map(|snapshot| snapshot.skips).sum();
        let scheduled_overlaps = snapshots.iter().map(|snapshot| snapshot.overlaps).sum();
        let scheduled_failures = snapshots.iter().map(|snapshot| snapshot.failures).sum();
        let snapshot_count = service.put_snapshot_count();
        let task_count_before_shutdown = service.task_count();
        let shutdown_started_at = Instant::now();
        let shutdown_report = service.shutdown_gracefully_with_report().await;
        let shutdown_elapsed_us = shutdown_started_at.elapsed().as_micros();
        let task_count_after_shutdown = service.task_count();
        let shutdown_healthy = shutdown_report
            .as_ref()
            .map(ShutdownReport::is_healthy)
            .unwrap_or(false);
        let healthy = snapshot_count > 0
            && scheduled_runs > 0
            && scheduled_overlaps == 0
            && scheduled_failures == 0
            && task_count_before_shutdown > 0
            && task_count_after_shutdown == 0
            && shutdown_healthy;

        StoreStatsServiceLifecycleProbe {
            task_count_before_shutdown,
            task_count_after_shutdown,
            scheduled_runs,
            scheduled_skips,
            scheduled_overlaps,
            scheduled_failures,
            snapshot_count,
            shutdown_elapsed_us,
            shutdown_report,
            healthy,
        }
    }

    pub async fn run_store_timer_scheduler_lifecycle_probe() -> StoreTimerSchedulerLifecycleProbe {
        let root = tempfile::tempdir().expect("timer scheduler benchmark root should be created");
        let config = Arc::new(MessageStoreConfig {
            store_path_root_dir: CheetahString::from_string(root.path().to_string_lossy().into_owned()),
            read_uncommitted: true,
            timer_precision_ms: 100,
            ..MessageStoreConfig::default()
        });
        let timer_store = Arc::new(TimerMessageStore::new_with_config(None, config));

        assert!(timer_store.load(), "timer store should load for lifecycle probe");
        timer_store.start();

        let mut snapshots = timer_store.scheduler_snapshot();
        for _ in 0..50 {
            if snapshots
                .iter()
                .any(|snapshot| snapshot.runs > 0 && snapshot.active_runs == 0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
            snapshots = timer_store.scheduler_snapshot();
        }
        let scheduled_runs = snapshots.iter().map(|snapshot| snapshot.runs).sum();
        let scheduled_skips = snapshots.iter().map(|snapshot| snapshot.skips).sum();
        let scheduled_overlaps = snapshots.iter().map(|snapshot| snapshot.overlaps).sum();
        let scheduled_failures = snapshots.iter().map(|snapshot| snapshot.failures).sum();
        let task_count_before_shutdown = timer_store.scheduler_task_count();
        let shutdown_started_at = Instant::now();
        let shutdown_report = timer_store.shutdown_gracefully_with_report().await;
        let shutdown_elapsed_us = shutdown_started_at.elapsed().as_micros();
        let task_count_after_shutdown = timer_store.scheduler_task_count();
        let shutdown_healthy = shutdown_report
            .as_ref()
            .map(ShutdownReport::is_healthy)
            .unwrap_or(false);
        let healthy = scheduled_runs > 0
            && scheduled_overlaps == 0
            && scheduled_failures == 0
            && task_count_before_shutdown > 0
            && task_count_after_shutdown == 0
            && shutdown_healthy;

        StoreTimerSchedulerLifecycleProbe {
            task_count_before_shutdown,
            task_count_after_shutdown,
            scheduled_runs,
            scheduled_skips,
            scheduled_overlaps,
            scheduled_failures,
            shutdown_elapsed_us,
            shutdown_report,
            healthy,
        }
    }

    pub async fn run_store_local_file_scheduled_lifecycle_probe() -> StoreLocalFileScheduledLifecycleProbe {
        let root = tempfile::tempdir().expect("local file store scheduled benchmark root should be created");
        let config = MessageStoreConfig {
            store_path_root_dir: CheetahString::from_string(root.path().to_string_lossy().into_owned()),
            clean_resource_interval: 1,
            ..MessageStoreConfig::default()
        };
        let mut store = new_legacy_shared_owner(
            Arc::new(config),
            Arc::new(BrokerConfig::default()),
            Arc::new(DashMap::<CheetahString, Arc<TopicConfig>>::new()),
            None,
            false,
        );
        store.init().await.expect("local file store benchmark should init");
        store.start().await.expect("local file store benchmark should start");

        let mut snapshots = store.scheduled_task_snapshot();
        for _ in 0..100 {
            if snapshots
                .iter()
                .any(|snapshot| snapshot.runs > 0 && snapshot.active_runs == 0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            snapshots = store.scheduled_task_snapshot();
        }

        let scheduled_runs = snapshots.iter().map(|snapshot| snapshot.runs).sum();
        let scheduled_skips = snapshots.iter().map(|snapshot| snapshot.skips).sum();
        let scheduled_overlaps = snapshots.iter().map(|snapshot| snapshot.overlaps).sum();
        let scheduled_failures = snapshots.iter().map(|snapshot| snapshot.failures).sum();
        let task_count_before_shutdown = store.scheduled_task_count();
        let shutdown_started_at = Instant::now();
        store.shutdown().await;
        let shutdown_elapsed_us = shutdown_started_at.elapsed().as_micros();
        let task_count_after_shutdown = store.scheduled_task_count();
        let healthy = snapshots.len() == 4
            && scheduled_runs > 0
            && scheduled_overlaps == 0
            && scheduled_failures == 0
            && task_count_before_shutdown >= 4
            && task_count_after_shutdown == 0;

        StoreLocalFileScheduledLifecycleProbe {
            task_count_before_shutdown,
            task_count_after_shutdown,
            scheduled_runs,
            scheduled_skips,
            scheduled_overlaps,
            scheduled_failures,
            shutdown_elapsed_us,
            healthy,
        }
    }

    #[cfg(feature = "rocksdb_store")]
    pub async fn run_store_rocksdb_maintenance_lifecycle_probe() -> StoreRocksDbMaintenanceLifecycleProbe {
        let root = tempfile::tempdir().expect("rocksdb maintenance benchmark root should be created");
        let config = RocksDbConfig {
            enabled: true,
            path: root.path().join("maintenance-db"),
            flush_interval_ms: 1,
            ..RocksDbConfig::default()
        };
        let store =
            Arc::new(RocksDbStore::open(config.clone()).expect("rocksdb maintenance benchmark store should open"));
        let mut service = RocksDbMaintenanceService::new(Arc::clone(&store), config);
        service.start();

        let mut snapshots = service.schedule_snapshot();
        for _ in 0..100 {
            if snapshots
                .iter()
                .any(|snapshot| snapshot.runs > 0 && snapshot.active_runs == 0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
            snapshots = service.schedule_snapshot();
        }

        let scheduled_runs = snapshots.iter().map(|snapshot| snapshot.runs).sum();
        let scheduled_skips = snapshots.iter().map(|snapshot| snapshot.skips).sum();
        let scheduled_overlaps = snapshots.iter().map(|snapshot| snapshot.overlaps).sum();
        let scheduled_failures = snapshots.iter().map(|snapshot| snapshot.failures).sum();
        let task_count_before_shutdown = service.task_count();
        let shutdown_started_at = Instant::now();
        let shutdown_report = service
            .shutdown_gracefully_with_report()
            .await
            .expect("rocksdb maintenance shutdown should complete");
        let shutdown_elapsed_us = shutdown_started_at.elapsed().as_micros();
        let task_count_after_shutdown = service.task_count();
        let shutdown_healthy = shutdown_report
            .as_ref()
            .map(ShutdownReport::is_healthy)
            .unwrap_or(false);
        let healthy = scheduled_runs > 0
            && scheduled_overlaps == 0
            && scheduled_failures == 0
            && task_count_before_shutdown > 0
            && task_count_after_shutdown == 0
            && shutdown_healthy;

        StoreRocksDbMaintenanceLifecycleProbe {
            task_count_before_shutdown,
            task_count_after_shutdown,
            scheduled_runs,
            scheduled_skips,
            scheduled_overlaps,
            scheduled_failures,
            shutdown_elapsed_us,
            shutdown_report,
            healthy,
        }
    }

    fn percentile_us(values: &[u128], percentile: usize) -> u128 {
        if values.is_empty() {
            return 0;
        }

        let mut sorted = values.to_vec();
        sorted.sort_unstable();
        let percentile = percentile.min(100);
        let rank = ((sorted.len() * percentile).saturating_add(99) / 100).saturating_sub(1);
        sorted[rank.min(sorted.len() - 1)]
    }
}

#[cfg(test)]
mod bench_support_tests {
    use std::time::Duration;

    use rocketmq_runtime::RuntimeContext;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn store_runtime_scope_parents_blocking_executor() {
        let context = RuntimeContext::from_current("store-runtime-scope-context-test");
        let service = context.service_context("store-service");
        let scope = super::runtime::StoreRuntimeScope::new(service.task_group().clone())
            .expect("store runtime scope should be created from parent task group");

        let value = scope
            .spawn_io("store.parented.blocking", || 7usize)
            .await
            .expect("parented store blocking task should complete");
        assert_eq!(value, 7);
        assert_eq!(scope.blocking_snapshot().blocking_still_running, 0);
        let child_group = scope.task_group("rocketmq-store.parented.child");
        assert_eq!(child_group.parent_id(), Some(service.task_group().id()));

        let report = service.task_group().shutdown(Duration::from_secs(1)).await;
        assert!(
            report
                .children
                .iter()
                .any(|child| child.name == "rocketmq-store.blocking"),
            "{}",
            report.to_json()
        );
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[cfg(feature = "rocksdb_store")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rocksdb_runtime_scope_parents_blocking_executor() {
        let context = RuntimeContext::from_current("rocksdb-runtime-scope-context-test");
        let service = context.service_context("rocksdb-service");
        let scope = super::rocksdb::runtime::RocksDbRuntimeScope::new(service.task_group().clone())
            .expect("rocksdb runtime scope should be created from parent task group");

        let value = scope
            .spawn_io("rocksdb.parented.blocking", || 11usize)
            .await
            .expect("parented rocksdb blocking task should complete");
        assert_eq!(value, 11);
        assert_eq!(scope.blocking_snapshot().blocking_still_running, 0);

        let child_group = scope.task_group("rocksdb.parented.child");
        assert_eq!(child_group.parent_id(), Some(service.task_group().id()));

        let report = service.task_group().shutdown(Duration::from_secs(1)).await;
        assert!(
            report
                .children
                .iter()
                .any(|child| child.name == "rocketmq-store.rocksdb.blocking"),
            "{}",
            report.to_json()
        );
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn store_blocking_io_probe_reports_no_running_tasks() {
        let probe = super::bench_support::run_store_blocking_io_probe(4, Duration::from_millis(1)).await;

        assert!(probe.healthy, "{probe:?}");
        assert_eq!(probe.snapshot.blocking_still_running, 0, "{probe:?}");
        assert!(probe.snapshot.tasks.is_empty(), "{probe:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn store_blocking_io_probe_waits_for_concurrent_store_tasks() {
        let (running_tx, running_rx) = tokio::sync::oneshot::channel();
        let in_flight = tokio::spawn(super::runtime::spawn_io("flush-consume-queue", move || {
            let _ = running_tx.send(());
            std::thread::sleep(Duration::from_millis(100));
        }));
        running_rx.await.expect("background store blocking task should start");

        let probe = super::bench_support::run_store_blocking_io_probe(1, Duration::from_millis(1)).await;
        in_flight
            .await
            .expect("background store blocking task should join")
            .expect("background store blocking task should complete");

        assert!(probe.healthy, "{probe:?}");
        assert_eq!(probe.snapshot.blocking_still_running, 0, "{probe:?}");
        assert!(probe.snapshot.tasks.is_empty(), "{probe:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn store_kv_compaction_lifecycle_probe_reports_clean_shutdown() {
        let probe = super::bench_support::run_store_kv_compaction_lifecycle_probe().await;

        assert!(probe.healthy, "{probe:?}");
        assert!(probe.compacted, "{probe:?}");
        assert_eq!(probe.task_count_after_shutdown, 0, "{probe:?}");
        assert_eq!(probe.scheduled_overlaps, 0, "{probe:?}");
        assert_eq!(probe.scheduled_failures, 0, "{probe:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn store_stats_service_lifecycle_probe_reports_clean_shutdown() {
        let probe = super::bench_support::run_store_stats_service_lifecycle_probe().await;

        assert!(probe.healthy, "{probe:?}");
        assert!(probe.snapshot_count > 0, "{probe:?}");
        assert_eq!(probe.task_count_after_shutdown, 0, "{probe:?}");
        assert_eq!(probe.scheduled_overlaps, 0, "{probe:?}");
        assert_eq!(probe.scheduled_failures, 0, "{probe:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn store_timer_scheduler_lifecycle_probe_reports_clean_shutdown() {
        let probe = super::bench_support::run_store_timer_scheduler_lifecycle_probe().await;

        assert!(probe.healthy, "{probe:?}");
        assert_eq!(probe.task_count_after_shutdown, 0, "{probe:?}");
        assert_eq!(probe.scheduled_overlaps, 0, "{probe:?}");
        assert_eq!(probe.scheduled_failures, 0, "{probe:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn store_local_file_scheduled_lifecycle_probe_reports_clean_shutdown() {
        let probe = super::bench_support::run_store_local_file_scheduled_lifecycle_probe().await;

        assert!(probe.healthy, "{probe:?}");
        assert_eq!(probe.task_count_after_shutdown, 0, "{probe:?}");
        assert_eq!(probe.scheduled_overlaps, 0, "{probe:?}");
        assert_eq!(probe.scheduled_failures, 0, "{probe:?}");
    }

    #[test]
    fn io_uring_flush_benchmark_cases_share_flush_manager_workload() {
        use super::bench_support::IoUringFlushBenchmarkPath;

        let cases = super::bench_support::io_uring_flush_benchmark_cases();
        assert_eq!(
            super::bench_support::IO_URING_FLUSH_BENCHMARK_GROUP,
            "io_uring/flush_semantics"
        );
        assert_eq!(cases.len(), 2);

        let baseline = cases
            .iter()
            .find(|case| case.path == IoUringFlushBenchmarkPath::DefaultMappedFileBaseline)
            .expect("default mapped-file baseline case should exist");
        let experimental = cases
            .iter()
            .find(|case| case.path == IoUringFlushBenchmarkPath::IoUringExperimental)
            .expect("io_uring experimental case should exist");

        assert_eq!(baseline.workload, experimental.workload);
        assert_eq!(baseline.workload.flush_least_pages, 0);
        assert_eq!(
            baseline.workload.total_bytes(),
            baseline.workload.message_size * baseline.workload.message_count
        );
        assert!(baseline.name.contains("default"));
        assert!(experimental.name.contains("io_uring"));
    }

    #[test]
    fn phase5_platform_acceptance_default_gate_requires_stable_benefit() {
        assert!(!super::bench_support::phase5_platform_optimization_default_enabled(
            None, false
        ));
        assert!(!super::bench_support::phase5_platform_optimization_default_enabled(
            Some(4.99),
            false
        ));
        assert!(!super::bench_support::phase5_platform_optimization_default_enabled(
            Some(8.0),
            true
        ));
        assert!(super::bench_support::phase5_platform_optimization_default_enabled(
            Some(5.0),
            false
        ));
    }

    #[test]
    fn phase5_platform_acceptance_report_covers_required_scenarios() {
        let report = super::bench_support::phase5_platform_optimization_acceptance_report();
        let scenario_ids = report
            .scenarios
            .iter()
            .map(|scenario| scenario.id)
            .collect::<std::collections::BTreeSet<_>>();

        assert!(scenario_ids.contains("linux_cold_boot_nvme"));
        assert!(scenario_ids.contains("linux_hot_page_cache"));
        assert!(scenario_ids.contains("linux_general_ssd_cold_boot"));
        assert!(scenario_ids.contains("windows_local_disk"));
        assert!(scenario_ids.contains("unsupported_platform"));
        assert!(report
            .scenarios
            .iter()
            .any(|scenario| scenario.page_cache_state == "hot_page_cache"));
        assert!(report
            .scenarios
            .iter()
            .any(|scenario| scenario.storage_medium == "nvme"));
        assert!(report
            .scenarios
            .iter()
            .any(|scenario| scenario.storage_medium == "general_ssd"));
    }

    #[test]
    fn phase5_platform_acceptance_report_keeps_unmeasured_defaults_disabled() {
        let report = super::bench_support::phase5_platform_optimization_acceptance_report();

        assert!(!report.default_policy.store_io_hint_enable_default);
        assert!(!report.default_policy.store_lazy_mmap_enable_default);
        assert!(report.default_policy.keep_unmeasured_disabled);
        assert_eq!(
            report.default_policy.min_benefit_percent,
            super::bench_support::PHASE5_PLATFORM_ACCEPTANCE_MIN_BENEFIT_PERCENT
        );
        assert!(report.scenarios.iter().all(|scenario| !scenario.default_enabled));
        assert!(report
            .recovery_correctness_commands
            .iter()
            .any(|command| command.contains("commitlog_recovery_tests")));
        assert!(!report.current_platform.store_io_hint_enable_default);
        assert!(!report.current_platform.store_lazy_mmap_enable_default);
    }

    #[tokio::test]
    async fn ha_bytes_vectored_benchmark_report_contains_required_comparison_data() {
        let report = super::bench_support::run_ha_bytes_vectored_benchmark_report(64 * 1024).await;

        assert!(report.frames_match, "{report:?}");
        assert!(report.ack_offsets_match, "{report:?}");
        assert_eq!(report.batch_count, 1);
        assert_eq!(report.bytes_baseline.engine, "bytes");
        assert_eq!(report.vectored_optimized.engine, "vectored");
        assert_eq!(report.bytes_baseline.body_bytes, 64 * 1024);
        assert_eq!(report.vectored_optimized.body_bytes, 64 * 1024);
        assert_eq!(report.bytes_baseline.frame_bytes, 64 * 1024 + 12);
        assert_eq!(report.vectored_optimized.frame_bytes, 64 * 1024 + 12);
        assert_eq!(report.bytes_baseline.write_syscall_count, 2);
        assert_eq!(report.vectored_optimized.write_syscall_count, 1);
        assert_eq!(report.syscall_reduction_percent, 50);
        assert!(report.bytes_baseline.ack_latency_nanos < 1_000_000_000);
        assert!(report.vectored_optimized.ack_latency_nanos < 1_000_000_000);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn ha_sendfile_large_message_benchmark_report_contains_cpu_comparison_data() {
        let report = super::bench_support::run_ha_vectored_sendfile_benchmark_report(1024 * 1024)
            .await
            .expect("sendfile benchmark report");

        assert!(report.frames_match, "{report:?}");
        assert!(report.ack_offsets_match, "{report:?}");
        assert_eq!(report.batch_count, 32);
        assert_eq!(report.vectored_baseline.engine, "vectored");
        assert_eq!(report.sendfile_optimized.engine, "sendfile");
        assert_eq!(report.vectored_baseline.body_bytes, 32 * 1024 * 1024);
        assert_eq!(report.sendfile_optimized.body_bytes, 32 * 1024 * 1024);
        assert_eq!(report.vectored_baseline.sendfile_syscall_count, 0);
        assert_eq!(report.sendfile_optimized.sendfile_syscall_count, 32);
        assert_eq!(report.sendfile_optimized.fallback_bytes, 0);
        assert!(report.vectored_baseline.user_cpu_nanos > 0, "{report:?}");
        assert!(report.sendfile_optimized.user_cpu_nanos > 0, "{report:?}");

        let expected_user_cpu_reduction_percent = report
            .vectored_baseline
            .user_cpu_nanos
            .saturating_sub(report.sendfile_optimized.user_cpu_nanos)
            .checked_mul(100)
            .and_then(|reduction| reduction.checked_div(report.vectored_baseline.user_cpu_nanos))
            .and_then(|percent| usize::try_from(percent).ok())
            .unwrap_or(0);
        assert_eq!(
            report.user_cpu_reduction_percent, expected_user_cpu_reduction_percent,
            "{report:?}"
        );
    }

    #[cfg(feature = "rocksdb_store")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn store_rocksdb_maintenance_lifecycle_probe_reports_clean_shutdown() {
        let probe = super::bench_support::run_store_rocksdb_maintenance_lifecycle_probe().await;

        assert!(probe.healthy, "{probe:?}");
        assert_eq!(probe.task_count_after_shutdown, 0, "{probe:?}");
        assert_eq!(probe.scheduled_overlaps, 0, "{probe:?}");
        assert_eq!(probe.scheduled_failures, 0, "{probe:?}");
    }
}
