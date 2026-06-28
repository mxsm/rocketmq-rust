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

use std::io::IoSlice;

use bytes::Bytes;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

use crate::ha::transfer_engine::batch_body_chunks;
use crate::ha::transfer_engine::write_zero_error;
use crate::ha::transfer_engine::TransferEngineKind;
use crate::ha::transfer_engine::TransferStats;
use crate::transfer::batch::TransferBatch;
use crate::transfer::error::TransferResult;

pub struct VectoredTransferEngine<W> {
    writer: W,
}

impl<W> VectoredTransferEngine<W> {
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    pub fn into_inner(self) -> W {
        self.writer
    }
}

impl<W> VectoredTransferEngine<W>
where
    W: AsyncWrite + Unpin,
{
    pub async fn send_batch(&mut self, batch: &TransferBatch) -> TransferResult<TransferStats> {
        let chunks = transfer_chunks(batch)?;
        let total_len = chunks.iter().map(Bytes::len).sum::<usize>();
        let mut bytes_written = 0;
        let mut write_call_count = 0;
        let mut partial_write_count = 0;
        let mut chunk_index = 0;
        let mut chunk_offset = 0;

        while chunk_index < chunks.len() {
            let remaining_before_write = total_len - bytes_written;
            let slices = io_slices(&chunks, chunk_index, chunk_offset);
            let written = self.writer.write_vectored(&slices).await?;
            if written == 0 {
                return Err(write_zero_error());
            }
            write_call_count += 1;
            bytes_written += written;
            if written < remaining_before_write {
                partial_write_count += 1;
            }
            advance_position(&chunks, &mut chunk_index, &mut chunk_offset, written);
        }

        self.writer.flush().await?;

        Ok(TransferStats {
            engine: TransferEngineKind::Vectored,
            bytes_written,
            body_bytes: batch.total_body_len,
            frame_count: 1,
            write_call_count,
            partial_write_count,
        })
    }
}

fn transfer_chunks(batch: &TransferBatch) -> TransferResult<Vec<Bytes>> {
    let body_chunks = batch_body_chunks(batch)?;
    let mut chunks = Vec::with_capacity(1 + body_chunks.len());
    if !batch.frame_header.is_empty() {
        chunks.push(batch.frame_header.clone());
    }
    chunks.extend(body_chunks.into_iter().filter(|chunk| !chunk.is_empty()));
    Ok(chunks)
}

fn io_slices(chunks: &[Bytes], chunk_index: usize, chunk_offset: usize) -> Vec<IoSlice<'_>> {
    let mut slices = Vec::with_capacity(chunks.len() - chunk_index);
    let first = &chunks[chunk_index][chunk_offset..];
    if !first.is_empty() {
        slices.push(IoSlice::new(first));
    }
    for chunk in &chunks[chunk_index + 1..] {
        if !chunk.is_empty() {
            slices.push(IoSlice::new(chunk));
        }
    }
    slices
}

fn advance_position(chunks: &[Bytes], chunk_index: &mut usize, chunk_offset: &mut usize, mut written: usize) {
    while written > 0 && *chunk_index < chunks.len() {
        let remaining_in_chunk = chunks[*chunk_index].len() - *chunk_offset;
        if written < remaining_in_chunk {
            *chunk_offset += written;
            return;
        }
        written -= remaining_in_chunk;
        *chunk_index += 1;
        *chunk_offset = 0;
    }
}
