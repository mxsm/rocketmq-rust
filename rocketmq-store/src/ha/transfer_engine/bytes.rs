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

use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

use crate::ha::transfer_engine::batch_body_bytes;
use crate::ha::transfer_engine::write_zero_error;
use crate::ha::transfer_engine::TransferEngineKind;
use crate::ha::transfer_engine::TransferStats;
use crate::transfer::batch::TransferBatch;
use crate::transfer::error::TransferResult;

pub struct BytesTransferEngine<W> {
    writer: W,
}

impl<W> BytesTransferEngine<W> {
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    pub fn into_inner(self) -> W {
        self.writer
    }
}

impl<W> BytesTransferEngine<W>
where
    W: AsyncWrite + Unpin,
{
    pub async fn send_batch(&mut self, batch: &TransferBatch) -> TransferResult<TransferStats> {
        let body = batch_body_bytes(batch)?;
        let expected_write_calls = usize::from(!batch.frame_header.is_empty()) + usize::from(!body.is_empty());
        let mut bytes_written = 0;
        let mut write_call_count = 0;

        let (header_written, header_calls) = write_all_counted(&mut self.writer, &batch.frame_header).await?;
        bytes_written += header_written;
        write_call_count += header_calls;

        let (body_written, body_calls) = write_all_counted(&mut self.writer, &body).await?;
        bytes_written += body_written;
        write_call_count += body_calls;

        self.writer.flush().await?;

        Ok(TransferStats {
            engine: TransferEngineKind::Bytes,
            bytes_written,
            body_bytes: batch.total_body_len,
            frame_count: 1,
            write_call_count,
            sendfile_call_count: 0,
            sendfile_bytes: 0,
            fallback_bytes: 0,
            partial_write_count: write_call_count.saturating_sub(expected_write_calls),
        })
    }
}

pub(crate) async fn write_all_counted<W>(writer: &mut W, mut bytes: &[u8]) -> TransferResult<(usize, usize)>
where
    W: AsyncWrite + Unpin,
{
    let mut bytes_written = 0;
    let mut write_call_count = 0;

    while !bytes.is_empty() {
        let written = writer.write(bytes).await?;
        if written == 0 {
            return Err(write_zero_error());
        }
        bytes = &bytes[written..];
        bytes_written += written;
        write_call_count += 1;
    }

    Ok((bytes_written, write_call_count))
}
