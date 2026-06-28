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

use ::bytes::Bytes;
use tokio::io::AsyncWrite;

use crate::ha::transfer_engine::bytes::BytesTransferEngine;
use crate::ha::transfer_engine::vectored::VectoredTransferEngine;
use crate::transfer::batch::TransferBatch;
use crate::transfer::error::TransferError;
use crate::transfer::error::TransferResult;

pub mod bytes;
pub mod sendfile;
pub mod vectored;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferEngineKind {
    Bytes,
    Vectored,
    Sendfile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferEnginePreference {
    Bytes,
    Vectored,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferEngineSelection {
    pub engine: TransferEngineKind,
    pub fallback_reason: Option<&'static str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransferStats {
    pub engine: TransferEngineKind,
    pub bytes_written: usize,
    pub body_bytes: usize,
    pub frame_count: usize,
    pub write_call_count: usize,
    pub sendfile_call_count: usize,
    pub sendfile_bytes: usize,
    pub fallback_bytes: usize,
    pub partial_write_count: usize,
}

pub fn select_transfer_engine(
    preference: TransferEnginePreference,
    vectored_write_available: bool,
) -> TransferEngineSelection {
    match (preference, vectored_write_available) {
        (TransferEnginePreference::Bytes, _) => TransferEngineSelection {
            engine: TransferEngineKind::Bytes,
            fallback_reason: None,
        },
        (TransferEnginePreference::Vectored, true) => TransferEngineSelection {
            engine: TransferEngineKind::Vectored,
            fallback_reason: None,
        },
        (TransferEnginePreference::Vectored, false) => TransferEngineSelection {
            engine: TransferEngineKind::Bytes,
            fallback_reason: Some("vectored write unavailable"),
        },
    }
}

pub enum HaTransferEngine<W> {
    Bytes(BytesTransferEngine<W>),
    Vectored(VectoredTransferEngine<W>),
}

impl<W> HaTransferEngine<W> {
    pub fn from_selection(writer: W, engine: TransferEngineKind) -> Self {
        match engine {
            TransferEngineKind::Bytes => Self::Bytes(BytesTransferEngine::new(writer)),
            TransferEngineKind::Vectored | TransferEngineKind::Sendfile => {
                Self::Vectored(VectoredTransferEngine::new(writer))
            }
        }
    }

    pub fn into_inner(self) -> W {
        match self {
            Self::Bytes(engine) => engine.into_inner(),
            Self::Vectored(engine) => engine.into_inner(),
        }
    }

    pub fn kind(&self) -> TransferEngineKind {
        match self {
            Self::Bytes(_) => TransferEngineKind::Bytes,
            Self::Vectored(_) => TransferEngineKind::Vectored,
        }
    }
}

impl<W> HaTransferEngine<W>
where
    W: AsyncWrite + Unpin,
{
    pub async fn send_batch(&mut self, batch: &TransferBatch) -> TransferResult<TransferStats> {
        match self {
            Self::Bytes(engine) => engine.send_batch(batch).await,
            Self::Vectored(engine) => engine.send_batch(batch).await,
        }
    }
}

pub(crate) fn batch_body_bytes(batch: &TransferBatch) -> TransferResult<Bytes> {
    let mut chunks = batch_body_chunks(batch)?;
    match chunks.len() {
        0 => Ok(Bytes::new()),
        1 => Ok(chunks.remove(0)),
        _ => {
            let mut body = Vec::with_capacity(batch.total_body_len);
            for chunk in chunks {
                body.extend_from_slice(&chunk);
            }
            Ok(Bytes::from(body))
        }
    }
}

pub(crate) fn batch_body_chunks(batch: &TransferBatch) -> TransferResult<Vec<Bytes>> {
    let mut remaining = batch.total_body_len;
    if remaining == 0 {
        return Ok(Vec::new());
    }

    let mut chunks = Vec::with_capacity(batch.segments.len());
    for segment in &batch.segments {
        let bytes = segment.as_bytes().ok_or(TransferError::UnsupportedSegmentSource(
            "byte-backed segment required for bytes or vectored HA transfer",
        ))?;
        let len = bytes.len().min(remaining);
        if len > 0 {
            chunks.push(bytes.slice(..len));
            remaining -= len;
        }
        if remaining == 0 {
            break;
        }
    }

    if remaining != 0 {
        return Err(TransferError::InvalidInput(format!(
            "transfer batch body underflow: expected {} bytes, missing {} bytes",
            batch.total_body_len, remaining
        )));
    }

    Ok(chunks)
}

pub(crate) fn write_zero_error() -> TransferError {
    TransferError::Io(std::io::Error::new(
        std::io::ErrorKind::WriteZero,
        "transfer writer returned zero bytes",
    ))
}
