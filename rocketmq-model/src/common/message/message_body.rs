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

use bytes::Bytes;

/// Represents the message body, handling both raw and compressed forms.
#[derive(Clone, Debug, Default)]
pub struct MessageBody {
    /// The uncompressed message body data.
    raw: Option<Bytes>,
    /// The compressed message body data (if compression is enabled).
    compressed: Option<Bytes>,
}

impl MessageBody {
    /// Creates a new message body from bytes.
    #[inline]
    pub fn new(data: impl Into<Bytes>) -> Self {
        Self {
            raw: Some(data.into()),
            compressed: None,
        }
    }

    /// Creates an empty message body.
    #[inline]
    pub const fn empty() -> Self {
        Self {
            raw: None,
            compressed: None,
        }
    }

    /// Creates a message body with both raw and compressed data.
    #[inline]
    pub fn from_compressed(raw: Bytes, compressed: Bytes) -> Self {
        Self {
            raw: Some(raw),
            compressed: Some(compressed),
        }
    }

    /// Returns the raw (uncompressed) body as slice.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        self.raw.as_ref().map(|b| &b[..]).unwrap_or(&[])
    }

    /// Returns the raw body as Option<&Bytes>.
    #[inline]
    pub fn raw(&self) -> Option<&Bytes> {
        self.raw.as_ref()
    }

    /// Returns a mutable reference to the raw body.
    #[inline]
    pub fn raw_mut(&mut self) -> &mut Option<Bytes> {
        &mut self.raw
    }

    /// Returns the compressed body, if available.
    #[inline]
    pub fn compressed(&self) -> Option<&Bytes> {
        self.compressed.as_ref()
    }

    /// Returns a mutable reference to the compressed body.
    #[inline]
    pub fn compressed_mut(&mut self) -> &mut Option<Bytes> {
        &mut self.compressed
    }

    /// Sets the compressed body.
    #[inline]
    pub(crate) fn set_compressed(&mut self, compressed: Bytes) {
        self.compressed = Some(compressed);
    }

    /// Returns whether the body is compressed.
    #[inline]
    pub fn is_compressed(&self) -> bool {
        self.compressed.is_some()
    }

    /// Consumes and returns the raw bytes.
    #[inline]
    pub fn into_bytes(self) -> Bytes {
        self.raw.unwrap_or_default()
    }

    /// Returns the size in bytes (raw body).
    #[inline]
    pub fn len(&self) -> usize {
        self.raw.as_ref().map_or(0, |b| b.len())
    }

    /// Returns true if the body is empty (both raw and compressed are None or empty).
    #[inline]
    pub fn is_empty(&self) -> bool {
        let raw_empty = self.raw.as_ref().is_none_or(|b| b.is_empty());
        let compressed_empty = self.compressed.as_ref().is_none_or(|b| b.is_empty());
        raw_empty && compressed_empty
    }
}

impl AsRef<[u8]> for MessageBody {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl From<Vec<u8>> for MessageBody {
    fn from(data: Vec<u8>) -> Self {
        Self::new(data)
    }
}

impl From<Bytes> for MessageBody {
    fn from(data: Bytes) -> Self {
        Self::new(data)
    }
}

impl From<&[u8]> for MessageBody {
    fn from(data: &[u8]) -> Self {
        Self::new(Bytes::copy_from_slice(data))
    }
}
