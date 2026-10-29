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

//! Full-value CSV generation for one applied DLQ query page.

use std::fmt;
pub const DLQ_CSV_MAX_BYTES: usize = 8 * 1024 * 1024;

/// Full values, never a Message preview. Deliberately neither Debug nor serializable.
pub struct DlqCsvRow {
    pub message_id: String,
    pub topic: String,
    pub keys: String,
    pub tags: String,
    pub store_timestamp: i64,
    pub queue_id: i32,
    pub queue_offset: i64,
    pub body: String,
}
impl Drop for DlqCsvRow {
    fn drop(&mut self) {
        self.body.clear();
    }
}

pub struct DlqCsv(String);
impl fmt::Debug for DlqCsv {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DlqCsv").field("bytes", &self.0.len()).finish()
    }
}
impl Drop for DlqCsv {
    fn drop(&mut self) {
        self.0.clear();
    }
}
impl DlqCsv {
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
    /// Generates complete values or fails before a file is written.
    ///
    /// # Errors
    /// Returns `TooLarge` if the escaped page would exceed the 8 MiB export bound.
    pub fn from_rows(rows: Vec<DlqCsvRow>) -> Result<Self, DlqCsvError> {
        let mut csv = Self("messageId,topic,keys,tags,storeTimestamp,queueId,queueOffset,body\n".into());
        for row in &rows {
            for (index, cell) in [
                row.message_id.as_str(),
                row.topic.as_str(),
                row.keys.as_str(),
                row.tags.as_str(),
                &row.store_timestamp.to_string(),
                &row.queue_id.to_string(),
                &row.queue_offset.to_string(),
                row.body.as_str(),
            ]
            .into_iter()
            .enumerate()
            {
                if index > 0 {
                    csv.push(",")?;
                }
                let neutralize = matches!(cell.chars().next(), Some('=' | '+' | '-' | '@' | '\t' | '\r' | '\n'));
                let quoted = cell.contains([',', '"', '\r', '\n']);
                if quoted {
                    csv.push("\"")?;
                }
                if neutralize {
                    csv.push("'")?;
                }
                for part in cell.split_inclusive('"') {
                    csv.push(part)?;
                    if part.ends_with('"') {
                        csv.push("\"")?;
                    }
                }
                if quoted {
                    csv.push("\"")?;
                }
            }
            csv.push("\n")?;
        }
        Ok(csv)
    }
    fn push(&mut self, value: &str) -> Result<(), DlqCsvError> {
        if value.len() > DLQ_CSV_MAX_BYTES.saturating_sub(self.0.len()) {
            return Err(DlqCsvError::TooLarge);
        }
        self.0.push_str(value);
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum DlqCsvError {
    #[error("The full CSV page exceeds 8 MiB. Reduce the page size and export again.")]
    TooLarge,
    #[error("The page contains a non-UTF-8 Body, which cannot be exported as full CSV text.")]
    NonUtf8,
}
impl DlqCsvError {
    pub const fn summary(self) -> &'static str {
        match self {
            Self::TooLarge => "The full CSV page exceeds 8 MiB. Reduce the page size and export again.",
            Self::NonUtf8 => "The page contains a non-UTF-8 Body, which cannot be exported as full CSV text.",
        }
    }
}
