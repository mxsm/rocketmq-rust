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

use rocketmq_sre_contracts::KnowledgeChunkId;
use sha2::Digest;
use sha2::Sha256;

use super::model::KnowledgeChunkDraft;
use crate::ControlPlaneError;

const MAX_CHUNK_BYTES: usize = 8 * 1024;
const MAX_CHUNKS: usize = 256;

pub(super) fn chunk_markdown(markdown: &str) -> Result<Vec<KnowledgeChunkDraft>, ControlPlaneError> {
    let mut chunks = Vec::new();
    let mut heading = None;
    let mut content = String::new();

    for line in markdown.lines() {
        if let Some(next_heading) = markdown_heading(line) {
            flush(&mut chunks, &heading, &mut content)?;
            heading = Some(next_heading.to_owned());
            continue;
        }
        for segment in bounded_segments(line, MAX_CHUNK_BYTES) {
            let extra = usize::from(!content.is_empty()) + segment.len();
            if !content.is_empty() && content.len() + extra > MAX_CHUNK_BYTES {
                flush(&mut chunks, &heading, &mut content)?;
            }
            if !content.is_empty() {
                content.push('\n');
            }
            content.push_str(segment);
        }
    }
    flush(&mut chunks, &heading, &mut content)?;
    if chunks.is_empty() {
        return Err(ControlPlaneError::validation(
            "invalid_request",
            "knowledge Markdown must contain searchable text",
        ));
    }
    Ok(chunks)
}

fn markdown_heading(line: &str) -> Option<&str> {
    let trimmed = line.trim_start();
    let marker_len = trimmed.bytes().take_while(|byte| *byte == b'#').count();
    if !(1..=6).contains(&marker_len) {
        return None;
    }
    trimmed
        .get(marker_len..)
        .and_then(|value| value.strip_prefix(' '))
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn bounded_segments(mut value: &str, max_bytes: usize) -> Vec<&str> {
    let mut segments = Vec::new();
    while value.len() > max_bytes {
        let mut end = max_bytes;
        while !value.is_char_boundary(end) {
            end -= 1;
        }
        segments.push(&value[..end]);
        value = &value[end..];
    }
    segments.push(value);
    segments
}

fn flush(
    chunks: &mut Vec<KnowledgeChunkDraft>,
    heading: &Option<String>,
    content: &mut String,
) -> Result<(), ControlPlaneError> {
    let trimmed = content.trim();
    if trimmed.is_empty() {
        content.clear();
        return Ok(());
    }
    if chunks.len() >= MAX_CHUNKS {
        return Err(ControlPlaneError::validation(
            "output_too_large",
            "knowledge document exceeds the maximum chunk count",
        ));
    }
    let ordinal = i32::try_from(chunks.len())
        .map_err(|_| ControlPlaneError::validation("output_too_large", "knowledge chunk count is too large"))?;
    let hash_material = format!("{}\n{trimmed}", heading.as_deref().unwrap_or_default());
    chunks.push(KnowledgeChunkDraft {
        id: KnowledgeChunkId::new(),
        ordinal,
        heading: heading.clone(),
        content: trimmed.to_owned(),
        content_hash: format!(
            "sha256:{}",
            rocketmq_sre_contracts::encode_lower_hex(Sha256::digest(hash_material.as_bytes()))
        ),
    });
    content.clear();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunks_by_heading_and_preserves_source_hashes() {
        let chunks = chunk_markdown("# Broker\ncheck broker_up\n## Store\ncheck store health").expect("chunks");
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].heading.as_deref(), Some("Broker"));
        assert!(chunks.iter().all(|chunk| chunk.content_hash.starts_with("sha256:")));
    }
}
