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

//! Terminal cell width measurement and width-aware slicing.

use std::borrow::Cow;

use ratatui::text::Span;

pub(crate) const ELLIPSIS: char = '…';

/// Returns the number of terminal cells occupied by `character`.
pub(crate) fn char_width(character: char) -> usize {
    if character.is_ascii() {
        return usize::from(!character.is_ascii_control());
    }
    let mut buffer = [0_u8; 4];
    Span::raw(&*character.encode_utf8(&mut buffer)).width()
}

/// Returns the number of terminal cells occupied by `text`.
pub(crate) fn display_width(text: &str) -> usize {
    if text.is_ascii() {
        text.bytes().filter(|byte| !byte.is_ascii_control()).count()
    } else {
        text.chars().map(char_width).sum()
    }
}

/// Shortens `text` to at most `max_width` cells, marking the cut with an ellipsis.
pub(crate) fn truncate(text: &str, max_width: usize) -> Cow<'_, str> {
    if display_width(text) <= max_width {
        return Cow::Borrowed(text);
    }
    if max_width == 0 {
        return Cow::Borrowed("");
    }

    let budget = max_width - 1;
    let mut used = 0;
    let mut truncated = String::new();
    for character in text.chars() {
        let width = char_width(character);
        if used + width > budget {
            break;
        }
        used += width;
        truncated.push(character);
    }
    truncated.push(ELLIPSIS);
    Cow::Owned(truncated)
}

/// Returns the part of `text` that covers cell columns `start..start + width`.
///
/// A wide character that straddles either edge is left out rather than split.
pub(crate) fn slice_columns(text: &str, start: usize, width: usize) -> &str {
    let end = start.saturating_add(width);
    let mut column = 0;
    let mut first_byte = None;
    let mut last_byte = text.len();
    for (index, character) in text.char_indices() {
        let next = column + char_width(character);
        if first_byte.is_none() && column >= start {
            first_byte = Some(index);
        }
        if next > end {
            last_byte = index;
            break;
        }
        column = next;
    }
    match first_byte {
        Some(first) if first <= last_byte => &text[first..last_byte],
        _ => "",
    }
}

/// Breaks `text` into rows of at most `width` cells and reports each as a byte range.
///
/// Rows end after the last whitespace that fits; a word longer than a row is split
/// where the row runs out. Empty text yields one empty row.
pub(crate) fn wrap(text: &str, width: usize, mut emit: impl FnMut(usize, usize)) {
    let width = width.max(1);
    let mut start = 0;
    let mut used = 0;
    let mut break_at = None;
    for (index, character) in text.char_indices() {
        let cells = char_width(character);
        while used > 0 && used + cells > width {
            let end = match break_at {
                Some(position) if position > start => position,
                _ => index,
            };
            emit(start, end);
            used = display_width(&text[end..index]);
            start = end;
            break_at = None;
        }
        used += cells;
        if character.is_whitespace() {
            break_at = Some(index + character.len_utf8());
        }
    }
    emit(start, text.len());
}

/// Returns the byte offset of the character at `char_index`, or the text length past the end.
pub(crate) fn byte_offset(text: &str, char_index: usize) -> usize {
    text.char_indices()
        .nth(char_index)
        .map_or(text.len(), |(offset, _)| offset)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn widths_count_cells_rather_than_characters() {
        assert_eq!(char_width('a'), 1);
        assert_eq!(char_width('\n'), 0);
        assert_eq!(char_width('主'), 2);
        assert_eq!(display_width("topic"), 5);
        assert_eq!(display_width("主题A"), 5);
        assert_eq!(display_width("a\tb"), 2);
    }

    #[test]
    fn truncate_keeps_short_text_and_marks_cut_text() {
        assert_eq!(truncate("short", 10), "short");
        assert_eq!(truncate("abcdefghijkl", 8), "abcdefg…");
        assert_eq!(truncate("主题主题主题", 5), "主题…");
        assert_eq!(truncate("abc", 0), "");
        assert_eq!(truncate("abc", 1), "…");
    }

    #[test]
    fn slice_columns_returns_the_requested_cell_window() {
        assert_eq!(slice_columns("abcdefgh", 2, 3), "cde");
        assert_eq!(slice_columns("abcdefgh", 6, 10), "gh");
        assert_eq!(slice_columns("abc", 9, 4), "");
        assert_eq!(slice_columns("abc", 0, 0), "");
        // A wide character is dropped instead of being split at either edge.
        assert_eq!(slice_columns("主题ab", 1, 4), "题a");
        assert_eq!(slice_columns("ab主题", 0, 3), "ab");
    }

    fn wrapped(text: &str, width: usize) -> Vec<&str> {
        let mut rows = Vec::new();
        wrap(text, width, |start, end| rows.push(&text[start..end]));
        rows
    }

    #[test]
    fn wrap_prefers_whitespace_and_splits_long_words() {
        assert_eq!(wrapped("alpha beta gamma", 8), ["alpha ", "beta ", "gamma"]);
        assert_eq!(wrapped("abcdefghij", 8), ["abcdefgh", "ij"]);
        assert_eq!(wrapped("", 8), [""]);
        assert_eq!(wrapped("主题主题主", 5), ["主题", "主题", "主"]);
        // A character wider than the row still makes progress.
        assert_eq!(wrapped("主题", 1), ["主", "题"]);
        assert_eq!(wrapped("ab", 0), ["a", "b"]);
    }

    #[test]
    fn byte_offset_maps_character_indices_and_saturates() {
        assert_eq!(byte_offset("a主b", 0), 0);
        assert_eq!(byte_offset("a主b", 1), 1);
        assert_eq!(byte_offset("a主b", 2), 4);
        assert_eq!(byte_offset("a主b", 9), 5);
    }
}
