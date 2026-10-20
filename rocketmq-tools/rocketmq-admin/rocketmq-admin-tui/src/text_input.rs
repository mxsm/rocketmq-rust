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

//! Single-line text editing with a movable cursor.
//!
//! The cursor is a character index in `0..=len`. Callers own the text and the
//! cursor, so one implementation serves every input on screen.

use crate::text::byte_offset;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TextEdit {
    Insert(char),
    InsertText(String),
    Backspace,
    Delete,
    /// Deletes the word before the cursor together with its trailing whitespace.
    DeleteWord,
    ClearBefore,
    ClearAfter,
    Left,
    Right,
    WordLeft,
    WordRight,
    Home,
    End,
}

/// Applies `edit` at `cursor` and returns whether `text` changed.
///
/// `cursor` is clamped to the text before the edit and always left in `0..=len`.
pub(crate) fn apply(text: &mut String, cursor: &mut usize, edit: TextEdit) -> bool {
    let length = text.chars().count();
    let position = (*cursor).min(length);
    *cursor = position;

    match edit {
        TextEdit::Insert(character) => {
            text.insert(byte_offset(text, position), character);
            *cursor = position + 1;
            true
        }
        TextEdit::InsertText(inserted) => {
            if inserted.is_empty() {
                return false;
            }
            text.insert_str(byte_offset(text, position), &inserted);
            *cursor = position + inserted.chars().count();
            true
        }
        TextEdit::Backspace => remove(text, cursor, position.saturating_sub(1), position),
        TextEdit::Delete => remove(text, cursor, position, (position + 1).min(length)),
        TextEdit::DeleteWord => {
            let start = word_start(text, position);
            remove(text, cursor, start, position)
        }
        TextEdit::ClearBefore => remove(text, cursor, 0, position),
        TextEdit::ClearAfter => remove(text, cursor, position, length),
        TextEdit::Left => {
            *cursor = position.saturating_sub(1);
            false
        }
        TextEdit::Right => {
            *cursor = (position + 1).min(length);
            false
        }
        TextEdit::WordLeft => {
            *cursor = word_start(text, position);
            false
        }
        TextEdit::WordRight => {
            *cursor = word_end(text, position);
            false
        }
        TextEdit::Home => {
            *cursor = 0;
            false
        }
        TextEdit::End => {
            *cursor = length;
            false
        }
    }
}

fn remove(text: &mut String, cursor: &mut usize, start: usize, end: usize) -> bool {
    if start >= end {
        return false;
    }
    text.replace_range(byte_offset(text, start)..byte_offset(text, end), "");
    *cursor = start;
    true
}

fn word_start(text: &str, cursor: usize) -> usize {
    let characters = text.chars().collect::<Vec<_>>();
    let mut index = cursor.min(characters.len());
    while index > 0 && characters[index - 1].is_whitespace() {
        index -= 1;
    }
    while index > 0 && !characters[index - 1].is_whitespace() {
        index -= 1;
    }
    index
}

fn word_end(text: &str, cursor: usize) -> usize {
    let characters = text.chars().collect::<Vec<_>>();
    let mut index = cursor.min(characters.len());
    while index < characters.len() && characters[index].is_whitespace() {
        index += 1;
    }
    while index < characters.len() && !characters[index].is_whitespace() {
        index += 1;
    }
    index
}

/// Flattens pasted text to a single line.
///
/// Line breaks become `separator` so a multi-line clipboard can never act as the
/// Enter key; other control characters are dropped.
pub(crate) fn single_line(pasted: &str, separator: &str) -> String {
    let mut flattened = String::with_capacity(pasted.len());
    for (index, line) in pasted
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .enumerate()
    {
        if index > 0 {
            flattened.push_str(separator);
        }
        flattened.extend(line.chars().filter(|character| !character.is_control()));
    }
    flattened
}

#[cfg(test)]
mod tests {
    use super::*;

    fn edited(text: &str, cursor: usize, edit: TextEdit) -> (String, usize, bool) {
        let mut text = text.to_string();
        let mut cursor = cursor;
        let changed = apply(&mut text, &mut cursor, edit);
        (text, cursor, changed)
    }

    #[test]
    fn insert_places_characters_at_the_cursor() {
        assert_eq!(edited("ac", 1, TextEdit::Insert('b')), ("abc".to_string(), 2, true));
        assert_eq!(edited("主A", 1, TextEdit::Insert('题')), ("主题A".to_string(), 2, true));
        assert_eq!(
            edited("ad", 1, TextEdit::InsertText("bc".to_string())),
            ("abcd".to_string(), 3, true)
        );
        assert_eq!(
            edited("ad", 1, TextEdit::InsertText(String::new())),
            ("ad".to_string(), 1, false)
        );
    }

    #[test]
    fn an_out_of_range_cursor_is_clamped_before_editing() {
        assert_eq!(edited("ab", 99, TextEdit::Insert('c')), ("abc".to_string(), 3, true));
        assert_eq!(edited("ab", 99, TextEdit::Left), ("ab".to_string(), 1, false));
    }

    #[test]
    fn backspace_and_delete_remove_one_character_on_each_side() {
        assert_eq!(edited("abc", 2, TextEdit::Backspace), ("ac".to_string(), 1, true));
        assert_eq!(edited("abc", 0, TextEdit::Backspace), ("abc".to_string(), 0, false));
        assert_eq!(edited("abc", 1, TextEdit::Delete), ("ac".to_string(), 1, true));
        assert_eq!(edited("abc", 3, TextEdit::Delete), ("abc".to_string(), 3, false));
        assert_eq!(edited("主题", 2, TextEdit::Backspace), ("主".to_string(), 1, true));
    }

    #[test]
    fn clearing_removes_everything_on_one_side_of_the_cursor() {
        assert_eq!(
            edited("abcdef", 2, TextEdit::ClearBefore),
            ("cdef".to_string(), 0, true)
        );
        assert_eq!(edited("abcdef", 2, TextEdit::ClearAfter), ("ab".to_string(), 2, true));
        assert_eq!(edited("abc", 0, TextEdit::ClearBefore), ("abc".to_string(), 0, false));
    }

    #[test]
    fn word_edits_stop_at_whitespace_boundaries() {
        assert_eq!(
            edited("topic one  two", 14, TextEdit::DeleteWord),
            ("topic one  ".to_string(), 11, true)
        );
        assert_eq!(
            edited("topic one  ", 11, TextEdit::DeleteWord),
            ("topic ".to_string(), 6, true)
        );
        assert_eq!(edited("topic one", 9, TextEdit::WordLeft).1, 6);
        assert_eq!(edited("topic one", 0, TextEdit::WordRight).1, 5);
        assert_eq!(edited("topic one", 5, TextEdit::WordRight).1, 9);
    }

    #[test]
    fn cursor_moves_stay_inside_the_text() {
        assert_eq!(edited("ab", 0, TextEdit::Left).1, 0);
        assert_eq!(edited("ab", 2, TextEdit::Right).1, 2);
        assert_eq!(edited("ab", 1, TextEdit::Home).1, 0);
        assert_eq!(edited("ab", 1, TextEdit::End).1, 2);
    }

    #[test]
    fn pasted_text_is_flattened_to_one_line() {
        assert_eq!(single_line("TopicA\n", "; "), "TopicA");
        assert_eq!(single_line("a=1\r\nb=2\n\n", "; "), "a=1; b=2");
        assert_eq!(single_line("  one\ttab \n two ", " "), "onetab two");
        assert_eq!(single_line("\n\n", " "), "");
    }
}
