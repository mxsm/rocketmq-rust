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

use ratatui::crossterm::event::KeyCode;
use ratatui::crossterm::event::KeyEvent;
use ratatui::crossterm::event::KeyModifiers;

pub fn key_char(key: &KeyEvent) -> Option<char> {
    match key.code {
        KeyCode::Char(value) if !key.modifiers.contains(KeyModifiers::CONTROL) => Some(value),
        _ => None,
    }
}

pub fn is_ctrl(key: &KeyEvent, expected: char) -> bool {
    matches!(key.code, KeyCode::Char(value) if value.eq_ignore_ascii_case(&expected))
        && key.modifiers.contains(KeyModifiers::CONTROL)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plain(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn ctrl(character: char) -> KeyEvent {
        KeyEvent::new(KeyCode::Char(character), KeyModifiers::CONTROL)
    }

    #[test]
    fn key_char_returns_text_for_non_control_character_events() {
        assert_eq!(key_char(&plain(KeyCode::Char('a'))), Some('a'));
        assert_eq!(
            key_char(&KeyEvent::new(KeyCode::Char('A'), KeyModifiers::SHIFT)),
            Some('A')
        );
        assert_eq!(
            key_char(&KeyEvent::new(KeyCode::Char('x'), KeyModifiers::ALT)),
            Some('x')
        );
    }

    #[test]
    fn key_char_rejects_control_and_non_character_events() {
        assert_eq!(key_char(&ctrl('a')), None);

        for code in [KeyCode::Enter, KeyCode::Esc, KeyCode::Backspace] {
            assert_eq!(key_char(&plain(code)), None);
        }
    }

    #[test]
    fn is_ctrl_matches_control_characters_case_insensitively() {
        let key = ctrl('r');

        assert!(is_ctrl(&key, 'r'));
        assert!(is_ctrl(&key, 'R'));
    }

    #[test]
    fn is_ctrl_rejects_events_without_a_control_character() {
        assert!(!is_ctrl(&plain(KeyCode::Char('r')), 'r'));
        assert!(!is_ctrl(&KeyEvent::new(KeyCode::Char('r'), KeyModifiers::ALT), 'r'));
        assert!(!is_ctrl(&KeyEvent::new(KeyCode::Enter, KeyModifiers::CONTROL), 'r'));
    }
}
