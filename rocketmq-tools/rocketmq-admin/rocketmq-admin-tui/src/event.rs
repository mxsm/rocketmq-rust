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
