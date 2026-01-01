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

use crossterm::event::KeyCode;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Color;
use ratatui::style::Style;
use ratatui::text::Line;
use ratatui::text::Span;
use ratatui::widgets::Block;
use ratatui::widgets::Paragraph;
use ratatui::widgets::Widget;

#[derive(Default)]
pub(crate) struct SearchInputWidget {
    // Whether the widget is focused
    focused: bool,

    // The input string
    input: String,
}

impl SearchInputWidget {
    pub fn new() -> Self {
        Self {
            focused: false,
            input: String::new(),
        }
    }

    pub fn set_input(&mut self, input: String) {
        self.input = input;
    }

    pub fn get_input(&self) -> &str {
        &self.input
    }

    pub fn get_input_mut(&mut self) -> &mut String {
        &mut self.input
    }

    pub fn set_focus(&mut self, focused: bool) {
        self.focused = focused;
    }

    pub fn is_focused(&self) -> bool {
        self.focused
    }

    pub fn handle_key_event(&mut self, key: KeyCode) {
        match key {
            KeyCode::Char(c) => {
                self.input.push(c);
            }
            KeyCode::Backspace => {
                self.input.pop();
            }
            KeyCode::Enter => {
                // Do nothing
            }
            _ => {}
        }
    }
}

impl Widget for &SearchInputWidget {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let style = if self.focused {
            Style::default().fg(Color::Yellow).bg(Color::Black)
        } else {
            Style::default().fg(Color::White).bg(Color::Black)
        };

        let block = Block::default()
            .borders(ratatui::widgets::Borders::ALL)
            .title("Search[Press s/S to focus]")
            .border_style(style);
        block.render(area, buf);

        let inner_area = Rect {
            x: area.x + 1,
            y: area.y + 1,
            width: area.width - 2,
            height: area.height - 2,
        };

        let paragraph = Paragraph::new(Line::from(Span::styled(
            self.input.as_str(),
            Style::default().fg(Color::White).bg(Color::Black),
        )));
        paragraph.render(inner_area, buf);
    }
}
