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

use std::time::Duration;

use ratatui::crossterm::event::Event;
use ratatui::crossterm::event::EventStream;
use ratatui::crossterm::event::KeyCode;
use ratatui::crossterm::event::KeyEventKind;
use ratatui::layout::Constraint;
use ratatui::layout::Direction;
use ratatui::layout::Layout;
use ratatui::widgets::Block;
use ratatui::DefaultTerminal;
use ratatui::Frame;
use tokio_stream::StreamExt;

use crate::ui::search_input_widget::SearchInputWidget;

#[derive(Default)]
pub struct RocketmqTuiApp {
    should_quit: bool,
    search_input: SearchInputWidget,
}

impl RocketmqTuiApp {
    pub fn new() -> Self {
        Self {
            should_quit: false,
            search_input: Default::default(),
        }
    }

    pub fn should_quit(&self) -> bool {
        self.should_quit
    }

    pub fn quit(&mut self) {
        self.should_quit = true;
    }
}

impl RocketmqTuiApp {
    const FRAMES_PER_SECOND: f32 = 60.0;

    pub async fn run(mut self, mut terminal: DefaultTerminal) -> anyhow::Result<()> {
        let period = Duration::from_secs_f32(1.0 / Self::FRAMES_PER_SECOND);
        let mut interval = tokio::time::interval(period);
        let mut events = EventStream::new();
        while !self.should_quit {
            tokio::select! {
                _ = interval.tick() => { terminal.draw(|frame| self.draw(frame))?; },
                Some(Ok(event)) = events.next() => self.handle_event(&event),
            }
        }
        Ok(())
    }

    fn handle_event(&mut self, event: &Event) {
        if let Event::Key(key) = event {
            if key.kind == KeyEventKind::Press {
                match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => self.should_quit = true,
                    _ => {}
                }
            }
        }
    }

    fn draw(&self, frame: &mut Frame) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints(
                [
                    Constraint::Percentage(5),
                    Constraint::Percentage(92),
                    Constraint::Percentage(3),
                ]
                .as_ref(),
            )
            .split(frame.area());

        let search = chunks[0];
        let middle = chunks[1];
        let progress_bar = chunks[2];

        let middle_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(30), Constraint::Percentage(70)].as_ref())
            .split(middle);

        let command_tree = middle_chunks[0];
        let middle_right = middle_chunks[1];

        let middle_right_chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints(
                [
                    Constraint::Percentage(50),
                    Constraint::Percentage(15),
                    Constraint::Percentage(35),
                ]
                .as_ref(),
            )
            .split(middle_right);

        let command_detail = middle_right_chunks[0];
        let command_args = middle_right_chunks[1];
        let execute_command_result = middle_right_chunks[2];

        frame.render_widget(&self.search_input, search);
        frame.render_widget(
            Block::default()
                .borders(ratatui::widgets::Borders::ALL)
                .title("Command Tree"),
            command_tree,
        );
        frame.render_widget(
            Block::default()
                .borders(ratatui::widgets::Borders::ALL)
                .title("Command Detail"),
            command_detail,
        );
        frame.render_widget(
            Block::default()
                .borders(ratatui::widgets::Borders::ALL)
                .title("Command Args"),
            command_args,
        );
        frame.render_widget(
            Block::default()
                .borders(ratatui::widgets::Borders::ALL)
                .title("Execute Command Result"),
            execute_command_result,
        );
        frame.render_widget(
            Block::default()
                .borders(ratatui::widgets::Borders::ALL)
                .title("Progress Bar"),
            progress_bar,
        );
    }
}
