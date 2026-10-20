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

//! Key bar: where focus is and what the keys do right there.

use ratatui::layout::Rect;
use ratatui::text::Line;
use ratatui::text::Span;
use ratatui::Frame;

use super::theme;
use super::widgets;
use super::Ctx;
use crate::state::AppState;
use crate::state::ExecutionPhase;
use crate::state::FocusArea;
use crate::state::Overlay;
use crate::text::display_width;

type Hint = (&'static str, &'static str);

pub(super) fn paint(frame: &mut Frame, ctx: &Ctx<'_>, area: Rect) {
    if area.is_empty() {
        return;
    }
    let buffer = frame.buffer_mut();
    let state = ctx.state;

    let mode = match state.overlay() {
        Overlay::None => state.focus.label(),
        Overlay::Help => "Help",
        Overlay::Confirm => "Confirm",
        Overlay::Detail => "Row",
    };
    let mode = widgets::chip(&mode.to_ascii_uppercase(), theme::BRIGHT, theme::PRIMARY_DEEP);
    let mut x = widgets::put_line(buffer, area.x, area.y, &Line::from(mode), area.width);
    x = x.saturating_add(1);

    // Help is always reachable, so its hint is pinned to the right edge.
    let pinned = Line::from(vec![widgets::keycap("F1"), Span::styled(" help ", theme::muted())]);
    let pinned_width = pinned.width() as u16;
    let limit = area.right().saturating_sub(pinned_width + 1);
    if limit > x {
        widgets::put_line_right(buffer, x, area.right(), area.y, &pinned);
    }

    // Hints are listed by importance; whatever does not fit is dropped from the end.
    for (key, action) in hints(state) {
        let width = (display_width(key) + display_width(action) + 5) as u16;
        if x.saturating_add(width) > limit {
            break;
        }
        let hint = Line::from(vec![
            widgets::keycap(key),
            Span::styled(format!(" {action}"), theme::muted()),
        ]);
        x = widgets::put_line(buffer, x, area.y, &hint, width).saturating_add(2);
    }
}

/// Returns the keys that matter in the current context, most useful first.
pub(super) fn hints(state: &AppState) -> Vec<Hint> {
    match state.overlay() {
        Overlay::Help => return vec![("↑↓", "scroll"), ("Esc", "close")],
        Overlay::Confirm => return vec![("Enter", "confirm"), ("Esc", "cancel")],
        Overlay::Detail => return vec![("↑↓", "scroll"), ("Esc", "close")],
        Overlay::None => {}
    }

    let mut hints = Vec::new();
    let running = state.execution.phase() == ExecutionPhase::Running;
    if running {
        hints.push(("Esc", "cancel run"));
    }
    let focused: &[Hint] = match state.focus {
        FocusArea::Namesrv => &[("Enter", "save"), ("Esc", "revert"), ("Tab", "next pane")],
        FocusArea::Search => &[
            ("↑↓", "select"),
            ("Enter", "to list"),
            ("Esc", "clear"),
            ("Tab", "next pane"),
        ],
        FocusArea::CommandTree => &[
            ("↑↓", "move"),
            ("Enter", "open"),
            ("←→", "fold"),
            ("/", "search"),
            ("n", "nameserver"),
            ("Ctrl+R", "run"),
            ("q", "quit"),
        ],
        FocusArea::Args => &[
            ("↑↓", "field"),
            ("←→", "choose"),
            ("Enter", "run"),
            ("Ctrl+D", "defaults"),
            ("Esc", "back"),
            ("Ctrl+F", "search"),
        ],
        FocusArea::Result if state.result().is_some_and(|result| result.grid().is_some()) => &[
            ("↑↓", "row"),
            ("←→", "columns"),
            ("Enter", "details"),
            ("z", "zoom"),
            ("Esc", "back"),
        ],
        FocusArea::Result => &[
            ("↑↓", "scroll"),
            ("←→", "pan"),
            ("w", "wrap"),
            ("z", "zoom"),
            ("Esc", "back"),
        ],
    };
    // While a command runs, Esc cancels it and does nothing else.
    hints.extend(focused.iter().filter(|(key, _)| !(running && *key == "Esc")));
    hints
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::CommandExecutionState;

    fn keys(state: &AppState) -> Vec<&'static str> {
        hints(state).into_iter().map(|(key, _)| key).collect()
    }

    #[test]
    fn hints_follow_the_focused_pane() {
        let mut state = AppState::new(None);
        assert!(hints(&state).contains(&("Enter", "open")));

        state.focus = FocusArea::Args;
        assert!(hints(&state).contains(&("Enter", "run")));

        state.focus = FocusArea::Result;
        assert!(hints(&state).contains(&("w", "wrap")));

        state.focus = FocusArea::Search;
        assert!(hints(&state).contains(&("Esc", "clear")));
    }

    #[test]
    fn a_running_command_puts_cancel_first() {
        let mut state = AppState::new(None);
        state.execution = CommandExecutionState::Running {
            execution_id: 1,
            command_id: "topic.list".to_string(),
        };

        assert_eq!(hints(&state)[0], ("Esc", "cancel run"));
        for focus in [
            FocusArea::Args,
            FocusArea::Result,
            FocusArea::Search,
            FocusArea::Namesrv,
        ] {
            state.focus = focus;
            assert_eq!(
                keys(&state).iter().filter(|key| **key == "Esc").count(),
                1,
                "Esc has one meaning while a command runs"
            );
        }
    }

    #[test]
    fn an_overlay_replaces_the_pane_hints() {
        let mut state = AppState::new(None);
        state.show_help = true;
        assert_eq!(keys(&state), ["↑↓", "Esc"]);

        state.show_help = false;
        state.execution = CommandExecutionState::Confirming {
            execution_id: 1,
            command_id: "topic.delete".to_string(),
            expected: "TopicA".to_string(),
        };
        assert_eq!(keys(&state), ["Enter", "Esc"]);
    }
}
