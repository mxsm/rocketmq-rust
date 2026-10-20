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

//! Terminal rendering.
//!
//! A frame is painted in three passes: the panes, the motion effects that recolor
//! what the panes drew, and the overlays. While painting, every clickable region is
//! registered in the [`ViewCache`], which is what turns a mouse position back into
//! an intent on the next event.

mod effects;
mod footer;
mod header;
mod overlay;
mod result;
mod sidebar;
mod theme;
mod widgets;
mod workspace;

use std::collections::BTreeMap;

use ratatui::layout::Position;
use ratatui::layout::Rect;
use ratatui::style::Color;
use ratatui::style::Style;
use ratatui::Frame;

use crate::commands::CommandCategory;
use crate::state::AppState;
use crate::state::CommandTreeItem;
use crate::state::ExecutionPhase;
use crate::state::FocusArea;
use crate::state::Overlay;
use crate::state::Pane;

pub(crate) use theme::ColorDepth;

const MIN_WIDTH: u16 = 48;
const MIN_HEIGHT: u16 = 12;
/// Narrower terminals show the command list and the workspace one at a time.
const SPLIT_MIN_WIDTH: u16 = 96;
const SIDEBAR_MIN_WIDTH: u16 = 30;
const SIDEBAR_MAX_WIDTH: u16 = 44;
const HEADER_HEIGHT: u16 = 2;
const FOOTER_HEIGHT: u16 = 1;
const COMMAND_MIN_HEIGHT: u16 = 5;
const RESULT_MIN_HEIGHT: u16 = 8;
/// Ticks a border takes to light up or settle when focus moves.
const FOCUS_TICKS: u64 = 7;
/// Ticks of the staggered reveal played once at startup.
const INTRO_TICKS: u64 = 26;

/// What a click or a wheel notch at some cell acts on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HitTarget {
    Namesrv,
    Search,
    Tree,
    /// Position in the visible command tree.
    TreeRow(usize),
    Command,
    ArgRow(usize),
    ArgChoice {
        arg: usize,
        choice: usize,
    },
    RunButton,
    Result,
    /// Absolute row of a tabular result.
    ResultRow(usize),
    /// Inside an open overlay.
    Overlay,
    /// Outside an open overlay.
    Backdrop,
}

/// Interface state that only exists because something was drawn.
///
/// It outlives a frame so list viewports stay where the operator left them and so
/// input can be resolved against the layout that is actually on screen.
#[derive(Debug, Default)]
pub(crate) struct ViewCache {
    color_depth: ColorDepth,
    hits: Vec<(Rect, HitTarget)>,
    hover: Option<Position>,
    tree_top: usize,
    args_top: usize,
    pub(crate) tree_rows: usize,
    pub(crate) result_rows: usize,
    /// Largest scroll offset the open overlay can use.
    pub(crate) overlay_max_scroll: usize,
    pub(crate) overlay_rows: usize,
}

impl ViewCache {
    pub(crate) fn new(color_depth: ColorDepth) -> Self {
        Self {
            color_depth,
            ..Self::default()
        }
    }

    /// Returns the topmost registered target at a terminal cell.
    pub(crate) fn target_at(&self, column: u16, row: u16) -> Option<HitTarget> {
        let position = Position::new(column, row);
        self.hits
            .iter()
            .rev()
            .find(|(area, _)| area.contains(position))
            .map(|(_, target)| *target)
    }

    /// Records where the mouse pointer is, or that keyboard input took over.
    pub(crate) fn set_hover(&mut self, position: Option<Position>) {
        self.hover = position;
    }

    fn register(&mut self, area: Rect, target: HitTarget) {
        if !area.is_empty() {
            self.hits.push((area, target));
        }
    }
}

/// Pane rectangles of one frame. A pane that is not shown has no rectangle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Frames {
    header: Rect,
    sidebar: Option<Rect>,
    command: Option<Rect>,
    result: Option<Rect>,
    footer: Rect,
}

fn frames(area: Rect, state: &AppState) -> Frames {
    let header = Rect {
        height: HEADER_HEIGHT.min(area.height),
        ..area
    };
    let footer_height = FOOTER_HEIGHT.min(area.height.saturating_sub(header.height));
    let footer = Rect {
        y: area.bottom() - footer_height,
        height: footer_height,
        ..area
    };
    let body = Rect {
        y: header.bottom(),
        height: area.height - header.height - footer_height,
        ..area
    };

    if state.result_zoom {
        return Frames {
            header,
            sidebar: None,
            command: None,
            result: Some(body),
            footer,
        };
    }

    let (sidebar, main) = if area.width >= SPLIT_MIN_WIDTH {
        let width = (area.width.saturating_mul(3) / 10).clamp(SIDEBAR_MIN_WIDTH, SIDEBAR_MAX_WIDTH);
        (
            Some(Rect { width, ..body }),
            Some(Rect {
                x: body.x + width,
                width: body.width - width,
                ..body
            }),
        )
    } else if matches!(state.focus, FocusArea::Args | FocusArea::Result) {
        (None, Some(body))
    } else {
        (Some(body), None)
    };

    let (command, result) = match main {
        Some(main) => {
            let height = command_height(main.height, state.selected_command().args.len());
            (
                Some(Rect { height, ..main }),
                Some(Rect {
                    y: main.y + height,
                    height: main.height - height,
                    ..main
                }),
            )
        }
        None => (None, None),
    };

    Frames {
        header,
        sidebar,
        command,
        result,
        footer,
    }
}

/// Sizes the command pane to its form, leaving the result pane a usable minimum.
fn command_height(available: u16, args: usize) -> u16 {
    // Two border rows, the description, and the action row surround the fields.
    let wanted = u16::try_from(args.max(1)).unwrap_or(u16::MAX).saturating_add(4);
    let limit = available.saturating_sub(RESULT_MIN_HEIGHT).max(COMMAND_MIN_HEIGHT);
    wanted.min(limit).min(available)
}

/// Per-frame context shared by the pane painters.
struct Ctx<'a> {
    state: &'a AppState,
    tick: u64,
    tree: Vec<CommandTreeItem>,
    category_sizes: BTreeMap<CommandCategory, usize>,
    hover: Option<Position>,
}

impl<'a> Ctx<'a> {
    fn new(state: &'a AppState, hover: Option<Position>) -> Self {
        let mut category_sizes = BTreeMap::new();
        for command in state
            .commands()
            .iter()
            .filter(|command| command.matches_query(&state.search))
        {
            *category_sizes.entry(command.category).or_default() += 1;
        }
        Self {
            state,
            tick: state.animation_tick(),
            tree: state.visible_tree_items(),
            category_sizes,
            hover,
        }
    }

    fn hovered(&self, area: Rect) -> bool {
        self.hover.is_some_and(|position| area.contains(position))
    }

    /// Returns the border color of `pane`, easing between the idle and focused colors.
    fn pane_border(&self, pane: Pane) -> Color {
        let motion = self.state.motion();
        let tracked = motion.pane();
        let focused = self.state.focus.pane();
        // A focus change made after the last observation has no start tick yet.
        let amount = if tracked.current() == focused {
            motion.transition(tracked.changed_at(), FOCUS_TICKS)
        } else {
            1.0
        };
        if pane == focused {
            theme::mix(theme::BORDER, theme::PRIMARY, amount)
        } else if pane == tracked.previous() && tracked.current() == focused {
            theme::mix(theme::PRIMARY, theme::BORDER, amount)
        } else {
            theme::BORDER
        }
    }

    /// Returns how many ticks ago the execution entered `phase`, if it is the current one.
    fn phase_age(&self, phase: ExecutionPhase) -> Option<u64> {
        let tracked = self.state.motion().phase();
        (self.state.execution.phase() == phase && tracked.current().0 == phase)
            .then(|| self.tick.saturating_sub(tracked.changed_at()))
    }
}

/// Paints one frame and records what was drawn where.
pub(crate) fn render(frame: &mut Frame, state: &mut AppState, view: &mut ViewCache) {
    let area = frame.area();
    view.hits.clear();
    frame
        .buffer_mut()
        .set_style(area, Style::new().fg(theme::TEXT).bg(theme::BACKGROUND));

    if area.width < MIN_WIDTH || area.height < MIN_HEIGHT {
        overlay::paint_too_small(frame, area);
        finish(frame, view);
        return;
    }

    let frames = frames(area, state);
    if let Some(result_area) = frames.result {
        let body = result::body_area(
            result_area,
            state.result().is_some_and(|result| result.grid().is_some()),
        );
        view.result_rows = usize::from(body.height);
        if let Some(result) = state.result_mut() {
            result.sync(body.width, usize::from(body.height));
        }
    }

    let ctx = Ctx::new(state, view.hover);
    header::paint(frame, &ctx, frames.header, view);
    if let Some(sidebar) = frames.sidebar {
        sidebar::paint(frame, &ctx, sidebar, view);
    }
    if let Some(command) = frames.command {
        workspace::paint(frame, &ctx, command, view);
    }
    if let Some(result) = frames.result {
        result::paint(frame, &ctx, result, view);
    }
    footer::paint(frame, &ctx, frames.footer);

    paint_comet(frame, &ctx, &frames);
    paint_intro(frame, &ctx, &frames);
    overlay::paint(frame, &ctx, area, view);
    overlay::paint_toast(frame, &ctx, area);
    finish(frame, view);
}

fn finish(frame: &mut Frame, view: &ViewCache) {
    if view.color_depth == ColorDepth::Indexed {
        effects::quantize(frame.buffer_mut());
    }
}

/// Sends a highlight around the border of the focused pane.
///
/// A running command and an open overlay have indicators of their own, so the
/// highlight rests then instead of adding to what every frame has to repaint.
fn paint_comet(frame: &mut Frame, ctx: &Ctx<'_>, frames: &Frames) {
    let state = ctx.state;
    let intensity = state.motion().ambient();
    if intensity <= 0.0 || state.execution.phase() == ExecutionPhase::Running || state.overlay() != Overlay::None {
        return;
    }
    let area = match state.focus.pane() {
        Pane::Header => None,
        Pane::Sidebar => frames.sidebar,
        Pane::Command => frames.command,
        Pane::Result => frames.result,
    };
    if let Some(area) = area {
        effects::comet(frame.buffer_mut(), area, ctx.tick, theme::BRIGHT, intensity);
    }
}

/// Reveals the panes one after another during the first frames after startup.
fn paint_intro(frame: &mut Frame, ctx: &Ctx<'_>, frames: &Frames) {
    let motion = ctx.state.motion();
    if !motion.is_playing(0, INTRO_TICKS) {
        return;
    }
    let buffer = frame.buffer_mut();
    let stages = [
        Some(frames.header),
        frames.sidebar,
        frames.command,
        frames.result,
        Some(frames.footer),
    ];
    for (stage, area) in stages.into_iter().enumerate() {
        if let Some(area) = area {
            effects::fade(buffer, area, motion.transition(stage as u64 * 3, 12));
        }
    }
}

#[cfg(test)]
mod tests;
