// Copyright 2025 The RocketMQ Rust Authors
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

//! Thin adapters over the official virtualized table component.

use gpui::{App, AppContext as _, Context, IntoElement, ParentElement as _, Styled as _, Window, div};
use gpui_component::{
    ActiveTheme as _, Sizable as _,
    table::{Column, Table, TableDelegate, TableState},
};

/// A zero-row delegate used while a route has no data capability in Delivery 01.
///
/// It preserves the official table's focus and virtualization behavior without manufacturing rows.
pub struct UnavailableTable {
    columns: Vec<Column>,
    message: &'static str,
}

impl UnavailableTable {
    /// Creates an explicit unavailable-state table for a future data route.
    pub fn new(message: &'static str) -> Self {
        Self {
            columns: vec![Column::new("resource", "Resource").resizable(false).movable(false)],
            message,
        }
    }
}

impl TableDelegate for UnavailableTable {
    fn columns_count(&self, _: &App) -> usize {
        self.columns.len()
    }

    fn rows_count(&self, _: &App) -> usize {
        0
    }

    fn column(&self, col_ix: usize, _: &App) -> &Column {
        &self.columns[col_ix]
    }

    fn render_td(&mut self, _: usize, _: usize, _: &mut Window, _: &mut Context<TableState<Self>>) -> impl IntoElement {
        div()
    }

    fn render_empty(&mut self, _: &mut Window, cx: &mut Context<TableState<Self>>) -> impl IntoElement {
        div()
            .size_full()
            .flex()
            .items_center()
            .justify_center()
            .text_sm()
            .text_color(cx.theme().muted_foreground)
            .child(self.message)
    }
}

/// Builds an official `TableState` once, allowing its owner to retain focus and scroll state.
pub fn unavailable_state<T: 'static>(
    message: &'static str,
    window: &mut Window,
    cx: &mut Context<T>,
) -> gpui::Entity<TableState<UnavailableTable>> {
    let delegate = UnavailableTable::new(message);
    cx.new(|cx| {
        TableState::new(delegate, window, cx)
            .col_selectable(false)
            .col_movable(false)
            .col_resizable(false)
            .sortable(false)
    })
}

/// Renders the official virtualized table without recreating table behavior in dashboard code.
pub fn render(state: &gpui::Entity<TableState<UnavailableTable>>) -> impl IntoElement {
    Table::new(state).bordered(true).small()
}
