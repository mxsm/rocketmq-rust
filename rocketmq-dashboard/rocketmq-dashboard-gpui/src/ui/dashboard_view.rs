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

//! Dashboard content only; navigation belongs to the typed application shell.

use gpui::{Context, IntoElement, ParentElement as _, Render, Styled as _, Window, div};
use gpui_component::{ActiveTheme as _, StyledExt as _};

/// The cached Dashboard content entity.
///
/// Delivery 01 intentionally provides no metrics: runtime data belongs to the connection delivery.
pub struct DashboardView;

impl DashboardView {
    /// Creates the dashboard content entity.
    pub const fn new() -> Self {
        Self
    }
}

impl Render for DashboardView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .size_full()
            .flex()
            .flex_col()
            .p_6()
            .gap_2()
            .bg(cx.theme().background)
            .child(
                div()
                    .text_xl()
                    .font_semibold()
                    .text_color(cx.theme().foreground)
                    .child("Dashboard"),
            )
            .child(
                div()
                    .text_sm()
                    .text_color(cx.theme().muted_foreground)
                    .child("Connect a RocketMQ environment to view operational metrics."),
            )
    }
}

impl Default for DashboardView {
    fn default() -> Self {
        Self::new()
    }
}
