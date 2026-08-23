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

//! Reusable page heading composition.

use gpui::{Div, Hsla, ParentElement as _, Styled as _, div};
use gpui_component::StyledExt as _;

/// Renders a page title with a short scoped description.
pub fn render(title: &str, description: &str, foreground: Hsla, muted_foreground: Hsla) -> Div {
    div()
        .flex()
        .flex_col()
        .gap_1()
        .child(
            div()
                .text_xl()
                .font_semibold()
                .text_color(foreground)
                .child(title.to_owned()),
        )
        .child(
            div()
                .text_sm()
                .text_color(muted_foreground)
                .child(description.to_owned()),
        )
}
