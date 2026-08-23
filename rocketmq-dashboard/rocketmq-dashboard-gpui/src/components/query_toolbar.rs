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

//! A compact, capability-neutral toolbar frame for future list and query pages.

use gpui::{Div, Hsla, ParentElement as _, Styled as _, div};
use gpui_component::{Disableable as _, button::Button};

/// Renders a disabled query toolbar when a page has no Delivery 01 data capability yet.
///
/// It is deliberately informational: the component does not invent a local search or make a
/// provider request before the owning delivery supplies one.
pub fn unavailable(description: &str, muted: Hsla, muted_foreground: Hsla) -> Div {
    div()
        .px_4()
        .py_3()
        .rounded_lg()
        .bg(muted)
        .flex()
        .items_center()
        .gap_3()
        .child(
            div()
                .flex_1()
                .text_sm()
                .text_color(muted_foreground)
                .child(description.to_owned()),
        )
        .child(
            Button::new("query-unavailable")
                .label("Query unavailable")
                .outline()
                .disabled(true),
        )
}
