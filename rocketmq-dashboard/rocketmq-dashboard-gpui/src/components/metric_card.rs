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

//! Compact theme-aware operational metric card.

use gpui::{Div, Hsla, ParentElement as _, Styled as _, div};
use gpui_component::StyledExt as _;

pub fn render(label: impl Into<String>, value: impl Into<String>, foreground: Hsla, muted: Hsla, border: Hsla) -> Div {
    div()
        .flex_1()
        .min_w_48()
        .p_4()
        .flex()
        .flex_col()
        .gap_2()
        .rounded_lg()
        .border_1()
        .border_color(border)
        .child(div().text_xs().text_color(muted).child(label.into()))
        .child(
            div()
                .text_2xl()
                .font_semibold()
                .text_color(foreground)
                .child(value.into()),
        )
}
