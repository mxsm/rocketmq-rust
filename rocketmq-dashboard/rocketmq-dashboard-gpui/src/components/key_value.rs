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

//! Wrapped key/value rendering shared by Broker Runtime and Config.

use gpui::{Div, Hsla, ParentElement as _, Styled as _, div};
use gpui_component::StyledExt as _;

pub fn render(key: impl Into<String>, value: impl Into<String>, foreground: Hsla, muted: Hsla, border: Hsla) -> Div {
    div()
        .w_full()
        .py_3()
        .flex()
        .gap_4()
        .border_b_1()
        .border_color(border)
        .child(
            div()
                .w_64()
                .flex_shrink_0()
                .text_sm()
                .font_medium()
                .text_color(foreground)
                .child(key.into()),
        )
        .child(
            div()
                .flex_1()
                .min_w_0()
                .text_sm()
                .text_color(muted)
                .whitespace_normal()
                .child(value.into()),
        )
}
