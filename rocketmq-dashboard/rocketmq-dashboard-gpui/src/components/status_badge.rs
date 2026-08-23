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

//! Small semantic status label used by the Topbar and future page headers.

use gpui::{Div, Hsla, ParentElement as _, Styled as _, div};
use gpui_component::StyledExt as _;

/// A thin status badge that inherits semantic colors from the active theme.
pub fn render(label: &str, background: Hsla, foreground: Hsla) -> Div {
    div()
        .px_2()
        .py_1()
        .rounded_full()
        .text_xs()
        .font_medium()
        .bg(background)
        .text_color(foreground)
        .child(label.to_owned())
}
