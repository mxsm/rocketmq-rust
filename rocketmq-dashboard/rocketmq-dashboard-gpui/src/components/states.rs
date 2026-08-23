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

//! Shared Loading, Empty, and Error state compositions.

use gpui::{ClickEvent, Div, Hsla, ParentElement as _, Styled as _, Window, div, prelude::FluentBuilder as _, px};
use gpui_component::{
    StyledExt as _,
    button::{Button, ButtonVariants as _},
    skeleton::Skeleton,
};

/// Renders the standard startup loading card using the official Skeleton component.
pub fn loading_state(foreground: Hsla, muted_foreground: Hsla) -> Div {
    div()
        .w(px(520.))
        .max_w_full()
        .p_6()
        .flex()
        .flex_col()
        .gap_4()
        .child(
            div()
                .text_xl()
                .font_semibold()
                .text_color(foreground)
                .child("Starting RocketMQ Dashboard"),
        )
        .child(
            div()
                .text_sm()
                .text_color(muted_foreground)
                .child("Loading local dashboard settings…"),
        )
        .child(Skeleton::new().h(px(18.)))
        .child(Skeleton::new().secondary().h(px(18.)).w(px(360.)))
}

/// Renders a standard empty or unavailable state without inventing business data.
pub fn empty_state(title: &str, description: &str, foreground: Hsla, muted_foreground: Hsla) -> Div {
    div()
        .flex()
        .flex_col()
        .gap_2()
        .p_6()
        .child(
            div()
                .text_lg()
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

/// Renders a recoverable error state with caller-owned intents.
///
/// The caller supplies the actions so this shared composition never accesses a service or mutates
/// application state itself.
pub fn error_state(
    title: &str,
    description: &str,
    foreground: Hsla,
    muted_foreground: Hsla,
    retry: Option<impl Fn(&ClickEvent, &mut Window, &mut gpui::App) + 'static>,
    open_config: impl Fn(&ClickEvent, &mut Window, &mut gpui::App) + 'static,
) -> Div {
    div()
        .w(px(520.))
        .max_w_full()
        .p_6()
        .rounded_lg()
        .flex()
        .flex_col()
        .gap_4()
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
        .when_some(retry, |this, retry| {
            this.child(Button::new("retry-startup").label("Retry").primary().on_click(retry))
        })
        .child(
            Button::new("open-config-location")
                .label("Open configuration")
                .outline()
                .on_click(open_config),
        )
}
