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

//! Dialog intent adapters that leave overlay and focus ownership to `gpui-component`.

use std::rc::Rc;

use gpui::{ClickEvent, ParentElement as _, Styled as _, Window, div};
use gpui_component::{
    WindowExt as _,
    dialog::{Dialog, DialogButtonProps},
};

/// Opens a confirmation dialog using the root-owned official dialog stack.
pub fn open_confirm(
    title: &'static str,
    description: &'static str,
    confirm_label: &'static str,
    on_confirm: impl Fn(&ClickEvent, &mut Window, &mut gpui::App) -> bool + 'static,
    window: &mut Window,
    cx: &mut gpui::App,
) {
    let on_confirm = Rc::new(on_confirm);
    window.open_dialog(cx, move |dialog: Dialog, _, _| {
        let on_confirm = on_confirm.clone();
        dialog
            .title(title)
            .child(div().text_sm().child(description))
            .confirm()
            .button_props(DialogButtonProps::default().ok_text(confirm_label))
            .on_ok(move |event, window, cx| on_confirm(event, window, cx))
    });
}
