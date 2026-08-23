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

//! Notification helpers backed by the root-owned `gpui-component` queue.

use gpui::Window;
use gpui_component::{WindowExt as _, notification::Notification};

/// Thin dashboard façade over the root-owned official notification queue.
pub struct ToastHost;

impl ToastHost {
    /// Pushes a successful operation summary through the root-owned notification host.
    pub fn success(summary: impl Into<gpui::SharedString>, window: &mut Window, cx: &mut gpui::App) {
        window.push_notification(Notification::success(summary), cx);
    }

    /// Pushes a safe error summary through the root-owned notification host.
    pub fn error(summary: impl Into<gpui::SharedString>, window: &mut Window, cx: &mut gpui::App) {
        window.push_notification(Notification::error(summary), cx);
    }
}
