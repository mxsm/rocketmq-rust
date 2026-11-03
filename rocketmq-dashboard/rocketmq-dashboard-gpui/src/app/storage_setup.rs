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

//! Storage recovery returns through the existing bootstrap and authentication decision.

use super::*;
use crate::features::storage_setup::{StorageSetupIntent, StorageSetupMode, StorageSetupView};

#[cfg(test)]
#[path = "storage_setup_tests.rs"]
mod tests;

impl RocketmqDashboard {
    pub(super) fn open_storage_setup(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !matches!(self.startup_state, StartupState::Failed(_)) || self.storage_setup_view.is_some() {
            return;
        }
        let view = cx.new(|cx| StorageSetupView::new(self.services.clone(), StorageSetupMode::Recovery, cx));
        self.storage_setup_subscription = Some(cx.subscribe_in(&view, window, |dashboard, _, event, window, cx| {
            dashboard.storage_setup_view = None;
            dashboard.storage_setup_subscription = None;
            match event {
                StorageSetupIntent::Completed => dashboard.start_bootstrap(window, cx),
                StorageSetupIntent::Back => cx.notify(),
            }
        }));
        view.update(cx, |view, cx| {
            view.refresh(cx);
            view.focus(window);
        });
        self.storage_setup_view = Some(view);
        cx.notify();
    }
}
