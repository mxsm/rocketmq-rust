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

//! Application-level audit persistence feedback survives feature replacement and sign out.

use gpui::{
    Context, InteractiveElement as _, IntoElement, ParentElement as _, Styled as _, div, prelude::FluentBuilder as _,
};
use gpui_component::{ActiveTheme as _, button::Button};

use super::RocketmqDashboard;

impl RocketmqDashboard {
    pub(super) fn subscribe_audit(&mut self, cx: &mut Context<Self>) {
        let Some(mut changes) = self.services.audit_changes() else {
            return;
        };
        self.audit_task = Some(cx.spawn(async move |this, cx| {
            while changes.changed().await.is_ok() {
                if this.update(cx, |_, cx| cx.notify()).is_err() {
                    break;
                }
            }
        }));
    }

    pub(super) fn render_audit_warning(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let failed = self.services.audit_write_warning();
        let recovery = self.services.audit_recovery_warning();
        let theme = cx.theme();
        div().id("audit-persistence-warning").when(failed || recovery, |banner| {
            banner.flex().flex_wrap().items_center().gap_3().px_4().py_2()
                .bg(theme.muted).text_color(theme.foreground).text_sm()
                .child(if failed {
                    "The operation result is preserved, but its local audit write failed. Retrying the audit will only save that result."
                } else {
                    "Some earlier desktop operations have no confirmed outcome. Verify their server state before submitting another change."
                })
                .when(failed, |banner| banner.child(div().debug_selector(|| "retry-audit-write".to_owned()).child(Button::new("retry-audit-write").label("Retry audit write")
                    .on_click(cx.listener(|this, _, _, cx| {
                        let services = this.services.clone();
                        this.audit_retry_task = Some(cx.spawn(async move |_, _| { let _ = services.retry_audit_writes().await; }));
                    })))))
        })
    }
}
