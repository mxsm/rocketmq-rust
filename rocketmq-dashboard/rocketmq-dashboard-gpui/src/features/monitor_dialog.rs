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

use gpui::prelude::FluentBuilder as _;
use gpui::{
    App, AppContext as _, Context, Entity, InteractiveElement as _, IntoElement, ParentElement as _, Render,
    Styled as _, Window, div, px,
};
use gpui_component::{
    ActiveTheme as _, Disableable as _,
    button::Button,
    checkbox::Checkbox,
    input::{Input, InputState},
    scroll::ScrollableElement as _,
};
use rocketmq_dashboard_common::{
    EnvironmentId, MonitorRuleChange, MonitorRuleMutation, MonitorRuleView, MonitorThresholds,
};

use crate::state::{UiError, UiErrorCode};

pub struct MonitorDialog {
    pub group: Entity<InputState>,
    pub min_count: Entity<InputState>,
    pub max_diff_total: Entity<InputState>,
    pub environment_id: EnvironmentId,
    pub configuration_revision: u64,
    pub expected_revision: u64,
    pub deleting: bool,
    pub enabled: bool,
    pub busy: bool,
    pub error: Option<String>,
    pub conflict: Option<Option<MonitorRuleView>>,
    adopt_legacy_id: Option<String>,
    editing: bool,
}

impl MonitorDialog {
    pub fn new(
        environment_id: EnvironmentId,
        configuration_revision: u64,
        rule: Option<MonitorRuleView>,
        deleting: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let group = cx.new(|cx| InputState::new(window, cx).placeholder("Consumer Group"));
        let min_count = cx.new(|cx| InputState::new(window, cx).placeholder("Nonnegative integer"));
        let max_diff_total = cx.new(|cx| InputState::new(window, cx).placeholder("Nonnegative integer"));
        if let Some(rule) = &rule {
            group.update(cx, |input, cx| input.set_value(rule.consumer_group.clone(), window, cx));
            if let Some(thresholds) = rule.thresholds {
                min_count.update(cx, |input, cx| {
                    input.set_value(thresholds.min_count.to_string(), window, cx)
                });
                max_diff_total.update(cx, |input, cx| {
                    input.set_value(thresholds.max_diff_total.to_string(), window, cx)
                });
            }
        }
        Self {
            group,
            min_count,
            max_diff_total,
            environment_id,
            configuration_revision,
            expected_revision: rule.as_ref().map_or(0, |rule| rule.revision),
            enabled: rule.as_ref().is_none_or(|rule| rule.enabled),
            adopt_legacy_id: rule
                .as_ref()
                .filter(|rule| rule.environment_id.is_none())
                .map(|rule| rule.legacy_id.clone()),
            editing: rule.is_some(),
            deleting,
            busy: false,
            error: None,
            conflict: None,
        }
    }

    pub fn prepare(&self, cx: &App) -> Result<MonitorRuleMutation, UiError> {
        if self.busy || self.conflict.is_some() {
            return Err(UiError::new(
                "Review the authoritative rule and reconfirm before submitting.",
                UiErrorCode::Validation,
                false,
            ));
        }
        let consumer_group = self.group.read(cx).value().trim().to_owned();
        let change = if self.deleting {
            MonitorRuleChange::Delete
        } else {
            let thresholds =
                MonitorThresholds::parse(&self.min_count.read(cx).value(), &self.max_diff_total.read(cx).value())
                    .map_err(|source| {
                        UiError::caused_by(
                            "Enter nonnegative integer thresholds.",
                            UiErrorCode::Validation,
                            false,
                            source,
                        )
                    })?;
            MonitorRuleChange::Save {
                thresholds,
                enabled: self.enabled,
                adopt_legacy_id: self.adopt_legacy_id.clone(),
            }
        };
        let request = MonitorRuleMutation {
            environment_id: self.environment_id.clone(),
            consumer_group,
            expected_revision: self.expected_revision,
            change,
        };
        request.validate().map_err(|source| {
            UiError::caused_by("The Monitor rule is invalid.", UiErrorCode::Validation, false, source)
        })?;
        Ok(request)
    }

    fn reconfirm(&mut self, cx: &mut Context<Self>) {
        let Some(latest) = self.conflict.take() else {
            return;
        };
        if self.deleting && latest.is_none() {
            self.conflict = Some(None);
            return;
        }
        self.expected_revision = latest.as_ref().map_or(0, |rule| rule.revision);
        self.editing = latest.is_some() || self.deleting;
        if latest.as_ref().is_none_or(|rule| rule.environment_id.is_some()) {
            self.adopt_legacy_id = None;
        }
        self.error = Some("Draft preserved. Review it, then submit again to use the loaded revision.".into());
        cx.notify();
    }
}

impl Render for MonitorDialog {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let mut form = div()
            .flex()
            .flex_col()
            .gap_3()
            .child("Rule configuration only. No evaluation, alerts, or notifications are performed.")
            .child(format!(
                "Environment: {} · expected rule revision {}",
                self.environment_id.as_str(),
                self.expected_revision
            ))
            .child("Consumer Group")
            .child(
                div()
                    .debug_selector(|| "monitor-group".into())
                    .child(Input::new(&self.group).disabled(self.editing || self.busy || self.conflict.is_some())),
            );
        if !self.deleting {
            form = form
                .child("Min Count · consumer count")
                .child(
                    div()
                        .debug_selector(|| "monitor-min-count".into())
                        .child(Input::new(&self.min_count).disabled(self.busy)),
                )
                .child("Max Diff Total · backlog messages")
                .child(
                    div()
                        .debug_selector(|| "monitor-max-diff-total".into())
                        .child(Input::new(&self.max_diff_total).disabled(self.busy)),
                )
                .child(
                    Checkbox::new("monitor-enabled")
                        .debug_selector(|| "monitor-enabled".into())
                        .label("Enabled")
                        .checked(self.enabled)
                        .disabled(self.busy)
                        .on_click(cx.listener(|form, checked, _, cx| {
                            form.enabled = *checked;
                            cx.notify();
                        })),
                )
                .child(
                    div()
                        .debug_selector(|| "monitor-enabled-explanation".into())
                        .w_full()
                        .min_w_0()
                        .whitespace_normal()
                        .text_sm()
                        .child("This setting only saves the rule configuration. It does not start evaluation, alerts, or notifications."),
                );
        } else {
            form = form.child("Delete this rule configuration? Consumer data is unaffected.");
        }
        if self.adopt_legacy_id.is_some() {
            form = form.child("Saving explicitly assigns this legacy entry to the selected environment.");
        }
        if let Some(latest) = &self.conflict {
            let summary = latest.as_ref().map_or_else(
                || "The rule is absent. Saving again would create it; a repeated delete cannot remove anything.".into(),
                |rule| {
                    format!(
                        "Authoritative revision {} · enabled {} · Min Count {} · Max Diff Total {}",
                        rule.revision,
                        rule.enabled,
                        rule.thresholds.map_or_else(
                            || "not configured".into(),
                            |thresholds| thresholds.min_count.to_string()
                        ),
                        rule.thresholds.map_or_else(
                            || "not configured".into(),
                            |thresholds| thresholds.max_diff_total.to_string()
                        )
                    )
                },
            );
            form = form.child(div().text_sm().child(summary)).child(
                Button::new("monitor-reconfirm")
                    .debug_selector(|| "monitor-reconfirm".into())
                    .label("Keep draft and reconfirm revision")
                    .disabled(self.deleting && latest.is_none())
                    .on_click(cx.listener(|form, _, _, cx| form.reconfirm(cx))),
            );
        }
        let form = form
            .when_some(self.error.clone(), |form, error| {
                form.child(div().text_sm().text_color(cx.theme().danger).child(error))
            })
            .when(self.busy, |form| {
                form.child("Saving the accepted operation. Closing this dialog does not cancel persistence.")
            });
        // Leave room for the official dialog title, footer, padding and window margins.
        let body_height = (window.viewport_size().height - px(200.)).max(px(120.));
        div()
            .debug_selector(|| "monitor-form-scroll".into())
            .max_h(body_height)
            .child(
                div()
                    .id("monitor-form-scroll")
                    .max_h(body_height)
                    .overflow_y_scrollbar()
                    .child(form),
            )
    }
}
