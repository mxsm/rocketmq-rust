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

use gpui::{
    App, AppContext as _, Context, Entity, InteractiveElement as _, IntoElement, ParentElement as _, Render,
    Styled as _, Window, div, px,
};
use gpui_component::{
    ActiveTheme as _, Disableable as _, Selectable as _,
    button::Button,
    checkbox::Checkbox,
    input::{Input, InputState},
    scroll::ScrollableElement as _,
};
use rocketmq_dashboard_common::*;

pub struct AclPolicyDialog {
    pub subject: Entity<InputState>,
    pub resource: Entity<InputState>,
    pub source_ips: Entity<InputState>,
    pub policy_type: String,
    pub actions: Vec<String>,
    pub decision: String,
    pub editing: bool,
    pub deleting: bool,
    pub busy: bool,
    pub attempted: bool,
    pub error: Option<String>,
    pub scope: AclScope,
    pub targets: Vec<BrokerIdentity>,
}
impl AclPolicyDialog {
    pub fn new(
        scope: AclScope,
        selected: Option<(AclPolicy, usize)>,
        deleting: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let entry = selected.as_ref().and_then(|(policy, index)| policy.entries.get(*index));
        let subject = cx.new(|cx| {
            InputState::new(window, cx).default_value(
                selected
                    .as_ref()
                    .map_or_else(String::new, |(policy, _)| policy.subject.clone()),
            )
        });
        let resource = cx.new(|cx| {
            InputState::new(window, cx)
                .default_value(entry.map_or_else(String::new, |entry| entry.resource.clone()))
                .placeholder("Topic:name, Group:name, Cluster:name, or *")
        });
        let source_ips = cx.new(|cx| {
            InputState::new(window, cx).default_value(entry.map_or(String::new(), |entry| entry.source_ips.join(",")))
        });
        let targets = selected
            .as_ref()
            .map_or_else(|| scope.targets().to_vec(), |(policy, _)| vec![policy.target.clone()]);
        Self {
            subject,
            resource,
            source_ips,
            policy_type: selected
                .as_ref()
                .map_or_else(|| "Custom".into(), |(policy, _)| policy.policy_type.clone()),
            actions: entry.map_or_else(|| vec!["Pub".into()], |entry| entry.actions.clone()),
            decision: entry.map_or_else(|| "Allow".into(), |entry| entry.decision.clone()),
            editing: selected.is_some(),
            deleting,
            busy: false,
            attempted: false,
            error: None,
            scope,
            targets,
        }
    }
    pub fn prepare(&self, cx: &App) -> Result<AclCommand, AclValidationError> {
        if self.busy || self.attempted {
            return Err(AclValidationError::Scope);
        }
        let subject = self.subject.read(cx).value().trim().to_owned();
        let resource = self.resource.read(cx).value().trim().to_owned();
        let change = if self.deleting {
            AclChange::DeletePolicyEntry {
                subject,
                policy_type: self.policy_type.clone(),
                resource,
            }
        } else {
            AclChange::SavePolicy {
                create: !self.editing,
                subject,
                policy_type: self.policy_type.clone(),
                entry: AclPolicyEntry {
                    resource,
                    actions: self.actions.clone(),
                    source_ips: self
                        .source_ips
                        .read(cx)
                        .value()
                        .split(',')
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(str::to_owned)
                        .collect(),
                    decision: self.decision.clone(),
                },
            }
        };
        let command = AclCommand {
            scope: self.scope.clone(),
            targets: self.targets.clone(),
            change,
        };
        command.validate()?;
        Ok(command)
    }
}
impl Render for AclPolicyDialog {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let locked = self.busy || self.attempted;
        let mut form = div().flex().flex_col().gap_3().min_w_0().child(format!(
            "Cluster {} · {} exact target(s)",
            self.scope.cluster(),
            self.targets.len()
        ));
        for target in &self.targets {
            form = form.child(format!(
                "{} / {} / {}",
                target.broker_name, target.broker_id, target.address
            ));
        }
        form = form
            .child("Subject (for example User:alice)")
            .child(
                div()
                    .debug_selector(|| "acl-policy-subject".into())
                    .child(Input::new(&self.subject).disabled(self.editing || locked)),
            )
            .child("Policy type");
        let mut types = div().flex().flex_wrap().gap_2();
        for (index, value) in ACL_POLICY_TYPES.iter().enumerate() {
            types = types.child(
                Button::new(("acl-policy-type", index))
                    .debug_selector(move || format!("acl-policy-type-{index}"))
                    .label(*value)
                    .selected(self.policy_type == *value)
                    .disabled(self.editing || locked)
                    .on_click(cx.listener(move |form, _, _, cx| {
                        form.policy_type = (*value).into();
                        cx.notify();
                    })),
            );
        }
        form = form
            .child(types)
            .child("Resource (identity is fixed when editing)")
            .child(
                div()
                    .debug_selector(|| "acl-policy-resource".into())
                    .child(Input::new(&self.resource).disabled(self.editing || locked)),
            );
        if self.deleting {
            form = form.child(
                "Delete only this resource entry in this policy type. Other resources and policy types are unaffected.",
            );
        } else {
            let mut actions = div().flex().flex_wrap().gap_2();
            for (index, value) in ACL_ACTIONS.iter().enumerate() {
                actions = actions.child(
                    Checkbox::new(("acl-action", index))
                        .debug_selector(move || format!("acl-action-{index}"))
                        .label(*value)
                        .checked(self.actions.iter().any(|action| action == value))
                        .disabled(locked)
                        .on_click(cx.listener(move |form, checked, _, cx| {
                            form.actions.retain(|action| action != value);
                            if *checked {
                                form.actions.push((*value).into());
                            }
                            cx.notify();
                        })),
                );
            }
            form = form
                .child("Actions")
                .child(actions)
                .child("Source IPs / CIDRs / wildcards / ranges (comma separated; empty means no restriction)")
                .child(
                    div()
                        .debug_selector(|| "acl-policy-source-ips".into())
                        .child(Input::new(&self.source_ips).disabled(locked)),
                );
            let mut decisions = div().flex().flex_wrap().gap_2();
            for (index, value) in ACL_DECISIONS.iter().enumerate() {
                decisions = decisions.child(
                    Button::new(("acl-decision", index))
                        .debug_selector(move || format!("acl-decision-{index}"))
                        .label(*value)
                        .selected(self.decision == *value)
                        .disabled(locked)
                        .on_click(cx.listener(move |form, _, _, cx| {
                            form.decision = (*value).into();
                            cx.notify();
                        })),
                );
            }
            form = form.child("Decision").child(decisions).child(
                "Only this structured resource entry is merged by the Broker; no full policy document is replaced.",
            );
        }
        if let Some(error) = &self.error {
            form = form.child(div().text_color(cx.theme().danger).child(error.clone()));
        }
        if self.attempted {
            form = form.child("This attempt cannot be replayed. Verify refreshed state and open a new form.");
        }
        div()
            .id("acl-policy-form")
            .max_h((window.viewport_size().height - px(220.)).max(px(120.)))
            .overflow_y_scrollbar()
            .child(form)
    }
}
