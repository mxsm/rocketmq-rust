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

//! Bounded, read-only detail of a local terminal event or unfinished accepted request.

use gpui::{
    ClipboardItem, Context, InteractiveElement as _, IntoElement, ParentElement as _, Render,
    StatefulInteractiveElement as _, Styled as _, Window, div, px,
};
use gpui_component::{StyledExt as _, button::Button};
use rocketmq_dashboard_common::{AuditAcceptedOperation, AuditAction, AuditEvent};

use super::audit_presentation::{
    action_label, actor_label, consumer_field_label, outcome_label, resource_label, timestamp, topic_field_label,
};
use crate::services::{AppServices, audit::AuditReadAccess};

#[derive(Clone)]
pub(super) enum AuditRecord {
    Terminal(AuditEvent),
    Pending(AuditAcceptedOperation),
}

pub(super) struct AuditDetail {
    pub record: Option<AuditRecord>,
    services: AppServices,
    access: AuditReadAccess,
}

impl AuditDetail {
    pub fn new(record: AuditRecord, services: AppServices, access: AuditReadAccess) -> Self {
        Self {
            record: Some(record),
            services,
            access,
        }
    }

    fn access_is_current(&self) -> bool {
        self.services
            .audit_read_access()
            .is_ok_and(|access| access == self.access)
    }
}

impl AuditRecord {
    pub fn operation(&self) -> &AuditAcceptedOperation {
        match self {
            Self::Terminal(event) => &event.operation,
            Self::Pending(operation) => operation,
        }
    }
}

fn line(label: &str, value: impl Into<String>) -> gpui::Div {
    div()
        .w_full()
        .min_w_0()
        .flex()
        .flex_col()
        .gap_1()
        .child(div().font_semibold().child(label.to_owned()))
        .child(
            div()
                .id(gpui::SharedString::from(label.to_owned()))
                .w_full()
                .min_w_0()
                .overflow_x_scroll()
                .child(div().whitespace_nowrap().child(value.into())),
        )
}

struct TargetCountProjection {
    label: &'static str,
    selector: &'static str,
    value: String,
}

fn target_count_projection(event: &AuditEvent) -> TargetCountProjection {
    let broker_unit = event.operation.action == AuditAction::BrokerUpdated
        || (matches!(
            event.operation.action,
            AuditAction::ConsumerCreated | AuditAction::ConsumerUpdated | AuditAction::ConsumerDeleted
        ) && event.facts.consumer_cleanup.is_some());
    // Older Consumer totals may combine Broker writes and cleanup steps. Missing unit evidence
    // must not relabel those stored totals, or generic queue/message counts, as Broker counts.
    let (label, selector) = if broker_unit {
        ("Broker targets", "audit-broker-target-facts")
    } else {
        ("Recorded targets (unit unspecified)", "audit-recorded-target-facts")
    };
    TargetCountProjection {
        label,
        selector,
        value: format!(
            "{} applied · {} unconfirmed",
            event.facts.applied_targets, event.facts.unconfirmed_targets
        ),
    }
}

fn render_target_counts(event: &AuditEvent) -> gpui::Div {
    let projection = target_count_projection(event);
    div()
        .debug_selector(move || projection.selector.into())
        .child(line(projection.label, projection.value))
}

impl Render for AuditDetail {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.access_is_current() {
            self.record = None;
            return div()
                .debug_selector(|| "audit-detail-access-changed".into())
                .child("Local Audit access changed. Reopen the record from an authorized query.")
                .into_any_element();
        }
        let Some(record) = self.record.as_ref() else {
            return div().into_any_element();
        };
        let operation = record.operation();
        let mut content = div()
            .w_full()
            .min_w_0()
            .flex()
            .flex_col()
            .gap_3()
            .child("Operations recorded by this desktop")
            .child(
                Button::new("audit-copy-request-id")
                    .debug_selector(|| "audit-copy-request-id".into())
                    .label("Copy request ID")
                    .outline()
                    .on_click(cx.listener(|detail, _, _, cx| {
                        if !detail.access_is_current() {
                            detail.record = None;
                            cx.notify();
                            return;
                        }
                        if let Some(record) = &detail.record {
                            cx.write_to_clipboard(ClipboardItem::new_string(
                                record.operation().context.request_id.clone(),
                            ));
                        }
                    })),
            )
            .child(line("Request ID", operation.context.request_id.clone()))
            .child(line("Actor", actor_label(&operation.context.actor)))
            .child(line("Action", action_label(operation.action)))
            .child(line("Resource", resource_label(&operation.context.resource)))
            .child(line("Environment ID", operation.context.environment_id.as_str()))
            .child(line(
                "Configuration revision",
                operation.context.configuration_revision.to_string(),
            ))
            .child(line("Accepted (UTC)", timestamp(operation.accepted_at_epoch_ms)));
        content = match record {
            AuditRecord::Terminal(event) => {
                let mut details = content
                    .child(line("Recorded (UTC)", timestamp(event.timestamp_epoch_ms)))
                    .child(line("Outcome", outcome_label(event.outcome)))
                    .child(line("Event ID", event.event_id.to_string()))
                    .child(render_target_counts(event))
                    .child(line(
                        "Authoritative refresh",
                        if event.facts.refresh_failed {
                            "Failed after operation"
                        } else {
                            "No refresh failure recorded"
                        },
                    ))
                    .child(line(
                        "Failure category",
                        event
                            .facts
                            .failure_code
                            .map_or_else(|| "None recorded".into(), |code| format!("{code:?}")),
                    ));
                if let Some(cleanup) = &event.facts.consumer_cleanup {
                    details = details.child(div().debug_selector(|| "audit-consumer-cleanup-facts".into()).child(
                        line(
                            "Consumer internal Topic cleanup steps",
                            format!(
                                "{} applied · {} failed · {} unconfirmed",
                                cleanup.applied_steps, cleanup.failed_steps, cleanup.unconfirmed_steps
                            ),
                        ),
                    ));
                }
                if let Some(actor) = &event.facts.authenticated_actor {
                    details = details.child(line("Authenticated actor", actor_label(actor)));
                }
                details
            }
            AuditRecord::Pending(_) => content.child(line(
                "State",
                "No terminal record. Verify authoritative state before any new operation; this page cannot replay it.",
            )),
        };
        if !operation.topic_fields.is_empty() {
            content = content.child(line(
                "Requested Topic field names",
                operation
                    .topic_fields
                    .iter()
                    .map(|field| topic_field_label(*field))
                    .collect::<Vec<_>>()
                    .join(", "),
            ));
        }
        if !operation.consumer_fields.is_empty() {
            content = content.child(
                div().debug_selector(|| "audit-consumer-field-facts".into()).child(line(
                    "Requested Consumer field names",
                    operation
                        .consumer_fields
                        .iter()
                        .map(|field| consumer_field_label(*field))
                        .collect::<Vec<_>>()
                        .join(", "),
                )),
            );
        }
        div()
            .id("audit-detail-scroll")
            .debug_selector(|| "audit-detail-scroll".into())
            .w_full()
            .max_w(window.viewport_size().width.min(px(640.)) - px(80.))
            .max_h((window.viewport_size().height - px(200.)).max(px(120.)))
            .overflow_y_scroll()
            .child(content)
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use gpui::{AppContext as _, Entity, point, size};
    use gpui_component::Root;
    use rocketmq_dashboard_common::AuditConsumerCleanupFacts;

    use super::*;

    fn legacy_delete() -> AuditEvent {
        serde_json::from_value(serde_json::json!({
            "eventId": 7,
            "operation": {
                "context": {
                    "environmentId": "env-a", "configurationRevision": 1,
                    "requestId": "legacy-consumer-delete",
                    "actor": { "kind": "local_desktop" },
                    "resource": { "kind": "consumer_group", "identity": "orders" }
                },
                "action": "consumer_deleted", "acceptedAtEpochMs": 1000
            },
            "outcome": "partial", "timestampEpochMs": 2000,
            "facts": { "appliedTargets": 3, "unconfirmedTargets": 2,
                "refreshFailed": false, "failureCode": null }
        }))
        .unwrap()
    }

    struct CountPreview {
        event: AuditEvent,
    }
    impl Render for CountPreview {
        fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
            render_target_counts(&self.event)
        }
    }

    #[gpui::test]
    fn legacy_and_separated_consumer_counts_render_without_inventing_broker_units(cx: &mut gpui::TestAppContext) {
        let legacy = legacy_delete();
        assert!(legacy.facts.consumer_cleanup.is_none());
        let projection = target_count_projection(&legacy);
        assert_eq!(projection.label, "Recorded targets (unit unspecified)");
        assert_eq!(projection.value, "3 applied · 2 unconfirmed");
        cx.update(gpui_component::init);
        let capture = std::rc::Rc::new(std::cell::RefCell::new(None));
        let captured = capture.clone();
        let (root, cx) = cx.add_window_view(move |window, cx| {
            let preview = cx.new(|_| CountPreview { event: legacy });
            captured.replace(Some(preview.clone()));
            Root::new(preview, window, cx)
        });
        let preview: Entity<CountPreview> = capture.borrow_mut().take().unwrap();
        let viewport = size(px(960.), px(640.));
        cx.simulate_resize(viewport);
        cx.draw(point(px(0.), px(0.)), viewport, |_, _| root.clone());
        let legacy_bounds = cx.debug_bounds("audit-recorded-target-facts").unwrap();
        assert!(legacy_bounds.size.height > px(20.) && legacy_bounds.right() <= viewport.width);
        let mut separated = legacy_delete();
        separated.facts.applied_targets = 1;
        separated.facts.unconfirmed_targets = 1;
        separated.facts.consumer_cleanup = Some(AuditConsumerCleanupFacts {
            applied_steps: 2,
            failed_steps: 0,
            unconfirmed_steps: 1,
        });
        let projection = target_count_projection(&separated);
        assert_eq!(projection.label, "Broker targets");
        assert_eq!(projection.value, "1 applied · 1 unconfirmed");
        assert_eq!(separated.facts.consumer_cleanup.as_ref().unwrap().applied_steps, 2);
        cx.update(|_, app| {
            preview.update(app, |preview, cx| {
                preview.event = separated;
                cx.notify();
            })
        });
        cx.draw(point(px(0.), px(0.)), viewport, |_, _| root.clone());
        let broker_bounds = cx.debug_bounds("audit-broker-target-facts").unwrap();
        assert!(broker_bounds.size.height > px(20.) && broker_bounds.right() <= viewport.width);
    }

    #[test]
    fn generic_queue_counts_preserve_their_numbers_without_a_broker_unit() {
        let mut event = legacy_delete();
        event.operation.action = AuditAction::ConsumerOffsetReset;
        event.facts.applied_targets = 23;
        event.facts.unconfirmed_targets = 4;
        let projection = target_count_projection(&event);
        assert_eq!(projection.label, "Recorded targets (unit unspecified)");
        assert_eq!(projection.value, "23 applied · 4 unconfirmed");
        event.operation.action = AuditAction::BrokerUpdated;
        event.facts.applied_targets = 1;
        event.facts.unconfirmed_targets = 0;
        let projection = target_count_projection(&event);
        assert_eq!(projection.label, "Broker targets");
        assert_eq!(projection.value, "1 applied · 0 unconfirmed");
    }

    #[gpui::test]
    fn detail_render_and_previously_rendered_copy_recheck_live_read_authority(cx: &mut gpui::TestAppContext) {
        use crate::features::audit::tests::Fixture;

        let fixture = Fixture::new(true);
        fixture.auth.authenticate("operator", "test-password").unwrap();
        let services = fixture.services.clone();
        let access = services.audit_read_access().unwrap();
        cx.update(gpui_component::init);
        let capture = std::rc::Rc::new(std::cell::RefCell::new(None));
        let captured = capture.clone();
        let (root, cx) = cx.add_window_view(move |window, cx| {
            let detail = cx.new(|_| AuditDetail::new(AuditRecord::Terminal(legacy_delete()), services, access));
            captured.replace(Some(detail.clone()));
            Root::new(detail, window, cx)
        });
        let detail: Entity<AuditDetail> = capture.borrow_mut().take().unwrap();
        let viewport = size(px(960.), px(640.));
        cx.simulate_resize(viewport);
        cx.draw(point(px(0.), px(0.)), viewport, |_, _| root.clone());
        let copy = cx.debug_bounds("audit-copy-request-id").unwrap();
        cx.simulate_click(copy.center(), gpui::Modifiers::default());
        assert_eq!(
            cx.update(|_, app| app.read_from_clipboard().and_then(|item| item.text())),
            Some("legacy-consumer-delete".into())
        );
        cx.update(|_, app| app.write_to_clipboard(ClipboardItem::new_string("unchanged".into())));
        fixture.auth.sign_out();
        // Do not redraw: the old frame still owns this actual Copy handler.
        cx.simulate_click(copy.center(), gpui::Modifiers::default());
        assert_eq!(
            cx.update(|_, app| app.read_from_clipboard().and_then(|item| item.text())),
            Some("unchanged".into())
        );
        assert!(cx.read(|app| detail.read(app).record.is_none()));

        fixture.auth.authenticate("operator", "test-password").unwrap();
        let access = fixture.services.audit_read_access().unwrap();
        cx.update(|_, app| {
            detail.update(app, |detail, cx| {
                *detail = AuditDetail::new(AuditRecord::Terminal(legacy_delete()), fixture.services.clone(), access);
                cx.notify();
            })
        });
        cx.draw(point(px(0.), px(0.)), viewport, |_, _| root.clone());
        assert!(cx.read(|app| detail.read(app).record.is_some()));
        fixture.auth.sign_out();
        cx.draw(point(px(0.), px(0.)), viewport, |_, _| root.clone());
        assert!(cx.read(|app| detail.read(app).record.is_none()));
        assert!(cx.debug_bounds("audit-detail-access-changed").is_some());
        fixture.runtime.shutdown_runtime_blocking().unwrap();
    }
}
