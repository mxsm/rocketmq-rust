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
    InteractiveElement as _, IntoElement, ParentElement as _, Render, Styled as _, div, prelude::FluentBuilder as _, px,
};
use gpui_component::{
    ActiveTheme as _, Disableable as _, StyledExt as _, button::Button, radio::Radio, scroll::ScrollableElement as _,
};

use super::*;
use crate::features::ops::key_values;

pub(super) fn engine_label(engine: StorageEngine) -> &'static str {
    match engine {
        StorageEngine::File => "File",
        StorageEngine::Sqlite => "SQLite",
        StorageEngine::MySql => "MySQL",
        StorageEngine::Postgres => "PostgreSQL",
    }
}

impl Render for StorageSetupView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let mut body = div().id("storage-setup-content").debug_selector(|| "storage-setup-content".into()).w_full().min_w_0().h_auto().flex_shrink_0().whitespace_normal().flex().flex_col().gap_3()
            .child(paragraph(if self.mode == StorageSetupMode::Recovery { "Storage setup" } else { "Storage engine" }).font_semibold())
            .child(paragraph("The chosen authority stores configuration, environments, History, Monitor, Sessions and Audit. Engine changes apply after restarting Dashboard."))
            .child(paragraph("Database connection details come only from ROCKETMQ_DASHBOARD_GPUI_DATABASE_URL. Set it externally before starting Dashboard. Its value is never displayed or saved here."));
        if let Some(snapshot) = &self.snapshot {
            body = body
                .child(paragraph(format!(
                    "Current engine: {} · selection source: {}",
                    snapshot.current_engine.map_or("Unavailable", engine_label),
                    match snapshot.source {
                        SelectionSource::Default => "default",
                        SelectionSource::Saved => "saved choice",
                        SelectionSource::Environment => "external environment override",
                        SelectionSource::Injected => "injected authority",
                    }
                )))
                .child(paragraph(format!(
                    "Saved next-launch choice: {}",
                    snapshot.saved_engine.map_or("None (File by default)", engine_label)
                )))
                .child(paragraph(format!("Choice file: {}", snapshot.selection_path.display())));
            if snapshot.source == SelectionSource::Environment {
                body = body.child(paragraph("ROCKETMQ_DASHBOARD_GPUI_STORAGE_ENGINE overrides the saved choice. Change or remove that external override and restart to use a different engine."));
            }
            if let Some(error) = &snapshot.selection_error {
                body = body.child(
                    paragraph(format!(
                        "The saved choice could not be read. {} An explicit Save choice replaces it.",
                        error.summary()
                    ))
                    .text_color(cx.theme().danger),
                );
            }
            body = match &snapshot.status {
                Ok(status) => body.child(
                    div()
                        .w_full()
                        .min_w_0()
                        .flex_shrink_0()
                        .debug_selector(|| "storage-authority-status".into())
                        .child(key_values(
                            [
                                ("Authority", engine_label(status.engine).into()),
                                ("Status checked", time_label(snapshot.checked_at_epoch_ms)),
                                (
                                    "Workspace",
                                    if status.engine == StorageEngine::File {
                                        "File authority — persistence reported separately"
                                    } else if status.initialized {
                                        "Initialized"
                                    } else {
                                        "Empty — explicit setup required"
                                    }
                                    .into(),
                                ),
                                ("Storage schema", status.schema_version.to_string()),
                                (
                                    "Loaded document schema",
                                    status
                                        .document_schema_version
                                        .map_or_else(|| "Not present".into(), |value| value.to_string()),
                                ),
                                (
                                    if status.engine == StorageEngine::File {
                                        "Last write observed by this process"
                                    } else {
                                        "Last write recorded by shared authority"
                                    },
                                    time_label(status.last_successful_write_epoch_ms),
                                ),
                                (
                                    "Pool connections",
                                    status
                                        .pool_connections
                                        .map_or_else(|| "Not applicable".into(), |value| value.to_string()),
                                ),
                                (
                                    "Idle connections",
                                    status
                                        .pool_idle_connections
                                        .map_or_else(|| "Not applicable".into(), |value| value.to_string()),
                                ),
                            ],
                            cx.theme().border,
                            cx.theme().muted_foreground,
                        )),
                ),
                Err(error) => body.child(
                    paragraph(format!("Current authority status unavailable. {}", error.summary()))
                        .debug_selector(|| "storage-authority-error".into())
                        .text_color(cx.theme().danger),
                ),
            };
        }
        let options = [
            StorageEngine::File,
            StorageEngine::Sqlite,
            StorageEngine::MySql,
            StorageEngine::Postgres,
        ];
        body = body
            .child(
                div()
                    .w_full()
                    .min_w_0()
                    .flex_shrink_0()
                    .flex()
                    .flex_wrap()
                    .gap_3()
                    .children(options.into_iter().enumerate().map(|(index, engine)| {
                        let disabled = self.busy
                            || self.loading
                            || !engine.is_compiled()
                            || self
                                .snapshot
                                .as_ref()
                                .is_none_or(|snapshot| snapshot.source == SelectionSource::Environment);
                        Radio::new(("storage-engine-choice", index))
                            .debug_selector(move || format!("storage-engine-{index}"))
                            .label(format!(
                                "{}{}",
                                engine_label(engine),
                                if engine.is_compiled() { "" } else { " (not compiled)" }
                            ))
                            .checked(self.draft_engine == engine)
                            .disabled(disabled)
                            .on_key_down(cx.listener(move |view, event: &gpui::KeyDownEvent, _, cx| {
                                if !disabled && matches!(event.keystroke.key.as_str(), "space" | "enter") {
                                    view.draft_engine = engine;
                                    cx.stop_propagation();
                                    cx.notify();
                                }
                            }))
                            .on_click(cx.listener(move |view, checked, _, cx| {
                                if *checked {
                                    view.draft_engine = engine;
                                    cx.notify();
                                }
                            }))
                    })),
            )
            .child(
                div()
                    .w_full()
                    .min_w_0()
                    .flex_shrink_0()
                    .flex()
                    .flex_wrap()
                    .gap_2()
                    .child(
                        Button::new("storage-save-engine")
                            .debug_selector(|| "storage-save-engine".into())
                            .label("Save choice for next launch")
                            .disabled(!self.can_save())
                            .on_click(cx.listener(|view, _, window, cx| view.confirm_save(window, cx))),
                    )
                    .child(
                        Button::new("storage-refresh")
                            .debug_selector(|| "storage-refresh".into())
                            .label("Refresh engine status")
                            .outline()
                            .disabled(self.busy || self.loading)
                            .on_click(cx.listener(|view, _, _, cx| view.refresh(cx))),
                    )
                    .when(self.mode == StorageSetupMode::Recovery, |controls| {
                        controls.child(
                            Button::new("storage-quit")
                                .debug_selector(|| "storage-quit".into())
                                .label("Quit Dashboard")
                                .outline()
                                .disabled(self.busy)
                                .on_click(|_, _, cx| cx.quit()),
                        )
                    }),
            );
        if self.loading {
            body = body.child(paragraph("Reading selected authority…"));
        }
        if self.busy {
            body = body.child(paragraph(
                "Saving… This accepted operation remains owned by the application if this view closes.",
            ));
        }
        if let Some(notice) = &self.notice {
            body = body.child(paragraph(notice.clone()));
        }
        if let Some(error) = &self.error {
            body = body.child(
                paragraph(error.summary().to_owned())
                    .debug_selector(|| "storage-setup-error".into())
                    .text_color(cx.theme().danger),
            );
        }
        if self.must_refresh {
            body = body.child(paragraph("Workspace setup was not confirmed. Source files remain unchanged. Refresh status to check the destination before another attempt."));
        }
        if self.mode == StorageSetupMode::Recovery {
            body = body
                .child(paragraph(format!(
                    "Legacy configuration source: {}. History and Sessions are read from the same directory.",
                    self.source_path.display()
                )))
                .child(
                    div()
                        .w_full()
                        .min_w_0()
                        .flex_shrink_0()
                        .flex()
                        .flex_wrap()
                        .gap_2()
                        .child(
                            Button::new("storage-initialize")
                                .debug_selector(|| "storage-initialize".into())
                                .label("Initialize workspace")
                                .disabled(!self.can_setup())
                                .on_click(cx.listener(|view, _, window, cx| {
                                    view.confirm_setup(WorkspaceSetupAction::Initialize, window, cx)
                                })),
                        )
                        .child(
                            Button::new("storage-choose-source")
                                .debug_selector(|| "storage-choose-source".into())
                                .label("Choose legacy configuration…")
                                .outline()
                                .disabled(!self.can_setup())
                                .on_click(cx.listener(|view, _, _, cx| view.choose_source(cx))),
                        )
                        .child(
                            Button::new("storage-import")
                                .debug_selector(|| "storage-import".into())
                                .label("Import legacy workspace")
                                .outline()
                                .disabled(!self.can_setup() || self.source_path.as_os_str().is_empty())
                                .on_click(cx.listener(|view, _, window, cx| {
                                    view.confirm_setup(
                                        WorkspaceSetupAction::ImportLegacy {
                                            config_path: view.source_path.clone(),
                                        },
                                        window,
                                        cx,
                                    )
                                })),
                        ),
                )
                .child(
                    div()
                        .w_full()
                        .min_w_0()
                        .flex_shrink_0()
                        .flex()
                        .flex_wrap()
                        .gap_2()
                        .child(
                            Button::new("storage-continue")
                                .debug_selector(|| "storage-continue".into())
                                .label("Continue startup")
                                .disabled(self.busy || self.loading || !self.initialized())
                                .on_click(cx.listener(|_, _, _, cx| cx.emit(StorageSetupIntent::Completed))),
                        )
                        .child(
                            Button::new("storage-back")
                                .debug_selector(|| "storage-back".into())
                                .label("Back")
                                .outline()
                                .disabled(self.busy)
                                .on_click(cx.listener(|_, _, _, cx| cx.emit(StorageSetupIntent::Back))),
                        ),
                );
            div()
                .id("storage-setup-viewport")
                .debug_selector(|| "storage-setup-viewport".into())
                .size_full()
                .track_focus(&self.focus_handle)
                .child(
                    div()
                        .size_full()
                        .overflow_y_scrollbar()
                        .p_6()
                        .child(body.max_w(px(1000.))),
                )
                .into_any_element()
        } else {
            body.into_any_element()
        }
    }
}

// Wrapped text must keep its measured height rather than share the flex column's shrink budget.
fn paragraph(text: impl Into<gpui::SharedString>) -> gpui::Div {
    div()
        .w_full()
        .min_w_0()
        .h_auto()
        .flex_shrink_0()
        .whitespace_normal()
        .child(text.into())
}

fn time_label(value: Option<u64>) -> String {
    value
        .and_then(|value| i64::try_from(value).ok())
        .and_then(chrono::DateTime::from_timestamp_millis)
        .map(|time| time.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| "Not observed".into())
}
