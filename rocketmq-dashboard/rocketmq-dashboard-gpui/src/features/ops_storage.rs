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

//! Native file evidence and explicit, revision-bound History settings drafts.

use gpui::{App, Subscription};
use gpui_component::{checkbox::Checkbox, input::InputEvent};
use rocketmq_dashboard_common::{ConnectionScope, HistoryRetention};

use super::*;
use crate::{
    infrastructure::{
        config_store::{HISTORY_MAX_POINTS_CAP, HISTORY_MAX_SERIES_CAP, HISTORY_MAX_TOTAL_POINTS_CAP},
        history_health::{CollectionAttempt, CollectionSideHealth},
        local_storage_status::{FileReadState, FileWriteState, LocalFileStatus},
    },
    services::local_storage::{HistorySettings, LocalRuntimeStatus},
    state::UiErrorCode,
};

pub(super) struct OpsStorage {
    pub revision: u64,
    pub enabled: bool,
    original: HistorySettings,
    pub inputs: [Entity<InputState>; 4],
    pub dirty: bool,
    pub validation: Option<UiError>,
    pub status: Option<LocalRuntimeStatus>,
    pub status_error: Option<UiError>,
    pub loading: bool,
    request_revision: Option<u64>,
    pub task: Option<Task<()>>,
    _subscriptions: Vec<Subscription>,
}

impl OpsStorage {
    pub fn new(config: &DesktopConfig, window: &mut Window, cx: &mut Context<OpsView>) -> Self {
        let settings = HistorySettings::from(config.foundations);
        let values = [
            settings.interval_seconds.to_string(),
            settings.retention.max_points_per_series.to_string(),
            settings.retention.max_series.to_string(),
            settings.retention.max_total_points.to_string(),
        ];
        let inputs = values.map(|value| cx.new(|cx| InputState::new(window, cx).default_value(value)));
        let subscriptions = inputs
            .iter()
            .map(|input| {
                cx.subscribe_in(input, window, |view: &mut OpsView, _, event, _, cx| {
                    if matches!(event, InputEvent::Change) {
                        view.storage.dirty = view.storage.read(cx) != Ok(view.storage.original);
                        view.storage.validation = None;
                        cx.notify();
                    }
                })
            })
            .collect();
        Self {
            revision: config.revision,
            enabled: settings.enabled,
            original: settings,
            inputs,
            dirty: false,
            validation: None,
            status: None,
            status_error: None,
            loading: false,
            request_revision: None,
            task: None,
            _subscriptions: subscriptions,
        }
    }

    pub fn reset(&mut self, config: &DesktopConfig, window: &mut Window, cx: &mut Context<OpsView>) {
        let settings = HistorySettings::from(config.foundations);
        let values = [
            settings.interval_seconds.to_string(),
            settings.retention.max_points_per_series.to_string(),
            settings.retention.max_series.to_string(),
            settings.retention.max_total_points.to_string(),
        ];
        for (input, value) in self.inputs.iter().zip(values) {
            input.update(cx, |input, cx| input.set_value(value, window, cx));
        }
        self.revision = config.revision;
        self.enabled = settings.enabled;
        self.original = settings;
        self.dirty = false;
        self.validation = None;
    }

    fn read(&self, cx: &App) -> Result<HistorySettings, UiError> {
        let invalid = || {
            UiError::new(
                "Enter whole nonnegative numbers for interval and retention limits.",
                UiErrorCode::Validation,
                false,
            )
        };
        HistorySettings {
            enabled: self.enabled,
            interval_seconds: self.inputs[0].read(cx).value().trim().parse().map_err(|_| invalid())?,
            retention: HistoryRetention {
                max_points_per_series: self.inputs[1].read(cx).value().trim().parse().map_err(|_| invalid())?,
                max_series: self.inputs[2].read(cx).value().trim().parse().map_err(|_| invalid())?,
                max_total_points: self.inputs[3].read(cx).value().trim().parse().map_err(|_| invalid())?,
            },
        }
        .validate()
    }
}

impl OpsView {
    pub fn refresh_visible_storage(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.section == OpsSection::Storage
            && (!self.storage.loading
                || self.storage.request_revision != Some(self.services.connection_state().config.revision))
        {
            self.refresh_storage(window, cx);
        }
    }

    pub(super) fn refresh_storage(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.storage.task = None;
        self.storage.loading = true;
        self.storage.status_error = None;
        let services = self.services.clone();
        let connection = services.connection_state();
        let revision = connection.config.revision;
        self.storage.request_revision = Some(revision);
        let environment = connection.environments.map(|catalog| catalog.active_id);
        self.storage.task = Some(cx.spawn_in(window, async move |this, cx| {
            let result = services.local_runtime_status().await;
            let _ = this.update_in(cx, |view, _, cx| {
                view.storage.loading = false;
                let current = services.connection_state();
                if current.config.revision != revision
                    || current.environments.map(|catalog| catalog.active_id) != environment
                {
                    view.storage.status = None;
                    view.storage.status_error = Some(UiError::new(
                        "Configuration changed while storage status was read. Refresh status.",
                        UiErrorCode::Configuration,
                        true,
                    ));
                    cx.notify();
                    return;
                }
                match result {
                    Ok(status) => {
                        view.storage.status = Some(status);
                        view.storage.status_error = None;
                    }
                    Err(error) => {
                        view.storage.status = None;
                        view.storage.status_error = Some(error);
                    }
                }
                cx.notify();
            });
        }));
        cx.notify();
    }

    fn save_history_settings(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.busy || !self.storage.dirty {
            return;
        }
        match self.storage.read(cx) {
            Ok(settings) => self.run_mutation(
                ConfigMutation::SaveHistorySettings {
                    expected_revision: self.storage.revision,
                    settings,
                },
                false,
                window,
                cx,
            ),
            Err(error) => {
                self.storage.validation = Some(error);
                cx.notify();
            }
        }
    }

    fn open_config_directory(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.busy {
            return;
        }
        self.busy = true;
        let services = self.services.clone();
        self.task = Some(cx.spawn_in(window, async move |this, cx| {
            let result = services.open_config_location().await;
            let _ = this.update_in(cx, |view, _, cx| {
                view.busy = false;
                view.error = result.err();
                cx.notify();
            });
        }));
        cx.notify();
    }

    pub(super) fn render_storage(&self, cx: &mut Context<Self>) -> gpui::Div {
        let theme = cx.theme();
        let mut status = div().flex().flex_col().gap_3()
            .child("Local file backend · single-writer directory mode. Observed ownership, reads and persistence are reported separately.")
            .child("Successful-write times below are observed in this process; they are not inferred from file dates or schema versions.");
        if self.storage.loading {
            status = status.child("Checking current local files…");
        }
        if let Some(error) = &self.storage.status_error {
            status = status.child(
                div()
                    .debug_selector(|| "ops-storage-status-error".into())
                    .text_color(theme.danger)
                    .child(error.summary().to_owned()),
            );
        }
        if let Some(snapshot) = &self.storage.status {
            status = status.child(
                div()
                    .debug_selector(|| "ops-local-runtime-status".into())
                    .child(key_values(
                        [
                            ("Environment", snapshot.environment_id.as_str().into()),
                            ("Loaded revision", snapshot.configuration_revision.to_string()),
                            (
                                "Configured scope",
                                match snapshot.scope {
                                    ConnectionScope::NameServer => "NameServer Direct",
                                    ConnectionScope::Proxy => "Proxy",
                                }
                                .into(),
                            ),
                            (
                                "Owned History collector",
                                if snapshot.collector_running {
                                    "Running"
                                } else {
                                    "Stopped"
                                }
                                .into(),
                            ),
                            (
                                "Effective History settings",
                                history_collection_label(FoundationFlags {
                                    history_enabled: snapshot.collection.enabled,
                                    history_interval_seconds: snapshot.collection.interval_seconds,
                                    ..self.config.foundations
                                }),
                            ),
                            (
                                "Latest stored sample (this session)",
                                time_label(snapshot.collection.latest_success()),
                            ),
                            ("Topic collection", collection_label(snapshot.collection.topics)),
                            ("Broker collection", collection_label(snapshot.collection.brokers)),
                            (
                                "Monitor",
                                "Rules are local; evaluation and notifications are not implemented".into(),
                            ),
                        ],
                        theme.border,
                        theme.muted_foreground,
                    )),
            );
        } else if !self.storage.loading {
            status = status.child("Local runtime status has not been read.");
        }
        let fields = [
            (
                "ops-history-interval",
                "Interval seconds (0 disables collection)",
                "Supported clock range; 0 disables",
                &self.storage.inputs[0],
            ),
            (
                "ops-history-points",
                "Points per series",
                "0 keeps no points",
                &self.storage.inputs[1],
            ),
            (
                "ops-history-series",
                "Maximum series",
                "0 keeps no series",
                &self.storage.inputs[2],
            ),
            (
                "ops-history-total",
                "Total points",
                "0 keeps no points",
                &self.storage.inputs[3],
            ),
        ];
        div().debug_selector(|| "ops-storage-page".into()).flex().flex_col().gap_4()
            .child(div().flex().flex_wrap().gap_2()
                .child(Button::new("refresh-storage-status").debug_selector(|| "refresh-storage-status".into()).label("Refresh status").disabled(self.storage.loading || self.busy).on_click(cx.listener(|view, _, window, cx| view.refresh_storage(window, cx))))
                .child(Button::new("reload-storage").debug_selector(|| "reload-storage".into()).label("Reload configuration").outline().disabled(self.busy).on_click(cx.listener(|view, _, window, cx| view.reload(window, cx))))
                .child(Button::new("ops-open-config-directory").debug_selector(|| "ops-open-config-directory".into()).label("Open config directory").outline().disabled(self.busy).on_click(cx.listener(|view, _, window, cx| view.open_config_directory(window, cx)))))
            .child(status)
            .child(div().font_semibold().child("History collection settings"))
            .child("These settings apply to this desktop. Samples keep their environment identity. Proxy scope can limit sampling capabilities; the collection results above report actual attempts.")
            .child(Checkbox::new("ops-history-enabled").debug_selector(|| "ops-history-enabled".into()).label("Enable History collection").checked(self.storage.enabled).disabled(self.busy).on_click(cx.listener(|view, enabled, _, cx| { view.storage.enabled = *enabled; view.storage.dirty = view.storage.read(cx) != Ok(view.storage.original); cx.notify(); })))
            .child(div().flex().flex_wrap().gap_3().children(fields.into_iter().map(|(id, label, hint, input)| div().flex_1().min_w(px(240.)).child(label).child(div().debug_selector(move || id.into()).child(Input::new(input).disabled(self.busy))).child(div().text_xs().text_color(theme.muted_foreground).child(hint)))))
            .child(div().text_sm().child(format!("Retention limits: up to {HISTORY_MAX_POINTS_CAP} points per series, {HISTORY_MAX_SERIES_CAP} series and {HISTORY_MAX_TOTAL_POINTS_CAP} total points.")))
            .child(div().text_sm().child("Retention takes effect on the next successful sample persistence."))
            .when(self.storage.dirty, |body| body.child(format!("Unsaved History draft based on revision {}. Current revision: {}. A stale draft cannot overwrite a newer configuration.", self.storage.revision, self.config.revision)))
            .when(self.storage.dirty && self.storage.revision != self.config.revision, |body| {
                let saved = HistorySettings::from(self.config.foundations);
                body.child(format!("Current saved History settings: enabled {}, interval {} seconds, {} points per series, {} series, {} total points.", saved.enabled, saved.interval_seconds, saved.retention.max_points_per_series, saved.retention.max_series, saved.retention.max_total_points))
                    .child(Button::new("ops-history-use-current-revision").debug_selector(|| "ops-history-use-current-revision".into()).label("Keep draft and use current revision").outline().disabled(self.busy).on_click(cx.listener(|view, _, _, cx| {
                        view.storage.revision = view.config.revision;
                        view.storage.original = HistorySettings::from(view.config.foundations);
                        view.storage.dirty = view.storage.read(cx) != Ok(view.storage.original);
                        view.storage.validation = None;
                        cx.notify();
                    })))
            })
            .children(self.storage.validation.as_ref().map(|error| div().text_color(theme.danger).child(error.summary().to_owned())))
            .child(div().flex().flex_wrap().gap_2()
                .child(Button::new("ops-save-history").debug_selector(|| "ops-save-history".into()).label("Save History settings").disabled(self.busy || !self.storage.dirty).on_click(cx.listener(|view, _, window, cx| view.save_history_settings(window, cx))))
                .child(Button::new("ops-discard-history").debug_selector(|| "ops-discard-history".into()).label("Discard History draft").outline().disabled(self.busy || !self.storage.dirty).on_click(cx.listener(|view, _, window, cx| { view.storage.reset(&view.config, window, cx); view.error = None; cx.notify(); }))))
            .children(self.storage.status.as_ref().map(|snapshot| div().flex().flex_col().gap_4()
                .child(render_file("Configuration / Monitor / Audit", &snapshot.configuration, cx))
                .child(render_file("History observations", &snapshot.history, cx))))
    }
}

fn render_file(title: &str, file: &LocalFileStatus, cx: &gpui::App) -> gpui::Div {
    let read = match file.evidence.read {
        FileReadState::Unknown => "Unknown / not validated",
        FileReadState::Readable => "Read and validation succeeded",
        FileReadState::Missing => "File is not present",
        FileReadState::MigrationRequired => "Older schema detected; reload to validate and migrate",
        FileReadState::Failed => "Current read or validation failed",
    };
    let write = match file.evidence.last_write {
        FileWriteState::NotObserved => "No write observed in this process",
        FileWriteState::Succeeded => "Last persistence attempt succeeded",
        FileWriteState::Failed => "Last persistence attempt failed",
    };
    div()
        .flex()
        .flex_col()
        .gap_2()
        .child(div().font_semibold().child(title.to_owned()))
        .child(key_values(
            [
                ("File", file.path.display().to_string()),
                (
                    "Directory writer",
                    if file.writer_owned {
                        "Owned by this process"
                    } else {
                        "Not owned"
                    }
                    .into(),
                ),
                ("Read health", read.into()),
                (
                    "Latest read attempt",
                    time_label(file.evidence.read_checked_at_epoch_ms),
                ),
                (
                    "Observed schema",
                    file.evidence
                        .schema_version
                        .map_or_else(|| "Unknown".into(), |value| value.to_string()),
                ),
                (
                    "Observed file revision",
                    file.evidence
                        .file_revision
                        .map_or_else(|| "Not available".into(), |value| value.to_string()),
                ),
                ("Persistence", write.into()),
                (
                    "Last successful write (this process)",
                    time_label(file.evidence.last_successful_write_epoch_ms),
                ),
            ],
            cx.theme().border,
            cx.theme().muted_foreground,
        ))
}

fn time_label(value: Option<u64>) -> String {
    value
        .and_then(|value| i64::try_from(value).ok())
        .and_then(chrono::DateTime::from_timestamp_millis)
        .map(|time| time.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| "Not observed in this process".into())
}

fn collection_label(side: CollectionSideHealth) -> String {
    let state = match side.last_attempt {
        None => "No attempt observed",
        Some(CollectionAttempt::OwnershipUnavailable) => "Waiting for collection ownership",
        Some(CollectionAttempt::NoObservations) => "No observations returned",
        Some(CollectionAttempt::NotRetained) => "No submitted observations retained",
        Some(CollectionAttempt::SamplingFailed) => "Sampling failed",
        Some(CollectionAttempt::PersistenceFailed) => "History persistence failed",
        Some(CollectionAttempt::Persisted) => "Sample persisted",
    };
    format!(
        "{state} · {} unavailable/omitted resources · last attempt: {}",
        side.failed_resources,
        time_label(side.last_attempt_epoch_ms)
    )
}

#[cfg(test)]
#[path = "ops_storage_tests.rs"]
mod tests;
