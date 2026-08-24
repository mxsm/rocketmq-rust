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

//! Bounded Topic dialog/form state. UI code owns at most one of these states
//! and never stores message bodies outside the active Send draft.

#[path = "topic_dialog_render.rs"]
mod render;

use std::fmt;

use gpui::{AppContext as _, Context, Entity, InteractiveElement as _, ParentElement as _, Styled as _, Window, div};
use gpui_component::{input::InputState, radio::Radio};
use rocketmq_dashboard_common::{
    TopicFailureCode, TopicFailureStage, TopicIdentity, TopicMessageType, TopicPartialOutcome, TopicPermission,
    TopicTargetIdentity,
};

use crate::{
    services::topics::{
        TopicCreateCommand, TopicDeleteBrokerCommand, TopicDeleteCommand, TopicMutationResult, TopicOffsetCommand,
        TopicQueuePatchCommand, TopicSendCommand,
    },
    state::{UiError, UiErrorCode},
};

pub struct TopicCreateDraft {
    pub topic_name: String,
    pub targets: Vec<TopicTargetIdentity>,
    pub read_queue_count: u32,
    pub write_queue_count: u32,
    pub permission: Option<TopicPermission>,
    pub ordered: Option<bool>,
    pub message_type: Option<TopicMessageType>,
    pub catalog_epoch: u64,
}

impl fmt::Debug for TopicCreateDraft {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TopicCreateDraft")
            .field("has_topic_name", &!self.topic_name.is_empty())
            .field("target_count", &self.targets.len())
            .field("read_queue_count", &self.read_queue_count)
            .field("write_queue_count", &self.write_queue_count)
            .field("permission_available", &self.permission.is_some())
            .field("ordered_selected", &self.ordered.is_some())
            .field("message_type", &self.message_type)
            .field("catalog_epoch", &self.catalog_epoch)
            .finish()
    }
}

pub struct TopicEditDraft {
    pub topic: TopicIdentity,
    pub target: TopicTargetIdentity,
    pub expected_version: u64,
    pub read_queue_count: u32,
    pub write_queue_count: u32,
}

impl fmt::Debug for TopicEditDraft {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TopicEditDraft")
            .field("expected_version", &self.expected_version)
            .field("read_queue_count", &self.read_queue_count)
            .field("write_queue_count", &self.write_queue_count)
            .finish_non_exhaustive()
    }
}

pub struct TopicSendDraft {
    pub topic: TopicIdentity,
    pub key: String,
    pub tag: String,
    body: String,
    pub trace_enabled: bool,
}

impl TopicSendDraft {
    pub fn new(topic: TopicIdentity) -> Self {
        Self {
            topic,
            key: String::new(),
            tag: String::new(),
            body: String::new(),
            trace_enabled: false,
        }
    }

    #[cfg(test)]
    pub fn set_body(&mut self, value: String) {
        self.body.clear();
        self.body = value;
    }

    #[cfg(test)]
    pub fn take_body(&mut self) -> String {
        std::mem::take(&mut self.body)
    }

    #[cfg(test)]
    pub fn body_is_empty(&self) -> bool {
        self.body.is_empty()
    }
}

impl fmt::Debug for TopicSendDraft {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TopicSendDraft")
            .field("has_key", &!self.key.is_empty())
            .field("has_tag", &!self.tag.is_empty())
            .field("body_length", &self.body.len())
            .field("trace_enabled", &self.trace_enabled)
            .finish_non_exhaustive()
    }
}

impl Drop for TopicSendDraft {
    fn drop(&mut self) {
        self.body.clear();
    }
}

pub enum TopicDialogKind {
    Create(TopicCreateDraft),
    Edit(TopicEditDraft),
    DeleteTopic {
        topic: TopicIdentity,
        clusters: Vec<String>,
    },
    DeleteBroker {
        topic: TopicIdentity,
        target: TopicTargetIdentity,
    },
    Send(TopicSendDraft),
    ResetOffset {
        topic: TopicIdentity,
        consumer_group: String,
        clusters: Vec<String>,
        timestamp: u64,
        force: bool,
    },
    SkipAccumulated {
        topic: TopicIdentity,
        consumer_group: String,
        clusters: Vec<String>,
        force: bool,
    },
}

impl fmt::Debug for TopicDialogKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Create(_) => "Create",
            Self::Edit(_) => "Edit",
            Self::DeleteTopic { .. } => "DeleteTopic",
            Self::DeleteBroker { .. } => "DeleteBroker",
            Self::Send(_) => "Send",
            Self::ResetOffset { .. } => "ResetOffset",
            Self::SkipAccumulated { .. } => "SkipAccumulated",
        };
        formatter
            .debug_struct("TopicDialogKind")
            .field("kind", &name)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TopicSubmitToken {
    revision: u64,
    epoch: u64,
}

#[derive(Debug)]
pub enum TopicSubmissionState {
    Idle,
    Submitting(TopicSubmitToken),
    Succeeded,
    PartiallySucceeded(TopicMutationResult),
    AppliedReloadFailed(TopicMutationResult),
    PatchAppliedReloadFailed {
        previous_version: u64,
        version: u64,
        error: UiError,
    },
    Rejected(TopicMutationResult),
    Conflict {
        actual_version: u64,
        submitted_read_queue_count: u32,
        submitted_write_queue_count: u32,
        authoritative_read_queue_count: u32,
        authoritative_write_queue_count: u32,
    },
    Failed(UiError),
}

pub struct TopicDialogState {
    pub kind: TopicDialogKind,
    pub submission: TopicSubmissionState,
    next_epoch: u64,
}

impl TopicDialogState {
    pub fn new(kind: TopicDialogKind) -> Self {
        Self {
            kind,
            submission: TopicSubmissionState::Idle,
            next_epoch: 0,
        }
    }

    pub fn begin_submit(&mut self, revision: u64) -> Option<TopicSubmitToken> {
        if !matches!(
            self.submission,
            TopicSubmissionState::Idle | TopicSubmissionState::Failed(_)
        ) {
            return None;
        }
        self.next_epoch = self.next_epoch.checked_add(1)?;
        let token = TopicSubmitToken {
            revision,
            epoch: self.next_epoch,
        };
        self.submission = TopicSubmissionState::Submitting(token);
        Some(token)
    }

    pub fn accepts(&self, token: TopicSubmitToken, revision: u64) -> bool {
        revision == token.revision
            && matches!(self.submission, TopicSubmissionState::Submitting(current) if current == token)
    }

    pub fn fail(&mut self, token: TopicSubmitToken, revision: u64, error: UiError) -> bool {
        if !self.accepts(token, revision) {
            return false;
        }
        self.submission = TopicSubmissionState::Failed(error);
        true
    }

    #[cfg(test)]
    pub fn invalidate(&mut self) {
        self.next_epoch = self.next_epoch.saturating_add(1);
        if matches!(self.submission, TopicSubmissionState::Submitting(_)) {
            self.submission = TopicSubmissionState::Idle;
        }
    }
}

pub enum PreparedTopicCommand {
    Create(TopicCreateCommand),
    Edit(TopicQueuePatchCommand),
    DeleteTopic(TopicDeleteCommand),
    DeleteBroker(TopicDeleteBrokerCommand),
    Send(TopicSendCommand),
    Reset(TopicOffsetCommand),
    Skip(TopicOffsetCommand),
}

enum TopicDialogInputs {
    Create {
        topic: Entity<InputState>,
        read_queues: Entity<InputState>,
        write_queues: Entity<InputState>,
        selected_targets: Vec<bool>,
        permission: Option<TopicPermission>,
        message_type: Option<TopicMessageType>,
        ordered: Option<bool>,
    },
    Edit {
        read_queues: Entity<InputState>,
        write_queues: Entity<InputState>,
    },
    Send {
        key: Entity<InputState>,
        tag: Entity<InputState>,
        body: Entity<InputState>,
    },
    Reset {
        timestamp: Entity<InputState>,
        selected_cluster: Option<String>,
    },
    DeleteTopic {
        confirmation: Entity<InputState>,
    },
    OffsetConfirmation {
        selected_cluster: Option<String>,
    },
    Confirmation,
}

pub struct TopicDialogForm {
    pub state: TopicDialogState,
    inputs: TopicDialogInputs,
    status: Option<String>,
}

impl TopicDialogForm {
    pub fn new(kind: TopicDialogKind, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let inputs = match &kind {
            TopicDialogKind::Create(draft) => TopicDialogInputs::Create {
                topic: cx.new(|cx| InputState::new(window, cx).placeholder("Topic name")),
                read_queues: number_input(draft.read_queue_count, "Read queues", window, cx),
                write_queues: number_input(draft.write_queue_count, "Write queues", window, cx),
                selected_targets: vec![false; draft.targets.len()],
                permission: draft.permission,
                message_type: draft.message_type,
                ordered: draft.ordered,
            },
            TopicDialogKind::Edit(draft) => TopicDialogInputs::Edit {
                read_queues: number_input(draft.read_queue_count, "Read queues", window, cx),
                write_queues: number_input(draft.write_queue_count, "Write queues", window, cx),
            },
            TopicDialogKind::Send(_) => TopicDialogInputs::Send {
                key: cx.new(|cx| InputState::new(window, cx).placeholder("Optional key")),
                tag: cx.new(|cx| InputState::new(window, cx).placeholder("Optional tag")),
                body: cx.new(|cx| InputState::new(window, cx).multi_line(true).placeholder("Message body")),
            },
            TopicDialogKind::ResetOffset {
                clusters, timestamp, ..
            } => TopicDialogInputs::Reset {
                timestamp: number_input(*timestamp, "Timestamp (epoch ms)", window, cx),
                selected_cluster: single_cluster(clusters),
            },
            TopicDialogKind::DeleteTopic { .. } => TopicDialogInputs::DeleteTopic {
                confirmation: cx.new(|cx| InputState::new(window, cx).placeholder("Type the complete Topic name")),
            },
            TopicDialogKind::SkipAccumulated { clusters, .. } => TopicDialogInputs::OffsetConfirmation {
                selected_cluster: single_cluster(clusters),
            },
            TopicDialogKind::DeleteBroker { .. } => TopicDialogInputs::Confirmation,
        };
        Self {
            state: TopicDialogState::new(kind),
            inputs,
            status: None,
        }
    }

    pub fn title(&self) -> &'static str {
        match self.state.kind {
            TopicDialogKind::Create(_) => "Create Topic",
            TopicDialogKind::Edit(_) => "Edit queue counts",
            TopicDialogKind::DeleteTopic { .. } => "Delete Topic",
            TopicDialogKind::DeleteBroker { .. } => "Delete Topic from Broker",
            TopicDialogKind::Send(_) => "Send message",
            TopicDialogKind::ResetOffset { .. } => "Reset consumer offset",
            TopicDialogKind::SkipAccumulated { .. } => "Skip accumulated messages",
        }
    }

    pub fn can_cancel(&self) -> bool {
        !matches!(self.state.submission, TopicSubmissionState::Submitting(_))
    }

    pub fn confirm_label(&self) -> &'static str {
        match self.state.kind {
            TopicDialogKind::Create(_) => "Create",
            TopicDialogKind::Edit(_) => "Apply",
            TopicDialogKind::DeleteTopic { .. } | TopicDialogKind::DeleteBroker { .. } => "Delete",
            TopicDialogKind::Send(_) => "Send",
            TopicDialogKind::ResetOffset { .. } => "Reset",
            TopicDialogKind::SkipAccumulated { .. } => "Skip",
        }
    }

    pub fn prepare(&self, cx: &gpui::App) -> Result<PreparedTopicCommand, UiError> {
        match (&self.state.kind, &self.inputs) {
            (
                TopicDialogKind::Create(draft),
                TopicDialogInputs::Create {
                    topic,
                    read_queues,
                    write_queues,
                    selected_targets,
                    permission,
                    message_type,
                    ordered,
                },
            ) => {
                let identity = TopicIdentity::parse(value(topic, cx)).map_err(validation)?;
                let targets = draft
                    .targets
                    .iter()
                    .zip(selected_targets)
                    .filter(|(_, selected)| **selected)
                    .map(|(target, _)| target.clone())
                    .collect::<Vec<_>>();
                if targets.is_empty() {
                    return Err(validation("select at least one exact Broker target"));
                }
                Ok(PreparedTopicCommand::Create(TopicCreateCommand {
                    topic: identity,
                    targets,
                    read_queue_count: queue_count(read_queues, cx)?,
                    write_queue_count: queue_count(write_queues, cx)?,
                    permission: permission.ok_or_else(|| validation("select a Topic permission"))?,
                    ordered: ordered.ok_or_else(|| validation("select ordered or unordered"))?,
                    message_type: message_type.ok_or_else(|| validation("select a message type"))?,
                }))
            }
            (
                TopicDialogKind::Edit(draft),
                TopicDialogInputs::Edit {
                    read_queues,
                    write_queues,
                },
            ) => Ok(PreparedTopicCommand::Edit(TopicQueuePatchCommand {
                topic: draft.topic.clone(),
                target: draft.target.clone(),
                expected_version: draft.expected_version,
                read_queue_count: Some(queue_count(read_queues, cx)?),
                write_queue_count: Some(queue_count(write_queues, cx)?),
            })),
            (TopicDialogKind::DeleteTopic { topic, clusters }, TopicDialogInputs::DeleteTopic { confirmation }) => {
                if value(confirmation, cx) != topic.as_str() {
                    return Err(validation("type the complete Topic name to confirm deletion"));
                }
                Ok(PreparedTopicCommand::DeleteTopic(TopicDeleteCommand {
                    topic: topic.clone(),
                    cluster_names: clusters.clone(),
                }))
            }
            (TopicDialogKind::DeleteBroker { topic, target }, TopicDialogInputs::Confirmation) => {
                Ok(PreparedTopicCommand::DeleteBroker(TopicDeleteBrokerCommand {
                    topic: topic.clone(),
                    target: target.clone(),
                }))
            }
            (TopicDialogKind::Send(draft), TopicDialogInputs::Send { key, tag, body }) => TopicSendCommand::new(
                draft.topic.clone(),
                value(key, cx),
                value(tag, cx),
                value(body, cx),
                draft.trace_enabled,
            )
            .map(PreparedTopicCommand::Send),
            (
                TopicDialogKind::ResetOffset {
                    topic,
                    consumer_group,
                    clusters,
                    force,
                    ..
                },
                TopicDialogInputs::Reset {
                    timestamp,
                    selected_cluster,
                },
            ) => Ok(PreparedTopicCommand::Reset(TopicOffsetCommand {
                topic: topic.clone(),
                consumer_group: consumer_group.clone(),
                cluster_name: exact_cluster(clusters, selected_cluster.as_deref())?,
                timestamp: Some(timestamp_value(timestamp, cx)?),
                force: *force,
            })),
            (
                TopicDialogKind::SkipAccumulated {
                    topic,
                    consumer_group,
                    clusters,
                    force,
                },
                TopicDialogInputs::OffsetConfirmation { selected_cluster },
            ) => Ok(PreparedTopicCommand::Skip(TopicOffsetCommand {
                topic: topic.clone(),
                consumer_group: consumer_group.clone(),
                cluster_name: exact_cluster(clusters, selected_cluster.as_deref())?,
                timestamp: None,
                force: *force,
            })),
            _ => Err(validation("dialog input state does not match the operation")),
        }
    }

    pub fn set_status(&mut self, status: impl Into<String>, cx: &mut Context<Self>) {
        self.status = Some(status.into());
        cx.notify();
    }

    pub fn reconcile_conflict(
        &mut self,
        actual_version: u64,
        submitted_read_queue_count: u32,
        submitted_write_queue_count: u32,
        authoritative_read_queue_count: u32,
        authoritative_write_queue_count: u32,
        cx: &mut Context<Self>,
    ) {
        self.state.submission = TopicSubmissionState::Conflict {
            actual_version,
            submitted_read_queue_count,
            submitted_write_queue_count,
            authoritative_read_queue_count,
            authoritative_write_queue_count,
        };
        self.status = Some(format!(
            "Configuration changed to version {actual_version}. Your submitted values are preserved; explicitly keep them or adopt the authoritative snapshot before confirming again."
        ));
        cx.notify();
    }

    fn resolve_conflict(&mut self, adopt_authoritative: bool, window: &mut Window, cx: &mut Context<Self>) {
        let (
            actual_version,
            submitted_read_queue_count,
            submitted_write_queue_count,
            authoritative_read_queue_count,
            authoritative_write_queue_count,
        ) = match &self.state.submission {
            TopicSubmissionState::Conflict {
                actual_version,
                submitted_read_queue_count,
                submitted_write_queue_count,
                authoritative_read_queue_count,
                authoritative_write_queue_count,
            } => (
                *actual_version,
                *submitted_read_queue_count,
                *submitted_write_queue_count,
                *authoritative_read_queue_count,
                *authoritative_write_queue_count,
            ),
            _ => return,
        };
        let (read, write, resolution) = if adopt_authoritative {
            (
                authoritative_read_queue_count,
                authoritative_write_queue_count,
                "Adopted the authoritative queue counts",
            )
        } else {
            (
                submitted_read_queue_count,
                submitted_write_queue_count,
                "Kept the submitted queue counts on the new version",
            )
        };
        if let TopicDialogKind::Edit(draft) = &mut self.state.kind {
            draft.expected_version = actual_version;
            draft.read_queue_count = read;
            draft.write_queue_count = write;
        }
        if let TopicDialogInputs::Edit {
            read_queues,
            write_queues,
        } = &self.inputs
        {
            read_queues.update(cx, |input, cx| input.set_value(read.to_string(), window, cx));
            write_queues.update(cx, |input, cx| input.set_value(write.to_string(), window, cx));
        }
        self.state.submission = TopicSubmissionState::Idle;
        self.status = Some(format!(
            "{resolution}; review and confirm against version {actual_version}."
        ));
        cx.notify();
    }

    #[cfg(test)]
    pub(crate) fn keep_submitted_after_conflict(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.resolve_conflict(false, window, cx);
    }

    pub fn clear_sensitive(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let TopicDialogInputs::Send { body, .. } = &self.inputs {
            body.update(cx, |input, cx| input.set_value(String::new(), window, cx));
        }
    }

    #[cfg(test)]
    pub(crate) fn set_create_text(
        &mut self,
        topic: &str,
        read_queues: u32,
        write_queues: u32,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let TopicDialogInputs::Create {
            topic: topic_input,
            read_queues: read_input,
            write_queues: write_input,
            ..
        } = &self.inputs
        {
            let topic = topic.to_owned();
            topic_input.update(cx, |input, cx| input.set_value(topic, window, cx));
            read_input.update(cx, |input, cx| input.set_value(read_queues.to_string(), window, cx));
            write_input.update(cx, |input, cx| input.set_value(write_queues.to_string(), window, cx));
        }
    }

    #[cfg(test)]
    pub(crate) fn set_create_options(
        &mut self,
        selected_target_indices: &[usize],
        permission_value: u8,
        message_type_value: TopicMessageType,
        ordered_value: bool,
        cx: &mut Context<Self>,
    ) {
        if let TopicDialogInputs::Create {
            selected_targets,
            permission,
            message_type,
            ordered,
            ..
        } = &mut self.inputs
        {
            selected_targets.fill(false);
            for index in selected_target_indices {
                if let Some(selected) = selected_targets.get_mut(*index) {
                    *selected = true;
                }
            }
            *permission = TopicPermission::parse(permission_value.into()).ok();
            *message_type = Some(message_type_value);
            *ordered = Some(ordered_value);
            cx.notify();
        }
    }

    #[cfg(test)]
    pub(crate) fn set_edit_queue_counts(&mut self, read: u32, write: u32, window: &mut Window, cx: &mut Context<Self>) {
        if let TopicDialogInputs::Edit {
            read_queues,
            write_queues,
        } = &self.inputs
        {
            read_queues.update(cx, |input, cx| input.set_value(read.to_string(), window, cx));
            write_queues.update(cx, |input, cx| input.set_value(write.to_string(), window, cx));
        }
    }

    #[cfg(test)]
    pub(crate) fn set_delete_confirmation(&mut self, value: &str, window: &mut Window, cx: &mut Context<Self>) {
        if let TopicDialogInputs::DeleteTopic { confirmation } = &self.inputs {
            let value = value.to_owned();
            confirmation.update(cx, |input, cx| input.set_value(value, window, cx));
        }
    }

    #[cfg(test)]
    pub(crate) fn set_send_text(
        &mut self,
        key: &str,
        tag: &str,
        body: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let TopicDialogInputs::Send {
            key: key_input,
            tag: tag_input,
            body: body_input,
        } = &self.inputs
        {
            let key = key.to_owned();
            let tag = tag.to_owned();
            let body = body.to_owned();
            key_input.update(cx, |input, cx| input.set_value(key, window, cx));
            tag_input.update(cx, |input, cx| input.set_value(tag, window, cx));
            body_input.update(cx, |input, cx| input.set_value(body, window, cx));
        }
    }

    #[cfg(test)]
    pub(crate) fn set_reset_timestamp(&mut self, timestamp: u64, window: &mut Window, cx: &mut Context<Self>) {
        if let TopicDialogInputs::Reset { timestamp: input, .. } = &self.inputs {
            input.update(cx, |input, cx| input.set_value(timestamp.to_string(), window, cx));
        }
    }

    #[cfg(test)]
    pub(crate) fn select_exact_cluster(&mut self, cluster: &str, cx: &mut Context<Self>) {
        let selected = Some(cluster.to_owned());
        match &mut self.inputs {
            TopicDialogInputs::Reset { selected_cluster, .. }
            | TopicDialogInputs::OffsetConfirmation { selected_cluster } => {
                *selected_cluster = selected;
                cx.notify();
            }
            _ => {}
        }
    }

    fn render_cluster_choices(&self, selected_cluster: Option<&str>, cx: &mut Context<Self>) -> gpui::Div {
        let clusters = match &self.state.kind {
            TopicDialogKind::ResetOffset { clusters, .. } | TopicDialogKind::SkipAccumulated { clusters, .. } => {
                clusters
            }
            _ => return div(),
        };
        div().child(
            div()
                .id("topic-offset-clusters")
                .flex()
                .flex_col()
                .gap_2()
                .child(div().text_sm().child("Exact cluster"))
                .children(clusters.iter().enumerate().map(|(index, cluster)| {
                    let owner = cx.entity().downgrade();
                    let selected = selected_cluster == Some(cluster.as_str());
                    let cluster = cluster.clone();
                    Radio::new(("topic-offset-cluster", index))
                        .label(cluster.clone())
                        .checked(selected)
                        .on_click(move |checked, _, cx| {
                            if *checked {
                                let _ = owner.update(cx, |form, cx| {
                                    match &mut form.inputs {
                                        TopicDialogInputs::Reset { selected_cluster, .. }
                                        | TopicDialogInputs::OffsetConfirmation { selected_cluster } => {
                                            *selected_cluster = Some(cluster.clone());
                                        }
                                        _ => return,
                                    }
                                    cx.notify();
                                });
                            }
                        })
                })),
        )
    }

    fn submission_summary(&self) -> Option<String> {
        match &self.state.submission {
            TopicSubmissionState::Idle => None,
            TopicSubmissionState::Submitting(_) => Some("Submitting once; duplicate submission is disabled.".into()),
            TopicSubmissionState::Succeeded => Some("Operation completed.".into()),
            TopicSubmissionState::PartiallySucceeded(TopicMutationResult::Applied { outcome, .. }) => {
                Some(render::format_outcome("Partial outcome", outcome))
            }
            TopicSubmissionState::AppliedReloadFailed(TopicMutationResult::AppliedReloadFailed {
                outcome,
                error,
                ..
            }) => Some(format!(
                "{}; reload failed: {}",
                render::format_outcome("Applied outcome", outcome),
                error.summary()
            )),
            TopicSubmissionState::PatchAppliedReloadFailed {
                previous_version,
                version,
                error,
            } => Some(format!(
                "Applied version {previous_version} → {version}, then reload failed: {}",
                error.summary()
            )),
            TopicSubmissionState::Rejected(TopicMutationResult::Rejected(outcome)) => {
                Some(render::format_outcome("Rejected", outcome))
            }
            TopicSubmissionState::Conflict {
                actual_version,
                submitted_read_queue_count,
                submitted_write_queue_count,
                authoritative_read_queue_count,
                authoritative_write_queue_count,
            } => Some(format!(
                "Version conflict at {actual_version}: submitted read/write {submitted_read_queue_count}/{submitted_write_queue_count}; authoritative read/write {authoritative_read_queue_count}/{authoritative_write_queue_count}."
            )),
            TopicSubmissionState::Failed(error) => Some(error.summary().to_owned()),
            TopicSubmissionState::PartiallySucceeded(_)
            | TopicSubmissionState::AppliedReloadFailed(_)
            | TopicSubmissionState::Rejected(_) => Some("Unexpected typed Topic outcome state.".into()),
        }
    }

    fn description(&self) -> String {
        topic_dialog_description(&self.state.kind)
    }
}

fn topic_dialog_description(kind: &TopicDialogKind) -> String {
    match kind {
        TopicDialogKind::Create(draft) => format!(
            "{} exact Broker target(s) selected from catalog generation {}. This is preflight + best effort; the protocol has no create-if-absent CAS.",
            draft.targets.len(),
            draft.catalog_epoch
        ),
        TopicDialogKind::Edit(draft) => format!(
            "Only read/write queue counts on {} / {} are writable. Permission, message type, and order remain read-only. Expected version {}.",
            draft.target.cluster_name(),
            draft.target.broker_name(),
            draft.expected_version
        ),
        TopicDialogKind::DeleteTopic { topic, clusters } => format!(
            "Delete {} from exact cluster set: {}. This is preflight + best effort, not CAS.",
            topic.as_str(),
            clusters.join(", ")
        ),
        TopicDialogKind::DeleteBroker { topic, target } => format!(
            "Delete {} from exact Broker {} / {}. This is preflight + best effort, not CAS.",
            topic.as_str(),
            target.cluster_name(),
            target.broker_name()
        ),
        TopicDialogKind::Send(draft) => format!(
            "Send one ephemeral message to {}. The body is cleared when this dialog closes.",
            draft.topic.as_str()
        ),
        TopicDialogKind::ResetOffset {
            topic,
            consumer_group,
            clusters,
            ..
        } => format!(
            "Reset exact Topic {} / consumer group {} in one selected cluster from [{}] to the supplied timestamp.",
            topic.as_str(),
            consumer_group,
            clusters.join(", ")
        ),
        TopicDialogKind::SkipAccumulated {
            topic,
            consumer_group,
            clusters,
            ..
        } => format!(
            "Skip accumulated messages for exact Topic {} / consumer group {} in one selected cluster from [{}]; this advances offsets to latest.",
            topic.as_str(),
            consumer_group,
            clusters.join(", ")
        ),
    }
}

fn number_input(
    value: impl ToString,
    placeholder: &'static str,
    window: &mut Window,
    cx: &mut Context<TopicDialogForm>,
) -> Entity<InputState> {
    let value = value.to_string();
    cx.new(|cx| {
        InputState::new(window, cx)
            .placeholder(placeholder)
            .default_value(value)
    })
}

fn value(input: &Entity<InputState>, cx: &gpui::App) -> String {
    input.read(cx).value().to_string()
}

fn queue_count(input: &Entity<InputState>, cx: &gpui::App) -> Result<u32, UiError> {
    let count = value(input, cx).parse::<u32>().map_err(validation)?;
    if !(1..=128).contains(&count) {
        return Err(validation("queue count must be between 1 and 128"));
    }
    Ok(count)
}

fn timestamp_value(input: &Entity<InputState>, cx: &gpui::App) -> Result<u64, UiError> {
    value(input, cx).parse::<u64>().map_err(validation)
}

fn single_cluster(clusters: &[String]) -> Option<String> {
    match clusters {
        [cluster] => Some(cluster.clone()),
        _ => None,
    }
}

fn exact_cluster(clusters: &[String], selected: Option<&str>) -> Result<String, UiError> {
    let selected = selected.ok_or_else(|| validation("select one exact cluster"))?;
    if clusters.iter().any(|cluster| cluster == selected) {
        Ok(selected.to_owned())
    } else {
        Err(validation("selected cluster is not in the authoritative Topic route"))
    }
}

fn validation(error: impl fmt::Display) -> UiError {
    UiError::new(
        format!("Invalid Topic operation: {error}"),
        UiErrorCode::Validation,
        false,
    )
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use gpui::AppContext as _;
    use gpui_component::Root;

    use crate::state::UiErrorCode;

    use super::*;

    #[test]
    fn create_description_discloses_best_effort_without_claiming_create_cas() {
        let description = topic_dialog_description(&TopicDialogKind::Create(TopicCreateDraft {
            topic_name: String::new(),
            targets: vec![
                TopicTargetIdentity::parse("cluster-a", "broker-a", "127.0.0.1:10911").expect("static target"),
            ],
            read_queue_count: 8,
            write_queue_count: 8,
            permission: TopicPermission::parse(6).ok(),
            ordered: Some(false),
            message_type: Some(TopicMessageType::Normal),
            catalog_epoch: 7,
        }));

        assert!(description.contains("preflight + best effort"));
        assert!(description.contains("no create-if-absent CAS"));
    }

    #[gpui::test]
    fn create_form_requires_explicit_options_and_prepares_the_complete_selected_command(cx: &mut gpui::TestAppContext) {
        cx.update(gpui_component::init);
        let second = TopicTargetIdentity::parse("cluster-b", "broker-b", "127.0.0.2:10911").expect("target");
        let captured = Rc::new(RefCell::new(None));
        let capture = captured.clone();
        let (_root, cx) = cx.add_window_view(move |window, cx| {
            let form = cx.new(|cx| {
                TopicDialogForm::new(
                    TopicDialogKind::Create(TopicCreateDraft {
                        topic_name: String::new(),
                        targets: vec![
                            TopicTargetIdentity::parse("cluster-a", "broker-a", "127.0.0.1:10911").expect("target"),
                            second,
                        ],
                        read_queue_count: 8,
                        write_queue_count: 8,
                        permission: None,
                        ordered: None,
                        message_type: None,
                        catalog_epoch: 11,
                    }),
                    window,
                    cx,
                )
            });
            capture.replace(Some(form.clone()));
            Root::new(form, window, cx)
        });
        let form = captured.borrow_mut().take().expect("form");
        cx.read(|app| assert!(form.read(app).prepare(app).is_err()));
        cx.update(|window, app| {
            form.update(app, |form, cx| {
                let TopicDialogInputs::Create {
                    topic,
                    read_queues,
                    write_queues,
                    selected_targets,
                    permission,
                    message_type,
                    ordered,
                } = &mut form.inputs
                else {
                    panic!("create inputs");
                };
                topic.update(cx, |input, cx| input.set_value("orders-v2", window, cx));
                read_queues.update(cx, |input, cx| input.set_value("12", window, cx));
                write_queues.update(cx, |input, cx| input.set_value("13", window, cx));
                selected_targets[1] = true;
                *permission = TopicPermission::parse(7).ok();
                *message_type = Some(TopicMessageType::Fifo);
                *ordered = Some(true);
            });
        });
        cx.read(|app| {
            let PreparedTopicCommand::Create(command) = form.read(app).prepare(app).expect("create command") else {
                panic!("create command");
            };
            assert_eq!(command.topic.as_str(), "orders-v2");
            assert_eq!(command.targets.len(), 1);
            assert_eq!(command.targets[0].broker_name(), "broker-b");
            assert_eq!(command.read_queue_count, 12);
            assert_eq!(command.write_queue_count, 13);
            assert_eq!(command.permission.bits(), 7);
            assert!(command.ordered);
            assert_eq!(command.message_type, TopicMessageType::Fifo);
        });
    }

    #[test]
    fn duplicate_submit_is_disabled_and_stale_result_is_rejected() {
        let mut dialog = TopicDialogState::new(TopicDialogKind::Send(TopicSendDraft::new(
            TopicIdentity::parse("orders").expect("topic"),
        )));
        let first = dialog.begin_submit(4).expect("first");
        assert!(dialog.begin_submit(4).is_none());
        dialog.invalidate();
        assert!(!dialog.fail(first, 4, UiError::new("stale", UiErrorCode::Connection, true)));
    }

    #[gpui::test]
    fn edit_conflict_preserves_submitted_draft_until_explicit_resolution(cx: &mut gpui::TestAppContext) {
        cx.update(gpui_component::init);
        let captured = Rc::new(RefCell::new(None));
        let capture = captured.clone();
        let (_root, cx) = cx.add_window_view(move |window, cx| {
            let form = cx.new(|cx| {
                TopicDialogForm::new(
                    TopicDialogKind::Edit(TopicEditDraft {
                        topic: TopicIdentity::parse("orders").expect("topic"),
                        target: TopicTargetIdentity::parse("cluster-a", "broker-a", "127.0.0.1:10911").expect("target"),
                        expected_version: 7,
                        read_queue_count: 8,
                        write_queue_count: 8,
                    }),
                    window,
                    cx,
                )
            });
            capture.replace(Some(form.clone()));
            Root::new(form, window, cx)
        });
        let form = captured.borrow_mut().take().expect("form");

        cx.update(|window, app| {
            form.update(app, |form, cx| {
                let (read_queues, write_queues) = match &form.inputs {
                    TopicDialogInputs::Edit {
                        read_queues,
                        write_queues,
                    } => (read_queues.clone(), write_queues.clone()),
                    _ => panic!("edit inputs"),
                };
                read_queues.update(cx, |input, cx| input.set_value("12", window, cx));
                write_queues.update(cx, |input, cx| input.set_value("13", window, cx));
                form.reconcile_conflict(9, 12, 13, 20, 21, cx);
                assert!(form.state.begin_submit(7).is_none());
                assert_eq!(value(&read_queues, cx), "12");
                assert_eq!(value(&write_queues, cx), "13");
            });
        });

        cx.update(|window, app| {
            form.update(app, |form, cx| form.resolve_conflict(false, window, cx));
            let PreparedTopicCommand::Edit(command) = form.read(app).prepare(app).expect("resolved command") else {
                panic!("edit command");
            };
            assert_eq!(command.expected_version, 9);
            assert_eq!(command.read_queue_count, Some(12));
            assert_eq!(command.write_queue_count, Some(13));
        });
    }

    #[test]
    fn send_body_is_not_debuggable_and_is_cleared_when_taken_or_closed() {
        let mut draft = TopicSendDraft::new(TopicIdentity::parse("orders").expect("topic"));
        draft.set_body("message-secret-value".into());
        assert!(!format!("{draft:?}").contains("message-secret-value"));
        assert_eq!(draft.take_body(), "message-secret-value");
        assert!(draft.body_is_empty());
    }

    #[test]
    fn reset_and_skip_are_distinct_exact_confirmed_intents() {
        let topic = TopicIdentity::parse("orders").expect("topic");
        let reset = TopicDialogKind::ResetOffset {
            topic: topic.clone(),
            consumer_group: "group-a".into(),
            clusters: vec!["cluster-a".into()],
            timestamp: 123,
            force: true,
        };
        let skip = TopicDialogKind::SkipAccumulated {
            topic,
            consumer_group: "group-a".into(),
            clusters: vec!["cluster-a".into()],
            force: true,
        };
        assert!(matches!(reset, TopicDialogKind::ResetOffset { timestamp: 123, .. }));
        assert!(matches!(skip, TopicDialogKind::SkipAccumulated { .. }));
        let clusters = vec!["cluster-a".into(), "cluster-b".into()];
        assert!(exact_cluster(&clusters, None).is_err());
        assert!(exact_cluster(&clusters, Some("cluster-c")).is_err());
        assert_eq!(exact_cluster(&clusters, Some("cluster-b")).expect("exact"), "cluster-b");
    }

    #[test]
    fn partial_and_applied_reload_failed_results_cannot_replay_the_same_command() {
        let topic = TopicIdentity::parse("orders").expect("topic");
        let outcome = TopicPartialOutcome {
            topic: topic.clone(),
            kind: rocketmq_dashboard_common::TopicMutationKind::DeleteTopic,
            guarantee: rocketmq_dashboard_common::TopicMutationGuarantee::PreflightBestEffort,
            targets: vec![rocketmq_dashboard_common::TopicTargetOutcome {
                target: "broker-a".into(),
                stage: TopicFailureStage::Mutation,
                applied: true,
                failure: None,
                retryable: false,
            }],
            reload_failed: false,
        };
        let mut dialog = TopicDialogState::new(TopicDialogKind::DeleteTopic {
            topic,
            clusters: vec!["cluster-a".into()],
        });
        dialog.submission = TopicSubmissionState::PartiallySucceeded(TopicMutationResult::Applied {
            outcome: outcome.clone(),
            inventory: None,
            consumers: None,
            invalidations: Vec::new(),
        });
        assert!(dialog.begin_submit(7).is_none());
        dialog.submission = TopicSubmissionState::AppliedReloadFailed(TopicMutationResult::AppliedReloadFailed {
            outcome,
            invalidations: Vec::new(),
            error: UiError::new("reload failed", UiErrorCode::Connection, true),
        });
        assert!(dialog.begin_submit(7).is_none());
    }

    #[test]
    fn reset_and_skip_partial_results_cannot_replay_applied_targets() {
        for (kind, dialog_kind) in [
            (
                rocketmq_dashboard_common::TopicMutationKind::ResetOffset,
                TopicDialogKind::ResetOffset {
                    topic: TopicIdentity::parse("orders").expect("topic"),
                    consumer_group: "group-a".into(),
                    clusters: vec!["cluster-a".into()],
                    timestamp: 0,
                    force: false,
                },
            ),
            (
                rocketmq_dashboard_common::TopicMutationKind::SkipBacklog,
                TopicDialogKind::SkipAccumulated {
                    topic: TopicIdentity::parse("orders").expect("topic"),
                    consumer_group: "group-a".into(),
                    clusters: vec!["cluster-a".into()],
                    force: false,
                },
            ),
        ] {
            let outcome = TopicPartialOutcome {
                topic: TopicIdentity::parse("orders").expect("topic"),
                kind,
                guarantee: rocketmq_dashboard_common::TopicMutationGuarantee::PreflightBestEffort,
                targets: vec![
                    rocketmq_dashboard_common::TopicTargetOutcome {
                        target: "broker-a / queue 0".into(),
                        stage: TopicFailureStage::Mutation,
                        applied: true,
                        failure: None,
                        retryable: false,
                    },
                    rocketmq_dashboard_common::TopicTargetOutcome {
                        target: "broker-a / queue 1".into(),
                        stage: TopicFailureStage::Mutation,
                        applied: false,
                        failure: Some(rocketmq_dashboard_common::TopicFailureCode::Unavailable),
                        retryable: true,
                    },
                ],
                reload_failed: false,
            };
            let mut dialog = TopicDialogState::new(dialog_kind);
            dialog.submission = TopicSubmissionState::PartiallySucceeded(TopicMutationResult::Applied {
                outcome,
                inventory: None,
                consumers: None,
                invalidations: Vec::new(),
            });
            assert!(dialog.begin_submit(7).is_none());
        }
    }
}
