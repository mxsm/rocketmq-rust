// Copyright 2023 The RocketMQ Rust Authors
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

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use rocketmq_sre_contracts::ConversationId;
use rocketmq_sre_contracts::ConversationTurnId;
use rocketmq_sre_contracts::ConversationTurnStatus;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::EvidenceId;
use serde::Serialize;
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;

use crate::workflow::ConversationTurnView;

pub(crate) const CONVERSATION_STREAM_SCHEMA: &str = "rocketmq-sre.conversation-stream-event.v1";
pub(crate) const CONVERSATION_STREAM_CAPACITY: usize = 32;

#[derive(Debug, Serialize)]
pub(crate) struct ConversationStreamEvent {
    pub(crate) schema_version: &'static str,
    pub(crate) sequence: u64,
    pub(crate) event_type: &'static str,
    pub(crate) conversation_id: ConversationId,
    pub(crate) turn_id: ConversationTurnId,
    pub(crate) correlation_id: CorrelationId,
    pub(crate) provisional: bool,
    pub(crate) evidence_ids: Vec<EvidenceId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) delta: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) diagnostic_pack: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) final_turn: Option<ConversationTurnView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) warning: Option<&'static str>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConversationStreamSendError {
    Backpressure,
    Closed,
    Terminal,
}

#[derive(Clone)]
pub(crate) struct ConversationStreamWriter {
    sender: mpsc::Sender<ConversationStreamEvent>,
    conversation_id: ConversationId,
    turn_id: ConversationTurnId,
    correlation_id: CorrelationId,
    sequence: Arc<AtomicU64>,
    terminal: Arc<AtomicBool>,
    cancelled: Arc<AtomicBool>,
    cancelled_notify: Arc<Notify>,
}

impl ConversationStreamWriter {
    pub(crate) fn channel(
        conversation_id: ConversationId,
        turn_id: ConversationTurnId,
        correlation_id: CorrelationId,
    ) -> (Self, mpsc::Receiver<ConversationStreamEvent>) {
        let (sender, receiver) = mpsc::channel(CONVERSATION_STREAM_CAPACITY);
        (
            Self {
                sender,
                conversation_id,
                turn_id,
                correlation_id,
                sequence: Arc::new(AtomicU64::new(0)),
                terminal: Arc::new(AtomicBool::new(false)),
                cancelled: Arc::new(AtomicBool::new(false)),
                cancelled_notify: Arc::new(Notify::new()),
            },
            receiver,
        )
    }

    pub(crate) fn accepted(&self) -> Result<(), ConversationStreamSendError> {
        self.emit("accepted", false, Vec::new(), None, None, None, None)
    }

    pub(crate) fn evidence_ready(&self, evidence_id: EvidenceId) -> Result<(), ConversationStreamSendError> {
        self.emit("evidence_ready", false, vec![evidence_id], None, None, None, None)
    }

    pub(crate) fn diagnosis_ready(
        &self,
        pack: impl Into<String>,
        evidence_id: EvidenceId,
    ) -> Result<(), ConversationStreamSendError> {
        self.emit(
            "diagnosis_ready",
            false,
            vec![evidence_id],
            None,
            Some(pack.into()),
            None,
            None,
        )
    }

    pub(crate) fn answer_delta(&self, delta: String) -> Result<(), ConversationStreamSendError> {
        if delta.is_empty() {
            return Ok(());
        }
        self.emit("answer_delta", true, Vec::new(), Some(delta), None, None, None)
    }

    pub(crate) fn preview_reset(&self) -> Result<(), ConversationStreamSendError> {
        self.emit(
            "preview_reset",
            true,
            Vec::new(),
            None,
            None,
            None,
            Some("provisional_answer_rejected"),
        )
    }

    pub(crate) fn finish(&self, view: ConversationTurnView) -> Result<(), ConversationStreamSendError> {
        let event_type = match view.turn.status {
            ConversationTurnStatus::Cancelled => "cancelled",
            ConversationTurnStatus::Failed => "failed",
            _ => "completed",
        };
        let evidence_ids = view
            .answer
            .as_ref()
            .map_or_else(Vec::new, |answer| answer.evidence_ids.clone());
        self.emit_terminal(event_type, evidence_ids, Some(view), None)
    }

    pub(crate) fn failed(&self) -> Result<(), ConversationStreamSendError> {
        self.emit_terminal("failed", Vec::new(), None, Some("conversation_query_failed"))
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire) || self.sender.is_closed()
    }

    pub(crate) async fn closed(&self) {
        let cancelled = self.cancelled_notify.notified();
        tokio::pin!(cancelled);
        if self.is_cancelled() {
            return;
        }
        tokio::select! {
            () = self.sender.closed() => {}
            () = &mut cancelled => {}
        }
    }

    fn emit_terminal(
        &self,
        event_type: &'static str,
        evidence_ids: Vec<EvidenceId>,
        final_turn: Option<ConversationTurnView>,
        warning: Option<&'static str>,
    ) -> Result<(), ConversationStreamSendError> {
        if self.terminal.swap(true, Ordering::AcqRel) {
            return Err(ConversationStreamSendError::Terminal);
        }
        self.emit(event_type, false, evidence_ids, None, None, final_turn, warning)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the fixed stream envelope keeps optional event payloads explicit"
    )]
    fn emit(
        &self,
        event_type: &'static str,
        provisional: bool,
        evidence_ids: Vec<EvidenceId>,
        delta: Option<String>,
        diagnostic_pack: Option<String>,
        final_turn: Option<ConversationTurnView>,
        warning: Option<&'static str>,
    ) -> Result<(), ConversationStreamSendError> {
        if self.terminal.load(Ordering::Acquire) && !matches!(event_type, "completed" | "cancelled" | "failed") {
            return Err(ConversationStreamSendError::Terminal);
        }
        let sequence = self.sequence.fetch_add(1, Ordering::AcqRel).saturating_add(1);
        let event = ConversationStreamEvent {
            schema_version: CONVERSATION_STREAM_SCHEMA,
            sequence,
            event_type,
            conversation_id: self.conversation_id,
            turn_id: self.turn_id,
            correlation_id: self.correlation_id,
            provisional,
            evidence_ids,
            delta,
            diagnostic_pack,
            final_turn,
            warning,
        };
        self.sender.try_send(event).map_err(|error| {
            self.cancelled.store(true, Ordering::Release);
            self.cancelled_notify.notify_one();
            match error {
                TrySendError::Full(_) => ConversationStreamSendError::Backpressure,
                TrySendError::Closed(_) => ConversationStreamSendError::Closed,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stream_writer_sequences_events_and_fails_closed_on_backpressure() {
        let (writer, mut receiver) =
            ConversationStreamWriter::channel(ConversationId::new(), ConversationTurnId::new(), CorrelationId::new());
        writer.accepted().expect("accepted event");
        let accepted = receiver.recv().await.expect("accepted event");
        assert_eq!(accepted.sequence, 1);
        assert_eq!(accepted.event_type, "accepted");

        for index in 0..CONVERSATION_STREAM_CAPACITY {
            writer.answer_delta(index.to_string()).expect("bounded delta");
        }
        assert_eq!(
            writer.answer_delta("overflow".to_owned()),
            Err(ConversationStreamSendError::Backpressure)
        );
        assert!(writer.is_cancelled());
    }

    #[tokio::test]
    async fn receiver_disconnect_cancels_the_writer() {
        let (writer, receiver) =
            ConversationStreamWriter::channel(ConversationId::new(), ConversationTurnId::new(), CorrelationId::new());
        drop(receiver);
        writer.closed().await;
        assert!(writer.is_cancelled());
    }
}
