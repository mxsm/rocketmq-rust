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

use std::sync::Arc;
use std::sync::Mutex;

use tokio::sync::oneshot;

use super::receiver_stop;
use super::TerminalResult;
use crate::dispatch::EmbeddedDispatchOutcome;
use crate::dispatch::RequestControlView;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TerminalPublishOutcome {
    Delivered,
    AlreadyCompleted,
    ReceiverDropped,
}

impl TerminalPublishOutcome {
    pub(super) const fn receiver_dropped(self) -> bool {
        matches!(self, Self::ReceiverDropped)
    }
}

#[derive(Clone)]
pub(super) struct EmbeddedTerminalSender {
    state: Arc<Mutex<EmbeddedTerminalState>>,
}

enum EmbeddedTerminalState {
    Open(oneshot::Sender<TerminalResult>),
    Sending,
    Completed,
    ReceiverDropped,
}

impl EmbeddedTerminalSender {
    pub(super) fn complete(&self, result: TerminalResult) -> TerminalPublishOutcome {
        let sender = {
            let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            match std::mem::replace(&mut *state, EmbeddedTerminalState::Sending) {
                EmbeddedTerminalState::Open(sender) => sender,
                EmbeddedTerminalState::Sending | EmbeddedTerminalState::Completed => {
                    *state = EmbeddedTerminalState::Completed;
                    return TerminalPublishOutcome::AlreadyCompleted;
                }
                EmbeddedTerminalState::ReceiverDropped => {
                    *state = EmbeddedTerminalState::ReceiverDropped;
                    return TerminalPublishOutcome::ReceiverDropped;
                }
            }
        };
        let delivered = sender.send(result).is_ok();
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        *state = if delivered {
            EmbeddedTerminalState::Completed
        } else {
            EmbeddedTerminalState::ReceiverDropped
        };
        if delivered {
            TerminalPublishOutcome::Delivered
        } else {
            TerminalPublishOutcome::ReceiverDropped
        }
    }

    fn close_receiver(&self) {
        let sender = {
            let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            match std::mem::replace(&mut *state, EmbeddedTerminalState::ReceiverDropped) {
                EmbeddedTerminalState::Open(sender) => Some(sender),
                EmbeddedTerminalState::Sending | EmbeddedTerminalState::ReceiverDropped => None,
                EmbeddedTerminalState::Completed => {
                    *state = EmbeddedTerminalState::Completed;
                    None
                }
            }
        };
        drop(sender);
    }
}

pub(super) struct EmbeddedTerminalReceiver {
    receiver: oneshot::Receiver<TerminalResult>,
    control: Option<RequestControlView>,
    original_one_way: bool,
    terminal: EmbeddedTerminalSender,
}

impl EmbeddedTerminalReceiver {
    pub(super) fn attach_control(&mut self, control: RequestControlView, original_one_way: bool) {
        self.control = Some(control);
        self.original_one_way = original_one_way;
    }

    pub(super) async fn receive(mut self) -> TerminalResult {
        match self.receiver.try_recv() {
            Ok(result) => return result,
            Err(oneshot::error::TryRecvError::Closed) => return Ok(EmbeddedDispatchOutcome::CompletionClosed),
            Err(oneshot::error::TryRecvError::Empty) => {}
        }
        let Some(control) = self.control.as_ref() else {
            return (&mut self.receiver)
                .await
                .unwrap_or(Ok(EmbeddedDispatchOutcome::CompletionClosed));
        };
        if let Some(outcome) = receiver_stop(control, self.original_one_way) {
            let _ = self.terminal.complete(Ok(outcome));
            return (&mut self.receiver)
                .await
                .unwrap_or(Ok(EmbeddedDispatchOutcome::CompletionClosed));
        }
        let external_stop = async {
            if self.original_one_way {
                control.parent_or_session_cancelled().await;
            } else {
                control.cancelled().await;
            }
        };
        tokio::select! {
            biased;
            () = external_stop => {
                let outcome = receiver_stop(control, self.original_one_way)
                    .unwrap_or(EmbeddedDispatchOutcome::Cancelled);
                let _ = self.terminal.complete(Ok(outcome));
                (&mut self.receiver).await.unwrap_or(Ok(EmbeddedDispatchOutcome::CompletionClosed))
            },
            result = &mut self.receiver => result.unwrap_or(Ok(EmbeddedDispatchOutcome::CompletionClosed)),
        }
    }
}

impl Drop for EmbeddedTerminalReceiver {
    fn drop(&mut self) {
        self.terminal.close_receiver();
    }
}

pub(super) fn terminal() -> (EmbeddedTerminalSender, EmbeddedTerminalReceiver) {
    let (sender, receiver) = oneshot::channel();
    let terminal = EmbeddedTerminalSender {
        state: Arc::new(Mutex::new(EmbeddedTerminalState::Open(sender))),
    };
    (
        terminal.clone(),
        EmbeddedTerminalReceiver {
            receiver,
            control: None,
            original_one_way: false,
            terminal,
        },
    )
}
