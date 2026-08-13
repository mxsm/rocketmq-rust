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

use bytes::Bytes;
use bytes::BytesMut;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use tokio::sync::oneshot;
use tokio_util::codec::Decoder;
use tokio_util::sync::CancellationToken;

use crate::codec::remoting_command_codec::FrameLimits;
use crate::codec::remoting_command_codec::RemotingCommandCodec;
use crate::deadline::RequestDeadline;
use crate::server::SessionHandle;

/// Typed failure produced by a response output capability.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ResponseSinkError {
    /// A single-response sink was completed more than once.
    #[error("response sink was already completed")]
    AlreadyCompleted,
    /// The caller stopped waiting before the response was delivered.
    #[error("response receiver was dropped")]
    ReceiverDropped,
    /// The request owner cancelled the response wait.
    #[error("response wait was cancelled")]
    Cancelled,
    /// The immutable request deadline expired.
    #[error("response deadline expired")]
    DeadlineExceeded,
    /// A network writer rejected or failed the response.
    #[error("response transport failed: {0}")]
    Transport(String),
    /// A processor-provided encoded response was malformed.
    #[error("encoded local response failed to decode: {0}")]
    Decode(String),
}

struct LocalResponseState {
    sender: Option<oneshot::Sender<Result<RemotingCommand, ResponseSinkError>>>,
    encoded: BytesMut,
}

#[derive(Clone)]
/// Cloneable single-response capability used by the embedded adapter.
///
/// Construction remains private to [`ResponseSink::local`].
pub struct LocalResponseSink {
    state: Arc<parking_lot::Mutex<LocalResponseState>>,
}

impl LocalResponseSink {
    fn complete(&self, result: Result<RemotingCommand, ResponseSinkError>) -> Result<(), ResponseSinkError> {
        let sender = self
            .state
            .lock()
            .sender
            .take()
            .ok_or(ResponseSinkError::AlreadyCompleted)?;
        sender.send(result).map_err(|_| ResponseSinkError::ReceiverDropped)
    }

    fn send_bytes(&self, bytes: Bytes) -> Result<(), ResponseSinkError> {
        let mut state = self.state.lock();
        if state.sender.is_none() {
            return Err(ResponseSinkError::AlreadyCompleted);
        }
        state.encoded.extend_from_slice(&bytes);
        let decoded = RemotingCommandCodec::with_limits(FrameLimits::java_compatibility())
            .decode(&mut state.encoded)
            .map_err(|error| ResponseSinkError::Decode(error.to_string()))?;
        let Some(command) = decoded else {
            return Ok(());
        };
        if !state.encoded.is_empty() {
            return Err(ResponseSinkError::Decode(
                "multiple commands were written to a single-response local sink".to_owned(),
            ));
        }
        let sender = state.sender.take().ok_or(ResponseSinkError::AlreadyCompleted)?;
        drop(state);
        sender.send(Ok(command)).map_err(|_| ResponseSinkError::ReceiverDropped)
    }
}

/// Closed response output variants for network and in-process dispatch.
#[derive(Clone)]
pub enum ResponseSink {
    /// A bounded canonical session writer.
    Network(Arc<SessionHandle>),
    /// A single-use in-process response channel.
    Local(LocalResponseSink),
}

impl ResponseSink {
    /// Creates an in-process sink and its single owner receiver.
    #[must_use]
    pub fn local() -> (Self, LocalResponseReceiver) {
        let (sender, receiver) = oneshot::channel();
        let sink = LocalResponseSink {
            state: Arc::new(parking_lot::Mutex::new(LocalResponseState {
                sender: Some(sender),
                encoded: BytesMut::new(),
            })),
        };
        (Self::Local(sink), LocalResponseReceiver { receiver })
    }

    /// Returns whether this sink performs no socket I/O.
    #[must_use]
    pub const fn is_local(&self) -> bool {
        matches!(self, Self::Local(_))
    }

    /// Delivers one materialized response.
    ///
    /// # Errors
    ///
    /// Returns a typed lifecycle, duplicate-response, or writer error.
    pub async fn send(&self, command: RemotingCommand) -> Result<(), ResponseSinkError> {
        match self {
            Self::Network(session) => session
                .connection()
                .send_command(command)
                .await
                .map_err(|error| ResponseSinkError::Transport(error.to_string())),
            Self::Local(sink) => sink.complete(Ok(command)),
        }
    }

    /// Delivers an already encoded response without introducing a local socket.
    ///
    /// The local branch incrementally decodes processor-generated response
    /// segments into one command. It never encodes the inbound request.
    ///
    /// # Errors
    ///
    /// Returns a typed decoding, lifecycle, or writer error.
    pub async fn send_bytes(&self, bytes: Bytes) -> Result<(), ResponseSinkError> {
        match self {
            Self::Network(session) => session
                .connection()
                .send_bytes(bytes)
                .await
                .map_err(|error| ResponseSinkError::Transport(error.to_string())),
            Self::Local(sink) => sink.send_bytes(bytes),
        }
    }

    /// Delivers one pre-encoded response as a validated immutable segment sequence.
    pub async fn send_frame_segments(&self, segments: Vec<Bytes>) -> Result<(), ResponseSinkError> {
        match self {
            Self::Network(session) => session
                .connection()
                .send_frame_segments(segments)
                .await
                .map_err(|error| ResponseSinkError::Transport(error.to_string())),
            Self::Local(sink) => {
                for segment in segments {
                    sink.send_bytes(segment)?;
                }
                Ok(())
            }
        }
    }
}

/// Single owner of an in-process response result.
pub struct LocalResponseReceiver {
    receiver: oneshot::Receiver<Result<RemotingCommand, ResponseSinkError>>,
}

impl LocalResponseReceiver {
    /// Waits under the parent cancellation token and immutable request deadline.
    ///
    /// # Errors
    ///
    /// Returns a typed cancellation, deadline, sender-drop, or response error.
    pub async fn receive(
        mut self,
        cancellation: &CancellationToken,
        deadline: Option<RequestDeadline>,
    ) -> Result<RemotingCommand, ResponseSinkError> {
        let result = if let Some(deadline) = deadline {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => return Err(ResponseSinkError::Cancelled),
                result = deadline.timeout(&mut self.receiver) => {
                    result.map_err(|_| ResponseSinkError::DeadlineExceeded)?
                }
            }
        } else {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => return Err(ResponseSinkError::Cancelled),
                result = &mut self.receiver => result,
            }
        };
        result.map_err(|_| ResponseSinkError::ReceiverDropped)?
    }
}
