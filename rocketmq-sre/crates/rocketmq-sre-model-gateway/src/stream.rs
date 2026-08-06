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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::mpsc::SyncSender;
use std::sync::mpsc::TrySendError;
use std::sync::mpsc::sync_channel;
use std::time::Duration;

use tokio::sync::watch;

use crate::error::ProviderError;
use crate::error::ProviderErrorCode;
use crate::ir::ModelStreamEvent;

/// Cooperative cancellation shared by an invocation and its stream.
#[derive(Clone, Debug)]
pub struct CancellationToken {
    cancelled: watch::Sender<bool>,
}

impl Default for CancellationToken {
    fn default() -> Self {
        let (cancelled, _) = watch::channel(false);
        Self { cancelled }
    }
}

impl CancellationToken {
    /// Cancels the operation.
    pub fn cancel(&self) {
        self.cancelled.send_replace(true);
    }

    /// Whether cancellation has been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        *self.cancelled.borrow()
    }

    async fn cancelled(&self) {
        let mut receiver = self.cancelled.subscribe();
        if *receiver.borrow() {
            return;
        }
        while receiver.changed().await.is_ok() {
            if *receiver.borrow() {
                return;
            }
        }
    }
}

/// Hard bounds for one streaming response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamBounds {
    pub channel_capacity: usize,
    pub max_events: usize,
    pub max_bytes: usize,
}

impl Default for StreamBounds {
    fn default() -> Self {
        Self {
            channel_capacity: 32,
            max_events: 4_096,
            max_bytes: 4 * 1024 * 1024,
        }
    }
}

#[derive(Default)]
struct StreamUsage {
    events: usize,
    bytes: usize,
}

/// Producer half of a bounded model stream.
#[derive(Clone)]
pub struct StreamSink {
    sender: SyncSender<ModelStreamEvent>,
    bounds: StreamBounds,
    usage: Arc<Mutex<StreamUsage>>,
    cancellation: CancellationToken,
}

impl StreamSink {
    /// Sends one event without waiting for an unbounded consumer backlog.
    ///
    /// # Errors
    ///
    /// Returns a stable cancellation, output-bound, backpressure, or channel
    /// error. The event is never cached after a failed send.
    pub fn try_send(&self, event: ModelStreamEvent) -> Result<(), ProviderError> {
        if self.cancellation.is_cancelled() {
            return Err(ProviderError::new(
                ProviderErrorCode::Cancelled,
                "model stream was cancelled",
            ));
        }
        let event_bytes = serde_json::to_vec(&event).map_err(|_| {
            ProviderError::new(
                ProviderErrorCode::ProtocolError,
                "model stream event could not be encoded",
            )
        })?;
        {
            let mut usage = self.usage.lock().map_err(|_| {
                ProviderError::new(
                    ProviderErrorCode::ServiceUnavailable,
                    "model stream accounting is unavailable",
                )
            })?;
            if usage.events.saturating_add(1) > self.bounds.max_events
                || usage.bytes.saturating_add(event_bytes.len()) > self.bounds.max_bytes
            {
                self.cancellation.cancel();
                return Err(ProviderError::new(
                    ProviderErrorCode::OutputTooLarge,
                    "model stream exceeded configured bounds",
                ));
            }
            usage.events += 1;
            usage.bytes += event_bytes.len();
        }
        match self.sender.try_send(event) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Err(ProviderError::new(
                ProviderErrorCode::StreamBackpressure,
                "model stream consumer is not keeping up",
            )),
            Err(TrySendError::Disconnected(_)) => Err(ProviderError::new(
                ProviderErrorCode::Cancelled,
                "model stream consumer disconnected",
            )),
        }
    }
}

/// Consumer half of a bounded model stream.
pub struct BoundedModelStream {
    receiver: Receiver<ModelStreamEvent>,
    cancellation: CancellationToken,
}

pub(crate) type StreamEventFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<ModelStreamEvent>, ProviderError>> + Send + 'a>>;

pub(crate) trait AsyncModelStreamSource: Send {
    fn next_event(&mut self) -> StreamEventFuture<'_>;
}

/// Pull-based, bounded asynchronous model stream.
///
/// The stream owns the provider response and reads it only when [`Self::recv`]
/// is called. It therefore needs no detached producer task and cannot build an
/// unbounded delta backlog behind a slow consumer.
pub struct AsyncBoundedModelStream {
    source: Box<dyn AsyncModelStreamSource>,
    bounds: StreamBounds,
    usage: StreamUsage,
    cancellation: CancellationToken,
    terminated: bool,
}

impl AsyncBoundedModelStream {
    pub(crate) fn new(
        source: Box<dyn AsyncModelStreamSource>,
        bounds: StreamBounds,
        cancellation: CancellationToken,
    ) -> Result<Self, ProviderError> {
        validate_bounds(bounds)?;
        Ok(Self {
            source,
            bounds,
            usage: StreamUsage::default(),
            cancellation,
            terminated: false,
        })
    }

    /// Receives the next normalized provider event.
    ///
    /// `Ok(None)` is returned only after a terminal `finish` or `error` event.
    /// Provider EOF before a terminal event fails closed.
    ///
    /// # Errors
    ///
    /// Returns a stable cancellation, protocol, transport, or output-bound
    /// error without retaining the rejected event.
    pub async fn recv(&mut self) -> Result<Option<ModelStreamEvent>, ProviderError> {
        if self.cancellation.is_cancelled() {
            return Err(cancelled_error());
        }
        if self.terminated {
            return Ok(None);
        }
        let cancellation = self.cancellation.clone();
        let event = tokio::select! {
            () = cancellation.cancelled() => return Err(cancelled_error()),
            event = self.source.next_event() => event?,
        };
        let Some(event) = event else {
            self.cancellation.cancel();
            return Err(ProviderError::new(
                ProviderErrorCode::ProtocolError,
                "model provider stream ended without a terminal event",
            ));
        };
        let event_bytes = serde_json::to_vec(&event).map_err(|_| {
            ProviderError::new(
                ProviderErrorCode::ProtocolError,
                "model stream event could not be encoded",
            )
        })?;
        if self.usage.events.saturating_add(1) > self.bounds.max_events
            || self.usage.bytes.saturating_add(event_bytes.len()) > self.bounds.max_bytes
        {
            self.cancellation.cancel();
            return Err(ProviderError::new(
                ProviderErrorCode::OutputTooLarge,
                "model stream exceeded configured bounds",
            ));
        }
        self.usage.events += 1;
        self.usage.bytes += event_bytes.len();
        self.terminated = matches!(event, ModelStreamEvent::Finish { .. } | ModelStreamEvent::Error { .. });
        Ok(Some(event))
    }

    /// Cancels the stream and releases the provider response after the current
    /// caller regains control.
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }
}

impl Debug for AsyncBoundedModelStream {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AsyncBoundedModelStream")
            .field("bounds", &self.bounds)
            .field("events", &self.usage.events)
            .field("bytes", &self.usage.bytes)
            .field("cancelled", &self.cancellation.is_cancelled())
            .field("terminated", &self.terminated)
            .finish_non_exhaustive()
    }
}

impl BoundedModelStream {
    /// Creates a bounded channel and cancellation-aware stream.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderErrorCode::InvalidRequest`] when any bound is zero.
    pub fn channel(bounds: StreamBounds, cancellation: CancellationToken) -> Result<(StreamSink, Self), ProviderError> {
        validate_bounds(bounds)?;
        let (sender, receiver) = sync_channel(bounds.channel_capacity);
        Ok((
            StreamSink {
                sender,
                bounds,
                usage: Arc::new(Mutex::new(StreamUsage::default())),
                cancellation: cancellation.clone(),
            },
            Self { receiver, cancellation },
        ))
    }

    /// Receives the next event up to the caller-provided timeout.
    ///
    /// # Errors
    ///
    /// Returns timeout, cancellation, or service-unavailable when the stream
    /// cannot yield an event.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<ModelStreamEvent, ProviderError> {
        if self.cancellation.is_cancelled() {
            return Err(ProviderError::new(
                ProviderErrorCode::Cancelled,
                "model stream was cancelled",
            ));
        }
        self.receiver.recv_timeout(timeout).map_err(|error| match error {
            RecvTimeoutError::Timeout => ProviderError::timeout("timed out waiting for a model stream event"),
            RecvTimeoutError::Disconnected => ProviderError::new(
                ProviderErrorCode::ServiceUnavailable,
                "model stream producer disconnected",
            ),
        })
    }

    /// Cancels the stream and its producer.
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }
}

fn validate_bounds(bounds: StreamBounds) -> Result<(), ProviderError> {
    if bounds.channel_capacity == 0 || bounds.max_events == 0 || bounds.max_bytes == 0 {
        return Err(ProviderError::new(
            ProviderErrorCode::InvalidRequest,
            "model stream bounds must be non-zero",
        ));
    }
    Ok(())
}

fn cancelled_error() -> ProviderError {
    ProviderError::new(ProviderErrorCode::Cancelled, "model stream was cancelled")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_stream_rejects_backpressure_without_caching_deltas() {
        let cancellation = CancellationToken::default();
        let (sink, stream) = BoundedModelStream::channel(
            StreamBounds {
                channel_capacity: 1,
                max_events: 10,
                max_bytes: 1_024,
            },
            cancellation,
        )
        .expect("stream");
        sink.try_send(ModelStreamEvent::TextDelta {
            delta: "first".to_owned(),
        })
        .expect("first event");
        let error = sink
            .try_send(ModelStreamEvent::TextDelta {
                delta: "second".to_owned(),
            })
            .expect_err("bounded channel must reject backpressure");
        assert_eq!(error.code, ProviderErrorCode::StreamBackpressure);
        let first = stream
            .recv_timeout(Duration::from_millis(10))
            .expect("first event remains");
        assert!(matches!(first, ModelStreamEvent::TextDelta { .. }));
    }

    #[test]
    fn output_bound_cancels_stream() {
        let cancellation = CancellationToken::default();
        let (sink, stream) = BoundedModelStream::channel(
            StreamBounds {
                channel_capacity: 2,
                max_events: 1,
                max_bytes: 1_024,
            },
            cancellation,
        )
        .expect("stream");
        sink.try_send(ModelStreamEvent::Finish {
            reason: crate::ir::FinishReason::Stop,
        })
        .expect("first event");
        assert_eq!(
            sink.try_send(ModelStreamEvent::Finish {
                reason: crate::ir::FinishReason::Stop,
            })
            .expect_err("event bound")
            .code,
            ProviderErrorCode::OutputTooLarge
        );
        assert_eq!(
            stream
                .recv_timeout(Duration::from_millis(10))
                .expect_err("cancelled")
                .code,
            ProviderErrorCode::Cancelled
        );
    }
}
