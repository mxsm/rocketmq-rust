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

//! Typed response completion contracts.

use std::error::Error;
use std::fmt;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use rocketmq_error::RocketMQError;

// V1 response writes have no request owner. Keep their receipts in a reserved,
// process-local namespace rather than deriving an identity from protocol opaque values.
const LEGACY_V1_RESPONSE_OWNER_ID: u64 = u64::MAX;
static LEGACY_V1_RESPONSE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

fn reserve_legacy_v1_request_id(sequence: &AtomicU64) -> Result<RequestId, ResponseError> {
    let sequence = sequence
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |sequence| {
            if sequence == 0 {
                None
            } else {
                sequence.checked_add(1)
            }
        })
        .map_err(|_| ResponseError::AlreadyCompleted {
            state: ResponseTerminalState::Closed,
        })?;
    Ok(RequestId {
        owner_id: LEGACY_V1_RESPONSE_OWNER_ID,
        sequence,
    })
}

/// Opaque process-local identity for one request allocation.
///
/// Real inbound requests use a process-local session owner and a sequence assigned within that
/// session. Legacy V1 direct-write receipts use the same type with a reserved synthetic owner. The
/// values are internal correlation data, not wire identifiers, peer identities, request bodies, or
/// other protocol content.
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::RequestId;
///
/// fn fields_are_private(request_id: RequestId) {
///     let _ = request_id.owner_id;
/// }
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RequestId {
    owner_id: u64,
    sequence: u64,
}

impl RequestId {
    /// Creates an identity in the namespace reserved for real request sessions.
    pub(crate) const fn real(owner_id: u64, sequence: u64) -> Option<Self> {
        if owner_id == 0 || owner_id == LEGACY_V1_RESPONSE_OWNER_ID || sequence == 0 || sequence == u64::MAX {
            None
        } else {
            Some(Self { owner_id, sequence })
        }
    }

    /// Returns the process-local request owner, including the reserved owner used by synthetic V1
    /// direct-write receipts.
    #[must_use]
    pub const fn owner_id(self) -> u64 {
        self.owner_id
    }

    /// Returns the sequence assigned within the process-local owner namespace.
    #[must_use]
    pub const fn sequence(self) -> u64 {
        self.sequence
    }
}

/// Progress made by a response write before a terminal failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum WriteProgress {
    /// The canonical writer has not begun this response's socket-I/O attempt.
    ///
    /// Enqueueing or admission alone remains `NotStarted`; no frame bytes have been written.
    NotStarted,
    /// A socket-I/O attempt began, or zero output cannot be proven.
    ///
    /// The socket may have accepted none, some, or all frame bytes, and flush or full-frame
    /// completion is ambiguous. Retrying is unsafe.
    PossiblyPartial,
}

impl WriteProgress {
    /// Returns the stable low-cardinality progress label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotStarted => "not_started",
            Self::PossiblyPartial => "possibly_partial",
        }
    }
}

/// Terminal lifecycle state of a single response owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ResponseTerminalState {
    /// The response completed successfully.
    Completed,
    /// The response failed after the indicated write progress.
    Failed {
        /// Progress made by the canonical writer before failure.
        progress: WriteProgress,
    },
    /// The response owner was cancelled before completion.
    Cancelled,
    /// The response owner was closed before completion.
    Closed,
}

impl ResponseTerminalState {
    /// Returns the stable low-cardinality terminal-state label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed { .. } => "failed",
            Self::Cancelled => "cancelled",
            Self::Closed => "closed",
        }
    }
}

/// Stable category for a response completion failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ResponseErrorKind {
    /// A single-response owner had already reached a terminal state.
    AlreadyCompleted,
    /// The immutable response deadline elapsed before completion.
    DeadlineExceeded,
    /// The request owner cancelled the response.
    Cancelled,
    /// The response session closed before completion.
    SessionClosed,
    /// The bounded response queue could not accept the response.
    QueueSaturated,
    /// Encoding the response failed before a write began.
    Encode,
    /// The canonical response transport failed.
    Transport,
}

impl ResponseErrorKind {
    /// Returns the stable low-cardinality error label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AlreadyCompleted => "already_completed",
            Self::DeadlineExceeded => "deadline_exceeded",
            Self::Cancelled => "cancelled",
            Self::SessionClosed => "session_closed",
            Self::QueueSaturated => "queue_saturated",
            Self::Encode => "encode",
            Self::Transport => "transport",
        }
    }
}

/// Typed response completion failure.
///
/// Formatting this error exposes only its stable category, terminal state or write progress, and
/// any source error's stable code. It never formats the source error itself.
pub enum ResponseError {
    /// The response owner had already reached the supplied terminal state.
    AlreadyCompleted {
        /// Terminal state that prevented another response completion.
        state: ResponseTerminalState,
    },
    /// The immutable response deadline elapsed before any write began.
    DeadlineExceeded,
    /// The response owner was cancelled before any write began.
    Cancelled,
    /// The response session closed before any write began.
    SessionClosed,
    /// The bounded response queue rejected the response before any write began.
    QueueSaturated,
    /// Encoding failed deterministically before any write began.
    Encode {
        /// Typed encoding failure preserved for programmatic inspection.
        source: RocketMQError,
    },
    /// The canonical response transport failed after the supplied write progress.
    Transport {
        /// Progress made by the canonical writer before the failure.
        progress: WriteProgress,
        /// Typed transport failure preserved for programmatic inspection.
        source: RocketMQError,
    },
}

impl ResponseError {
    /// Returns this error's stable low-cardinality category.
    #[must_use]
    pub const fn kind(&self) -> ResponseErrorKind {
        match self {
            Self::AlreadyCompleted { .. } => ResponseErrorKind::AlreadyCompleted,
            Self::DeadlineExceeded => ResponseErrorKind::DeadlineExceeded,
            Self::Cancelled => ResponseErrorKind::Cancelled,
            Self::SessionClosed => ResponseErrorKind::SessionClosed,
            Self::QueueSaturated => ResponseErrorKind::QueueSaturated,
            Self::Encode { .. } => ResponseErrorKind::Encode,
            Self::Transport { .. } => ResponseErrorKind::Transport,
        }
    }

    /// Returns the write progress associated with this failure.
    ///
    /// `None` is reserved for [`Self::AlreadyCompleted`], because it reports a prior terminal
    /// state rather than a new write attempt.
    #[must_use]
    pub const fn write_progress(&self) -> Option<WriteProgress> {
        match self {
            Self::AlreadyCompleted { .. } => None,
            Self::DeadlineExceeded
            | Self::Cancelled
            | Self::SessionClosed
            | Self::QueueSaturated
            | Self::Encode { .. } => Some(WriteProgress::NotStarted),
            Self::Transport { progress, .. } => Some(*progress),
        }
    }

    /// Returns whether this failure is eligible for retry by response policy.
    ///
    /// Eligibility is not a retry decision. A retry also requires an idempotent operation, an
    /// unexpired deadline, and remaining retry budget. Encoding failures are deterministic and
    /// nonretryable; a possibly partial transport write is never retryable.
    #[must_use]
    pub const fn retryable(&self) -> bool {
        matches!(
            self,
            Self::QueueSaturated
                | Self::Transport {
                    progress: WriteProgress::NotStarted,
                    ..
                }
        )
    }
}

impl fmt::Debug for ResponseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct("ResponseError");
        debug.field("kind", &self.kind().as_str());
        match self {
            Self::AlreadyCompleted { state } => {
                debug.field("state", &state.as_str());
                if let ResponseTerminalState::Failed { progress } = state {
                    debug.field("progress", &progress.as_str());
                }
            }
            Self::DeadlineExceeded | Self::Cancelled | Self::SessionClosed | Self::QueueSaturated => {
                debug.field("progress", &WriteProgress::NotStarted.as_str());
            }
            Self::Encode { source } => {
                debug.field("progress", &WriteProgress::NotStarted.as_str());
                debug.field("source_code", &source.kind().code().as_str());
            }
            Self::Transport { progress, source } => {
                debug.field("progress", &progress.as_str());
                debug.field("source_code", &source.kind().code().as_str());
            }
        }
        debug.finish()
    }
}

impl fmt::Display for ResponseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "response error: {}", self.kind().as_str())?;
        match self {
            Self::AlreadyCompleted { state } => {
                write!(formatter, " (state={})", state.as_str())?;
                if let ResponseTerminalState::Failed { progress } = state {
                    write!(formatter, ", progress={}", progress.as_str())?;
                }
            }
            Self::DeadlineExceeded | Self::Cancelled | Self::SessionClosed | Self::QueueSaturated => {
                write!(formatter, " (progress={})", WriteProgress::NotStarted.as_str())?;
            }
            Self::Encode { source } => {
                write!(
                    formatter,
                    " (progress={}, source_code={})",
                    WriteProgress::NotStarted.as_str(),
                    source.kind().code().as_str()
                )?;
            }
            Self::Transport { progress, source } => {
                write!(
                    formatter,
                    " (progress={}, source_code={})",
                    progress.as_str(),
                    source.kind().code().as_str()
                )?;
            }
        }
        Ok(())
    }
}

impl Error for ResponseError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Encode { source } | Self::Transport { source, .. } => Some(source),
            Self::AlreadyCompleted { .. }
            | Self::DeadlineExceeded
            | Self::Cancelled
            | Self::SessionClosed
            | Self::QueueSaturated => None,
        }
    }
}

/// Receipt returned after a response reaches a transport-specific disposition.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ResponseReceipt {
    request_id: RequestId,
    disposition: ResponseDisposition,
}

impl ResponseReceipt {
    #[allow(
        dead_code,
        reason = "RSP-05 exact request receipts are created by later private dispatcher wiring"
    )]
    pub(crate) const fn new(request_id: RequestId, disposition: ResponseDisposition) -> Self {
        Self {
            request_id,
            disposition,
        }
    }

    /// Reserves a synthetic V1 receipt before local completion starts.
    ///
    /// The fixed V1 namespace is process-local and intentionally has no request ownership. Its
    /// sequence never wraps: after exhaustion, the legacy owner remains permanently closed and
    /// this returns `AlreadyCompleted(Closed)` before any write begins.
    pub(crate) fn legacy_v1(disposition: ResponseDisposition) -> Result<Self, ResponseError> {
        Ok(Self {
            request_id: reserve_legacy_v1_request_id(&LEGACY_V1_RESPONSE_SEQUENCE)?,
            disposition,
        })
    }

    /// Returns the response owner that produced this receipt.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Returns the transport-specific disposition represented by this receipt.
    #[must_use]
    pub const fn disposition(&self) -> ResponseDisposition {
        self.disposition
    }
}

/// Transport-specific completion represented by a [`ResponseReceipt`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ResponseDisposition {
    /// The local canonical writer completed its socket write and flush.
    ///
    /// This confirms local socket write-and-flush only; it does not confirm peer receipt or
    /// business completion.
    TransportWritten,
    /// The response was handed to the embedded single-response owner.
    ///
    /// This confirms that handoff only; it does not confirm receiver consumption or any socket
    /// I/O.
    InProcessAccepted,
}

impl ResponseDisposition {
    /// Returns the stable low-cardinality delivery label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TransportWritten => "transport_written",
            Self::InProcessAccepted => "in_process_accepted",
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use rocketmq_error::ErrorKind;

    use super::*;

    fn response_error_kind_label(kind: ResponseErrorKind) -> &'static str {
        match kind {
            ResponseErrorKind::AlreadyCompleted => "already_completed",
            ResponseErrorKind::DeadlineExceeded => "deadline_exceeded",
            ResponseErrorKind::Cancelled => "cancelled",
            ResponseErrorKind::SessionClosed => "session_closed",
            ResponseErrorKind::QueueSaturated => "queue_saturated",
            ResponseErrorKind::Encode => "encode",
            ResponseErrorKind::Transport => "transport",
        }
    }

    #[test]
    fn response_error_variants_have_stable_kind_progress_and_retry_policy() {
        let cases = [
            (
                ResponseError::AlreadyCompleted {
                    state: ResponseTerminalState::Failed {
                        progress: WriteProgress::PossiblyPartial,
                    },
                },
                ResponseErrorKind::AlreadyCompleted,
                None,
                false,
            ),
            (
                ResponseError::DeadlineExceeded,
                ResponseErrorKind::DeadlineExceeded,
                Some(WriteProgress::NotStarted),
                false,
            ),
            (
                ResponseError::Cancelled,
                ResponseErrorKind::Cancelled,
                Some(WriteProgress::NotStarted),
                false,
            ),
            (
                ResponseError::SessionClosed,
                ResponseErrorKind::SessionClosed,
                Some(WriteProgress::NotStarted),
                false,
            ),
            (
                ResponseError::QueueSaturated,
                ResponseErrorKind::QueueSaturated,
                Some(WriteProgress::NotStarted),
                true,
            ),
            (
                ResponseError::Encode {
                    source: RocketMQError::InvalidProperty("encode-canary".to_owned()),
                },
                ResponseErrorKind::Encode,
                Some(WriteProgress::NotStarted),
                false,
            ),
            (
                ResponseError::Transport {
                    progress: WriteProgress::NotStarted,
                    source: RocketMQError::InvalidProperty("transport-canary".to_owned()),
                },
                ResponseErrorKind::Transport,
                Some(WriteProgress::NotStarted),
                true,
            ),
            (
                ResponseError::Transport {
                    progress: WriteProgress::PossiblyPartial,
                    source: RocketMQError::InvalidProperty("partial-canary".to_owned()),
                },
                ResponseErrorKind::Transport,
                Some(WriteProgress::PossiblyPartial),
                false,
            ),
        ];

        for (error, kind, progress, retryable) in cases {
            assert_eq!(error.kind(), kind);
            assert_eq!(error.kind().as_str(), response_error_kind_label(kind));
            assert_eq!(error.write_progress(), progress);
            assert_eq!(error.retryable(), retryable);
        }
    }

    #[test]
    fn source_errors_retain_their_concrete_type_and_identity() {
        let errors = [
            ResponseError::Encode {
                source: RocketMQError::InvalidProperty("encode-source-canary".to_owned()),
            },
            ResponseError::Transport {
                progress: WriteProgress::NotStarted,
                source: RocketMQError::InvalidProperty("transport-source-canary".to_owned()),
            },
        ];

        for error in &errors {
            let expected = match error {
                ResponseError::Encode { source } | ResponseError::Transport { source, .. } => source,
                _ => unreachable!("test constructed source-carrying response errors"),
            };
            let source = Error::source(error).expect("response error should preserve its source");
            let typed = source
                .downcast_ref::<RocketMQError>()
                .expect("response source should remain a RocketMQError");

            assert!(std::ptr::eq(typed, expected));
            assert_eq!(typed.kind(), ErrorKind::InvalidProperty);
        }
        assert!(Error::source(&ResponseError::Cancelled).is_none());
    }

    #[test]
    fn response_error_formatting_does_not_expose_sensitive_source_text() {
        const CANARY: &str = "response-secret-canary";
        let errors = [
            ResponseError::Encode {
                source: RocketMQError::InvalidProperty(CANARY.to_owned()),
            },
            ResponseError::Transport {
                progress: WriteProgress::PossiblyPartial,
                source: RocketMQError::InvalidProperty(CANARY.to_owned()),
            },
        ];

        for error in errors {
            let display = error.to_string();
            let debug = format!("{error:?}");
            assert!(
                !display.contains(CANARY),
                "display leaked sensitive source text: {display}"
            );
            assert!(!debug.contains(CANARY), "debug leaked sensitive source text: {debug}");
            assert!(display.contains("source_code=INVALID_PROPERTY"));
            assert!(debug.contains("source_code: \"INVALID_PROPERTY\""));
        }
    }

    #[test]
    fn legacy_v1_receipts_use_a_reserved_owner_and_distinct_sequences() {
        let first = ResponseReceipt::legacy_v1(ResponseDisposition::TransportWritten)
            .expect("V1 receipt identity should be available");
        let second = ResponseReceipt::legacy_v1(ResponseDisposition::TransportWritten)
            .expect("V1 receipt identity should be available");

        assert_eq!(first.request_id().owner_id(), LEGACY_V1_RESPONSE_OWNER_ID);
        assert_eq!(second.request_id().owner_id(), LEGACY_V1_RESPONSE_OWNER_ID);
        assert_ne!(first.request_id(), second.request_id());
        assert!(second.request_id().sequence() > first.request_id().sequence());
    }

    #[test]
    fn legacy_v1_receipt_ids_stop_before_maximum_sequence_wraps() {
        let sequence = AtomicU64::new(u64::MAX - 1);

        let final_id = reserve_legacy_v1_request_id(&sequence).expect("final safe sequence should be issued");
        let error = reserve_legacy_v1_request_id(&sequence).expect_err("exhausted V1 owner must stay closed");

        assert_eq!(final_id.owner_id(), LEGACY_V1_RESPONSE_OWNER_ID);
        assert_eq!(final_id.sequence(), u64::MAX - 1);
        assert_eq!(sequence.load(Ordering::Relaxed), u64::MAX);
        assert!(matches!(
            error,
            ResponseError::AlreadyCompleted {
                state: ResponseTerminalState::Closed
            }
        ));
    }

    #[test]
    fn legacy_v1_receipt_ids_reject_a_wrapped_sequence_cursor() {
        let sequence = AtomicU64::new(0);

        let error = reserve_legacy_v1_request_id(&sequence).expect_err("wrapped V1 owner must stay closed");

        assert!(matches!(
            error,
            ResponseError::AlreadyCompleted {
                state: ResponseTerminalState::Closed
            }
        ));
        assert_eq!(sequence.load(Ordering::Relaxed), 0);
    }
}
