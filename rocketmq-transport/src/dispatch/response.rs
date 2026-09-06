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

use rocketmq_error::RocketMQError;
use rocketmq_error::SharedError;

const RESERVED_RESPONSE_OWNER_ID: u64 = u64::MAX;

/// Opaque process-local identity for one request allocation.
///
/// Real inbound requests use a process-local session owner and a sequence assigned within that
/// session. The reserved maximum owner value remains unavailable to callers. These values are
/// internal correlation data, not wire identifiers, peer identities, request bodies, or
/// other protocol content.
///
/// ```compile_fail
/// use rocketmq_transport::api::RequestId;
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
        if owner_id == 0 || owner_id == RESERVED_RESPONSE_OWNER_ID || sequence == 0 || sequence == u64::MAX {
            None
        } else {
            Some(Self { owner_id, sequence })
        }
    }

    /// Returns the process-local request owner, including the reserved owner used by synthetic
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

/// Typed operational failure from canonical response delivery.
///
/// Normal lifecycle, deadline, and capacity rejections are represented by
/// [`ResponseCompletionOutcome`] instead. Formatting this error exposes only
/// its closed operation and the source error's stable code.
pub(crate) enum ResponseOperationalFailure {
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
        source: SharedError,
    },
}

/// Source-free result at a canonical response completion boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[must_use]
pub enum ResponseCompletionOutcome {
    /// Canonical response delivery completed.
    Completed(ResponseReceipt),
    /// A prior terminal operation already completed the response.
    AlreadyCompleted(ResponseTerminalState),
    /// The immutable deadline elapsed before completion.
    DeadlineExpired,
    /// Request ownership was cancelled.
    Cancelled,
    /// The canonical session closed.
    SessionClosed,
    /// The bounded response queue rejected the response.
    QueueSaturated,
}

/// Private completion carrier for a canonical writer attempt.
#[must_use]
pub(crate) enum ResponseSendOutcome {
    /// The canonical writer completed the payload.
    Written,
    /// A source-free normal response state prevented the write.
    Rejected(ResponseCompletionOutcome),
    /// Encoding or transport I/O failed with a typed cause.
    OperationalFailure(ResponseOperationalFailure),
}

impl ResponseCompletionOutcome {
    /// Returns the stable low-cardinality outcome label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Completed(_) => "completed",
            Self::AlreadyCompleted(_) => "already_completed",
            Self::DeadlineExpired => "deadline_expired",
            Self::Cancelled => "cancelled",
            Self::SessionClosed => "session_closed",
            Self::QueueSaturated => "queue_saturated",
        }
    }
}

impl ResponseOperationalFailure {
    pub(crate) const fn encode(source: RocketMQError) -> Self {
        Self::Encode { source }
    }

    pub(crate) const fn transport(progress: WriteProgress, source: SharedError) -> Self {
        Self::Transport { progress, source }
    }

    /// Returns this failure's stable low-cardinality operation.
    #[must_use]
    pub const fn operation(&self) -> &'static str {
        match self {
            Self::Encode { .. } => "encode",
            Self::Transport { .. } => "transport",
        }
    }

    /// Returns the write progress associated with this operational failure.
    #[must_use]
    pub const fn write_progress(&self) -> WriteProgress {
        match self {
            Self::Encode { .. } => WriteProgress::NotStarted,
            Self::Transport { progress, .. } => *progress,
        }
    }

    /// Returns whether this failure is eligible for retry by response policy.
    ///
    /// Eligibility is not a retry decision. A retry also requires an idempotent operation, an
    /// unexpired deadline, and remaining retry budget. Encoding failures are deterministic and
    /// nonretryable; a possibly partial transport write is never retryable.
    #[must_use]
    #[cfg(test)]
    pub const fn retryable(&self) -> bool {
        matches!(
            self,
            Self::Transport {
                progress: WriteProgress::NotStarted,
                ..
            }
        )
    }
}

impl fmt::Debug for ResponseOperationalFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct("ResponseOperationalFailure");
        debug.field("operation", &self.operation());
        match self {
            Self::Encode { source } => {
                debug.field("progress", &WriteProgress::NotStarted.as_str());
                debug.field("source_code", &source.descriptor().code().as_str());
            }
            Self::Transport { progress, source } => {
                debug.field("progress", &progress.as_str());
                debug.field("source_code", &source.descriptor().code().as_str());
            }
        }
        debug.finish()
    }
}

impl fmt::Display for ResponseOperationalFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "response delivery failed: {}", self.operation())?;
        match self {
            Self::Encode { source } => {
                write!(
                    formatter,
                    " (progress={}, source_code={})",
                    WriteProgress::NotStarted.as_str(),
                    source.descriptor().code().as_str()
                )?;
            }
            Self::Transport { progress, source } => {
                write!(
                    formatter,
                    " (progress={}, source_code={})",
                    progress.as_str(),
                    source.descriptor().code().as_str()
                )?;
            }
        }
        Ok(())
    }
}

impl Error for ResponseOperationalFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Encode { source } => Some(source),
            Self::Transport { source, .. } => Some(source.as_ref()),
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
    pub(crate) const fn new(request_id: RequestId, disposition: ResponseDisposition) -> Self {
        Self {
            request_id,
            disposition,
        }
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
    use std::sync::Arc;

    use rocketmq_error::Error as CanonicalError;
    use rocketmq_error::PROTOCOL_MESSAGE_PROPERTY_INVALID;
    use rocketmq_error::TRANSPORT_CONNECTION_FAILED;

    use super::*;

    #[test]
    fn operational_failures_have_stable_operation_progress_and_retry_policy() {
        let cases = [
            (
                ResponseOperationalFailure::Encode {
                    source: RocketMQError::InvalidProperty("encode-canary".to_owned()),
                },
                "encode",
                WriteProgress::NotStarted,
                false,
            ),
            (
                ResponseOperationalFailure::Transport {
                    progress: WriteProgress::NotStarted,
                    source: Arc::new(CanonicalError::new(&TRANSPORT_CONNECTION_FAILED)),
                },
                "transport",
                WriteProgress::NotStarted,
                true,
            ),
            (
                ResponseOperationalFailure::Transport {
                    progress: WriteProgress::PossiblyPartial,
                    source: Arc::new(CanonicalError::new(&TRANSPORT_CONNECTION_FAILED)),
                },
                "transport",
                WriteProgress::PossiblyPartial,
                false,
            ),
        ];

        for (error, operation, progress, retryable) in cases {
            assert_eq!(error.operation(), operation);
            assert_eq!(error.write_progress(), progress);
            assert_eq!(error.retryable(), retryable);
        }
    }

    #[test]
    fn source_errors_retain_their_concrete_type_and_identity() {
        let encode = ResponseOperationalFailure::Encode {
            source: RocketMQError::InvalidProperty("encode-source-canary".to_owned()),
        };
        let ResponseOperationalFailure::Encode { source: expected } = &encode else {
            unreachable!()
        };
        let source = Error::source(&encode).expect("encode error should preserve its source");
        let typed = source
            .downcast_ref::<RocketMQError>()
            .expect("encode source should remain a RocketMQError");
        assert!(std::ptr::eq(typed, expected));
        assert_eq!(typed.descriptor(), &PROTOCOL_MESSAGE_PROPERTY_INVALID);

        let canonical = Arc::new(CanonicalError::new(&TRANSPORT_CONNECTION_FAILED));
        let transport = ResponseOperationalFailure::Transport {
            progress: WriteProgress::NotStarted,
            source: Arc::clone(&canonical),
        };
        let source = Error::source(&transport).expect("transport error should preserve its canonical source");
        let typed = source
            .downcast_ref::<CanonicalError>()
            .expect("transport source should remain the canonical Error");
        assert!(std::ptr::eq(typed, canonical.as_ref()));
    }

    #[test]
    fn response_error_formatting_does_not_expose_sensitive_source_text() {
        const CANARY: &str = "response-secret-canary";
        let errors = [
            ResponseOperationalFailure::Encode {
                source: RocketMQError::InvalidProperty(CANARY.to_owned()),
            },
            ResponseOperationalFailure::Transport {
                progress: WriteProgress::PossiblyPartial,
                source: Arc::new(CanonicalError::new(&TRANSPORT_CONNECTION_FAILED)),
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
            assert!(display.contains("source_code="));
            assert!(debug.contains("source_code:"));
        }
    }
}
