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

//! Bounded terminal compatibility conversion for locally delivered response plans.

use std::collections::TryReserveError;
use std::error::Error;
use std::fmt;
use std::io;

use bytes::Bytes;
use rocketmq_error::RocketMQError;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::BlockingExecutor;
use rocketmq_runtime::RuntimeError;
use rocketmq_runtime::ShutdownDeadline;

use super::ResponseBody;
use crate::codec::remoting_command_codec::FrameLimits;
use crate::dispatch::LocalResponsePlanReceiver;
use crate::dispatch::RequestControlView;
use crate::dispatch::ResponseError;
use crate::dispatch::WriteProgress;
use crate::file_region::FileRegionSequence;
use crate::file_region_io::read_file_region_chunk;
use crate::file_region_io::FILE_REGION_READ_CHUNK_BYTES;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LegacyMaterializationLimits {
    frame_limits: FrameLimits,
    max_materialized_body_bytes: usize,
    max_body_parts: usize,
}

impl LegacyMaterializationLimits {
    pub(crate) fn try_new(
        frame_limits: FrameLimits,
        max_materialized_body_bytes: usize,
        max_body_parts: usize,
    ) -> Result<Self, LegacyLocalMaterializationError> {
        frame_limits
            .validate()
            .map_err(LegacyLocalMaterializationError::limits)?;
        if max_materialized_body_bytes > frame_limits.max_body_bytes {
            return Err(LegacyLocalMaterializationError::limits(
                RocketMQError::illegal_argument(format!(
                    "legacy materialized body limit {max_materialized_body_bytes} exceeds frame body limit {}",
                    frame_limits.max_body_bytes
                )),
            ));
        }
        Ok(Self {
            frame_limits,
            max_materialized_body_bytes,
            max_body_parts,
        })
    }

    fn validate_plan(self, body_len: usize, body_part_count: usize) -> Result<(), LegacyLocalMaterializationError> {
        if body_len > self.max_materialized_body_bytes {
            return Err(LegacyLocalMaterializationError::limits(
                RocketMQError::illegal_argument(format!(
                    "legacy response body length {body_len} exceeds materialization limit {}",
                    self.max_materialized_body_bytes
                )),
            ));
        }
        if body_part_count > self.max_body_parts {
            return Err(LegacyLocalMaterializationError::limits(
                RocketMQError::illegal_argument(format!(
                    "legacy response body part count {body_part_count} exceeds materialization limit {}",
                    self.max_body_parts
                )),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LegacyLocalMaterializationErrorKind {
    Cancelled,
    SessionClosed,
    DeadlineExceeded,
    Response,
    Limits,
    Frame,
    Allocation,
    Runtime,
    FileIo,
}

impl LegacyLocalMaterializationErrorKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Cancelled => "cancelled",
            Self::SessionClosed => "session_closed",
            Self::DeadlineExceeded => "deadline_exceeded",
            Self::Response => "response",
            Self::Limits => "limits",
            Self::Frame => "frame",
            Self::Allocation => "allocation",
            Self::Runtime => "runtime",
            Self::FileIo => "file_io",
        }
    }
}

pub(crate) enum LegacyLocalMaterializationError {
    Cancelled,
    SessionClosed,
    DeadlineExceeded,
    Response { source: ResponseError },
    Limits { source: RocketMQError },
    Frame { source: RocketMQError },
    Allocation { source: TryReserveError },
    Runtime { source: RuntimeError },
    FileIo { source: io::Error },
}

impl LegacyLocalMaterializationError {
    fn response(source: ResponseError) -> Self {
        match source {
            ResponseError::Cancelled => Self::Cancelled,
            ResponseError::SessionClosed => Self::SessionClosed,
            ResponseError::DeadlineExceeded => Self::DeadlineExceeded,
            source => Self::Response { source },
        }
    }

    fn limits(source: RocketMQError) -> Self {
        Self::Limits { source }
    }

    fn frame(source: RocketMQError) -> Self {
        Self::Frame { source }
    }

    fn allocation(source: TryReserveError) -> Self {
        Self::Allocation { source }
    }

    fn runtime(source: RuntimeError) -> Self {
        Self::Runtime { source }
    }

    fn file_io(source: io::Error) -> Self {
        Self::FileIo { source }
    }

    const fn kind(&self) -> LegacyLocalMaterializationErrorKind {
        match self {
            Self::Cancelled => LegacyLocalMaterializationErrorKind::Cancelled,
            Self::SessionClosed => LegacyLocalMaterializationErrorKind::SessionClosed,
            Self::DeadlineExceeded => LegacyLocalMaterializationErrorKind::DeadlineExceeded,
            Self::Response { .. } => LegacyLocalMaterializationErrorKind::Response,
            Self::Limits { .. } => LegacyLocalMaterializationErrorKind::Limits,
            Self::Frame { .. } => LegacyLocalMaterializationErrorKind::Frame,
            Self::Allocation { .. } => LegacyLocalMaterializationErrorKind::Allocation,
            Self::Runtime { .. } => LegacyLocalMaterializationErrorKind::Runtime,
            Self::FileIo { .. } => LegacyLocalMaterializationErrorKind::FileIo,
        }
    }

    pub(crate) const fn write_progress(&self) -> WriteProgress {
        WriteProgress::NotStarted
    }

    pub(crate) const fn retryable(&self) -> bool {
        false
    }
}

impl fmt::Debug for LegacyLocalMaterializationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct("LegacyLocalMaterializationError");
        debug.field("kind", &self.kind().as_str());
        debug.field("progress", &self.write_progress().as_str());
        debug.field("retryable", &self.retryable());
        if let Self::FileIo { source } = self {
            debug.field("io_kind", &source.kind());
        }
        debug.finish()
    }
}

impl fmt::Display for LegacyLocalMaterializationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "legacy local response materialization failed: {} (progress={}, retryable=false)",
            self.kind().as_str(),
            self.write_progress().as_str()
        )?;
        if let Self::FileIo { source } = self {
            write!(formatter, ", io_kind={:?}", source.kind())?;
        }
        Ok(())
    }
}

impl Error for LegacyLocalMaterializationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Cancelled | Self::SessionClosed | Self::DeadlineExceeded => None,
            Self::Response { source } => Some(source),
            Self::Limits { source } | Self::Frame { source } => Some(source),
            Self::Allocation { source } => Some(source),
            Self::Runtime { source } => Some(source),
            Self::FileIo { source } => Some(source),
        }
    }
}

impl LocalResponsePlanReceiver {
    pub(crate) async fn receive_command(
        self,
        limits: LegacyMaterializationLimits,
        blocking: &BlockingExecutor,
    ) -> Result<RemotingCommand, LegacyLocalMaterializationError> {
        let control = self.control().clone();
        ensure_running(&control)?;
        let plan = self
            .receive()
            .await
            .map_err(LegacyLocalMaterializationError::response)?;
        ensure_running(&control)?;

        let body_len = plan.body_len();
        let body_part_count = plan.body_part_count();
        limits.validate_plan(body_len, body_part_count)?;

        let (head, body, body_len, _) = plan.into_materialization_parts();
        limits
            .frame_limits
            .encode_frame_head(head.clone(), body_len)
            .map_err(LegacyLocalMaterializationError::frame)?;
        ensure_running(&control)?;

        match body {
            ResponseBody::Empty => Ok(head),
            ResponseBody::Bytes(body) => Ok(head.set_body(body)),
            ResponseBody::Segments(segments) => materialize_segments(head, segments, body_len),
            ResponseBody::FileRegions(regions) => {
                materialize_file_regions(head, regions, body_len, control, blocking).await
            }
        }
    }
}

fn materialize_segments(
    head: RemotingCommand,
    mut segments: Vec<Bytes>,
    body_len: usize,
) -> Result<RemotingCommand, LegacyLocalMaterializationError> {
    if segments.len() == 1 {
        let body = segments.pop().ok_or_else(body_length_mismatch)?;
        if body.len() != body_len {
            return Err(body_length_mismatch());
        }
        return Ok(head.set_body(body));
    }

    let mut body = allocate_body(body_len)?;
    let mut copied = 0_usize;
    for segment in segments {
        copied = copied
            .checked_add(segment.len())
            .filter(|copied| *copied <= body_len)
            .ok_or_else(body_length_mismatch)?;
        body.extend_from_slice(&segment);
    }
    if copied != body_len || body.len() != body_len {
        return Err(body_length_mismatch());
    }
    Ok(head.set_body(Bytes::from(body)))
}

async fn materialize_file_regions(
    head: RemotingCommand,
    regions: FileRegionSequence,
    body_len: usize,
    control: RequestControlView,
    blocking: &BlockingExecutor,
) -> Result<RemotingCommand, LegacyLocalMaterializationError> {
    ensure_running(&control)?;
    let operation_control = control.clone();
    let deadline = control.deadline();
    let operation = move || read_file_regions(regions, body_len, operation_control);
    let execution = async {
        match deadline {
            Some(deadline) => {
                blocking
                    .spawn_io_until(
                        "transport.legacy-local-response.materialize-file",
                        ShutdownDeadline::at(deadline.instant().into_std()),
                        operation,
                    )
                    .await
            }
            None => {
                blocking
                    .spawn_io("transport.legacy-local-response.materialize-file", operation)
                    .await
            }
        }
    };
    tokio::pin!(execution);

    let body = tokio::select! {
        biased;
        () = control.cancelled() => {
            let stop = current_stop(&control).unwrap_or(MaterializationStop::Cancelled);
            return Err(stop.into_error());
        }
        result = &mut execution => result.map_err(LegacyLocalMaterializationError::runtime)??,
    };
    ensure_running(&control)?;
    Ok(head.set_body(body))
}

fn read_file_regions(
    regions: FileRegionSequence,
    body_len: usize,
    control: RequestControlView,
) -> Result<Bytes, LegacyLocalMaterializationError> {
    let mut body = allocate_body(body_len)?;
    body.resize(body_len, 0);
    let mut destination_offset = 0_usize;

    for region in regions.regions() {
        let mut region_progress = 0_u64;
        while region_progress < region.len() {
            ensure_running(&control)?;
            let remaining = region.len() - region_progress;
            let chunk_len = usize::try_from(remaining.min(FILE_REGION_READ_CHUNK_BYTES as u64))
                .map_err(|_| file_io_error(io::ErrorKind::InvalidInput, "file-region chunk length exceeds usize"))?;
            let destination_end = destination_offset
                .checked_add(chunk_len)
                .filter(|end| *end <= body_len)
                .ok_or_else(body_length_mismatch)?;
            let read = read_file_region_chunk(
                region.lease().file(),
                &mut body[destination_offset..destination_end],
                region.offset(),
                region_progress,
            )
            .map_err(LegacyLocalMaterializationError::file_io)?;
            if read == 0 {
                return Err(file_io_error(
                    io::ErrorKind::UnexpectedEof,
                    "leased file region ended before its validated length",
                ));
            }
            destination_offset = destination_offset
                .checked_add(read)
                .filter(|offset| *offset <= body_len)
                .ok_or_else(body_length_mismatch)?;
            region_progress = region_progress
                .checked_add(read as u64)
                .ok_or_else(|| file_io_error(io::ErrorKind::InvalidData, "file-region progress overflow"))?;
        }
    }

    if destination_offset != body_len {
        return Err(body_length_mismatch());
    }
    Ok(Bytes::from(body))
}

fn allocate_body(body_len: usize) -> Result<Vec<u8>, LegacyLocalMaterializationError> {
    let mut body = Vec::new();
    body.try_reserve_exact(body_len)
        .map_err(LegacyLocalMaterializationError::allocation)?;
    Ok(body)
}

fn body_length_mismatch() -> LegacyLocalMaterializationError {
    LegacyLocalMaterializationError::limits(RocketMQError::illegal_argument(
        "legacy response cached body metadata did not match its owned body",
    ))
}

fn file_io_error(kind: io::ErrorKind, message: &'static str) -> LegacyLocalMaterializationError {
    LegacyLocalMaterializationError::file_io(io::Error::new(kind, message))
}

fn ensure_running(control: &RequestControlView) -> Result<(), LegacyLocalMaterializationError> {
    current_stop(control).map_or(Ok(()), |stop| Err(stop.into_error()))
}

#[derive(Clone, Copy)]
enum MaterializationStop {
    Cancelled,
    SessionClosed,
    DeadlineExceeded,
}

impl MaterializationStop {
    fn into_error(self) -> LegacyLocalMaterializationError {
        match self {
            Self::Cancelled => LegacyLocalMaterializationError::Cancelled,
            Self::SessionClosed => LegacyLocalMaterializationError::SessionClosed,
            Self::DeadlineExceeded => LegacyLocalMaterializationError::DeadlineExceeded,
        }
    }
}

fn current_stop(control: &RequestControlView) -> Option<MaterializationStop> {
    if control.parent_is_cancelled() {
        Some(MaterializationStop::Cancelled)
    } else if control.session_is_closed() {
        Some(MaterializationStop::SessionClosed)
    } else if control
        .deadline()
        .is_some_and(crate::deadline::RequestDeadline::is_expired)
    {
        Some(MaterializationStop::DeadlineExceeded)
    } else {
        None
    }
}

#[cfg(test)]
#[path = "materializer/materializer_tests.rs"]
mod tests;
