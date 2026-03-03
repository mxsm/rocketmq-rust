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

use std::fmt;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::MessageTrait;

use crate::implementation::communication_mode::CommunicationMode;
use crate::producer::send_result::SendResult;

/// Context information for forbidden operation checks.
///
/// Contains all relevant information about a message send operation,
/// allowing hooks to inspect and validate the operation before execution.
///
/// # Examples
///
/// ```ignore
/// use rocketmq_client::hook::check_forbidden_context::CheckForbiddenContext;
/// use cheetah_string::CheetahString;
///
/// let mut context = CheckForbiddenContext::default();
/// context.group = Some(CheetahString::from("test_group"));
/// context.broker_addr = Some(CheetahString::from("127.0.0.1:10911"));
/// context.unit_mode = false;
/// ```
#[derive(Default)]
pub struct CheckForbiddenContext<'a> {
    /// Name server address
    pub name_srv_addr: Option<CheetahString>,
    /// Producer or consumer group name
    pub group: Option<CheetahString>,
    /// Message being sent (borrowed reference)
    pub message: Option<&'a dyn MessageTrait>,
    /// Target message queue (borrowed reference)
    pub mq: Option<&'a MessageQueue>,
    /// Broker address
    pub broker_addr: Option<CheetahString>,
    /// Communication mode (sync, async, or oneway)
    pub communication_mode: Option<CommunicationMode>,
    /// Send result (available after sending)
    pub send_result: Option<SendResult>,
    /// Exception that occurred during sending
    pub exception: Option<Box<dyn std::error::Error + Send + Sync>>,
    /// Custom argument
    pub arg: Option<Box<dyn std::any::Any + Send + Sync>>,
    /// Whether unit mode is enabled
    pub unit_mode: bool,
}

impl<'a> CheckForbiddenContext<'a> {
    /// Creates a new context.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the name server address.
    #[inline]
    pub fn with_name_srv_addr(mut self, addr: impl Into<CheetahString>) -> Self {
        self.name_srv_addr = Some(addr.into());
        self
    }

    /// Sets the group name.
    #[inline]
    pub fn with_group(mut self, group: impl Into<CheetahString>) -> Self {
        self.group = Some(group.into());
        self
    }

    /// Sets the broker address.
    #[inline]
    pub fn with_broker_addr(mut self, addr: impl Into<CheetahString>) -> Self {
        self.broker_addr = Some(addr.into());
        self
    }

    /// Sets the communication mode.
    #[inline]
    pub fn with_communication_mode(mut self, mode: CommunicationMode) -> Self {
        self.communication_mode = Some(mode);
        self
    }

    /// Sets the unit mode flag.
    #[inline]
    pub fn with_unit_mode(mut self, unit_mode: bool) -> Self {
        self.unit_mode = unit_mode;
        self
    }
}

impl<'a> fmt::Display for CheckForbiddenContext<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CheckForbiddenContext {{ ")?;

        if let Some(ref addr) = self.name_srv_addr {
            write!(f, "name_srv_addr: {}, ", addr)?;
        }

        if let Some(ref group) = self.group {
            write!(f, "group: {}, ", group)?;
        }

        if self.message.is_some() {
            write!(f, "message: Some(_), ")?;
        }

        if let Some(ref mq) = self.mq {
            write!(f, "mq: {}, ", mq)?;
        }

        if let Some(ref addr) = self.broker_addr {
            write!(f, "broker_addr: {}, ", addr)?;
        }

        if let Some(ref mode) = self.communication_mode {
            write!(f, "communication_mode: {:?}, ", mode)?;
        }

        if self.send_result.is_some() {
            write!(f, "send_result: Some(_), ")?;
        }

        if self.exception.is_some() {
            write!(f, "exception: Some(_), ")?;
        }

        write!(f, "unit_mode: {} }}", self.unit_mode)
    }
}

impl<'a> fmt::Debug for CheckForbiddenContext<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CheckForbiddenContext")
            .field("name_srv_addr", &self.name_srv_addr)
            .field("group", &self.group)
            .field("message", &self.message.as_ref().map(|_| "Some(_)"))
            .field("mq", &self.mq)
            .field("broker_addr", &self.broker_addr)
            .field("communication_mode", &self.communication_mode)
            .field("send_result", &self.send_result.as_ref().map(|_| "Some(_)"))
            .field("exception", &self.exception.as_ref().map(|_| "Some(_)"))
            .field("arg", &self.arg.as_ref().map(|_| "Some(_)"))
            .field("unit_mode", &self.unit_mode)
            .finish()
    }
}
