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

use crate::hook::send_message_context::SendMessageContext;
use crate::hook::send_message_hook::SendMessageHook;
use crate::trace::trace_dispatcher::TraceDispatcher;

pub struct SendMessageTraceHookImpl {
    trace_dispatcher: Arc<Box<dyn TraceDispatcher + Send + Sync>>,
}

impl SendMessageTraceHookImpl {
    pub fn new(trace_dispatcher: Arc<Box<dyn TraceDispatcher + Send + Sync>>) -> Self {
        Self { trace_dispatcher }
    }
}
impl SendMessageHook for SendMessageTraceHookImpl {
    fn hook_name(&self) -> &str {
        todo!()
    }

    fn send_message_before(&self, context: &Option<SendMessageContext<'_>>) {
        todo!()
    }

    fn send_message_after(&self, context: &Option<SendMessageContext<'_>>) {
        todo!()
    }
}
