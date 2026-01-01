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

use crate::hook::end_transaction_context::EndTransactionContext;
use crate::hook::end_transaction_hook::EndTransactionHook;
use crate::trace::trace_dispatcher::TraceDispatcher;

pub struct EndTransactionTraceHookImpl {
    trace_dispatcher: Arc<Box<dyn TraceDispatcher + Send + Sync>>,
}

impl EndTransactionTraceHookImpl {
    pub fn new(trace_dispatcher: Arc<Box<dyn TraceDispatcher + Send + Sync>>) -> Self {
        Self { trace_dispatcher }
    }
}

impl EndTransactionHook for EndTransactionTraceHookImpl {
    fn hook_name(&self) -> &str {
        "EndTransactionTraceHookImpl"
    }

    fn end_transaction(&self, context: &EndTransactionContext) {
        todo!()
    }
}
