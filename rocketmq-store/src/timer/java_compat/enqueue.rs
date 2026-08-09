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

use crate::timer::engine::WorkBudget;
use crate::timer::error::TimerEngineError;
use crate::timer::request::EngineBatchProgress;
use crate::timer::timer_message_store::TimerMessageStore;

pub(super) async fn process(
    store: &std::sync::Arc<TimerMessageStore>,
    budget: WorkBudget,
) -> Result<EngineBatchProgress, TimerEngineError> {
    if budget.is_exhausted(0, 0) {
        return Err(TimerEngineError::InvalidBudget);
    }
    let (messages, durable) = store.process_pipeline_enqueue_stage(budget.max_messages).await?;
    Ok(EngineBatchProgress {
        messages,
        bytes: 0,
        continuation: None,
        durable,
    })
}
