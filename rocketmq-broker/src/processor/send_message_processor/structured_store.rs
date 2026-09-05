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

use std::future::Future;

use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_store_api::MessageAppender;
use rocketmq_store_api::StoreError;
use rocketmq_transport::api::HandlerOutcome;
use rocketmq_transport::api::RemotingResponse;
use rocketmq_transport::api::RequestControlView;
use rocketmq_transport::api::TransportContractViolation;

#[derive(Clone)]
pub(crate) enum StoreAwaitControl {
    Legacy,
    Request(RequestControlView),
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("request lifecycle stopped while awaiting the message store")]
pub(crate) struct StoreAwaitStopped;

pub(crate) async fn await_store<F>(control: StoreAwaitControl, store_future: F) -> Result<F::Output, StoreAwaitStopped>
where
    F: Future + Send,
{
    let StoreAwaitControl::Request(control) = control else {
        return Ok(store_future.await);
    };
    if control.is_cancelled() {
        return Err(StoreAwaitStopped);
    }
    let result = tokio::select! {
        biased;
        () = control.cancelled() => return Err(StoreAwaitStopped),
        result = store_future => result,
    };
    if control.is_cancelled() {
        return Err(StoreAwaitStopped);
    }
    Ok(result)
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum StructuredStoreReplyError {
    #[error("request lifecycle stopped while awaiting the message store")]
    Cancelled,
    #[error("store response construction failed: {0}")]
    ResponseConstruction(#[from] TransportContractViolation),
}

/// Affine hook-completion token owned by exactly one structured store reply.
/// Consuming the reply makes the caller handle every legacy-compatible timing
/// class before handing the response to the canonical writer.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum StoreHookCompletion {
    AfterCanonicalWrite,
    BeforeReply,
    NoAfterHook,
}

pub(crate) struct StructuredStoreReply {
    outcome: HandlerOutcome,
    hook_completion: StoreHookCompletion,
}

impl StructuredStoreReply {
    pub(crate) fn into_parts(self) -> (HandlerOutcome, StoreHookCompletion) {
        (self.outcome, self.hook_completion)
    }
}

/// Awaits one store operation inside the admitted request task and returns one
/// canonical reply. Cancellation stops waiting and suppresses a reply; it
/// cannot prove that a store operation already handed to the backend did not
/// commit. The only request-lifecycle value retained across the await is the
/// read-only control view.
async fn await_store_reply<F, T, E, B>(
    control: RequestControlView,
    store_future: F,
    build_response: B,
) -> Result<StructuredStoreReply, StructuredStoreReplyError>
where
    F: Future<Output = Result<T, E>> + Send,
    B: FnOnce(Result<T, E>) -> (RemotingCommand, StoreHookCompletion),
{
    let result = await_store(StoreAwaitControl::Request(control), store_future)
        .await
        .map_err(|StoreAwaitStopped| StructuredStoreReplyError::Cancelled)?;
    let (response, hook_completion) = build_response(result);
    let outcome = RemotingResponse::command(response)
        .map(HandlerOutcome::Reply)
        .map_err(StructuredStoreReplyError::from)?;
    Ok(StructuredStoreReply {
        outcome,
        hook_completion,
    })
}

/// Structured processor for an ordinary message-store append. This remains
/// route-neutral so transport context cannot leak into the store future.
pub(crate) async fn append_message_with_control_reply<S, M, B>(
    control: RequestControlView,
    store: &mut S,
    message: M,
    build_response: B,
) -> Result<StructuredStoreReply, StructuredStoreReplyError>
where
    S: MessageAppender<M>,
    M: Send,
    B: FnOnce(Result<S::Receipt, StoreError>) -> (RemotingCommand, StoreHookCompletion),
{
    await_store_reply(control, store.append_message(message), build_response).await
}

#[cfg(test)]
#[path = "../../../tests/unit/processor/send_message/structured_store.rs"]
mod tests;
