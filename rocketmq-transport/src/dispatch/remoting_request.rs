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

//! The trusted V2 request aggregate and immutable ingress ordering view.

use std::collections::HashMap;

use cheetah_string::CheetahString;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;

use super::request_control::LazyExtensions;
use super::AuthenticationState;
use super::HandlerOutcome;
use super::HandlerOutcomeContractError;
use super::InlineResponseSlot;
use super::OriginalRequestIdentity;
use super::ProtocolNoResponse;
use super::ProtocolNoResponseError;
use super::ProtocolNoResponseReason;
use super::RequestControlView;
use super::RequestMeta;
use super::RequestOrigin;
use crate::session_view::SessionView;

mod builder;

#[allow(
    unused_imports,
    reason = "REQ-06 exposes the crate-private builder to later dispatcher wiring without expanding the public API"
)]
pub(crate) use builder::RemotingRequestBuildError;
#[allow(
    unused_imports,
    reason = "REQ-06 exposes the crate-private builder to later dispatcher wiring without expanding the public API"
)]
pub(crate) use builder::RemotingRequestBuilder;
#[allow(
    unused_imports,
    reason = "REQ-06 exposes sealed lifecycle provenance to later dispatcher wiring without expanding the public API"
)]
pub(crate) use builder::RequestLifecycleProvenance;

/// One trusted request with immutable ingress facts and one mutable command.
///
/// The request owns the [`RemotingCommand`] handed to a V2 processor. Its
/// original identity is captured before hooks or processors can mutate that
/// command. Ordering receives a short-lived [`IngressRequestView`] from the
/// trusted builder before this aggregate is built; processors use
/// [`Self::command_mut`] for command changes.
///
/// Instances are assembled only by the transport's trusted builder. This type
/// is intentionally not [`Clone`]: cloning would duplicate mutable command
/// ownership and blur response/lifecycle boundaries.
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::RemotingRequest;
///
/// fn cannot_clone(request: &RemotingRequest) {
///     let _: RemotingRequest = request.clone();
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::RemotingRequest;
///
/// fn cannot_recover_legacy_transport_capabilities(request: &RemotingRequest) {
///     let _ = request.channel();
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::RemotingRequest;
///
/// fn cannot_cancel_or_close_through_the_request(request: &RemotingRequest) {
///     request.cancel();
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::RemotingRequest;
///
/// fn cannot_recover_a_pre_mutation_ingress_view(request: &RemotingRequest) {
///     let _ = request.ingress();
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::RemotingRequestBuilder;
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::DeferredSlot;
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::RemotingCommand;
/// ```
pub struct RemotingRequest {
    original: OriginalRequestIdentity,
    meta: RequestMeta,
    origin: RequestOrigin,
    authentication: AuthenticationState,
    session: SessionView,
    control: RequestControlView,
    extensions: LazyExtensions,
    inline_response: InlineResponseSlot,
    command: RemotingCommand,
}

impl RemotingRequest {
    /// Returns the immutable identity captured when this request entered the
    /// trusted transport boundary.
    #[must_use]
    pub const fn original_identity(&self) -> OriginalRequestIdentity {
        self.original
    }

    /// Returns metadata captured when this request entered the transport.
    #[must_use]
    pub const fn meta(&self) -> &RequestMeta {
        &self.meta
    }

    /// Returns the trusted request origin.
    #[must_use]
    pub const fn origin(&self) -> &RequestOrigin {
        &self.origin
    }

    /// Returns authentication facts established by trusted ingress.
    #[must_use]
    pub const fn authentication(&self) -> &AuthenticationState {
        &self.authentication
    }

    /// Returns the read-only session view for this request.
    #[must_use]
    pub const fn session(&self) -> &SessionView {
        &self.session
    }

    /// Returns observer-only request deadline and cancellation state.
    #[must_use]
    pub const fn control(&self) -> &RequestControlView {
        &self.control
    }

    /// Creates an affine no-response marker from immutable ingress identity.
    ///
    /// The mutable processor command is deliberately not consulted. Only the
    /// audited original request code/reason pairs can create a marker.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolNoResponseError::OneWayRequest`] for every original
    /// one-way request. Returns [`ProtocolNoResponseError::Unsupported`] when
    /// the original raw request code and supplied reason are not allowlisted.
    ///
    /// ```
    /// use rocketmq_error::RocketMQResult;
    /// use rocketmq_transport::api::v2::{
    ///     HandlerOutcome, ProtocolNoResponseReason, RemotingRequest,
    /// };
    ///
    /// fn callback_outcome(request: &RemotingRequest) -> RocketMQResult<HandlerOutcome> {
    ///     let marker = request.protocol_no_response(ProtocolNoResponseReason::CallbackHandled)?;
    ///     Ok(HandlerOutcome::NoReply(marker))
    /// }
    /// ```
    pub fn protocol_no_response(
        &self,
        reason: ProtocolNoResponseReason,
    ) -> Result<ProtocolNoResponse, ProtocolNoResponseError> {
        ProtocolNoResponse::from_original(self.original, reason)
    }

    /// Returns the owned command as it currently stands.
    ///
    /// This command may differ from [`Self::original_identity`] after hooks or
    /// processors mutate it.
    #[must_use]
    pub const fn command(&self) -> &RemotingCommand {
        &self.command
    }

    /// Returns the command for processor-owned mutation.
    ///
    /// Mutating this command cannot alter the captured original identity.
    /// Ingress ordering has already observed its borrowed extension fields
    /// before the builder created this request.
    pub fn command_mut(&mut self) -> &mut RemotingCommand {
        &mut self.command
    }

    /// Returns the mutable command exclusively for the crate-private V1
    /// compatibility adapter.
    ///
    /// Keeping this narrow alias inside the request aggregate makes the
    /// adapter's use of the historical command contract explicit without
    /// adding transport capabilities to the public V2 surface.
    #[allow(
        dead_code,
        reason = "DSP-05 legacy access remains private and dormant until DSP-06 routing"
    )]
    pub(crate) fn legacy_command_mut(&mut self) -> &mut RemotingCommand {
        &mut self.command
    }

    /// Returns the request-local extension of type `T`, when one was inserted.
    ///
    /// This is allocation-free when no extension has previously been inserted.
    #[must_use]
    pub fn extension<T>(&self) -> Option<&T>
    where
        T: Send + Sync + 'static,
    {
        self.extensions.get()
    }

    /// Inserts a request-local extension, replacing an existing value of the
    /// same type.
    ///
    /// # Errors
    ///
    /// Returns the supplied value unchanged when `T` is a reserved ingress
    /// fact or lifecycle capability. Rejection does not allocate extension
    /// storage.
    pub fn try_insert_extension<T>(&mut self, value: T) -> Result<Option<T>, T>
    where
        T: Send + Sync + 'static,
    {
        self.extensions.try_insert(value)
    }

    #[allow(
        dead_code,
        reason = "DSP-02 preserves the deferred-capability builder regression before dispatcher wiring"
    )]
    pub(crate) const fn has_reserved_deferred_response(&self) -> bool {
        self.inline_response.has_deferred_capability()
    }

    #[allow(
        dead_code,
        reason = "DSP-02 exposes inline deferred transition to later dispatcher wiring"
    )]
    pub(crate) fn mark_deferred_response_taken(&mut self) -> Result<(), HandlerOutcomeContractError> {
        self.inline_response.mark_deferred_taken(self.original)
    }

    #[allow(
        dead_code,
        reason = "DSP-02 exposes terminal outcome validation to later dispatcher wiring"
    )]
    pub(crate) fn resolve_handler_outcome(
        &mut self,
        outcome: HandlerOutcome,
    ) -> Result<HandlerOutcome, HandlerOutcomeContractError> {
        self.inline_response.resolve(self.original, outcome)
    }

    /// Consumes the affine reply state for an original one-way legacy request
    /// without requiring its discarded command to become a response plan.
    pub(crate) fn resolve_legacy_oneway_reply(&mut self) -> Result<(), HandlerOutcomeContractError> {
        self.inline_response.resolve_legacy_oneway_reply()
    }

    pub(crate) fn consume_oneway_deferred(
        &mut self,
        registration: crate::dispatch::DeferredRegistration,
    ) -> Result<(), HandlerOutcomeContractError> {
        self.inline_response
            .consume_oneway_deferred(self.original, registration)
    }

    pub(crate) fn consume_oneway_no_reply(
        &mut self,
        marker: crate::dispatch::ProtocolNoResponse,
    ) -> Result<(), HandlerOutcomeContractError> {
        self.inline_response.consume_oneway_no_reply(self.original, marker)
    }

    #[allow(
        dead_code,
        reason = "DSP-03 body-free hook projection is consumed by the not-yet-wired private dispatcher"
    )]
    pub(crate) fn with_body_free_hook_command<T>(
        &mut self,
        apply: impl FnOnce(&mut RemotingCommand) -> rocketmq_error::RocketMQResult<T>,
    ) -> rocketmq_error::RocketMQResult<T> {
        let body = self.command.take_body();
        let result = apply(&mut self.command);
        let attached_body = self.command.take_body();
        if let Some(body) = body {
            self.command.set_body_mut_ref(body);
        }
        if attached_body.is_some() {
            return Err(rocketmq_error::RocketMQError::invariant_violated(
                "RPC hook attached a request body through the body-free V2 projection",
            ));
        }
        result
    }

    #[allow(
        dead_code,
        reason = "DSP-03 body-free hook projection is consumed by the not-yet-wired private dispatcher"
    )]
    pub(crate) fn with_body_free_hook_request<T>(
        &mut self,
        apply: impl FnOnce(&RemotingCommand) -> rocketmq_error::RocketMQResult<T>,
    ) -> rocketmq_error::RocketMQResult<T> {
        let body = self.command.take_body();
        let result = apply(&self.command);
        debug_assert!(self.command.body().is_none());
        if let Some(body) = body {
            self.command.set_body_mut_ref(body);
        }
        result
    }
}

/// Immutable request facts used before processor mutation, such as ordering.
///
/// The trusted builder creates this view before transferring command ownership
/// to [`RemotingRequest`]. It borrows the command's ingress extension fields
/// directly and never contains a request body, so ordering can inspect headers
/// without copying or retaining the payload.
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::IngressRequestView;
///
/// fn ingress_view_cannot_expose_the_request_body(view: IngressRequestView<'_>) {
///     let _ = view.body();
/// }
/// ```
pub struct IngressRequestView<'a> {
    original: OriginalRequestIdentity,
    ext_fields: Option<&'a HashMap<CheetahString, CheetahString>>,
}

impl<'a> IngressRequestView<'a> {
    /// Returns the immutable identity captured at ingress.
    #[must_use]
    pub const fn original_identity(&self) -> OriginalRequestIdentity {
        self.original
    }

    /// Returns the captured ingress extension fields, when the inbound command
    /// carried any.
    ///
    /// The builder consumes itself after ordering finishes, so later processor
    /// command mutation cannot be observed through this view.
    #[must_use]
    pub const fn ext_fields(&self) -> Option<&'a HashMap<CheetahString, CheetahString>> {
        self.ext_fields
    }
}

/// Compatibility name retained only for the private extension-type denylist.
pub(crate) type DeferredSlot = InlineResponseSlot;

#[cfg(test)]
mod tests;
