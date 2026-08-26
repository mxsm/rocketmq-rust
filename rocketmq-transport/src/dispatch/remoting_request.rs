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
use super::OriginalRequestIdentity;
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
    #[allow(
        dead_code,
        reason = "REQ-06 reserves the inert deferred slot for later DEF lifecycle work"
    )]
    deferred: DeferredSlot,
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
        reason = "REQ-06 tests the inert deferred slot before later DEF lifecycle work"
    )]
    pub(crate) const fn has_reserved_deferred_response(&self) -> bool {
        self.deferred.is_reserved()
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

#[derive(Default)]
#[allow(
    dead_code,
    reason = "REQ-06 reserves this crate-private placeholder before later DEF lifecycle work"
)]
pub(crate) struct DeferredSlot {
    reserved: bool,
}

#[allow(
    dead_code,
    reason = "REQ-06 reserves inert deferred-state operations before later DEF lifecycle work"
)]
impl DeferredSlot {
    pub(crate) const fn reserved() -> Self {
        Self { reserved: true }
    }

    pub(crate) const fn is_reserved(&self) -> bool {
        self.reserved
    }
}

#[cfg(test)]
mod tests;
