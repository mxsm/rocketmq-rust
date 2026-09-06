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

use rocketmq_error::PublicErrorView;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;

/// Owner-managed command state used to construct a safe remoting error response.
pub enum RemotingErrorTarget<'a> {
    /// Create a new response with the supplied factory's defaults.
    Fresh(&'a RemotingCommandFactory),
    /// Create a new response and preserve the request opaque.
    Reply {
        /// Factory that owns the response defaults.
        factory: &'a RemotingCommandFactory,
        /// Opaque copied from the request.
        opaque: i32,
    },
    /// Replace only the error code and remark on an existing response.
    Existing(RemotingCommand),
}

/// Builds a descriptor-backed remoting error response.
///
/// The adapter reads only the catalog-owned remoting projection and fixed
/// public message from `view`. Factory defaults and all existing response state
/// remain owned by `target`.
#[must_use]
pub fn error_response(view: PublicErrorView<'_>, target: RemotingErrorTarget<'_>) -> RemotingCommand {
    let code = view.projection().remoting().code.as_i32();
    let message = view.message();
    match target {
        RemotingErrorTarget::Fresh(factory) => factory.create_response_command_with_code_remark(code, message),
        RemotingErrorTarget::Reply { factory, opaque } => factory
            .create_response_command_with_code_remark(code, message)
            .set_opaque(opaque),
        RemotingErrorTarget::Existing(response) => response.set_code(code).set_remark(message),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::Bytes;
    use cheetah_string::CheetahString;
    use rocketmq_error::Error;
    use rocketmq_error::ErrorContext;
    use rocketmq_error::PublicErrorView;
    use rocketmq_error::CORE_INTERNAL_FAILURE;
    use rocketmq_error::PROTOCOL_BODY_INVALID;
    use rocketmq_protocol::protocol::header::empty_header::EmptyHeader;
    use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandDefaults;
    use rocketmq_protocol::protocol::SerializeType;

    use super::*;

    #[test]
    fn fresh_error_response_uses_supplied_factory_defaults() {
        let factory = RemotingCommandFactory::new(RemotingCommandDefaults::new(654, SerializeType::ROCKETMQ));
        let view = PublicErrorView::descriptor_only(&CORE_INTERNAL_FAILURE);

        let response = error_response(view, RemotingErrorTarget::Fresh(&factory));

        assert_eq!(response.version(), 654);
        assert_eq!(response.serialize_type(), SerializeType::ROCKETMQ);
        assert!(response.is_response_type());
        assert_eq!(response.code(), 1);
        assert_eq!(response.remark().map(CheetahString::as_str), Some("Internal error"));
    }

    #[test]
    fn reply_error_response_preserves_request_opaque() {
        let factory = RemotingCommandFactory::new(RemotingCommandDefaults::default());
        let view = PublicErrorView::descriptor_only(&PROTOCOL_BODY_INVALID);

        let response = error_response(
            view,
            RemotingErrorTarget::Reply {
                factory: &factory,
                opaque: 73,
            },
        );

        assert_eq!(response.opaque(), 73);
        assert_eq!(response.code(), 29);
        assert_eq!(
            response.remark().map(CheetahString::as_str),
            Some("Request body is invalid")
        );
    }

    #[test]
    fn existing_error_response_preserves_owner_managed_state() {
        let mut ext_fields = HashMap::new();
        ext_fields.insert(
            CheetahString::from_static_str("owner"),
            CheetahString::from_static_str("broker"),
        );
        let existing = RemotingCommand::create_response_command_with_code(7)
            .set_opaque(41)
            .set_version(321)
            .set_serialize_type(SerializeType::ROCKETMQ)
            .set_ext_fields(ext_fields.clone())
            .set_command_custom_header(EmptyHeader::default())
            .set_body(Bytes::from_static(b"retained"))
            .set_suspended(true);
        let original_flag = existing.flag();
        let view = PublicErrorView::descriptor_only(&PROTOCOL_BODY_INVALID);

        let response = error_response(view, RemotingErrorTarget::Existing(existing));

        assert_eq!(response.code(), 29);
        assert_eq!(response.opaque(), 41);
        assert_eq!(response.version(), 321);
        assert_eq!(response.serialize_type(), SerializeType::ROCKETMQ);
        assert_eq!(response.ext_fields(), Some(&ext_fields));
        assert_eq!(response.body().map(Bytes::as_ref), Some(b"retained".as_slice()));
        assert!(response.command_custom_header_ref().is_some());
        assert!(response.suspended());
        assert_eq!(response.flag(), original_flag);
        assert_eq!(
            response.remark().map(CheetahString::as_str),
            Some("Request body is invalid")
        );
    }

    #[test]
    fn invalid_context_fallback_keeps_descriptor_code_and_fixed_message() {
        let factory = RemotingCommandFactory::new(RemotingCommandDefaults::default());
        let error = Error::new(&PROTOCOL_BODY_INVALID)
            .with_context(ErrorContext::new().with_text(rocketmq_error::fields::TOPIC, "password=secret"));
        let view = error
            .public_view()
            .unwrap_or_else(|_| PublicErrorView::descriptor_only(error.descriptor()));

        let response = error_response(view, RemotingErrorTarget::Fresh(&factory));

        assert_eq!(response.code(), 29);
        assert_eq!(
            response.remark().map(CheetahString::as_str),
            Some("Request body is invalid")
        );
        assert!(!response.remark().expect("fixed remark").contains("secret"));
    }

    #[test]
    fn legacy_error_policy_surfaces_are_absent() {
        let module = include_str!("error_response.rs");
        let public_api = include_str!("public_api.rs");
        let protocol_factory = include_str!("../../rocketmq-protocol/src/protocol/remoting_command_defaults.rs");

        for removed in [
            concat!("command_from", "_error"),
            concat!("request_code", "_not_supported"),
            concat!("invalid_parameter", "_with_remark"),
            concat!("no_permission", "_with_remark"),
            concat!("query_not_found", "_with_remark"),
            concat!("internal_error", "_with_opaque"),
            concat!("apply_error", "_to_response"),
            concat!("remoting_response", "_from_error"),
        ] {
            assert!(!module.contains(removed), "legacy Transport helper returned: {removed}");
            assert!(
                !public_api.contains(removed),
                "legacy Transport re-export returned: {removed}"
            );
        }
        assert!(!protocol_factory.contains(concat!("create_response_command", "_from_error")));
        assert_eq!(public_api.matches("crate::error_response::").count(), 2);
    }
}
