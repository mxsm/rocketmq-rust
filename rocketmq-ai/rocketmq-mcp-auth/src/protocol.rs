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

//! The MCP protocol revisions that the RocketMQ MCP servers answer.
//!
//! A client can name a revision in three places: in `initialize`, in the `MCP-Protocol-Version`
//! header of an HTTP request, and in the metadata of a request that describes itself. Both servers
//! hold all three to the one set stated here, so they cannot come to answer different revisions.

use http::HeaderMap;

/// The MCP protocol revision that the servers implement.
pub const MCP_PROTOCOL_VERSION: &str = "2025-11-25";

/// Every MCP protocol revision that the servers answer, oldest first.
pub const SUPPORTED_MCP_PROTOCOL_VERSIONS: &[&str] = &[MCP_PROTOCOL_VERSION];

/// The header in which an HTTP request names the protocol revision it speaks.
pub const MCP_PROTOCOL_VERSION_HEADER: &str = "mcp-protocol-version";

/// What the `MCP-Protocol-Version` header of a request says about its protocol revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestedProtocolVersion {
    /// The request names no revision.
    Unstated,
    /// The request names a revision that the servers answer.
    Supported,
    /// The request names another revision, more than one value, or a value that is not text.
    Unsupported,
}

/// Returns whether the servers answer the protocol revision `version`.
#[must_use]
pub fn is_supported_mcp_protocol_version(version: &str) -> bool {
    SUPPORTED_MCP_PROTOCOL_VERSIONS.contains(&version)
}

/// Reads the protocol revision that the headers of an HTTP request name.
///
/// The value is compared as it was sent. A header that repeats is unsupported even when every
/// value is a supported revision, because which of them a later reader takes is not defined.
#[must_use]
pub fn requested_protocol_version(headers: &HeaderMap) -> RequestedProtocolVersion {
    let mut stated = headers.get_all(MCP_PROTOCOL_VERSION_HEADER).iter();
    let Some(first) = stated.next() else {
        return RequestedProtocolVersion::Unstated;
    };
    let supported = stated.next().is_none() && first.to_str().is_ok_and(is_supported_mcp_protocol_version);
    if supported {
        RequestedProtocolVersion::Supported
    } else {
        RequestedProtocolVersion::Unsupported
    }
}

#[cfg(test)]
mod tests {
    use http::HeaderValue;

    use super::*;

    fn headers(values: &[&[u8]]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for value in values {
            headers.append(MCP_PROTOCOL_VERSION_HEADER, HeaderValue::from_bytes(value).unwrap());
        }
        headers
    }

    #[test]
    fn the_implemented_revision_is_the_only_supported_one() {
        assert_eq!(SUPPORTED_MCP_PROTOCOL_VERSIONS, [MCP_PROTOCOL_VERSION]);
        assert!(is_supported_mcp_protocol_version(MCP_PROTOCOL_VERSION));
        for other in [
            "2024-11-05",
            "2025-03-26",
            "2025-06-18",
            "2026-07-28",
            "",
            "2025-11-25 ",
        ] {
            assert!(!is_supported_mcp_protocol_version(other), "{other:?}");
        }
    }

    #[test]
    fn a_request_header_is_unstated_supported_or_unsupported() {
        assert_eq!(
            requested_protocol_version(&HeaderMap::new()),
            RequestedProtocolVersion::Unstated
        );
        assert_eq!(
            requested_protocol_version(&headers(&[MCP_PROTOCOL_VERSION.as_bytes()])),
            RequestedProtocolVersion::Supported
        );
        // The header name is matched without regard to case, as every HTTP header is.
        let mut mixed_case = HeaderMap::new();
        mixed_case.insert(
            "MCP-Protocol-Version".parse::<http::HeaderName>().unwrap(),
            HeaderValue::from_static(MCP_PROTOCOL_VERSION),
        );
        assert_eq!(
            requested_protocol_version(&mixed_case),
            RequestedProtocolVersion::Supported
        );
        for unsupported in [
            &[&b"2025-06-18"[..]][..],
            &[b"2026-07-28"],
            &[b"1999-01-01"],
            &[b""],
            &[b" 2025-11-25"],
            &[b"2025-11-25, 2025-11-25"],
            &[b"2025-11-25\xff"],
            // A header that repeats, with a supported value in either place.
            &[b"2025-11-25", b"2025-06-18"],
            &[b"2025-06-18", b"2025-11-25"],
            &[b"2025-11-25", b"2025-11-25"],
        ] {
            assert_eq!(
                requested_protocol_version(&headers(unsupported)),
                RequestedProtocolVersion::Unsupported,
                "{unsupported:?}"
            );
        }
    }
}
