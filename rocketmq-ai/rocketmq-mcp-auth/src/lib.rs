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

//! JWKS retrieval, caching, and RS256 key selection shared by the RocketMQ MCP servers.
//!
//! A server that accepts OAuth Bearer tokens asks this crate one question: which public key must
//! verify this token. [`bearer_token`] takes the token out of the request headers,
//! [`JwksVerifier::decoding_key`] reads its header and returns the matching key of the issuer's
//! JWKS document, and [`HttpJwksSource`] fetches that document over HTTPS.
//!
//! Verifying the signature, checking claims, mapping them to a principal, and answering the client
//! stay with each server. So does every limit: a server passes its own [`JwksPolicy`] and
//! [`OutboundAddressPolicy`].
//!
//! Only RS256 is supported. A token without a `kid` is rejected, because the key is always chosen
//! by `kid`.

mod bearer;
mod document;
mod http_source;
mod policy;
#[cfg(test)]
mod test_support;
mod verifier;

pub use bearer::bearer_token;
pub use document::parse_jwks;
pub use document::JwksDocumentError;
pub use http_source::HttpJwksSource;
pub use http_source::HttpJwksSourceError;
pub use http_source::JwksFetchError;
pub use policy::JwksPolicy;
pub use policy::KidCharset;
pub use policy::OutboundAddressPolicy;
pub use verifier::JwksSource;
pub use verifier::JwksVerifier;
pub use verifier::KeyError;
pub use verifier::KeySetError;
pub use verifier::TokenRejection;
