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

use http::header::AUTHORIZATION;
use http::HeaderMap;

/// Returns the token of an `Authorization: Bearer` header.
///
/// `None` covers every unusable header alike: absent, not visible ASCII, another scheme, an empty
/// token, or a token longer than `max_token_bytes`. The length is the only thing checked about the
/// token, so an oversized one is refused before anything parses it.
pub fn bearer_token(headers: &HeaderMap, max_token_bytes: usize) -> Option<&str> {
    let header = headers.get(AUTHORIZATION).and_then(|value| value.to_str().ok())?;
    header
        .strip_prefix("Bearer ")
        .or_else(|| header.strip_prefix("bearer "))
        .filter(|token| !token.is_empty() && token.len() <= max_token_bytes)
}

#[cfg(test)]
mod tests {
    use http::HeaderValue;

    use super::*;

    fn headers(value: &[u8]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_bytes(value).unwrap());
        headers
    }

    #[test]
    fn bearer_scheme_yields_the_token() {
        assert_eq!(bearer_token(&headers(b"Bearer abc.def.ghi"), 16), Some("abc.def.ghi"));
        assert_eq!(bearer_token(&headers(b"bearer abc.def.ghi"), 16), Some("abc.def.ghi"));
    }

    #[test]
    fn unusable_headers_yield_nothing() {
        assert_eq!(bearer_token(&HeaderMap::new(), 16), None);
        for value in [
            &b"Bearer "[..],
            b"Bearer",
            b"Basic dXNlcjpwYXNz",
            b"BEARER abc",
            b"abc.def.ghi",
            b"Bearer \xffabc",
        ] {
            assert_eq!(bearer_token(&headers(value), 16), None);
        }
    }

    #[test]
    fn oversized_token_is_refused_by_length_alone() {
        let limit = 16 * 1024;
        let largest = format!("Bearer {}", "a".repeat(limit));
        let oversized = format!("Bearer {}", "a".repeat(limit + 1));
        assert_eq!(
            bearer_token(&headers(largest.as_bytes()), limit).map(str::len),
            Some(limit)
        );
        assert_eq!(bearer_token(&headers(oversized.as_bytes()), limit), None);
    }
}
