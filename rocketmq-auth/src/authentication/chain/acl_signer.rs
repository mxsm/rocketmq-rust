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

//! ACL signature calculator.

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use rocketmq_error::AuthError;

/// Calculate ACL signature using HMAC-SHA1.
///
/// # Arguments
///
/// * `content` - The content to sign
/// * `secret_key` - The secret key for signing
///
/// # Returns
///
/// Base64-encoded signature
pub fn cal_signature(content: &[u8], secret_key: &str) -> Result<String, AuthError> {
    use hmac::Hmac;
    use hmac::Mac;
    use sha1::Sha1;

    type HmacSha1 = Hmac<Sha1>;

    let mut mac = HmacSha1::new_from_slice(secret_key.as_bytes())
        .map_err(|e| AuthError::AuthenticationFailed(format!("Invalid key: {}", e)))?;

    mac.update(content);
    let result = mac.finalize();
    let code_bytes = result.into_bytes();

    Ok(STANDARD.encode(code_bytes.as_slice()))
}

/// Default charset for ACL signing.
pub const DEFAULT_CHARSET: &str = "UTF-8";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cal_signature() {
        let content = b"test content";
        let secret_key = "test_secret_key";

        let result = cal_signature(content, secret_key);
        assert!(result.is_ok());

        let signature = result.unwrap();
        assert!(!signature.is_empty());
    }

    #[test]
    fn test_cal_signature_consistency() {
        let content = b"consistent content";
        let secret_key = "my_secret";

        let sig1 = cal_signature(content, secret_key).unwrap();
        let sig2 = cal_signature(content, secret_key).unwrap();

        assert_eq!(sig1, sig2);
    }

    #[test]
    fn test_cal_signature_different_keys() {
        let content = b"same content";
        let key1 = "key1";
        let key2 = "key2";

        let sig1 = cal_signature(content, key1).unwrap();
        let sig2 = cal_signature(content, key2).unwrap();

        assert_ne!(sig1, sig2);
    }

    #[test]
    fn test_cal_signature_empty_content() {
        let content = b"";
        let secret_key = "secret";

        let result = cal_signature(content, secret_key);
        assert!(result.is_ok());
    }
}
