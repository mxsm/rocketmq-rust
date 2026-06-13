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
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SignatureAlgorithm {
    #[default]
    #[serde(
        rename = "HmacSHA1",
        alias = "hmacSHA1",
        alias = "HMAC_SHA1",
        alias = "HmacSha1",
        alias = "sha1"
    )]
    HmacSha1,
    #[serde(
        rename = "HmacSHA256",
        alias = "hmacSHA256",
        alias = "HMAC_SHA256",
        alias = "HmacSha256",
        alias = "sha256"
    )]
    HmacSha256,
    #[serde(
        rename = "HmacMD5",
        alias = "hmacMD5",
        alias = "HMAC_MD5",
        alias = "HmacMd5",
        alias = "md5"
    )]
    HmacMd5,
}

impl SignatureAlgorithm {
    pub const fn java_name(self) -> &'static str {
        match self {
            Self::HmacSha1 => "HmacSHA1",
            Self::HmacSha256 => "HmacSHA256",
            Self::HmacMd5 => "HmacMD5",
        }
    }

    pub fn from_java_name(value: &str) -> Option<Self> {
        match value.trim().to_ascii_uppercase().replace(['-', '_'], "").as_str() {
            "HMACSHA1" | "SHA1" => Some(Self::HmacSha1),
            "HMACSHA256" | "SHA256" => Some(Self::HmacSha256),
            "HMACMD5" | "MD5" => Some(Self::HmacMd5),
            _ => None,
        }
    }
}

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
    cal_signature_with_algorithm(content, secret_key, SignatureAlgorithm::default())
}

pub fn cal_signature_segments<'a, I>(segments: I, secret_key: &str) -> Result<String, AuthError>
where
    I: IntoIterator<Item = &'a [u8]>,
{
    cal_signature_segments_with_algorithm(segments, secret_key, SignatureAlgorithm::default())
}

pub fn cal_signature_with_algorithm(
    content: &[u8],
    secret_key: &str,
    algorithm: SignatureAlgorithm,
) -> Result<String, AuthError> {
    cal_signature_segments_with_algorithm([content], secret_key, algorithm)
}

pub fn cal_signature_segments_with_algorithm<'a, I>(
    segments: I,
    secret_key: &str,
    algorithm: SignatureAlgorithm,
) -> Result<String, AuthError>
where
    I: IntoIterator<Item = &'a [u8]>,
{
    use hmac::digest::KeyInit;
    use hmac::Hmac;
    use hmac::Mac;

    match algorithm {
        SignatureAlgorithm::HmacSha1 => {
            type HmacSha1 = Hmac<sha1::Sha1>;
            let mut mac = HmacSha1::new_from_slice(secret_key.as_bytes()).map_err(|error| {
                AuthError::AuthenticationFailed(format!("Invalid {} key: {error}", algorithm.java_name()))
            })?;
            for segment in segments {
                mac.update(segment);
            }
            Ok(STANDARD.encode(mac.finalize().into_bytes().as_slice()))
        }
        SignatureAlgorithm::HmacSha256 => {
            type HmacSha256 = Hmac<sha2::Sha256>;
            let mut mac = HmacSha256::new_from_slice(secret_key.as_bytes()).map_err(|error| {
                AuthError::AuthenticationFailed(format!("Invalid {} key: {error}", algorithm.java_name()))
            })?;
            for segment in segments {
                mac.update(segment);
            }
            Ok(STANDARD.encode(mac.finalize().into_bytes().as_slice()))
        }
        SignatureAlgorithm::HmacMd5 => {
            type HmacMd5 = Hmac<md5::Md5>;
            let mut mac = HmacMd5::new_from_slice(secret_key.as_bytes()).map_err(|error| {
                AuthError::AuthenticationFailed(format!("Invalid {} key: {error}", algorithm.java_name()))
            })?;
            for segment in segments {
                mac.update(segment);
            }
            Ok(STANDARD.encode(mac.finalize().into_bytes().as_slice()))
        }
    }
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

    #[test]
    fn test_cal_signature_matches_java_hmac_sha1_base64() {
        let signature = cal_signature(b"alicetopic-a", "secret").unwrap();

        assert_eq!(signature, "3yomro7y0WqWcbV+9tQa4av5w3Q=");
    }

    #[test]
    fn test_cal_signature_segments_matches_contiguous_content() {
        let contiguous = cal_signature(b"alicetokentopic-abody", "secret").unwrap();
        let segmented = cal_signature_segments([b"alice".as_slice(), b"token", b"topic-a", b"body"], "secret").unwrap();

        assert_eq!(segmented, contiguous);
    }

    #[test]
    fn test_cal_signature_segments_supports_all_java_algorithms() {
        for algorithm in [
            SignatureAlgorithm::HmacSha1,
            SignatureAlgorithm::HmacSha256,
            SignatureAlgorithm::HmacMd5,
        ] {
            let contiguous = cal_signature_with_algorithm(b"alicetopic-a", "secret", algorithm).unwrap();
            let segmented =
                cal_signature_segments_with_algorithm([b"alice".as_slice(), b"topic-a"], "secret", algorithm).unwrap();

            assert_eq!(segmented, contiguous);
        }
    }

    #[test]
    fn test_cal_signature_supports_java_signing_algorithm_enum() {
        assert_eq!(
            cal_signature_with_algorithm(b"alicetopic-a", "secret", SignatureAlgorithm::HmacSha1).unwrap(),
            "3yomro7y0WqWcbV+9tQa4av5w3Q="
        );
        assert_eq!(
            cal_signature_with_algorithm(b"alicetopic-a", "secret", SignatureAlgorithm::HmacSha256).unwrap(),
            "+VoSl/q1CHGyZYyeI+H1C8SK6N+inDouZi6grscuA/Q="
        );
        assert_eq!(
            cal_signature_with_algorithm(b"alicetopic-a", "secret", SignatureAlgorithm::HmacMd5).unwrap(),
            "8NLZd12a3GBoWPkyeUsOJQ=="
        );
    }

    #[test]
    fn test_signature_algorithm_deserializes_java_names() {
        assert_eq!(
            serde_yaml::from_str::<SignatureAlgorithm>("HmacSHA1").unwrap(),
            SignatureAlgorithm::HmacSha1
        );
        assert_eq!(
            serde_yaml::from_str::<SignatureAlgorithm>("HmacSHA256").unwrap(),
            SignatureAlgorithm::HmacSha256
        );
        assert_eq!(
            serde_yaml::from_str::<SignatureAlgorithm>("HmacMD5").unwrap(),
            SignatureAlgorithm::HmacMd5
        );
    }

    #[test]
    fn test_signature_algorithm_from_java_name_accepts_common_aliases() {
        assert_eq!(
            SignatureAlgorithm::from_java_name("HmacSHA1"),
            Some(SignatureAlgorithm::HmacSha1)
        );
        assert_eq!(
            SignatureAlgorithm::from_java_name("HMAC_SHA256"),
            Some(SignatureAlgorithm::HmacSha256)
        );
        assert_eq!(
            SignatureAlgorithm::from_java_name("md5"),
            Some(SignatureAlgorithm::HmacMd5)
        );
        assert_eq!(SignatureAlgorithm::from_java_name("unknown"), None);
    }
}
