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

use chrono::DateTime;
use chrono::Utc;
use hmac::Hmac;
use hmac::KeyInit;
use hmac::Mac;
use reqwest::RequestBuilder;
use reqwest::header::HeaderValue;
use serde::Deserialize;
use sha2::Digest;
use sha2::Sha256;
use url::Host;
use url::Url;

use crate::error::ProviderError;
use crate::error::ProviderErrorCode;
use crate::secret::SecretMaterial;

type HmacSha256 = Hmac<Sha256>;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AwsCredentialMaterial {
    #[serde(alias = "AccessKeyId", alias = "aws_access_key_id")]
    access_key_id: String,
    #[serde(alias = "SecretAccessKey", alias = "aws_secret_access_key")]
    secret_access_key: String,
    #[serde(default, alias = "SessionToken", alias = "aws_session_token")]
    session_token: Option<String>,
    #[serde(default)]
    region: Option<String>,
}

pub(crate) fn sign_bedrock_request(
    builder: RequestBuilder,
    url: &Url,
    body: &[u8],
    credential: &SecretMaterial,
    now: DateTime<Utc>,
) -> Result<RequestBuilder, ProviderError> {
    let material: AwsCredentialMaterial = serde_json::from_str(credential.expose_to_transport()).map_err(|_| {
        ProviderError::new(
            ProviderErrorCode::AuthenticationFailed,
            "Bedrock credential material must use the documented JSON shape",
        )
    })?;
    validate_material(&material)?;
    let region = material
        .region
        .as_deref()
        .or_else(|| region_from_bedrock_host(url))
        .ok_or_else(|| {
            ProviderError::new(
                ProviderErrorCode::AuthenticationFailed,
                "Bedrock credential region is unavailable",
            )
        })?;

    let date = now.format("%Y%m%d").to_string();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let payload_hash = sha256_hex(body);
    let host = canonical_host(url)?;
    let canonical_query = canonical_query(url);

    let mut canonical_headers = format!(
        "content-type:application/json\nhost:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n"
    );
    let mut signed_headers = "content-type;host;x-amz-content-sha256;x-amz-date".to_owned();
    if let Some(token) = material.session_token.as_deref() {
        canonical_headers.push_str(&format!("x-amz-security-token:{token}\n"));
        signed_headers.push_str(";x-amz-security-token");
    }

    let canonical_request = format!(
        "POST\n{}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}",
        canonical_uri(url)
    );
    let scope = format!("{date}/{region}/bedrock/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );
    let signing_key = signing_key(&material.secret_access_key, &date, region)?;
    let signature = hmac_hex(&signing_key, string_to_sign.as_bytes())?;
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        material.access_key_id
    );

    let mut authorization_header = secret_header(&authorization)?;
    authorization_header.set_sensitive(true);
    let mut builder = builder
        .header("host", host)
        .header("x-amz-content-sha256", payload_hash)
        .header("x-amz-date", amz_date)
        .header("authorization", authorization_header);
    if let Some(token) = material.session_token.as_deref() {
        let mut token_header = secret_header(token)?;
        token_header.set_sensitive(true);
        builder = builder.header("x-amz-security-token", token_header);
    }
    Ok(builder)
}

fn validate_material(material: &AwsCredentialMaterial) -> Result<(), ProviderError> {
    let required = [&material.access_key_id, &material.secret_access_key];
    if required
        .iter()
        .any(|value| value.is_empty() || value.chars().any(char::is_control))
        || material
            .session_token
            .as_ref()
            .is_some_and(|value| value.is_empty() || value.chars().any(char::is_control))
        || material
            .region
            .as_ref()
            .is_some_and(|value| value.is_empty() || value.chars().any(char::is_control))
    {
        return Err(ProviderError::new(
            ProviderErrorCode::AuthenticationFailed,
            "Bedrock credential material is invalid",
        ));
    }
    Ok(())
}

fn region_from_bedrock_host(url: &Url) -> Option<&str> {
    let host = url.host_str()?;
    host.strip_prefix("bedrock-runtime.")
        .or_else(|| host.strip_prefix("bedrock-runtime-fips."))
        .and_then(|remainder| remainder.split('.').next())
        .filter(|region| !region.is_empty())
}

fn canonical_host(url: &Url) -> Result<String, ProviderError> {
    let host = match url.host() {
        Some(Host::Domain(domain)) => domain.to_owned(),
        Some(Host::Ipv4(address)) => address.to_string(),
        Some(Host::Ipv6(address)) => format!("[{address}]"),
        None => {
            return Err(ProviderError::new(
                ProviderErrorCode::InvalidRequest,
                "provider endpoint has no host",
            ));
        }
    };
    Ok(url.port().map_or(host.clone(), |port| format!("{host}:{port}")))
}

fn canonical_uri(url: &Url) -> &str {
    let path = url.path();
    if path.is_empty() { "/" } else { path }
}

fn canonical_query(url: &Url) -> String {
    let mut pairs = url
        .query_pairs()
        .map(|(key, value)| (aws_percent_encode(key.as_bytes()), aws_percent_encode(value.as_bytes())))
        .collect::<Vec<_>>();
    pairs.sort();
    pairs
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn aws_percent_encode(value: &[u8]) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(*byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

fn signing_key(secret: &str, date: &str, region: &str) -> Result<Vec<u8>, ProviderError> {
    let date_key = hmac_bytes(format!("AWS4{secret}").as_bytes(), date.as_bytes())?;
    let region_key = hmac_bytes(&date_key, region.as_bytes())?;
    let service_key = hmac_bytes(&region_key, b"bedrock")?;
    hmac_bytes(&service_key, b"aws4_request")
}

fn hmac_hex(key: &[u8], value: &[u8]) -> Result<String, ProviderError> {
    hmac_bytes(key, value).map(|bytes| hex(&bytes))
}

fn hmac_bytes(key: &[u8], value: &[u8]) -> Result<Vec<u8>, ProviderError> {
    let mut mac = HmacSha256::new_from_slice(key).map_err(|_| {
        ProviderError::new(
            ProviderErrorCode::AuthenticationFailed,
            "Bedrock credential signing failed",
        )
    })?;
    mac.update(value);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn sha256_hex(value: &[u8]) -> String {
    hex(&Sha256::digest(value))
}

fn hex(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value {
        output.push(char::from(HEX[(byte >> 4) as usize]));
        output.push(char::from(HEX[(byte & 0x0f) as usize]));
    }
    output
}

fn secret_header(value: &str) -> Result<HeaderValue, ProviderError> {
    HeaderValue::from_str(value).map_err(|_| {
        ProviderError::new(
            ProviderErrorCode::AuthenticationFailed,
            "provider credential contains invalid header material",
        )
    })
}
