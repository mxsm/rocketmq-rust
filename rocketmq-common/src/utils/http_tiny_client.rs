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

use std::collections::HashMap;
use std::io::Read;
use std::io::{self};
use std::str::FromStr;
use std::sync::OnceLock;
use std::time::Duration;

use reqwest::header::HeaderMap;
use reqwest::header::HeaderName;
use reqwest::header::HeaderValue;
use reqwest::header::CONTENT_TYPE;
use reqwest::Client;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;

use crate::common::mq_version::RocketMqVersion;
use crate::common::mq_version::CURRENT_VERSION;
use crate::TimeUtils::get_current_millis;

/// Global HTTP client with connection pool
/// Reuses connections across requests for better performance
static HTTP_CLIENT: OnceLock<Client> = OnceLock::new();

/// Initialize the global HTTP client (called lazily on first use)
fn get_http_client() -> &'static Client {
    HTTP_CLIENT.get_or_init(|| {
        Client::builder()
            .pool_idle_timeout(Duration::from_secs(90))
            .pool_max_idle_per_host(16)
            .connect_timeout(Duration::from_secs(3))
            .build()
            .expect("Failed to build HTTP client")
    })
}

pub struct HttpTinyClient;

/// HTTP response result (mirrors Java HttpResult)
#[derive(Debug, Clone)]
pub struct HttpResult {
    /// HTTP status code
    pub code: i32,
    /// Response content
    pub content: String,
}

impl HttpResult {
    /// Create a new HttpResult
    pub fn new(code: i32, content: String) -> Self {
        Self { code, content }
    }

    /// Check if the response is successful (2xx status codes)
    pub fn is_success(&self) -> bool {
        self.code >= 200 && self.code < 300
    }

    /// Check if the response is OK (200 status code)
    pub fn is_ok(&self) -> bool {
        self.code == 200
    }
}

impl std::fmt::Display for HttpResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "HttpResult(code: {}, content_length: {})",
            self.code,
            self.content.len()
        )
    }
}

impl HttpTinyClient {
    /// Get the shared HTTP client with connection pool
    fn client() -> &'static Client {
        get_http_client()
    }

    /// Validate URL format
    ///
    /// Ensures the URL is not empty and is a valid HTTP/HTTPS URL
    fn validate_url(url: &str) -> RocketMQResult<()> {
        if url.is_empty() {
            return Err(RocketMQError::validation_failed("url", "URL cannot be empty"));
        }

        // Parse URL to validate format
        reqwest::Url::parse(url)
            .map_err(|e| RocketMQError::validation_failed("url", format!("Invalid URL format: {}", e)))?;

        Ok(())
    }

    /// Validate headers format (must be in key-value pairs)
    fn validate_headers(headers: Option<&[String]>) -> RocketMQResult<()> {
        if let Some(h) = headers {
            if h.len() % 2 != 0 {
                return Err(RocketMQError::validation_failed(
                    "headers",
                    format!("Headers must be in key-value pairs, got {} items", h.len()),
                ));
            }

            // Validate each header name (cannot be empty)
            let mut iter = h.iter();
            while let Some(key) = iter.next() {
                if key.is_empty() {
                    return Err(RocketMQError::validation_failed(
                        "headers",
                        "Header name cannot be empty",
                    ));
                }
                // Skip value
                iter.next();
            }
        }
        Ok(())
    }

    /// Validate encoding format
    ///
    /// Only supports common encodings to prevent errors
    fn validate_encoding(encoding: &str) -> RocketMQResult<()> {
        if encoding.is_empty() {
            return Err(RocketMQError::validation_failed("encoding", "Encoding cannot be empty"));
        }

        // Support common encodings (case-insensitive)
        let encoding_upper = encoding.to_uppercase();
        match encoding_upper.as_str() {
            "UTF-8" | "UTF8" | "GBK" | "GB2312" | "GB18030" | "ISO-8859-1" | "US-ASCII" => Ok(()),
            _ => Err(RocketMQError::validation_failed(
                "encoding",
                format!(
                    "Unsupported encoding: '{}'. Supported: UTF-8, GBK, GB2312, GB18030, ISO-8859-1, US-ASCII",
                    encoding
                ),
            )),
        }
    }

    /// Validate timeout value
    fn validate_timeout(timeout_ms: u64) -> RocketMQResult<()> {
        if timeout_ms == 0 {
            return Err(RocketMQError::validation_failed(
                "timeout",
                "Timeout must be greater than 0",
            ));
        }

        // Warn about very long timeouts (> 5 minutes)
        if timeout_ms > 300_000 {
            tracing::warn!(
                "Very long timeout configured: {}ms ({}s). This may cause thread blocking.",
                timeout_ms,
                timeout_ms / 1000
            );
        }

        Ok(())
    }

    /// Perform HTTP GET request (async version)
    ///
    /// # Arguments
    /// * `url` - The URL to request
    /// * `headers` - Optional list of headers (key-value pairs)
    /// * `param_values` - Optional list of query parameters (key-value pairs)
    /// * `encoding` - Character encoding (e.g., "UTF-8")
    /// * `read_timeout_ms` - Timeout in milliseconds
    ///
    /// # Returns
    /// `HttpResult` containing response code and content
    pub async fn http_get_async(
        url: &str,
        headers: Option<&[String]>,
        param_values: Option<&[String]>,
        encoding: &str,
        read_timeout_ms: u64,
    ) -> RocketMQResult<HttpResult> {
        // Validate parameters before making the request
        Self::validate_url(url)?;
        Self::validate_headers(headers)?;
        Self::validate_encoding(encoding)?;
        Self::validate_timeout(read_timeout_ms)?;

        let encoded_content = Self::encoding_params(param_values, encoding)?;
        let full_url = if let Some(params) = encoded_content {
            format!("{url}?{params}")
        } else {
            url.to_string()
        };

        let client = Self::client();

        let mut request_builder = client.get(&full_url);
        request_builder = request_builder.timeout(Duration::from_millis(read_timeout_ms));
        request_builder = Self::set_headers_async(request_builder, headers, encoding)?;

        let response = request_builder.send().await.map_err(|e| {
            if e.is_timeout() {
                RocketMQError::network_timeout(url, Duration::from_millis(read_timeout_ms))
            } else if e.is_connect() {
                RocketMQError::network_connection_failed(url, e.to_string())
            } else {
                RocketMQError::network_request_failed(url, e.to_string())
            }
        })?;

        let status_code = response.status().as_u16() as i32;
        let content = response
            .text()
            .await
            .map_err(|e| RocketMQError::deserialization_failed("response_body", e.to_string()))?;

        Ok(HttpResult::new(status_code, content))
    }

    /// Perform HTTP POST request (async version)
    ///
    /// # Arguments
    /// * `url` - The URL to request
    /// * `headers` - Optional list of headers (key-value pairs)
    /// * `param_values` - Optional list of form parameters (key-value pairs)
    /// * `encoding` - Character encoding (e.g., "UTF-8")
    /// * `read_timeout_ms` - Timeout in milliseconds
    ///
    /// # Returns
    /// `HttpResult` containing response code and content
    pub async fn http_post_async(
        url: &str,
        headers: Option<&[String]>,
        param_values: Option<&[String]>,
        encoding: &str,
        read_timeout_ms: u64,
    ) -> RocketMQResult<HttpResult> {
        // Validate parameters before making the request
        Self::validate_url(url)?;
        Self::validate_headers(headers)?;
        Self::validate_encoding(encoding)?;
        Self::validate_timeout(read_timeout_ms)?;

        let encoded_content = Self::encoding_params(param_values, encoding)?.unwrap_or_default();

        let client = Self::client();

        let mut request_builder = client.post(url);
        request_builder = request_builder.timeout(Duration::from_millis(read_timeout_ms));
        request_builder = Self::set_headers_async(request_builder, headers, encoding)?;
        request_builder = request_builder.body(encoded_content);

        let response = request_builder.send().await.map_err(|e| {
            if e.is_timeout() {
                RocketMQError::network_timeout(url, Duration::from_millis(read_timeout_ms))
            } else if e.is_connect() {
                RocketMQError::network_connection_failed(url, e.to_string())
            } else {
                RocketMQError::network_request_failed(url, e.to_string())
            }
        })?;

        let status_code = response.status().as_u16() as i32;
        let content = response
            .text()
            .await
            .map_err(|e| RocketMQError::deserialization_failed("response_body", e.to_string()))?;

        Ok(HttpResult::new(status_code, content))
    }

    /// Perform HTTP GET request (blocking version - DEPRECATED)
    ///
    /// **DEPRECATED**: Use `http_get_async()` instead. This blocking version
    /// should not be used in async contexts as it will block the entire Tokio thread.
    ///
    /// # Arguments
    /// * `url` - The URL to request
    /// * `headers` - Optional list of headers (key-value pairs)
    /// * `param_values` - Optional list of query parameters (key-value pairs)
    /// * `encoding` - Character encoding (e.g., "UTF-8")
    /// * `read_timeout_ms` - Timeout in milliseconds
    ///
    /// # Returns
    /// `HttpResult` containing response code and content
    #[deprecated(
        since = "0.8.0",
        note = "Use http_get_async() instead. This blocking version blocks Tokio threads."
    )]
    pub fn http_get(
        url: &str,
        headers: Option<&[String]>,
        param_values: Option<&[String]>,
        encoding: &str,
        read_timeout_ms: u64,
    ) -> Result<HttpResult, io::Error> {
        // Use tokio::runtime to bridge to async world
        tokio::runtime::Runtime::new()
            .map_err(io::Error::other)?
            .block_on(async {
                Self::http_get_async(url, headers, param_values, encoding, read_timeout_ms)
                    .await
                    .map_err(|e| io::Error::other(e.to_string()))
            })
    }

    /// Perform HTTP POST request (blocking version - DEPRECATED)
    ///
    /// **DEPRECATED**: Use `http_post_async()` instead. This blocking version
    /// should not be used in async contexts as it will block the entire Tokio thread.
    ///
    /// # Arguments
    /// * `url` - The URL to request
    /// * `headers` - Optional list of headers (key-value pairs)
    /// * `param_values` - Optional list of form parameters (key-value pairs)
    /// * `encoding` - Character encoding (e.g., "UTF-8")
    /// * `read_timeout_ms` - Timeout in milliseconds
    ///
    /// # Returns
    /// `HttpResult` containing response code and content
    #[deprecated(
        since = "0.8.0",
        note = "Use http_post_async() instead. This blocking version blocks Tokio threads."
    )]
    pub fn http_post(
        url: &str,
        headers: Option<&[String]>,
        param_values: Option<&[String]>,
        encoding: &str,
        read_timeout_ms: u64,
    ) -> Result<HttpResult, io::Error> {
        // Use tokio::runtime to bridge to async world
        tokio::runtime::Runtime::new()
            .map_err(io::Error::other)?
            .block_on(async {
                Self::http_post_async(url, headers, param_values, encoding, read_timeout_ms)
                    .await
                    .map_err(|e| io::Error::other(e.to_string()))
            })
    }

    /// Encode parameters for URL or form data using form_urlencoded
    fn encoding_params(param_values: Option<&[String]>, _encoding: &str) -> RocketMQResult<Option<String>> {
        let params = match param_values {
            Some(params) if !params.is_empty() => params,
            _ => return Ok(None),
        };

        if params.len() % 2 != 0 {
            return Err(RocketMQError::validation_failed(
                "param_values",
                "Parameter values must be in key-value pairs",
            ));
        }

        let mut encoder = form_urlencoded::Serializer::new(String::new());

        let mut iter = params.iter();
        while let (Some(key), Some(value)) = (iter.next(), iter.next()) {
            encoder.append_pair(key, value);
        }

        let encoded = encoder.finish();
        if encoded.is_empty() {
            Ok(None)
        } else {
            Ok(Some(encoded))
        }
    }

    /// Set headers on the request builder (async version)
    fn set_headers_async(
        mut request_builder: reqwest::RequestBuilder,
        headers: Option<&[String]>,
        encoding: &str,
    ) -> RocketMQResult<reqwest::RequestBuilder> {
        if let Some(headers) = headers {
            let mut iter = headers.iter();
            while let (Some(key), Some(value)) = (iter.next(), iter.next()) {
                request_builder = request_builder.header(key, value);
            }
        }

        request_builder = request_builder.header("Client-Version", CURRENT_VERSION.name()).header(
            "Content-Type",
            format!("application/x-www-form-urlencoded;charset={encoding}"),
        );

        let timestamp = get_current_millis();
        request_builder = request_builder.header("Metaq-Client-RequestTS", timestamp.to_string());

        Ok(request_builder)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_http_get_async_basic() {
        // Test with httpbin.org (public test API)
        let result = HttpTinyClient::http_get_async("https://httpbin.org/get", None, None, "UTF-8", 10000).await;

        match result {
            Ok(response) => {
                println!("Status: {}", response.code);
                println!("Content length: {}", response.content.len());
                // Don't assert on success if external service is down (503)
                if response.code == 503 {
                    eprintln!("httpbin.org is unavailable (503), skipping test");
                } else {
                    assert!(response.is_success());
                }
            }
            Err(e) => {
                // Network might be unavailable in CI, log but don't fail
                eprintln!("HTTP request failed (this is OK in CI): {}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_http_get_async_with_query_params() {
        let params = vec![
            "key1".to_string(),
            "value1".to_string(),
            "key2".to_string(),
            "value2".to_string(),
        ];

        let result =
            HttpTinyClient::http_get_async("https://httpbin.org/get", None, Some(&params), "UTF-8", 10000).await;

        match result {
            Ok(response) => {
                println!("Response with params: {}", response.code);
                if response.code == 503 {
                    eprintln!("httpbin.org is unavailable (503), skipping test");
                } else {
                    assert!(response.is_success());
                    // httpbin echoes back query params
                    assert!(response.content.contains("key1"));
                    assert!(response.content.contains("value1"));
                }
            }
            Err(e) => {
                eprintln!("HTTP request failed (this is OK in CI): {}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_http_post_async_with_form_data() {
        let params = vec![
            "username".to_string(),
            "testuser".to_string(),
            "password".to_string(),
            "testpass".to_string(),
        ];

        let result =
            HttpTinyClient::http_post_async("https://httpbin.org/post", None, Some(&params), "UTF-8", 10000).await;

        match result {
            Ok(response) => {
                println!("POST response: {}", response.code);
                if response.code == 503 {
                    eprintln!("httpbin.org is unavailable (503), skipping test");
                } else {
                    assert!(response.is_success());
                    // httpbin echoes back form data
                    assert!(response.content.contains("username"));
                    assert!(response.content.contains("testuser"));
                }
            }
            Err(e) => {
                eprintln!("HTTP request failed (this is OK in CI): {}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_http_get_async_with_custom_headers() {
        let headers = vec![
            "User-Agent".to_string(),
            "RocketMQ-Rust/0.8.0".to_string(),
            "Accept".to_string(),
            "application/json".to_string(),
        ];

        let result =
            HttpTinyClient::http_get_async("https://httpbin.org/get", Some(&headers), None, "UTF-8", 10000).await;

        match result {
            Ok(response) => {
                if response.code == 503 {
                    eprintln!("httpbin.org is unavailable (503), skipping test");
                } else {
                    assert!(response.is_success());
                    // httpbin echoes back headers
                    assert!(response.content.contains("RocketMQ-Rust"));
                }
            }
            Err(e) => {
                eprintln!("HTTP request failed (this is OK in CI): {}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_error_handling_timeout() {
        let result = HttpTinyClient::http_get_async("https://httpbin.org/delay/10", None, None, "UTF-8", 2000).await;

        match result {
            Err(e) => {
                println!("Expected error: {}", e);
                assert!(matches!(e, RocketMQError::Network(_)));
            }
            Ok(response) => {
                panic!(
                    "Expected timeout error but got success response with code: {}",
                    response.code
                );
            }
        }
    }

    #[test]
    fn test_encoding_params() {
        let params = vec![
            "key1".to_string(),
            "value 1".to_string(),
            "key2".to_string(),
            "value&special".to_string(),
        ];

        let encoded = HttpTinyClient::encoding_params(Some(&params), "UTF-8").unwrap();
        assert!(encoded.is_some());

        let encoded_str = encoded.unwrap();
        println!("Encoded: {}", encoded_str);
        assert!(encoded_str.contains("key1=value+1"));
        assert!(encoded_str.contains("key2=value%26special"));
    }

    #[test]
    fn test_encoding_params_invalid() {
        // Odd number of params should fail
        let params = vec!["key1".to_string(), "value1".to_string(), "key2".to_string()];

        let result = HttpTinyClient::encoding_params(Some(&params), "UTF-8");
        assert!(result.is_err());
    }

    #[test]
    fn test_http_result_helpers() {
        let success_result = HttpResult::new(200, "OK".to_string());
        assert!(success_result.is_ok());
        assert!(success_result.is_success());

        let accepted_result = HttpResult::new(202, "Accepted".to_string());
        assert!(!accepted_result.is_ok());
        assert!(accepted_result.is_success());

        let error_result = HttpResult::new(404, "Not Found".to_string());
        assert!(!error_result.is_ok());
        assert!(!error_result.is_success());
    }

    #[tokio::test]
    async fn test_validation_empty_url() {
        let result = HttpTinyClient::http_get_async("", None, None, "UTF-8", 5000).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("URL cannot be empty"));
    }

    #[tokio::test]
    async fn test_validation_invalid_url() {
        let result = HttpTinyClient::http_get_async("not-a-valid-url", None, None, "UTF-8", 5000).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Invalid URL format"));
    }

    #[tokio::test]
    async fn test_validation_malformed_url() {
        let result = HttpTinyClient::http_get_async("http://", None, None, "UTF-8", 5000).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_validation_headers_odd_length() {
        let headers = vec![
            "Authorization".to_string(),
            "Bearer token".to_string(),
            "User-Agent".to_string(),
            // Missing value for User-Agent
        ];

        let result =
            HttpTinyClient::http_get_async("https://httpbin.org/get", Some(&headers), None, "UTF-8", 5000).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Headers must be in key-value pairs"));
        assert!(err.to_string().contains("got 3 items"));
    }

    #[tokio::test]
    async fn test_validation_empty_header_name() {
        let headers = vec![
            "".to_string(), // Empty header name
            "value".to_string(),
        ];

        let result =
            HttpTinyClient::http_get_async("https://httpbin.org/get", Some(&headers), None, "UTF-8", 5000).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Header name cannot be empty"));
    }

    #[tokio::test]
    async fn test_validation_empty_encoding() {
        let result = HttpTinyClient::http_get_async("https://httpbin.org/get", None, None, "", 5000).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Encoding cannot be empty"));
    }

    #[tokio::test]
    async fn test_validation_unsupported_encoding() {
        let result =
            HttpTinyClient::http_get_async("https://httpbin.org/get", None, None, "INVALID-ENCODING", 5000).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Unsupported encoding"));
        assert!(err.to_string().contains("INVALID-ENCODING"));
    }

    #[tokio::test]
    async fn test_validation_supported_encodings() {
        let encodings = vec!["UTF-8", "utf-8", "GBK", "gbk", "GB2312", "ISO-8859-1"];

        for encoding in encodings {
            // Just test validation passes, don't actually make request
            let result = HttpTinyClient::validate_encoding(encoding);
            assert!(result.is_ok(), "Encoding '{}' should be supported", encoding);
        }
    }

    #[tokio::test]
    async fn test_validation_zero_timeout() {
        let result = HttpTinyClient::http_get_async("https://httpbin.org/get", None, None, "UTF-8", 0).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Timeout must be greater than 0"));
    }

    #[test]
    fn test_validate_url_success() {
        assert!(HttpTinyClient::validate_url("http://example.com").is_ok());
        assert!(HttpTinyClient::validate_url("https://example.com").is_ok());
        assert!(HttpTinyClient::validate_url("http://example.com:8080/path").is_ok());
        assert!(HttpTinyClient::validate_url("https://example.com/path?query=value").is_ok());
    }

    #[test]
    fn test_validate_url_failure() {
        assert!(HttpTinyClient::validate_url("").is_err());
        assert!(HttpTinyClient::validate_url("not-a-url").is_err());
        assert!(HttpTinyClient::validate_url("ftp://example.com").is_ok()); // Valid URL, just not
                                                                            // HTTP
    }

    #[test]
    fn test_validate_headers_success() {
        let headers = vec![
            "Key1".to_string(),
            "Value1".to_string(),
            "Key2".to_string(),
            "Value2".to_string(),
        ];
        assert!(HttpTinyClient::validate_headers(Some(&headers)).is_ok());
        assert!(HttpTinyClient::validate_headers(None).is_ok());
        assert!(HttpTinyClient::validate_headers(Some(&[])).is_ok());
    }

    #[test]
    fn test_validate_headers_failure() {
        let odd_headers = vec!["Key1".to_string(), "Value1".to_string(), "Key2".to_string()];
        assert!(HttpTinyClient::validate_headers(Some(&odd_headers)).is_err());

        let empty_key = vec!["".to_string(), "Value1".to_string()];
        assert!(HttpTinyClient::validate_headers(Some(&empty_key)).is_err());
    }

    #[test]
    fn test_validate_encoding_case_insensitive() {
        assert!(HttpTinyClient::validate_encoding("utf-8").is_ok());
        assert!(HttpTinyClient::validate_encoding("UTF-8").is_ok());
        assert!(HttpTinyClient::validate_encoding("Utf-8").is_ok());
        assert!(HttpTinyClient::validate_encoding("gbk").is_ok());
        assert!(HttpTinyClient::validate_encoding("GBK").is_ok());
    }

    #[test]
    fn test_validate_timeout_success() {
        assert!(HttpTinyClient::validate_timeout(1000).is_ok());
        assert!(HttpTinyClient::validate_timeout(5000).is_ok());
        assert!(HttpTinyClient::validate_timeout(60000).is_ok());
    }

    #[test]
    fn test_validate_timeout_failure() {
        assert!(HttpTinyClient::validate_timeout(0).is_err());
    }

    #[tokio::test]
    async fn test_combined_validation_errors() {
        // Test multiple validation errors at once
        let result = HttpTinyClient::http_get_async("", None, None, "", 0).await;

        assert!(result.is_err());
        // Should fail on first validation (URL)
        let err = result.unwrap_err();
        assert!(err.to_string().contains("URL cannot be empty"));
    }

    #[tokio::test]
    async fn test_validation_with_valid_complex_request() {
        let headers = vec![
            "Authorization".to_string(),
            "Bearer token123".to_string(),
            "User-Agent".to_string(),
            "RocketMQ-Rust/0.8".to_string(),
        ];

        let params = vec![
            "key1".to_string(),
            "value1".to_string(),
            "key2".to_string(),
            "value2".to_string(),
        ];

        // This should pass all validations (though the request itself may fail due to network)
        let result =
            HttpTinyClient::http_get_async("https://httpbin.org/get", Some(&headers), Some(&params), "UTF-8", 5000)
                .await;

        // Either success or network error, but not validation error
        match result {
            Ok(_) => {
                // Success
            }
            Err(e) => {
                // Should be network error, not validation error
                let err_str = e.to_string();
                assert!(
                    !err_str.contains("validation") && !err_str.contains("Invalid"),
                    "Should not be a validation error: {}",
                    err_str
                );
            }
        }
    }
}
