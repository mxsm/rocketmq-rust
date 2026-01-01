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
use std::time::Duration;

use reqwest::blocking::Client;
use reqwest::blocking::Response;
use reqwest::header::HeaderMap;
use reqwest::header::HeaderName;
use reqwest::header::HeaderValue;
use reqwest::header::CONTENT_TYPE;

use crate::common::mq_version::RocketMqVersion;
use crate::common::mq_version::CURRENT_VERSION;
use crate::TimeUtils::get_current_millis;

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
    /// Perform HTTP GET request
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
    pub fn http_get(
        url: &str,
        headers: Option<&[String]>,
        param_values: Option<&[String]>,
        encoding: &str,
        read_timeout_ms: u64,
    ) -> Result<HttpResult, io::Error> {
        let encoded_content = Self::encoding_params(param_values, encoding)?;
        let full_url = if let Some(params) = encoded_content {
            format!("{url}?{params}")
        } else {
            url.to_string()
        };

        let client = Client::builder()
            .timeout(Duration::from_millis(read_timeout_ms))
            .build()
            .map_err(io::Error::other)?;

        let mut request_builder = client.get(&full_url);

        // Set headers
        request_builder = Self::set_headers(request_builder, headers, encoding);

        let response = request_builder.send().map_err(io::Error::other)?;

        let status_code = response.status().as_u16() as i32;

        // Read response content based on status code
        let content = if response.status().is_success() {
            response.text()
        } else {
            // For error responses, still try to read the body
            response.text()
        }
        .map_err(io::Error::other)?;

        Ok(HttpResult::new(status_code, content))
    }

    /// Perform HTTP POST request
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
    pub fn http_post(
        url: &str,
        headers: Option<&[String]>,
        param_values: Option<&[String]>,
        encoding: &str,
        read_timeout_ms: u64,
    ) -> Result<HttpResult, io::Error> {
        let encoded_content = Self::encoding_params(param_values, encoding)?.unwrap_or_default();

        let client = Client::builder()
            .timeout(Duration::from_millis(read_timeout_ms))
            .connect_timeout(Duration::from_millis(3000)) // Fixed 3 second connect timeout like Java
            .build()
            .map_err(io::Error::other)?;

        let mut request_builder = client.post(url);

        // Set headers
        request_builder = Self::set_headers(request_builder, headers, encoding);

        // Set body content
        request_builder = request_builder.body(encoded_content);

        let response = request_builder.send().map_err(io::Error::other)?;

        let status_code = response.status().as_u16() as i32;

        // Read response content based on status code
        let content = if response.status().is_success() {
            response.text()
        } else {
            // For error responses, still try to read the body
            response.text()
        }
        .map_err(io::Error::other)?;

        Ok(HttpResult::new(status_code, content))
    }

    /// Encode parameters for URL or form data using form_urlencoded
    fn encoding_params(param_values: Option<&[String]>, _encoding: &str) -> Result<Option<String>, io::Error> {
        let params = match param_values {
            Some(params) if !params.is_empty() => params,
            _ => return Ok(None),
        };

        if params.len() % 2 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
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

    /// Set headers on the request builder
    fn set_headers(
        mut request_builder: reqwest::blocking::RequestBuilder,
        headers: Option<&[String]>,
        encoding: &str,
    ) -> reqwest::blocking::RequestBuilder {
        // Set custom headers (key-value pairs)
        if let Some(headers) = headers {
            if headers.len() % 2 == 0 {
                let mut iter = headers.iter();
                while let (Some(key), Some(value)) = (iter.next(), iter.next()) {
                    request_builder = request_builder.header(key, value);
                }
            }
        }

        // Set standard headers (matching Java implementation)
        request_builder = request_builder.header("Client-Version", CURRENT_VERSION.name()).header(
            "Content-Type",
            format!("application/x-www-form-urlencoded;charset={encoding}"),
        );

        // Set timestamp header
        let timestamp = get_current_millis();
        request_builder = request_builder.header("Metaq-Client-RequestTS", timestamp.to_string());

        request_builder
    }
}
