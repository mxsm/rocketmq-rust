/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
use url::form_urlencoded;

pub struct HttpTinyClient;

pub struct HttpResult {
    pub code: u16,
    pub content: String,
}

impl HttpTinyClient {
    pub fn http_get(
        url: &str,
        headers: Option<HashMap<String, String>>,
        param_values: Option<HashMap<String, String>>,
        encoding: &str,
        read_timeout_ms: u64,
    ) -> Result<HttpResult, reqwest::Error> {
        let client = Client::new();
        let url = if let Some(params) = param_values {
            let encoded_content = HttpTinyClient::encode_params(params, encoding);
            format!("{url}?{encoded_content}")
        } else {
            url.to_string()
        };

        let mut request = client.get(&url);
        if let Some(headers) = headers {
            let header_map = HttpTinyClient::set_headers(headers, encoding);
            request = request.headers(header_map);
        }

        let response = request
            .timeout(Duration::from_millis(read_timeout_ms))
            .send()?;
        HttpTinyClient::process_response(response, encoding)
    }

    pub fn http_post(
        url: &str,
        headers: Option<HashMap<String, String>>,
        param_values: Option<HashMap<String, String>>,
        encoding: &str,
        read_timeout_ms: u64,
    ) -> Result<HttpResult, reqwest::Error> {
        let client = Client::new();
        let encoded_content =
            HttpTinyClient::encode_params(param_values.unwrap_or_default(), encoding);

        let mut request = client.post(url).body(encoded_content.clone());
        if let Some(headers) = headers {
            let header_map = HttpTinyClient::set_headers(headers, encoding);
            request = request.headers(header_map);
        }

        let response = request
            .timeout(Duration::from_millis(read_timeout_ms))
            .send()?;
        HttpTinyClient::process_response(response, encoding)
    }

    fn encode_params(params: HashMap<String, String>, encoding: &str) -> String {
        let encoded: String = form_urlencoded::Serializer::new(String::new())
            .extend_pairs(params)
            .finish();
        encoded
    }

    fn set_headers(headers: HashMap<String, String>, encoding: &str) -> HeaderMap {
        let mut header_map = HeaderMap::new();
        for (key, value) in headers {
            header_map.insert(
                HeaderName::from_str(&key).unwrap(),
                HeaderValue::from_str(&value).unwrap(),
            );
        }
        header_map.insert(
            "Client-Version",
            HeaderValue::from_str(&format!("MyClient/{}", env!("CARGO_PKG_VERSION"))).unwrap(),
        );
        header_map.insert(
            CONTENT_TYPE,
            HeaderValue::from_str(&format!(
                "application/x-www-form-urlencoded;charset={encoding}",
            ))
            .unwrap(),
        );
        header_map.insert(
            "Metaq-Client-RequestTS",
            HeaderValue::from_str(&format!("{}", chrono::Utc::now().timestamp_millis())).unwrap(),
        );
        header_map
    }

    fn process_response(response: Response, encoding: &str) -> Result<HttpResult, reqwest::Error> {
        let status = response.status();
        let content = response.text()?;

        Ok(HttpResult {
            code: status.as_u16(),
            content,
        })
    }
}
