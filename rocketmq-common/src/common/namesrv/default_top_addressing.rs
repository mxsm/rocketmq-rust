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
use std::sync::Arc;

use cheetah_string::CheetahString;
use tracing::error;
use tracing::warn;

use crate::common::namesrv::name_server_update_callback::NameServerUpdateCallback;
use crate::common::namesrv::top_addressing::TopAddressing;
use crate::utils::http_tiny_client::HttpTinyClient;

pub struct DefaultTopAddressing {
    ns_addr: Option<CheetahString>,
    ws_addr: CheetahString,
    unit_name: Option<CheetahString>,
    para: Option<HashMap<CheetahString, CheetahString>>,
    top_addressing_list: Vec<Arc<dyn TopAddressing>>,
}

impl DefaultTopAddressing {
    pub fn new(ws_addr: CheetahString, unit_name: Option<CheetahString>) -> Self {
        let top_addressing_list = Self::load_custom_top_addressing();
        DefaultTopAddressing {
            ns_addr: None,
            ws_addr,
            unit_name,
            para: None,
            top_addressing_list,
        }
    }

    /// Load custom top addressing implementations
    /// Note: Rust doesn't have Java's ServiceLoader, so this would need to be implemented
    /// using a plugin system or static registration
    fn load_custom_top_addressing() -> Vec<Arc<dyn TopAddressing>> {
        // In a real implementation, you might use a registry pattern
        // or dynamic loading mechanism here
        Vec::new()
    }

    /// Clear newline characters from string
    fn clear_new_line(input: &str) -> String {
        let trimmed = input.trim();

        if let Some(index) = trimmed.find('\r') {
            return trimmed[..index].to_string();
        }

        if let Some(index) = trimmed.find('\n') {
            return trimmed[..index].to_string();
        }

        trimmed.to_string()
    }

    /// Build the URL for fetching name server address
    fn build_url(&self) -> String {
        let mut url = self.ws_addr.clone().to_string();
        if let Some(ref para) = self.para {
            if !para.is_empty() {
                if let Some(ref unit_name) = self.unit_name {
                    if !unit_name.is_empty() {
                        url.push_str(&format!("-{unit_name}?nofix=1&"));
                    }
                } else {
                    url.push('?');
                }

                let mut query_params = Vec::new();
                for (key, value) in para {
                    query_params.push(format!("{key}={value}"));
                }
                url.push_str(&query_params.join("&"));
            }
        } else if let Some(ref unit_name) = self.unit_name {
            if !unit_name.is_empty() {
                url.push_str(&format!("-{unit_name}?nofix=1"));
            }
        }
        url
    }

    pub fn fetch_ns_addr_inner(&self, verbose: bool, timeout_millis: u64) -> Option<String> {
        let url = self.build_url();
        match HttpTinyClient::http_get(&url, None, None, "UTF-8", timeout_millis) {
            Ok(response) => {
                if response.code == 200 {
                    if !response.content.is_empty() {
                        return Some(Self::clear_new_line(&response.content));
                    } else {
                        error!("fetch nameserver address is null");
                    }
                } else {
                    error!("fetch nameserver address failed. statusCode={}", response.code);
                }
            }
            Err(e) => {
                if verbose {
                    error!("fetch name remoting_server address exception: {}", e);
                }
            }
        }

        if verbose {
            warn!(
                "connect to {} failed, maybe the domain name not bind in /etc/hosts",
                url
            );
        }
        None
    }
}

impl TopAddressing for DefaultTopAddressing {
    fn fetch_ns_addr(&self) -> Option<String> {
        // First try custom implementations
        for top_addressing in &self.top_addressing_list {
            if let Some(ns_address) = top_addressing.fetch_ns_addr() {
                if !ns_address.trim().is_empty() {
                    return Some(ns_address);
                }
            }
        }
        // Fall back to default implementation
        self.fetch_ns_addr_inner(true, 3000)
    }

    fn register_change_callback(&self, change_callback: Arc<dyn NameServerUpdateCallback>) {
        // Register callback with all custom implementations
        for top_addressing in &self.top_addressing_list {
            top_addressing.register_change_callback(change_callback.clone());
        }
    }
}
