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
use std::sync::Arc;

use tracing::error;
use tracing::warn;

use crate::common::namesrv::name_server_update_callback::NameServerUpdateCallback;
use crate::common::namesrv::top_addressing::TopAddressing;
use crate::utils::http_tiny_client::HttpTinyClient;

pub struct DefaultTopAddressing {
    ns_addr: Option<String>,
    ws_addr: String,
    unit_name: Option<String>,
    para: Option<HashMap<String, String>>,
    top_addressing_list: Vec<Arc<dyn TopAddressing>>,
}

impl DefaultTopAddressing {
    pub fn new(ws_addr: String, unit_name: Option<String>) -> Self {
        let top_addressing_list = Self::load_custom_top_addressing();
        DefaultTopAddressing {
            ns_addr: None,
            ws_addr,
            unit_name,
            para: None,
            top_addressing_list,
        }
    }

    fn load_custom_top_addressing() -> Vec<Arc<dyn TopAddressing>> {
        // In real scenarios, load from configuration or plugins.
        vec![]
    }

    fn clear_new_line(s: &str) -> String {
        s.lines().next().unwrap_or("").to_string()
    }

    pub fn fetch_ns_addr_inner(&self, verbose: bool, timeout_millis: u64) -> Option<String> {
        for top_addressing in &self.top_addressing_list {
            if let Some(ns_addr) = top_addressing.fetch_ns_addr() {
                return Some(ns_addr);
            }
        }

        let mut url = self.ws_addr.clone();
        if let Some(para) = &self.para {
            if let Some(unit_name) = &self.unit_name {
                url.push_str(&format!("-{}?nofix=1&", unit_name));
            } else {
                url.push('?');
            }
            for (key, value) in para {
                url.push_str(&format!("{}={}&", key, value));
            }
            url.pop(); // Remove the last '&'
        } else if let Some(unit_name) = &self.unit_name {
            url.push_str(&format!("-{}?nofix=1", unit_name));
        }

        match HttpTinyClient::http_get(&url, None, None, "UTF-8", timeout_millis) {
            Ok(response) => {
                if response.code == 200 {
                    if !response.content.is_empty() {
                        return Some(Self::clear_new_line(&response.content));
                    } else {
                        error!("fetch nameserver address is null");
                    }
                } else {
                    error!(
                        "fetch nameserver address failed. statusCode={}",
                        response.code
                    );
                }
            }
            Err(e) => {
                if verbose {
                    error!("fetch name server address exception: {}", e);
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
        self.fetch_ns_addr_inner(true, 3000)
    }

    fn register_change_callback(&self, change_callback: Arc<dyn NameServerUpdateCallback>) {
        for top_addressing in &self.top_addressing_list {
            top_addressing.register_change_callback(change_callback.clone());
        }
    }
}
