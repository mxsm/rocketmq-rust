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

use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    time::Duration,
};

pub use blocking_client::BlockingClient;
pub use client::Client;

use crate::protocol::remoting_command::RemotingCommand;

mod async_client;
mod blocking_client;

mod client;

#[derive(Default)]
pub struct RemoteClient {
    inner: HashMap<String, BlockingClient>,
}

impl Clone for RemoteClient {
    fn clone(&self) -> Self {
        Self {
            inner: HashMap::new(),
        }
    }
}

impl Debug for RemoteClient {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("RemoteClient")
    }
}

impl RemoteClient {
    /// Create a new `RemoteClient` instance.
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    pub fn invoke_oneway(
        &mut self,
        addr: String,
        request: RemotingCommand,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        self.inner
            .entry(addr.clone())
            .or_insert_with(|| BlockingClient::connect(addr).unwrap())
            .invoke_oneway(request, timeout)
    }
}
