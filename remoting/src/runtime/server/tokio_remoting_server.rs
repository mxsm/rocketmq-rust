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

 use crate::runtime::{
    remoting_service::RemotingService,
    server::{remoting_server::RemotingServer, server_inner::ServerBootstrap},
    RPCHook,
};

pub struct TokioRemotingServer {
    boot_strap: ServerBootstrap,
}
impl TokioRemotingServer {
    pub fn new(port: u32) -> TokioRemotingServer {
        let boot_strap = ServerBootstrap::new("", port);
        TokioRemotingServer { boot_strap }
    }
}

impl RemotingService for TokioRemotingServer {
    fn start(&self) {
        todo!()
    }

    fn shutdown(&self) {
        todo!()
    }

    fn register_rpc_hook(&self, rpc_hook: Box<dyn RPCHook>) {
        todo!()
    }

    fn clear_rpc_hook(&self) {
        todo!()
    }
}

impl RemotingServer for TokioRemotingServer {}
