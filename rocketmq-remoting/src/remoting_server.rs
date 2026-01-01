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

use crate::remoting::RemotingService;

pub mod rocketmq_tokio_server;

pub trait RemotingServer: RemotingService {
    /*fn register_processor(
        &mut self,
        request_code: impl Into<i32>,
        processor: Arc<dyn RequestProcessor + Send + Sync + 'static>,
    );

    fn register_default_processor(
        &mut self,
        processor: impl RequestProcessor + Send + Sync + 'static,
    );
    fn local_listen_port(&mut self) -> i32;
    fn get_processor_pair(
        &mut self,
        request_code: i32,
    ) -> (Arc<dyn RequestProcessor>, Arc<TokioExecutorService>);
    fn get_default_processor_pair(
        &mut self,
    ) -> (Arc<dyn RequestProcessor>, Arc<TokioExecutorService>);
    //fn new_remoting_server(&self, port: i32) -> Box<dyn RemotingServer>;
    fn remove_remoting_server(&mut self, port: i32);
    fn invoke_sync(
        &mut self,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> Result<RemotingCommand, Box<dyn std::error::Error>>;
    fn invoke_async(
        &mut self,
        request: RemotingCommand,
        timeout_millis: u64,
        invoke_callback: Box<dyn InvokeCallback>,
    ) -> Result<(), Box<dyn std::error::Error>>;
    fn invoke_oneway(
        &mut self,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> Result<(), Box<dyn std::error::Error>>;*/
}
