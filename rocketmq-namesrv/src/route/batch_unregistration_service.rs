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
use rocketmq_remoting::protocol::header::namesrv::broker_request::UnRegisterBrokerRequestHeader;
use rocketmq_rust::ArcMut;
use tracing::warn;

use crate::bootstrap::NameServerRuntimeInner;

pub(crate) struct BatchUnregistrationService {
    name_server_runtime_inner: ArcMut<NameServerRuntimeInner>,
    tx: tokio::sync::mpsc::Sender<UnRegisterBrokerRequestHeader>,
    rx: Option<tokio::sync::mpsc::Receiver<UnRegisterBrokerRequestHeader>>,
}

impl BatchUnregistrationService {
    pub(crate) fn new(name_server_runtime_inner: ArcMut<NameServerRuntimeInner>) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel::<UnRegisterBrokerRequestHeader>(
            name_server_runtime_inner
                .name_server_config()
                .unregister_broker_queue_capacity as usize,
        );
        BatchUnregistrationService {
            name_server_runtime_inner,
            tx,
            rx: Some(rx),
        }
    }

    pub fn submit(&self, request: UnRegisterBrokerRequestHeader) -> bool {
        if let Err(e) = self.tx.try_send(request) {
            warn!("submit unregister broker request failed: {:?}", e);
            return false;
        }
        true
    }

    pub fn start(&mut self) {
        let mut name_server_runtime_inner = self.name_server_runtime_inner.clone();
        let mut rx = self.rx.take().expect("rx is None");
        let limit = 10;
        tokio::spawn(async move {
            loop {
                let mut unregistration_requests = Vec::with_capacity(limit);
                tokio::select! {
                    _ = rx.recv_many(&mut unregistration_requests,limit) => {
                        name_server_runtime_inner.route_info_manager_mut().un_register_broker(unregistration_requests);
                    }
                }
            }
        });
    }
}
