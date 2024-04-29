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

use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{
    base::{commit_log_dispatcher::CommitLogDispatcher, dispatch_request::DispatchRequest},
    config::message_store_config::MessageStoreConfig,
    index::index_service::IndexService,
};

#[derive(Clone)]
pub struct CommitLogDispatcherBuildIndex {
    index_service: Arc<Mutex<IndexService>>,
    message_store_config: Arc<MessageStoreConfig>,
}

impl CommitLogDispatcherBuildIndex {
    pub fn new(
        index_service: Arc<Mutex<IndexService>>,
        message_store_config: Arc<MessageStoreConfig>,
    ) -> Self {
        Self {
            index_service,
            message_store_config,
        }
    }
}

impl CommitLogDispatcher for CommitLogDispatcherBuildIndex {
    async fn dispatch(&mut self, dispatch_request: &DispatchRequest) {
        if self.message_store_config.message_index_enable {
            self.index_service
                .lock()
                .await
                .build_index(dispatch_request);
        }
    }
}
