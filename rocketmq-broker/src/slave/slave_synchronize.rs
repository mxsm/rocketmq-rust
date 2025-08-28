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

use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use tracing::error;
use tracing::info;

use crate::broker_runtime::BrokerRuntimeInner;

pub(crate) struct SlaveSynchronize<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    master_addr: Option<String>,
}

impl<MS> SlaveSynchronize<MS>
where
    MS: MessageStore,
{
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self {
            broker_runtime_inner,
            master_addr: None,
        }
    }

    pub fn master_addr(&self) -> Option<&String> {
        self.master_addr.as_ref()
    }

    pub fn set_master_addr(&mut self, addr: String) {
        if let Some(current_addr) = &self.master_addr {
            if current_addr == &addr {
                return;
            }
        }
        info!(
            "Update master address from {} to {}",
            self.master_addr.as_deref().unwrap_or("None"),
            addr
        );
        self.master_addr = Some(addr);
    }

    pub async fn sync_all(&self) {
        error!("SlaveSynchronize::sync_all is not implemented yet");
    }
    pub async fn sync_timer_check_point(&self) {
        error!("SlaveSynchronize::sync_timer_check_point is not implemented yet");
    }
}
