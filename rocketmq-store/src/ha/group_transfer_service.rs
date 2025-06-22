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

use tracing::error;

use crate::config::message_store_config::MessageStoreConfig;
use crate::ha::general_ha_service::GeneralHAService;
use crate::store_error::HAResult;

pub struct GroupTransferService {
    message_store_config: Arc<MessageStoreConfig>,
    ha_service: GeneralHAService,
}

impl GroupTransferService {
    pub fn new(
        message_store_config: Arc<MessageStoreConfig>,
        ha_service: GeneralHAService,
    ) -> Self {
        GroupTransferService {
            message_store_config,
            ha_service,
        }
    }

    pub async fn start(&mut self) -> HAResult<()> {
        error!("GroupTransferService is not implemented yet");
        Ok(())
    }
}
