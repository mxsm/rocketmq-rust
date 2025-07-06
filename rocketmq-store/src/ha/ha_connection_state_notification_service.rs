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
use tracing::error;

use crate::ha::general_ha_service::GeneralHAService;
use crate::message_store::local_file_message_store::LocalFileMessageStore;
use crate::store_error::HAResult;

pub struct HAConnectionStateNotificationService {
    ha_service: GeneralHAService,
    default_message_store: ArcMut<LocalFileMessageStore>,
}

impl HAConnectionStateNotificationService {
    pub fn new(
        ha_service: GeneralHAService,
        default_message_store: ArcMut<LocalFileMessageStore>,
    ) -> Self {
        HAConnectionStateNotificationService {
            ha_service,
            default_message_store,
        }
    }

    pub async fn start(&mut self) -> HAResult<()> {
        error!("HAConnectionStateNotificationService is not implemented yet");
        Ok(())
    }
}
