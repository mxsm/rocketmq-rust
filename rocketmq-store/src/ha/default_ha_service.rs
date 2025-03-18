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

use std::sync::atomic::AtomicI32;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;

use rocketmq_remoting::protocol::body::ha_runtime_info::HARuntimeInfo;
use rocketmq_rust::ArcMut;
use tracing::error;

use crate::base::message_store::MessageStoreRefactor;
use crate::ha::ha_client::HAClient;
use crate::ha::ha_connection::HAConnection;
use crate::ha::ha_connection_state_notification_request::HAConnectionStateNotificationRequest;
use crate::ha::ha_service::HAService;
use crate::ha::wait_notify_object::WaitNotifyObject;
use crate::log_file::flush_manager_impl::group_commit_request::GroupCommitRequest;
use crate::store_error::HAResult;

pub struct DefaultHAService;

impl HAService for DefaultHAService {
    fn init<MS: MessageStoreRefactor>(&mut self, message_store: ArcMut<MS>) -> HAResult<()> {
        error!("DefaultHAService init not implemented");
    }

    fn start(&mut self) -> HAResult<()> {
        error!("DefaultHAService start not implemented");
    }

    fn shutdown(&self) {
        todo!()
    }

    async fn change_to_master(&self, master_epoch: i32) -> HAResult<bool> {
        todo!()
    }

    async fn change_to_master_when_last_role_is_master(&self, master_epoch: i32) -> HAResult<bool> {
        todo!()
    }

    async fn change_to_slave(
        &self,
        new_master_addr: &str,
        new_master_epoch: i32,
        slave_id: Option<i64>,
    ) -> HAResult<bool> {
        todo!()
    }

    async fn change_to_slave_when_master_not_change(
        &self,
        new_master_addr: &str,
        new_master_epoch: i32,
    ) -> HAResult<bool> {
        todo!()
    }

    fn update_master_address(&self, new_addr: &str) {
        todo!()
    }

    fn update_ha_master_address(&self, new_addr: &str) {
        todo!()
    }

    fn in_sync_replicas_nums(&self, master_put_where: i64) -> i32 {
        todo!()
    }

    fn get_connection_count(&self) -> &AtomicI32 {
        todo!()
    }

    fn put_request(&self, request: GroupCommitRequest) {
        todo!()
    }

    fn put_group_connection_state_request(&self, request: HAConnectionStateNotificationRequest) {
        todo!()
    }

    fn get_connection_list<CN: HAConnection>(&self) -> Vec<Arc<CN>> {
        todo!()
    }

    fn get_ha_client<CL: HAClient>(&self) -> Arc<CL> {
        todo!()
    }

    fn get_push_to_slave_max_offset(&self) -> &AtomicI64 {
        todo!()
    }

    fn get_runtime_info(&self, master_put_where: i64) -> HARuntimeInfo {
        todo!()
    }

    fn get_wait_notify_object(&self) -> Arc<WaitNotifyObject> {
        todo!()
    }

    fn is_slave_ok(&self, master_put_where: i64) -> bool {
        todo!()
    }
}
