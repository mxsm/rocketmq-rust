// Copyright 2026 The RocketMQ Rust Authors
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

use crate::proxy::ProxyManager;
use rocketmq_dashboard_common::ProxyConfigSnapshot;
use rocketmq_dashboard_common::ProxyMutationResult;
use tauri::State;

#[tauri::command]
pub async fn get_proxy_home_page(
    session_id: String,
    proxy_manager: State<'_, ProxyManager>,
    session_state: State<'_, SessionState>,
) -> CommandResult<ProxyConfigSnapshot> {
    authorize_command(&session_id, &session_state).await?;
    proxy_manager.home_page_info().map_err(Into::into)
}

#[tauri::command]
pub async fn add_proxy_addr(
    session_id: String,
    address: String,
    proxy_manager: State<'_, ProxyManager>,
    session_state: State<'_, SessionState>,
) -> CommandResult<ProxyMutationResult> {
    authorize_command(&session_id, &session_state).await?;
    proxy_manager.add_proxy_addr(&address).map_err(Into::into)
}

#[tauri::command]
pub async fn switch_proxy_addr(
    session_id: String,
    address: String,
    proxy_manager: State<'_, ProxyManager>,
    session_state: State<'_, SessionState>,
) -> CommandResult<ProxyMutationResult> {
    authorize_command(&session_id, &session_state).await?;
    proxy_manager.switch_proxy_addr(&address).map_err(Into::into)
}

#[tauri::command]
pub async fn delete_proxy_addr(
    session_id: String,
    address: String,
    proxy_manager: State<'_, ProxyManager>,
    session_state: State<'_, SessionState>,
) -> CommandResult<ProxyMutationResult> {
    authorize_command(&session_id, &session_state).await?;
    proxy_manager.delete_proxy_addr(&address).map_err(Into::into)
}
use crate::auth::SessionState;
use crate::error::CommandResult;
use crate::error::authorize_command;
