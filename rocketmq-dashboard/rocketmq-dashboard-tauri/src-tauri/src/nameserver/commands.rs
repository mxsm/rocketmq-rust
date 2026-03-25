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

use crate::nameserver::NameServerManager;
use crate::nameserver::types::NameServerHomePageView;
use rocketmq_dashboard_common::NameServerMutationResult;
use tauri::State;

#[tauri::command]
pub async fn get_name_server_home_page(
    nameserver_manager: State<'_, NameServerManager>,
) -> Result<NameServerHomePageView, String> {
    nameserver_manager.home_page_info().await.map_err(|error| {
        log::error!("Failed to load NameServer home page: {}", error);
        error.to_string()
    })
}

#[tauri::command]
pub fn add_name_server(
    address: String,
    nameserver_manager: State<'_, NameServerManager>,
) -> Result<NameServerMutationResult, String> {
    nameserver_manager.add_name_server(&address).map_err(|error| {
        log::warn!("Failed to add NameServer `{}`: {}", address, error);
        error.to_string()
    })
}

#[tauri::command]
pub fn switch_name_server(
    address: String,
    nameserver_manager: State<'_, NameServerManager>,
) -> Result<NameServerMutationResult, String> {
    nameserver_manager.switch_name_server(&address).map_err(|error| {
        log::warn!("Failed to switch NameServer to `{}`: {}", address, error);
        error.to_string()
    })
}

#[tauri::command]
pub fn delete_name_server(
    address: String,
    nameserver_manager: State<'_, NameServerManager>,
) -> Result<NameServerMutationResult, String> {
    nameserver_manager.delete_name_server(&address).map_err(|error| {
        log::warn!("Failed to delete NameServer `{}`: {}", address, error);
        error.to_string()
    })
}

#[tauri::command]
pub fn update_vip_channel(
    enabled: bool,
    nameserver_manager: State<'_, NameServerManager>,
) -> Result<NameServerMutationResult, String> {
    nameserver_manager.update_vip_channel(enabled).map_err(|error| {
        log::warn!("Failed to update VIP channel: {}", error);
        error.to_string()
    })
}

#[tauri::command]
pub fn update_use_tls(
    enabled: bool,
    nameserver_manager: State<'_, NameServerManager>,
) -> Result<NameServerMutationResult, String> {
    nameserver_manager.update_use_tls(enabled).map_err(|error| {
        log::warn!("Failed to update TLS setting: {}", error);
        error.to_string()
    })
}
