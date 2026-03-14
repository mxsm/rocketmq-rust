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

use crate::nameserver::service::NameServerService;
use crate::nameserver::types::NameServerHomePageResponse;
use crate::nameserver::types::NameServerMutationResponse;
use crate::nameserver::types::NameServerResult;
use tauri::State;

#[tauri::command]
pub fn get_name_server_home_page(nameserver_service: State<'_, NameServerService>) -> NameServerHomePageResponse {
    match nameserver_service.get_home_page() {
        Ok(response) => response,
        Err(error) => {
            log::error!("Failed to load NameServer settings: {}", error);
            NameServerHomePageResponse {
                success: false,
                message: error
                    .to_string()
                    .replace("Validation error: ", "")
                    .replace("Database error: ", ""),
                namesrv_addr_list: Vec::new(),
                current_namesrv: None,
                use_vip_channel: true,
                use_tls: false,
            }
        }
    }
}

#[tauri::command]
pub fn add_name_server(
    address: String,
    nameserver_service: State<'_, NameServerService>,
) -> NameServerMutationResponse {
    handle_mutation("add Name Server", || nameserver_service.add_name_server(&address))
}

#[tauri::command]
pub fn switch_name_server(
    address: String,
    nameserver_service: State<'_, NameServerService>,
) -> NameServerMutationResponse {
    handle_mutation("switch Name Server", || nameserver_service.switch_name_server(&address))
}

#[tauri::command]
pub fn delete_name_server(
    address: String,
    nameserver_service: State<'_, NameServerService>,
) -> NameServerMutationResponse {
    handle_mutation("delete Name Server", || nameserver_service.delete_name_server(&address))
}

#[tauri::command]
pub fn update_vip_channel(
    enabled: bool,
    nameserver_service: State<'_, NameServerService>,
) -> NameServerMutationResponse {
    handle_mutation("update VIP channel", || nameserver_service.update_vip_channel(enabled))
}

#[tauri::command]
pub fn update_use_tls(enabled: bool, nameserver_service: State<'_, NameServerService>) -> NameServerMutationResponse {
    handle_mutation("update TLS setting", || nameserver_service.update_use_tls(enabled))
}

fn handle_mutation(
    operation: &str,
    action: impl FnOnce() -> NameServerResult<NameServerMutationResponse>,
) -> NameServerMutationResponse {
    match action() {
        Ok(response) => response,
        Err(error) => {
            log::warn!("Failed to {}: {}", operation, error);
            NameServerMutationResponse {
                success: false,
                message: error
                    .to_string()
                    .replace("Validation error: ", "")
                    .replace("Database error: ", ""),
            }
        }
    }
}
