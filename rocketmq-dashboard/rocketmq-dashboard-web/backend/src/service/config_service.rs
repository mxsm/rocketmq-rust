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
use crate::error::DashboardError;
use crate::model::AddressRequest;
use crate::model::BoolSettingRequest;
use crate::model::ConfigMutationResult;
use crate::model::DashboardConfigView;
use crate::model::NameserverListRequest;
use crate::state::AppState;

pub async fn get_config(state: &AppState) -> DashboardConfigView {
    state.dashboard_config.read().await.clone()
}

pub async fn replace_nameservers(
    state: &AppState,
    request: NameserverListRequest,
) -> Result<ConfigMutationResult, DashboardError> {
    let nameservers = normalize_address_list(&request.namesrv_addr_list, "NameServer")?;
    let current_namesrv = request
        .current_namesrv
        .as_deref()
        .map(|value| normalize_address(value, "NameServer"))
        .transpose()?
        .or_else(|| nameservers.first().cloned());

    if let Some(current) = &current_namesrv
        && !nameservers.iter().any(|item| item == current)
    {
        return Err(DashboardError::Validation(
            "Current NameServer must exist in the NameServer list".to_string(),
        ));
    }

    persist_config(state, |config| {
        config.namesrv_addr_list = nameservers;
        config.current_namesrv = current_namesrv;
    })
    .await
    .map(|config| ConfigMutationResult {
        message: "NameServer list updated".to_string(),
        config,
    })
}

pub async fn add_nameserver(state: &AppState, request: AddressRequest) -> Result<ConfigMutationResult, DashboardError> {
    let address = normalize_address(&request.address, "NameServer")?;
    persist_config(state, |config| {
        if !config.namesrv_addr_list.iter().any(|item| item == &address) {
            config.namesrv_addr_list.push(address.clone());
        }
        if config.current_namesrv.is_none() {
            config.current_namesrv = Some(address.clone());
        }
    })
    .await
    .map(|config| ConfigMutationResult {
        message: format!("NameServer `{address}` added"),
        config,
    })
}

pub async fn set_vip_channel(
    state: &AppState,
    request: BoolSettingRequest,
) -> Result<ConfigMutationResult, DashboardError> {
    persist_config(state, |config| {
        config.use_vip_channel = request.enabled;
    })
    .await
    .map(|config| ConfigMutationResult {
        message: "VIP channel setting updated".to_string(),
        config,
    })
}

pub async fn set_tls(state: &AppState, request: BoolSettingRequest) -> Result<ConfigMutationResult, DashboardError> {
    persist_config(state, |config| {
        config.use_tls = request.enabled;
    })
    .await
    .map(|config| ConfigMutationResult {
        message: "TLS setting updated".to_string(),
        config,
    })
}

pub async fn add_proxy(state: &AppState, request: AddressRequest) -> Result<ConfigMutationResult, DashboardError> {
    let address = normalize_address(&request.address, "Proxy")?;
    persist_config(state, |config| {
        if !config.proxy_addr_list.iter().any(|item| item == &address) {
            config.proxy_addr_list.push(address.clone());
        }
        if config.current_proxy_addr.is_none() {
            config.current_proxy_addr = Some(address.clone());
        }
    })
    .await
    .map(|config| ConfigMutationResult {
        message: format!("Proxy `{address}` added"),
        config,
    })
}

pub async fn switch_proxy(state: &AppState, request: AddressRequest) -> Result<ConfigMutationResult, DashboardError> {
    let address = normalize_address(&request.address, "Proxy")?;
    {
        let config = state.dashboard_config.read().await;
        if !config.proxy_addr_list.iter().any(|item| item == &address) {
            return Err(DashboardError::Validation(format!("Proxy `{address}` does not exist")));
        }
    }
    persist_config(state, |config| {
        config.current_proxy_addr = Some(address.clone());
    })
    .await
    .map(|config| ConfigMutationResult {
        message: format!("Current Proxy switched to `{address}`"),
        config,
    })
}

pub async fn delete_proxy(state: &AppState, address: &str) -> Result<ConfigMutationResult, DashboardError> {
    let address = normalize_address(address, "Proxy")?;
    persist_config(state, |config| {
        config.proxy_addr_list.retain(|item| item != &address);
        if config.current_proxy_addr.as_deref() == Some(address.as_str()) {
            config.current_proxy_addr = config.proxy_addr_list.first().cloned();
        }
    })
    .await
    .map(|config| ConfigMutationResult {
        message: format!("Proxy `{address}` deleted"),
        config,
    })
}

async fn persist_config<F>(state: &AppState, operation: F) -> Result<DashboardConfigView, DashboardError>
where
    F: FnOnce(&mut DashboardConfigView),
{
    let mut config = state.dashboard_config.write().await;
    operation(&mut config);
    state.config_store.save(&config)?;
    Ok(config.clone())
}

fn normalize_address_list(values: &[String], label: &str) -> Result<Vec<String>, DashboardError> {
    let mut normalized = Vec::new();
    for value in values {
        let address = normalize_address(value, label)?;
        if !normalized.iter().any(|item| item == &address) {
            normalized.push(address);
        }
    }
    Ok(normalized)
}

fn normalize_address(value: &str, label: &str) -> Result<String, DashboardError> {
    let trimmed = value.trim();
    let (host, port) = trimmed
        .rsplit_once(':')
        .ok_or_else(|| DashboardError::Validation(format!("{label} address must be in host:port format")))?;
    let host = host.trim().to_ascii_lowercase();
    if host.is_empty() || host.chars().any(char::is_whitespace) {
        return Err(DashboardError::Validation(format!("{label} host is invalid")));
    }
    let port_number: u16 = port
        .trim()
        .parse()
        .map_err(|_| DashboardError::Validation(format!("{label} port is invalid")))?;

    Ok(format!("{host}:{port_number}"))
}

#[cfg(test)]
mod tests {
    use super::normalize_address;

    #[test]
    fn normalize_address_trims_and_lowercases_host() {
        let address = normalize_address(" LOCALHOST : 9876 ", "NameServer").expect("valid address");

        assert_eq!(address, "localhost:9876");
    }
}
