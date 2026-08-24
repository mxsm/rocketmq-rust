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
use crate::model::AuditEvent;
use crate::model::{
    AddressRequest, BoolSettingRequest, ConfigMutationResult, DashboardConfigView, DashboardEnvironment, Endpoint,
    EndpointId, EndpointRequest, EndpointRole, EndpointType, NameserverAvailabilityStatus, NameserverAvailabilityView,
    NameserverEndpointAvailability, NameserverListRequest,
};
use crate::persistence::Revision;
use crate::persistence::error::PersistenceError;
use crate::state::AppState;
use chrono::Utc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;

const NAMESERVER_AVAILABILITY_TIMEOUT: Duration = Duration::from_millis(800);

pub async fn get_config(state: &AppState) -> Result<DashboardConfigView, DashboardError> {
    // File is normally single-process, but a post-dispatch mutation can be
    // resolved after its HTTP caller disappears. Refresh every backend so GET
    // is an immediate defensive convergence point as well as the background
    // monotonic reconciler.
    state.refresh_default_environment().await?;
    Ok(state.published().config)
}

pub async fn get_nameserver_availability(state: &AppState) -> Result<NameserverAvailabilityView, DashboardError> {
    let addresses = get_config(state).await?.namesrv_addr_list;
    let mut endpoints = Vec::with_capacity(addresses.len());
    for address in addresses {
        endpoints.push(check_nameserver_endpoint(address).await);
    }
    Ok(NameserverAvailabilityView { endpoints })
}

async fn check_nameserver_endpoint(address: String) -> NameserverEndpointAvailability {
    let status = match timeout(NAMESERVER_AVAILABILITY_TIMEOUT, TcpStream::connect(&address)).await {
        Ok(Ok(stream)) => {
            drop(stream);
            NameserverAvailabilityStatus::Available
        }
        Ok(Err(_)) | Err(_) => NameserverAvailabilityStatus::Unavailable,
    };
    NameserverEndpointAvailability {
        address,
        status,
        checked_at: Utc::now().timestamp_millis(),
    }
}

pub async fn replace_nameservers(
    state: &AppState,
    request: NameserverListRequest,
    audit: Option<AuditEvent>,
) -> Result<ConfigMutationResult, DashboardError> {
    let addresses = normalize_address_list(&request.namesrv_addr_list, "NameServer")?;
    let active = request
        .current_namesrv
        .as_deref()
        .map(|value| normalize_address(value, "NameServer"))
        .transpose()?
        .or_else(|| addresses.first().cloned());
    if let Some(active) = &active
        && !addresses.iter().any(|address| address == active)
    {
        return Err(DashboardError::Validation(
            "Current NameServer must exist in the NameServer list".to_string(),
        ));
    }
    persist_environment(state, request.expected_revision, audit, move |environment| {
        replace_endpoints(environment, EndpointType::Nameserver, addresses, active);
        Ok(())
    })
    .await
    .map(|config| ConfigMutationResult {
        message: "NameServer list updated".to_string(),
        config,
    })
}

pub async fn add_nameserver(
    state: &AppState,
    request: AddressRequest,
    audit: Option<AuditEvent>,
) -> Result<ConfigMutationResult, DashboardError> {
    let address = normalize_address(&request.address, "NameServer")?;
    persist_environment(state, request.expected_revision, audit, move |environment| {
        add_endpoint(environment, EndpointType::Nameserver, address);
        Ok(())
    })
    .await
    .map(|config| ConfigMutationResult {
        message: "NameServer added".to_string(),
        config,
    })
}

pub async fn set_vip_channel(
    state: &AppState,
    request: BoolSettingRequest,
    audit: Option<AuditEvent>,
) -> Result<ConfigMutationResult, DashboardError> {
    persist_environment(state, request.expected_revision, audit, move |environment| {
        environment.use_vip_channel = request.enabled;
        Ok(())
    })
    .await
    .map(|config| ConfigMutationResult {
        message: "VIP channel setting updated".to_string(),
        config,
    })
}

pub async fn set_tls(
    state: &AppState,
    request: BoolSettingRequest,
    audit: Option<AuditEvent>,
) -> Result<ConfigMutationResult, DashboardError> {
    persist_environment(state, request.expected_revision, audit, move |environment| {
        environment.use_tls = request.enabled;
        Ok(())
    })
    .await
    .map(|config| ConfigMutationResult {
        message: "TLS setting updated".to_string(),
        config,
    })
}

pub async fn add_proxy(
    state: &AppState,
    request: AddressRequest,
    audit: Option<AuditEvent>,
) -> Result<ConfigMutationResult, DashboardError> {
    let address = normalize_address(&request.address, "Proxy")?;
    persist_environment(state, request.expected_revision, audit, move |environment| {
        add_endpoint(environment, EndpointType::Proxy, address);
        Ok(())
    })
    .await
    .map(|config| ConfigMutationResult {
        message: "Proxy added".to_string(),
        config,
    })
}

pub async fn switch_proxy(
    state: &AppState,
    request: EndpointRequest,
    audit: Option<AuditEvent>,
) -> Result<ConfigMutationResult, DashboardError> {
    switch_endpoint(
        state,
        request,
        audit,
        EndpointType::Proxy,
        "Proxy",
        "Current Proxy switched",
    )
    .await
}

pub async fn switch_nameserver(
    state: &AppState,
    request: EndpointRequest,
    audit: Option<AuditEvent>,
) -> Result<ConfigMutationResult, DashboardError> {
    switch_endpoint(
        state,
        request,
        audit,
        EndpointType::Nameserver,
        "NameServer",
        "Current NameServer switched",
    )
    .await
}

async fn switch_endpoint(
    state: &AppState,
    request: EndpointRequest,
    audit: Option<AuditEvent>,
    endpoint_type: EndpointType,
    label: &'static str,
    message: &'static str,
) -> Result<ConfigMutationResult, DashboardError> {
    persist_environment(state, request.expected_revision, audit, move |environment| {
        let mut found = false;
        let now_ms = Utc::now().timestamp_millis();
        for endpoint in &mut environment.endpoints {
            if endpoint.endpoint_type == endpoint_type {
                endpoint.is_active = endpoint.endpoint_id == request.endpoint_id;
                endpoint.role = if endpoint.is_active {
                    EndpointRole::Primary
                } else {
                    EndpointRole::Secondary
                };
                endpoint.updated_at_ms = now_ms;
                found |= endpoint.is_active;
            }
        }
        found
            .then_some(())
            .ok_or_else(|| DashboardError::Validation(format!("{label} endpoint does not exist")))
    })
    .await
    .map(|config| ConfigMutationResult {
        message: message.to_string(),
        config,
    })
}

pub async fn delete_proxy(
    state: &AppState,
    endpoint_id: &EndpointId,
    expected_revision: Revision,
    audit: Option<AuditEvent>,
) -> Result<ConfigMutationResult, DashboardError> {
    delete_endpoint(
        state,
        endpoint_id,
        expected_revision,
        audit,
        EndpointType::Proxy,
        "Proxy",
        "Proxy deleted",
    )
    .await
}

pub async fn delete_nameserver(
    state: &AppState,
    endpoint_id: &EndpointId,
    expected_revision: Revision,
    audit: Option<AuditEvent>,
) -> Result<ConfigMutationResult, DashboardError> {
    delete_endpoint(
        state,
        endpoint_id,
        expected_revision,
        audit,
        EndpointType::Nameserver,
        "NameServer",
        "NameServer deleted",
    )
    .await
}

async fn delete_endpoint(
    state: &AppState,
    endpoint_id: &EndpointId,
    expected_revision: Revision,
    audit: Option<AuditEvent>,
    endpoint_type: EndpointType,
    label: &'static str,
    message: &'static str,
) -> Result<ConfigMutationResult, DashboardError> {
    let endpoint_id = endpoint_id.clone();
    persist_environment(state, expected_revision, audit, move |environment| {
        let previous_len = environment.endpoints.len();
        environment
            .endpoints
            .retain(|endpoint| !(endpoint.endpoint_type == endpoint_type && endpoint.endpoint_id == endpoint_id));
        if previous_len == environment.endpoints.len() {
            return Err(DashboardError::Validation(format!("{label} endpoint does not exist")));
        }
        activate_first_if_needed(environment, endpoint_type);
        Ok(())
    })
    .await
    .map(|config| ConfigMutationResult {
        message: message.to_string(),
        config,
    })
}

async fn persist_environment<F>(
    state: &AppState,
    expected_revision: Revision,
    audit: Option<AuditEvent>,
    operation: F,
) -> Result<DashboardConfigView, DashboardError>
where
    F: FnOnce(&mut DashboardEnvironment) -> Result<(), DashboardError> + Send + 'static,
{
    state
        .run_persisted_mutation("dashboard-config-candidate-persist-publish", move |state| async move {
            persist_environment_owned(&state, expected_revision, audit, operation).await
        })
        .await
}

async fn persist_environment_owned<F>(
    state: &AppState,
    expected_revision: Revision,
    audit: Option<AuditEvent>,
    operation: F,
) -> Result<DashboardConfigView, DashboardError>
where
    F: FnOnce(&mut DashboardEnvironment) -> Result<(), DashboardError>,
{
    let _mutation = state.config_mutation_lock.lock().await;
    state.refresh_default_environment().await?;
    let mut candidate = state.published().environment;
    if candidate.revision != expected_revision {
        return Err(PersistenceError::Conflict.into());
    }
    operation(&mut candidate)?;
    candidate.updated_at_ms = Utc::now().timestamp_millis();
    let persisted = match audit {
        Some(audit) => {
            state
                .persistence
                .update_environment_with_audit(expected_revision, candidate, audit)
                .await
        }
        None => state.persistence.update_environment(expected_revision, candidate).await,
    };
    let persisted = match persisted {
        Ok(environment) => environment,
        Err(PersistenceError::Conflict) => {
            // Reconcile before reporting the stable conflict so a following
            // GET and every admin consumer see the winning durable revision.
            state.refresh_default_environment().await?;
            return Err(PersistenceError::Conflict.into());
        }
        Err(error) => return Err(error.into()),
    };
    #[cfg(test)]
    let publish_completion = state.wait_before_config_publish_for_tests().await;
    state.publish_environment(persisted);
    #[cfg(test)]
    if let Some(completion) = publish_completion {
        let _ = completion.send(());
    }
    Ok(state.published().config)
}

fn replace_endpoints(
    environment: &mut DashboardEnvironment,
    endpoint_type: EndpointType,
    addresses: Vec<String>,
    active_address: Option<String>,
) {
    let now_ms = Utc::now().timestamp_millis();
    let existing = environment
        .endpoints
        .iter()
        .filter(|endpoint| endpoint.endpoint_type == endpoint_type)
        .map(|endpoint| (endpoint.address.clone(), endpoint.clone()))
        .collect::<std::collections::BTreeMap<_, _>>();
    environment
        .endpoints
        .retain(|endpoint| endpoint.endpoint_type != endpoint_type);
    for (sort_order, address) in addresses.into_iter().enumerate() {
        let mut endpoint = existing.get(&address).cloned().unwrap_or(Endpoint {
            endpoint_id: EndpointId::new(),
            endpoint_type,
            address: address.clone(),
            role: EndpointRole::Secondary,
            is_enabled: true,
            is_active: false,
            sort_order: sort_order as i32,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
        });
        endpoint.is_active = active_address.as_deref() == Some(address.as_str());
        endpoint.role = if endpoint.is_active {
            EndpointRole::Primary
        } else {
            EndpointRole::Secondary
        };
        endpoint.sort_order = sort_order as i32;
        endpoint.updated_at_ms = now_ms;
        environment.endpoints.push(endpoint);
    }
}

fn add_endpoint(environment: &mut DashboardEnvironment, endpoint_type: EndpointType, address: String) {
    if environment
        .endpoints
        .iter()
        .any(|endpoint| endpoint.endpoint_type == endpoint_type && endpoint.address == address)
    {
        return;
    }
    let now_ms = Utc::now().timestamp_millis();
    let is_active = !environment
        .endpoints
        .iter()
        .any(|endpoint| endpoint.endpoint_type == endpoint_type && endpoint.is_active);
    let sort_order = environment
        .endpoints
        .iter()
        .filter(|endpoint| endpoint.endpoint_type == endpoint_type)
        .count() as i32;
    environment.endpoints.push(Endpoint {
        endpoint_id: EndpointId::new(),
        endpoint_type,
        address,
        role: if is_active {
            EndpointRole::Primary
        } else {
            EndpointRole::Secondary
        },
        is_enabled: true,
        is_active,
        sort_order,
        created_at_ms: now_ms,
        updated_at_ms: now_ms,
    });
}

fn activate_first_if_needed(environment: &mut DashboardEnvironment, endpoint_type: EndpointType) {
    if environment
        .endpoints
        .iter()
        .any(|endpoint| endpoint.endpoint_type == endpoint_type && endpoint.is_active)
    {
        return;
    }
    if let Some(endpoint) = environment
        .endpoints
        .iter_mut()
        .filter(|endpoint| endpoint.endpoint_type == endpoint_type)
        .min_by_key(|endpoint| endpoint.sort_order)
    {
        endpoint.is_active = true;
        endpoint.role = EndpointRole::Primary;
        endpoint.updated_at_ms = Utc::now().timestamp_millis();
    }
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
