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

use super::types::*;
use crate::error::{DashboardError, DashboardResult};
use rocketmq_dashboard_common::{normalize_nameserver_address, normalize_proxy_address};
use rusqlite::{Connection, params};
use uuid::Uuid;

pub(super) fn ensure_identities(connection: &Connection) -> DashboardResult<()> {
    let nameserver = crate::nameserver::db::load_snapshot_from_connection(connection)?;
    let proxy = crate::proxy::db::load_snapshot_from_connection(connection)?;
    for (kind, addresses) in [
        (EndpointKind::NameServer, nameserver.namesrv_addr_list),
        (EndpointKind::Proxy, proxy.proxy_addr_list),
    ] {
        for address in addresses {
            connection.execute("INSERT INTO endpoint_identity(kind, address, endpoint_id, environment_id) VALUES (?1, ?2, ?3, ?4) ON CONFLICT(kind, address) DO NOTHING", params![kind.as_str(), address, Uuid::new_v4().to_string(), if kind == EndpointKind::NameServer { Some(Uuid::new_v4().to_string()) } else { None }])?;
        }
    }
    Ok(())
}

pub(super) fn load(connection: &Connection) -> DashboardResult<ConnectionSettingsView> {
    let revision = connection.query_row("SELECT revision FROM connection_metadata WHERE id = 1", [], |row| {
        row.get(0)
    })?;
    let nameserver = crate::nameserver::db::load_snapshot_from_connection(connection)?;
    let proxy = crate::proxy::db::load_snapshot_from_connection(connection)?;
    let mut endpoints = Vec::new();
    for (kind, addresses) in [
        (EndpointKind::NameServer, &nameserver.namesrv_addr_list),
        (EndpointKind::Proxy, &proxy.proxy_addr_list),
    ] {
        for address in addresses {
            let item = connection.query_row(
                "SELECT endpoint_id, environment_id FROM endpoint_identity WHERE kind = ?1 AND address = ?2",
                params![kind.as_str(), address],
                |row| {
                    Ok(EndpointView {
                        endpoint_id: row.get(0)?,
                        environment_id: row.get(1)?,
                        kind,
                        address: address.clone(),
                    })
                },
            )?;
            endpoints.push(item);
        }
    }
    let selected = endpoints.iter().find(|endpoint| {
        endpoint.kind == EndpointKind::NameServer && Some(&endpoint.address) == nameserver.current_namesrv.as_ref()
    });
    let current_nameserver_id = selected.map(|endpoint| endpoint.endpoint_id.clone());
    let environment_id = selected.and_then(|endpoint| endpoint.environment_id.clone());
    let current_proxy_id = endpoints
        .iter()
        .find(|endpoint| {
            endpoint.kind == EndpointKind::Proxy && Some(&endpoint.address) == proxy.current_proxy_addr.as_ref()
        })
        .map(|endpoint| endpoint.endpoint_id.clone());
    Ok(ConnectionSettingsView {
        credentials_configured: false,
        revision,
        endpoints,
        current_nameserver_id,
        current_proxy_id,
        environment_id,
        nameserver,
        proxy,
    })
}

fn normalize(kind: EndpointKind, address: &str) -> DashboardResult<String> {
    Ok(match kind {
        EndpointKind::NameServer => normalize_nameserver_address(address)?,
        EndpointKind::Proxy => normalize_proxy_address(address)?,
    })
}

pub(super) fn apply(view: &mut ConnectionSettingsView, change: ConnectionChange) -> DashboardResult<()> {
    match change {
        ConnectionChange::Vip(enabled) => view.nameserver.use_vip_channel = enabled,
        ConnectionChange::Tls(enabled) => view.nameserver.use_tls = enabled,
        ConnectionChange::Replace {
            addresses,
            current_endpoint,
        } => {
            let mut normalized = Vec::new();
            if addresses.len() > 256 {
                return Err(DashboardError::Validation("too many endpoints".into()));
            }
            for address in addresses {
                if address.trim().is_empty() {
                    continue;
                }
                let address = normalize(EndpointKind::NameServer, &address)?;
                if !normalized.contains(&address) {
                    normalized.push(address);
                }
            }
            let current = current_endpoint
                .map(|selection| match selection {
                    NameServerSelection::ExistingId(id) => view
                        .endpoints
                        .iter()
                        .find(|endpoint| endpoint.kind == EndpointKind::NameServer && endpoint.endpoint_id == id)
                        .map(|endpoint| endpoint.address.clone())
                        .ok_or_else(|| DashboardError::Validation("unknown current endpoint".into())),
                    NameServerSelection::Address(address) => normalize(EndpointKind::NameServer, &address),
                })
                .transpose()?;
            if current.as_ref().is_some_and(|address| !normalized.contains(address))
                || (current.is_none() && !normalized.is_empty())
            {
                return Err(DashboardError::Validation(
                    "current endpoint must belong to the replacement list".into(),
                ));
            }
            view.nameserver.namesrv_addr_list = normalized;
            view.nameserver.current_namesrv = current;
        }
        change => {
            let (kind, address) = match &change {
                ConnectionChange::Add { kind, address }
                | ConnectionChange::Switch { kind, address }
                | ConnectionChange::Delete { kind, address } => (*kind, normalize(*kind, address)?),
                ConnectionChange::Vip(_) | ConnectionChange::Tls(_) | ConnectionChange::Replace { .. } => {
                    return Err(DashboardError::Internal("invalid connection operation"));
                }
            };
            let (addresses, current) = match kind {
                EndpointKind::NameServer => (
                    &mut view.nameserver.namesrv_addr_list,
                    &mut view.nameserver.current_namesrv,
                ),
                EndpointKind::Proxy => (&mut view.proxy.proxy_addr_list, &mut view.proxy.current_proxy_addr),
            };
            match change {
                ConnectionChange::Add { .. } => {
                    if addresses.contains(&address) {
                        return Err(DashboardError::Validation("endpoint already exists".into()));
                    }
                    if addresses.len() >= 256 {
                        return Err(DashboardError::Validation("too many endpoints".into()));
                    }
                    addresses.push(address.clone());
                    if current.is_none() {
                        *current = Some(address);
                    }
                }
                ConnectionChange::Switch { .. } => {
                    if !addresses.contains(&address) {
                        return Err(DashboardError::Validation("endpoint does not exist".into()));
                    }
                    *current = Some(address);
                }
                ConnectionChange::Delete { .. } => {
                    if !addresses.contains(&address) {
                        return Err(DashboardError::Validation("endpoint does not exist".into()));
                    }
                    addresses.retain(|item| item != &address);
                    if current.as_ref() == Some(&address) {
                        *current = addresses.first().cloned();
                    }
                }
                ConnectionChange::Vip(_) | ConnectionChange::Tls(_) | ConnectionChange::Replace { .. } => {
                    return Err(DashboardError::Internal("invalid connection operation"));
                }
            }
        }
    }
    Ok(())
}
