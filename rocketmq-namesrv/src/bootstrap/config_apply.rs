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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_runtime::MetadataDeadline;
use rocketmq_runtime::MetadataWriteRequest;

use crate::config::is_tls_config_key;
use crate::config::validate_namesrv_property;
use crate::config::ConfigMutability;
use crate::config::NamesrvConfigKey;

use super::parse_config_value;
use super::push_config_entry;
use super::NameServerRuntimeConfig;
use super::NameServerRuntimeInner;

const CONFIG_RESOURCE: &str = "namesrv.runtime-config";
const CONFIG_PERSIST_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ConfigApplyOutcome {
    pub(crate) desired_generation: u64,
    pub(crate) durable_generation: u64,
    pub(crate) effective_generation: u64,
    pub(crate) applied_keys: Vec<String>,
    pub(crate) restart_required_keys: Vec<String>,
}

pub(super) struct ConfigGenerationState {
    desired: Arc<NameServerRuntimeConfig>,
    desired_generation: u64,
    durable_generation: u64,
    effective_generation: u64,
}

impl ConfigGenerationState {
    pub(super) fn new(initial: Arc<NameServerRuntimeConfig>) -> Self {
        Self {
            desired: initial,
            desired_generation: 0,
            durable_generation: 0,
            effective_generation: 0,
        }
    }
}

pub(crate) async fn apply_runtime_updates(
    runtime: &NameServerRuntimeInner,
    updates: HashMap<CheetahString, CheetahString>,
) -> RocketMQResult<ConfigApplyOutcome> {
    let _transaction_guard = runtime.config_transaction_lock.lock().await;
    let classified = classify_runtime_updates(updates)?;
    let (current_desired, previous_generation, previous_effective_generation) = {
        let generations = runtime.config_generations.read();
        (
            Arc::clone(&generations.desired),
            generations.desired_generation,
            generations.effective_generation,
        )
    };

    let desired = apply_to_snapshot(&current_desired, &classified, false)?;
    let effective = apply_to_snapshot(&runtime.config_snapshot(), &classified, true)?;
    let desired_generation = previous_generation
        .checked_add(1)
        .ok_or_else(|| RocketMQError::nameserver_config_invalid("NameServer configuration generation overflow"))?;
    let desired_bytes = format_runtime_config(&desired)?.into_bytes();
    let target = desired.name_server_config.config_store_path.clone();
    let actor = runtime
        .config_metadata_io
        .as_ref()
        .ok_or_else(|| RocketMQError::storage_write_failed(&target, "metadata I/O actor is unavailable"))?
        .as_ref()
        .map_err(|error| RocketMQError::storage_write_failed(&target, error.to_string()))?;
    let deadline = MetadataDeadline::after(CONFIG_PERSIST_TIMEOUT);
    let durable_generation = actor
        .submit_durable(
            MetadataWriteRequest::new(CONFIG_RESOURCE, desired_generation, &target, desired_bytes),
            deadline,
        )
        .await
        .map_err(|error| RocketMQError::storage_write_failed(&target, error.to_string()))?
        .get();

    let mut applied_keys = classified
        .iter()
        .filter(|update| update.mutability == ConfigMutability::Live)
        .map(|update| update.key.to_string())
        .collect::<Vec<_>>();
    let mut restart_required_keys = classified
        .iter()
        .filter(|update| update.mutability == ConfigMutability::RestartRequired)
        .map(|update| update.key.to_string())
        .collect::<Vec<_>>();
    applied_keys.sort();
    restart_required_keys.sort();

    let effective_generation = if applied_keys.is_empty() {
        previous_effective_generation
    } else {
        previous_effective_generation.checked_add(1).ok_or_else(|| {
            RocketMQError::nameserver_config_invalid("NameServer effective configuration generation overflow")
        })?
    };
    if !applied_keys.is_empty() {
        runtime.config.store(effective);
    }
    {
        let mut generations = runtime.config_generations.write();
        generations.desired = desired;
        generations.desired_generation = desired_generation;
        generations.durable_generation = durable_generation;
        generations.effective_generation = effective_generation;
    }

    Ok(ConfigApplyOutcome {
        desired_generation,
        durable_generation,
        effective_generation,
        applied_keys,
        restart_required_keys,
    })
}

fn apply_to_snapshot(
    base: &NameServerRuntimeConfig,
    updates: &[ClassifiedConfigUpdate],
    live_only: bool,
) -> RocketMQResult<Arc<NameServerRuntimeConfig>> {
    let mut name_server_config = (*base.name_server_config).clone();
    let mut tokio_client_config = (*base.tokio_client_config).clone();
    let mut server_config = (*base.server_config).clone();
    let mut namesrv_updates = HashMap::new();

    for update in updates {
        if live_only && update.mutability != ConfigMutability::Live {
            continue;
        }
        let key = &update.key;
        let value = &update.value;
        match key.as_str() {
            "rocketmqHome"
            | "kvConfigPath"
            | "configStorePath"
            | "productEnvName"
            | "clusterTest"
            | "orderMessageEnable"
            | "routeFreshnessSampleInterval"
            | "namesrvTypedZoneRouteEnable"
            | "namesrvTypedZoneRouteShadow"
            | "returnOrderTopicConfigToBroker"
            | "clientRequestThreadPoolNums"
            | "defaultThreadPoolNums"
            | "clientRequestThreadPoolQueueCapacity"
            | "defaultThreadPoolQueueCapacity"
            | "scanNotActiveBrokerInterval"
            | "unRegisterBrokerQueueCapacity"
            | "supportActingMaster"
            | "enableAllTopicList"
            | "enableTopicList"
            | "notifyMinBrokerIdChanged"
            | "enableControllerInNamesrv"
            | "needWaitForService"
            | "waitSecondsForService"
            | "deleteTopicWithBrokerRegistration"
            | "allowInsecurePublicListener"
            | "configBlackList" => {
                namesrv_updates.insert(key.clone(), value.clone());
            }
            "listenPort" => server_config.listen_port = parse_config_value(key, value)?,
            "bindAddress" => server_config.bind_address = value.to_string(),
            key if is_tls_config_key(key) => {
                server_config.tls_config.apply_java_property(key, value.as_str());
            }
            "connectTimeoutMillis" => {
                let timeout_millis = parse_config_value::<u64>(key, value)?;
                tokio_client_config.connect.timeout = Duration::from_millis(timeout_millis);
            }
            "channelNotActiveInterval" => {
                let interval_millis = parse_config_value::<u64>(key, value)?;
                tokio_client_config.maintenance.idle_scan_interval =
                    (interval_millis > 0).then(|| Duration::from_millis(interval_millis));
            }
            _ => {
                return Err(RocketMQError::nameserver_config_invalid(format!(
                    "unknown configuration key '{key}'"
                )));
            }
        }
    }

    if !namesrv_updates.is_empty() {
        name_server_config.update(namesrv_updates)?;
    }
    Ok(Arc::new(NameServerRuntimeConfig {
        name_server_config: Arc::new(name_server_config),
        tokio_client_config: Arc::new(tokio_client_config),
        server_config: Arc::new(server_config),
        #[cfg(feature = "embedded-controller")]
        controller_config: base.controller_config.clone(),
    }))
}

fn format_runtime_config(config_snapshot: &NameServerRuntimeConfig) -> RocketMQResult<String> {
    let name_server_config = &config_snapshot.name_server_config;
    let server_config = &config_snapshot.server_config;
    let tokio_client_config = &config_snapshot.tokio_client_config;
    let mut entries = Vec::with_capacity(48);

    push_config_entry(&mut entries, "rocketmqHome", &name_server_config.rocketmq_home);
    push_config_entry(&mut entries, "kvConfigPath", &name_server_config.kv_config_path);
    push_config_entry(&mut entries, "configStorePath", &name_server_config.config_store_path);
    push_config_entry(&mut entries, "productEnvName", &name_server_config.product_env_name);
    push_config_entry(&mut entries, "clusterTest", name_server_config.cluster_test);
    push_config_entry(
        &mut entries,
        "orderMessageEnable",
        name_server_config.order_message_enable,
    );
    push_config_entry(
        &mut entries,
        "routeFreshnessSampleInterval",
        name_server_config.route_freshness_sample_interval,
    );
    push_config_entry(
        &mut entries,
        "namesrvTypedZoneRouteEnable",
        name_server_config.namesrv_typed_zone_route_enable,
    );
    push_config_entry(
        &mut entries,
        "namesrvTypedZoneRouteShadow",
        name_server_config.namesrv_typed_zone_route_shadow,
    );
    push_config_entry(
        &mut entries,
        "returnOrderTopicConfigToBroker",
        name_server_config.return_order_topic_config_to_broker,
    );
    push_config_entry(
        &mut entries,
        "clientRequestThreadPoolNums",
        name_server_config.client_request_thread_pool_nums,
    );
    push_config_entry(
        &mut entries,
        "defaultThreadPoolNums",
        name_server_config.default_thread_pool_nums,
    );
    push_config_entry(
        &mut entries,
        "clientRequestThreadPoolQueueCapacity",
        name_server_config.client_request_thread_pool_queue_capacity,
    );
    push_config_entry(
        &mut entries,
        "defaultThreadPoolQueueCapacity",
        name_server_config.default_thread_pool_queue_capacity,
    );
    push_config_entry(
        &mut entries,
        "scanNotActiveBrokerInterval",
        name_server_config.scan_not_active_broker_interval,
    );
    push_config_entry(
        &mut entries,
        "unRegisterBrokerQueueCapacity",
        name_server_config.unregister_broker_queue_capacity,
    );
    push_config_entry(
        &mut entries,
        "supportActingMaster",
        name_server_config.support_acting_master,
    );
    push_config_entry(
        &mut entries,
        "enableAllTopicList",
        name_server_config.enable_all_topic_list,
    );
    push_config_entry(&mut entries, "enableTopicList", name_server_config.enable_topic_list);
    push_config_entry(
        &mut entries,
        "notifyMinBrokerIdChanged",
        name_server_config.notify_min_broker_id_changed,
    );
    push_config_entry(
        &mut entries,
        "enableControllerInNamesrv",
        name_server_config.enable_controller_in_namesrv,
    );
    push_config_entry(
        &mut entries,
        "needWaitForService",
        name_server_config.need_wait_for_service,
    );
    push_config_entry(
        &mut entries,
        "waitSecondsForService",
        name_server_config.wait_seconds_for_service,
    );
    push_config_entry(
        &mut entries,
        "deleteTopicWithBrokerRegistration",
        name_server_config.delete_topic_with_broker_registration,
    );
    push_config_entry(
        &mut entries,
        "allowInsecurePublicListener",
        name_server_config.allow_insecure_public_listener,
    );
    push_config_entry(&mut entries, "configBlackList", &name_server_config.config_black_list);
    push_config_entry(&mut entries, "listenPort", server_config.listen_port);
    push_config_entry(&mut entries, "bindAddress", &server_config.bind_address);
    for (key, value) in server_config.tls_config.java_property_entries() {
        push_config_entry(&mut entries, key, value);
    }
    push_config_entry(
        &mut entries,
        "connectTimeoutMillis",
        tokio_client_config.connect.timeout.as_millis(),
    );
    push_config_entry(
        &mut entries,
        "channelNotActiveInterval",
        tokio_client_config
            .maintenance
            .idle_scan_interval
            .map_or(0, |interval| interval.as_millis()),
    );
    entries.sort_by_key(|(key, _)| *key);

    let mut output = String::new();
    for (index, (key, value)) in entries.into_iter().enumerate() {
        if index > 0 {
            output.push('\n');
        }
        output.push_str(key);
        output.push('=');
        output.push_str(&value);
    }
    output.push('\n');
    Ok(output)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ClassifiedConfigUpdate {
    pub(crate) key: CheetahString,
    pub(crate) value: CheetahString,
    pub(crate) mutability: ConfigMutability,
}

pub(crate) fn classify_runtime_updates(
    updates: impl IntoIterator<Item = (CheetahString, CheetahString)>,
) -> RocketMQResult<Vec<ClassifiedConfigUpdate>> {
    updates
        .into_iter()
        .map(|(key, value)| {
            let mutability = classify_runtime_update(&key, &value)?;
            if mutability == ConfigMutability::Unsupported {
                return Err(RocketMQError::nameserver_config_invalid(format!(
                    "configuration key '{key}' cannot be changed remotely"
                )));
            }
            Ok(ClassifiedConfigUpdate { key, value, mutability })
        })
        .collect()
}

fn classify_runtime_update(key: &str, value: &str) -> RocketMQResult<ConfigMutability> {
    if let Some(namesrv_key) = NamesrvConfigKey::from_java_name(key) {
        validate_namesrv_property(namesrv_key, value)?;
        return Ok(namesrv_key.mutability());
    }

    match key {
        "listenPort" => {
            parse_bounded_u64(key, value, 1, u16::MAX as u64)?;
            Ok(ConfigMutability::RestartRequired)
        }
        "bindAddress" => {
            if value.trim().is_empty() {
                return Err(invalid_value(key, "must not be empty"));
            }
            Ok(ConfigMutability::RestartRequired)
        }
        "connectTimeoutMillis" => {
            parse_bounded_u64(key, value, 1, 3_600_000)?;
            Ok(ConfigMutability::RestartRequired)
        }
        "channelNotActiveInterval" => {
            parse_bounded_u64(key, value, 0, 86_400_000)?;
            Ok(ConfigMutability::RestartRequired)
        }
        key if is_tls_config_key(key) => {
            if value.trim().is_empty() && key != "tls.ciphers" && key != "tls.protocols" {
                return Err(invalid_value(key, "must not be empty"));
            }
            Ok(ConfigMutability::RestartRequired)
        }
        _ => Err(RocketMQError::nameserver_config_invalid(format!(
            "unknown configuration key '{key}'"
        ))),
    }
}

fn parse_bounded_u64(key: &str, value: &str, minimum: u64, maximum: u64) -> RocketMQResult<u64> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| invalid_value(key, "expected a non-negative integer"))?;
    if !(minimum..=maximum).contains(&parsed) {
        return Err(invalid_value(key, &format!("must be between {minimum} and {maximum}")));
    }
    Ok(parsed)
}

fn invalid_value(key: &str, reason: &str) -> RocketMQError {
    RocketMQError::nameserver_config_invalid(format!("invalid value for '{key}': {reason}"))
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::classify_runtime_updates;
    use super::ConfigMutability;

    #[test]
    fn classifies_mixed_live_and_restart_required_updates() {
        let classified = classify_runtime_updates([
            (
                CheetahString::from_static_str("enableTopicList"),
                CheetahString::from_static_str("false"),
            ),
            (
                CheetahString::from_static_str("listenPort"),
                CheetahString::from_static_str("19876"),
            ),
        ])
        .expect("valid updates should classify");

        assert_eq!(classified[0].mutability, ConfigMutability::Live);
        assert_eq!(classified[1].mutability, ConfigMutability::RestartRequired);
    }

    #[test]
    fn rejects_unknown_and_out_of_domain_updates() {
        for (key, value) in [
            ("unknownNameServerKey", "1"),
            ("unRegisterBrokerQueueCapacity", "0"),
            ("unRegisterBrokerQueueCapacity", "-1"),
            ("defaultThreadPoolNums", "0"),
            ("defaultThreadPoolQueueCapacity", "10000001"),
            ("scanNotActiveBrokerInterval", "0"),
            ("routeFreshnessSampleInterval", "0"),
            ("routeFreshnessSampleInterval", "1000001"),
            ("connectTimeoutMillis", "-1"),
        ] {
            let result = classify_runtime_updates([(CheetahString::from(key), CheetahString::from(value))]);
            assert!(result.is_err(), "{key}={value} must be rejected");
        }
    }
}
