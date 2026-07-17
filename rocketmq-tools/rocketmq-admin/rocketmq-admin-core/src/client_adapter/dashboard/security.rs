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

use super::*;

pub(super) async fn resolve_acl_targets(
    admin: &rocketmq_client_rust::DefaultMQAdminExt,
    selector: &dashboard::TargetSelector,
) -> Result<Vec<AclTarget>, AdminError> {
    if let Some(broker_name) = normalized_selector_value(selector.broker_name.as_deref()) {
        return Ok(vec![AclTarget {
            broker_name: broker_name.to_string(),
            broker_addr: resolve_broker_address(admin, broker_name).await?,
        }]);
    }
    let cluster_info = admin
        .examine_broker_cluster_info()
        .await
        .map_err(|error| backend_error("examine_broker_cluster_info", error))?;
    let broker_table = cluster_info
        .broker_addr_table
        .as_ref()
        .ok_or_else(|| AdminError::backend("resolve_acl_targets", "missing broker address data"))?;
    let allowed = selector
        .cluster_name
        .as_deref()
        .and_then(|value| normalized_selector_value(Some(value)))
        .map(|cluster| {
            cluster_info
                .cluster_addr_table
                .as_ref()
                .and_then(|table| table.get(cluster))
                .cloned()
                .ok_or_else(|| {
                    AdminError::invalid_argument("clusterName", format!("cluster `{cluster}` was not found"))
                })
        })
        .transpose()?;
    let mut targets = Vec::new();
    for (name, broker) in broker_table {
        if allowed
            .as_ref()
            .is_some_and(|brokers| !brokers.iter().any(|broker_name| broker_name.as_str() == name.as_str()))
        {
            continue;
        }
        targets.extend(broker.broker_addrs().values().map(|address| AclTarget {
            broker_name: name.to_string(),
            broker_addr: address.to_string(),
        }));
    }
    targets.sort_by(|left, right| {
        left.broker_name
            .cmp(&right.broker_name)
            .then(left.broker_addr.cmp(&right.broker_addr))
    });
    if targets.is_empty() {
        Err(AdminError::not_found("ACL broker target", "matching selector"))
    } else {
        Ok(targets)
    }
}

fn normalized_selector_value(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

pub(super) fn map_acl_user(target: &AclTarget, user: &UserInfo) -> dashboard::DashboardAclUser {
    dashboard::DashboardAclUser {
        broker_name: target.broker_name.clone(),
        broker_addr: target.broker_addr.clone(),
        username: user.username.as_ref().map(ToString::to_string).unwrap_or_default(),
        user_type: user.user_type.as_ref().map(ToString::to_string),
        user_status: user.user_status.as_ref().map(ToString::to_string),
    }
}

pub(super) fn map_acl_policy(target: &AclTarget, acl: &AclInfo) -> Vec<dashboard::DashboardAclPolicy> {
    acl.policies
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|policy| dashboard::DashboardAclPolicy {
            broker_name: target.broker_name.clone(),
            broker_addr: target.broker_addr.clone(),
            subject: acl.subject.as_ref().map(ToString::to_string),
            policy_type: policy.policy_type.as_ref().map(ToString::to_string),
            entries: policy
                .entries
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(map_acl_policy_entry)
                .collect(),
        })
        .collect()
}

pub(super) fn map_acl_policy_entry(entry: &PolicyEntryInfo) -> dashboard::DashboardAclPolicyEntry {
    dashboard::DashboardAclPolicyEntry {
        resource: entry.resource.as_ref().map(ToString::to_string),
        actions: entry
            .actions
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(|value| value.to_string())
            .collect(),
        source_ips: entry
            .source_ips
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(|value| value.to_string())
            .collect(),
        decision: entry.decision.as_ref().map(ToString::to_string),
    }
}

pub(super) fn build_acl_info(request: &dashboard::DashboardAclPolicyMutationRequest) -> Result<AclInfo, AdminError> {
    if request.subject.trim().is_empty() {
        return Err(AdminError::invalid_argument("subject", "must not be empty"));
    }
    let policies = request
        .policies
        .iter()
        .map(|policy| PolicyInfo {
            policy_type: Some(policy.policy_type.as_str().into()),
            entries: Some(
                policy
                    .entries
                    .iter()
                    .flat_map(|entry| {
                        entry.resources.iter().map(|resource| PolicyEntryInfo {
                            resource: Some(resource.as_str().into()),
                            actions: Some(
                                entry
                                    .actions
                                    .iter()
                                    .map(|value| CheetahString::from(value.as_str()))
                                    .collect(),
                            ),
                            source_ips: Some(
                                entry
                                    .source_ips
                                    .iter()
                                    .map(|value| CheetahString::from(value.as_str()))
                                    .collect(),
                            ),
                            decision: Some(entry.decision.as_str().into()),
                        })
                    })
                    .collect(),
            ),
        })
        .collect();
    Ok(AclInfo {
        subject: Some(request.subject.as_str().into()),
        policies: Some(policies),
    })
}

#[cfg(test)]
mod tests {
    use super::normalized_selector_value;

    #[test]
    fn selector_values_are_trimmed_before_lookup() {
        assert_eq!(normalized_selector_value(Some(" broker-a ")), Some("broker-a"));
        assert_eq!(normalized_selector_value(Some("   ")), None);
        assert_eq!(normalized_selector_value(None), None);
    }
}
