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

//! Read-client implementation of the address-free, secret-free ACL user observation.

use std::collections::BTreeSet;

use cheetah_string::CheetahString;
use rocketmq_client_rust::AclUserRead;
use rocketmq_client_rust::DefaultMQAdminExt;
use rocketmq_client_rust::MQAdminAclReadExt;
use rocketmq_client_rust::MQAdminReadExt;
use rocketmq_error::Error as CanonicalError;
use rocketmq_model::common::mix_all;
use rocketmq_protocol::protocol::body::broker_body::cluster_info::ClusterInfo;

use crate::core::broker::is_valid_remoting_endpoint;
use crate::core::security::AclUserObservation;
use crate::core::security::AclUserQueryRequest;
use crate::core::security::AclUserQueryResult;
use crate::core::security::MAX_ACL_USER_BROKERS;
use crate::core::security::MAX_ACL_USER_ROWS;
use crate::core::AdminError;
use crate::core::AdminResult;
use crate::topic_observation::promote_to_admin_error;

const OPERATION: &str = "query_acl_users";
const MAX_USERNAME_BYTES: usize = 255;
const MAX_LABEL_BYTES: usize = 32;

#[allow(async_fn_in_trait)]
trait AclUserSource: Send {
    async fn cluster_info(&self) -> Result<ClusterInfo, CanonicalError>;
    async fn users(&self, broker_addr: CheetahString) -> Result<Vec<AclUserRead>, CanonicalError>;
}

impl AclUserSource for DefaultMQAdminExt {
    async fn cluster_info(&self) -> Result<ClusterInfo, CanonicalError> {
        MQAdminReadExt::examine_broker_cluster_info(self)
            .await
            .map_err(crate::IntoCanonicalError::into_canonical_error)
    }

    async fn users(&self, broker_addr: CheetahString) -> Result<Vec<AclUserRead>, CanonicalError> {
        MQAdminAclReadExt::acl_users_at(self, broker_addr)
            .await
            .map_err(crate::IntoCanonicalError::into_canonical_error)
    }
}

pub(crate) async fn query_acl_users(
    admin: &DefaultMQAdminExt,
    request: &AclUserQueryRequest,
) -> AdminResult<AclUserQueryResult> {
    query_acl_users_from(admin, request).await
}

/// Lists the users of every master of the selected cluster.
///
/// Each Broker keeps its own users, so a user can exist on one Broker and not on another. The
/// rows therefore name the Broker they come from, and a Broker that does not answer is named
/// instead of being left out silently.
async fn query_acl_users_from<S: AclUserSource>(
    source: &S,
    request: &AclUserQueryRequest,
) -> AdminResult<AclUserQueryResult> {
    let request = AclUserQueryRequest::try_new(&request.cluster)?;
    let cluster_info = source
        .cluster_info()
        .await
        .map_err(|source| promote_to_admin_error("examine_broker_cluster_info", source))?;
    let ClusterMasters {
        targets,
        mut unanswered,
    } = cluster_masters(&cluster_info, &request.cluster)?;
    let queried_broker_count = targets.len().saturating_add(unanswered.len());

    let mut users = Vec::new();
    let mut answered = 0usize;
    for (broker_name, broker_addr) in targets {
        match source.users(broker_addr).await {
            Ok(listed) => {
                answered += 1;
                users.extend(listed.into_iter().filter_map(|user| observation(&broker_name, user)));
            }
            Err(_) => unanswered.push(broker_name),
        }
    }
    unanswered.sort();
    if answered == 0 && !unanswered.is_empty() {
        return Err(AdminError::unavailable(OPERATION, "No Broker of the cluster answered"));
    }

    users.sort_by(|left, right| (&left.username, &left.broker_name).cmp(&(&right.username, &right.broker_name)));
    users.dedup();
    let user_count = users
        .iter()
        .map(|user| user.username.as_str())
        .collect::<BTreeSet<_>>()
        .len();
    let truncated = users.len() > MAX_ACL_USER_ROWS;
    users.truncate(MAX_ACL_USER_ROWS);
    Ok(AclUserQueryResult {
        users,
        user_count,
        truncated,
        queried_broker_count,
        unanswered_brokers: unanswered,
    })
}

/// The Brokers of a cluster, split by whether there is a master to ask.
struct ClusterMasters {
    /// The name and the master address of every Broker that has one.
    targets: Vec<(String, CheetahString)>,
    /// The names of the Brokers that have none.
    unanswered: Vec<String>,
}

/// Returns the master of every Broker of the cluster, and the Brokers that have none to ask.
fn cluster_masters(cluster_info: &ClusterInfo, cluster: &str) -> AdminResult<ClusterMasters> {
    let broker_names = cluster_info
        .cluster_addr_table
        .as_ref()
        .and_then(|table| table.get(cluster))
        .ok_or_else(|| AdminError::cluster_not_found(cluster))?;
    if broker_names.len() > MAX_ACL_USER_BROKERS {
        return Err(AdminError::target_limit(OPERATION, "The cluster has too many Brokers"));
    }
    let mut names = broker_names.iter().map(ToString::to_string).collect::<Vec<_>>();
    names.sort();

    let mut targets = Vec::new();
    let mut unanswered = Vec::new();
    for broker_name in names {
        let master = cluster_info
            .broker_addr_table
            .as_ref()
            .and_then(|table| table.get(broker_name.as_str()))
            .filter(|broker| broker.cluster() == cluster)
            .and_then(|broker| broker.broker_addrs().get(&mix_all::MASTER_ID))
            .filter(|address| is_valid_remoting_endpoint(address.as_str()));
        match master {
            Some(address) => targets.push((broker_name, address.clone())),
            None => unanswered.push(broker_name),
        }
    }
    Ok(ClusterMasters { targets, unanswered })
}

/// Keeps a listed user whose name is one a Broker would accept, with its type and status when
/// they are short labels.
fn observation(broker_name: &str, user: AclUserRead) -> Option<AclUserObservation> {
    let username = user.username;
    if username.is_empty() || username.len() > MAX_USERNAME_BYTES || username.chars().any(char::is_control) {
        return None;
    }
    Some(AclUserObservation {
        broker_name: broker_name.to_owned(),
        username,
        user_type: user.user_type.as_deref().and_then(label),
        user_status: user.user_status.as_deref().and_then(label),
    })
}

fn label(value: &str) -> Option<String> {
    (!value.is_empty()
        && value.len() <= MAX_LABEL_BYTES
        && value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-')))
    .then(|| value.to_owned())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::sync::Mutex;

    use rocketmq_protocol::protocol::route::route_data_view::BrokerData;

    use super::*;
    use crate::core::AdminFailure;

    const ADDRESS_A: &str = "127.0.0.1:10911";
    const ADDRESS_B: &str = "127.0.0.2:10911";
    const REPLICA_B: &str = "127.0.0.3:10911";

    struct FakeSource {
        cluster_info: ClusterInfo,
        /// The users each master lists; an address that is not listed fails.
        users: HashMap<&'static str, Vec<AclUserRead>>,
        asked: Mutex<Vec<String>>,
    }

    impl AclUserSource for FakeSource {
        async fn cluster_info(&self) -> Result<ClusterInfo, CanonicalError> {
            Ok(self.cluster_info.clone())
        }

        async fn users(&self, broker_addr: CheetahString) -> Result<Vec<AclUserRead>, CanonicalError> {
            self.asked.lock().unwrap().push(broker_addr.to_string());
            self.users.get(broker_addr.as_str()).cloned().ok_or_else(|| {
                CanonicalError::new(&rocketmq_error::PROTOCOL_RESPONSE_FAILED).with_context(
                    rocketmq_error::ErrorContext::new()
                        .with_text(rocketmq_error::fields::OPERATION_DIAGNOSTIC, "acl_users_test")
                        .with_secret_presence(rocketmq_error::fields::REASON_PRESENT),
                )
            })
        }
    }

    fn user(username: &str, user_type: &str, user_status: &str) -> AclUserRead {
        AclUserRead {
            username: username.to_owned(),
            user_type: Some(user_type.to_owned()),
            user_status: Some(user_status.to_owned()),
        }
    }

    /// `cluster-a` with two Brokers; the second one also has a replica, which is never asked.
    fn source(users: &[(&'static str, Vec<AclUserRead>)]) -> FakeSource {
        let brokers = [
            ("broker-a", vec![(mix_all::MASTER_ID, ADDRESS_A)]),
            ("broker-b", vec![(mix_all::MASTER_ID, ADDRESS_B), (1, REPLICA_B)]),
        ]
        .map(|(name, instances)| {
            BrokerData::new(
                "cluster-a".into(),
                name.into(),
                instances
                    .into_iter()
                    .map(|(id, address)| (id, address.into()))
                    .collect(),
                None,
            )
        });
        FakeSource {
            cluster_info: ClusterInfo::new(
                Some(
                    brokers
                        .iter()
                        .map(|broker| (broker.broker_name().clone(), broker.clone()))
                        .collect(),
                ),
                Some(HashMap::from([(
                    CheetahString::from("cluster-a"),
                    brokers
                        .iter()
                        .map(|broker| broker.broker_name().clone())
                        .collect::<HashSet<_>>(),
                )])),
            ),
            users: users.iter().cloned().collect(),
            asked: Mutex::default(),
        }
    }

    fn request(cluster: &str) -> AclUserQueryRequest {
        AclUserQueryRequest::try_new(cluster).unwrap()
    }

    #[tokio::test]
    async fn every_master_is_asked_and_a_row_names_the_broker_that_holds_the_user() {
        let source = source(&[
            (
                ADDRESS_A,
                vec![user("ops", "Super", "enable"), user("reader", "Normal", "enable")],
            ),
            // The second Broker does not know `reader`, and holds `ops` disabled.
            (ADDRESS_B, vec![user("ops", "Super", "disable")]),
        ]);

        let result = query_acl_users_from(&source, &request("cluster-a")).await.unwrap();

        assert_eq!(*source.asked.lock().unwrap(), [ADDRESS_A, ADDRESS_B]);
        assert_eq!(
            result
                .users
                .iter()
                .map(|user| (
                    user.username.as_str(),
                    user.broker_name.as_str(),
                    user.user_status.as_deref()
                ))
                .collect::<Vec<_>>(),
            [
                ("ops", "broker-a", Some("enable")),
                ("ops", "broker-b", Some("disable")),
                ("reader", "broker-a", Some("enable")),
            ]
        );
        assert_eq!(result.user_count, 2);
        assert_eq!(result.queried_broker_count, 2);
        assert!(!result.truncated && result.unanswered_brokers.is_empty());
        let published = serde_json::to_string(&result).unwrap();
        for private in [ADDRESS_A, ADDRESS_B, REPLICA_B, "password"] {
            assert!(!published.contains(private), "the result holds {private}");
        }
    }

    #[tokio::test]
    async fn a_broker_that_does_not_answer_is_named_and_no_answer_at_all_is_an_error() {
        let partial = source(&[(ADDRESS_A, vec![user("ops", "Super", "enable")])]);
        let result = query_acl_users_from(&partial, &request("cluster-a")).await.unwrap();
        assert_eq!(result.users.len(), 1);
        assert_eq!(result.unanswered_brokers, ["broker-b"]);

        let down = source(&[]);
        let error = query_acl_users_from(&down, &request("cluster-a")).await.unwrap_err();
        assert_eq!(error.failure(), AdminFailure::OperationFailed);

        let error = query_acl_users_from(&source(&[]), &request("cluster-z"))
            .await
            .unwrap_err();
        assert_eq!(error.failure(), AdminFailure::NotFound);
        assert_eq!(error.resource(), Some("cluster"));
    }

    #[tokio::test]
    async fn rows_are_bounded_and_a_value_no_broker_would_report_is_not_passed_on() {
        let many = (0..=MAX_ACL_USER_ROWS)
            .map(|index| user(&format!("user-{index:04}"), "Normal", "enable"))
            .collect::<Vec<_>>();
        let source = source(&[
            (ADDRESS_A, many),
            (
                ADDRESS_B,
                vec![
                    user("bad\u{0}name", "Normal", "enable"),
                    user("ops", "not a label", &"x".repeat(MAX_LABEL_BYTES + 1)),
                ],
            ),
        ]);

        let result = query_acl_users_from(&source, &request("cluster-a")).await.unwrap();

        assert_eq!(result.users.len(), MAX_ACL_USER_ROWS);
        assert!(result.truncated);
        // The count covers every user, also those the row cap left out.
        assert_eq!(result.user_count, MAX_ACL_USER_ROWS + 2);
        let ops = result.users.iter().find(|user| user.username == "ops").unwrap();
        assert_eq!((ops.user_type.as_deref(), ops.user_status.as_deref()), (None, None));
        assert!(result.users.iter().all(|user| !user.username.contains('\u{0}')));
    }
}
