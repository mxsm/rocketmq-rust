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

//! The ACL users the masters of a cluster hold. This module exists only with the
//! `security-admin` feature.

use super::normalized_logical_identifier;
use super::AdminSession;
use super::AdminSessionFactory;
use super::QueryFacade;
use crate::model::contract::observed_at;
use crate::model::contract::CacheStatus;
use crate::model::contract::QueryCompleteness;
use crate::model::contract::QueryResult;
use crate::tools::executor::ToolFailure;
use crate::tools::security_tools::ListAclUsersArgs;
use crate::tools::security_tools::ListAclUsersOutput;

/// Warning of a list that misses the users of the Brokers in `unanswered_brokers`.
const BROKERS_UNANSWERED_WARNING: &str = "acl_user_brokers_unanswered";
const USERS_TRUNCATED_WARNING: &str = "acl_users_truncated";

impl<F> QueryFacade<F>
where
    F: AdminSessionFactory,
{
    /// Lists the ACL users of a cluster. The list is read every time and kept nowhere: who may
    /// sign requests is answered from the Brokers and not from an earlier answer.
    pub(crate) async fn list_acl_users(
        &self,
        mut args: ListAclUsersArgs,
    ) -> Result<QueryResult<ListAclUsersOutput>, ToolFailure> {
        args.cluster = normalized_logical_identifier("cluster", &args.cluster)?;
        let cluster = self.resolve_cluster(&args.cluster)?;
        let listed = self
            .run_workflow(cluster, move |session, _| {
                Box::pin(async move { session.acl_users().await })
            })
            .await?;
        let mut warnings = Vec::new();
        if !listed.unanswered_brokers.is_empty() {
            warnings.push(BROKERS_UNANSWERED_WARNING.to_string());
        }
        if listed.truncated {
            warnings.push(USERS_TRUNCATED_WARNING.to_string());
        }
        let completeness = QueryCompleteness {
            partial: !warnings.is_empty(),
            warnings,
            source_failures: Vec::new(),
        };
        Ok(QueryResult::from_payload(
            completeness.wrap(listed),
            observed_at(),
            0,
            CacheStatus::Bypass,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::super::observation_fixture::config;
    use super::super::observation_fixture::StubFactory;
    use super::*;
    use crate::tools::security_tools::AclUserRow;

    fn args(cluster: &str) -> ListAclUsersArgs {
        ListAclUsersArgs {
            cluster: cluster.to_string(),
        }
    }

    fn listed(unanswered: &[&str], truncated: bool) -> ListAclUsersOutput {
        ListAclUsersOutput {
            cluster: "local-dev".to_string(),
            user_count: 1,
            users: vec![AclUserRow {
                username: "ops".to_string(),
                broker_name: "broker-a".to_string(),
                user_type: Some("Super".to_string()),
                user_status: Some("enable".to_string()),
            }],
            truncated,
            queried_broker_count: 2,
            unanswered_brokers: unanswered.iter().map(|name| (*name).to_string()).collect(),
            generated_at: "1".to_string(),
        }
    }

    #[tokio::test]
    async fn the_users_of_a_cluster_are_read_every_time_and_say_what_they_miss() {
        let factory = StubFactory {
            acl_users: Some(listed(&[], false)),
            ..Default::default()
        };
        let facade = QueryFacade::with_factory(config(), factory.clone());

        let first = facade.list_acl_users(args(" local-dev ")).await.unwrap();
        let second = facade.list_acl_users(args("local-dev")).await.unwrap();

        assert_eq!(first.data, listed(&[], false));
        assert!(!first.partial && first.warnings.is_empty());
        assert_eq!(
            (first.cache_status, second.cache_status),
            (CacheStatus::Bypass, CacheStatus::Bypass)
        );
        assert_eq!(factory.used(), (2, 2, 2));

        let incomplete = StubFactory {
            acl_users: Some(listed(&["broker-b"], true)),
            ..Default::default()
        };
        let result = QueryFacade::with_factory(config(), incomplete)
            .list_acl_users(args("local-dev"))
            .await
            .unwrap();
        assert!(result.partial);
        assert_eq!(result.warnings, ["acl_user_brokers_unanswered", "acl_users_truncated"]);
        assert!(result.source_failures.is_empty());
    }

    #[tokio::test]
    async fn an_unconfigured_cluster_reads_nothing_and_a_failed_read_is_an_error() {
        let factory = StubFactory::default();
        let facade = QueryFacade::with_factory(config(), factory.clone());

        for cluster in ["unconfigured", "127.0.0.1:9876", " "] {
            assert!(facade.list_acl_users(args(cluster)).await.is_err(), "{cluster}");
        }
        assert_eq!(factory.used(), (0, 0, 0));

        let error = facade.list_acl_users(args("local-dev")).await.unwrap_err();
        assert_eq!(error.code(), "source_unavailable");
        assert_eq!(factory.used(), (1, 1, 1));
    }
}
