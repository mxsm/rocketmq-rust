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

//! Names for argument completion, read from the observations the listing Tools already keep.

use super::AdminSessionFactory;
use super::QueryFacade;
use crate::completion::CompletionClass;
use crate::infrastructure::snapshot::SnapshotView;
use crate::model::contract::PageRequest;
use crate::tools::broker_tools::ListBrokersArgs;
use crate::tools::executor::ToolFailure;
use crate::tools::executor::ToolRejection;

impl<F> QueryFacade<F>
where
    F: AdminSessionFactory,
{
    /// Returns the names of `class` in one cluster that can match `prefix`, sorted and distinct.
    ///
    /// The names come from the inventory snapshot of the matching listing Tool, or from the Broker
    /// list, so completing while typing does not read a source again. An inventory too large to
    /// keep whole is read with `prefix` as its filter instead; without a prefix it has no
    /// candidates. The result may hold names that merely contain `prefix`: the caller keeps the
    /// ones that start with it.
    ///
    /// # Errors
    ///
    /// Fails like the listing Tool does: an unconfigured cluster, an unavailable source, a timeout,
    /// or a cancelled request. `CompletionClass::Cluster` reads no cluster and is an invalid
    /// argument here.
    pub(crate) async fn completion_names(
        &self,
        cluster: &str,
        class: CompletionClass,
        prefix: &str,
    ) -> Result<Vec<String>, ToolFailure> {
        let cluster = self.resolve_cluster(cluster)?;
        let page = PageRequest::default();
        match class {
            CompletionClass::Cluster => Err(ToolFailure::Rejected(ToolRejection::InvalidArguments { _source: None })),
            CompletionClass::Topic => {
                inventory_names(prefix, |filter| {
                    self.topic_inventory_snapshot(cluster.clone(), filter, &page)
                })
                .await
            }
            CompletionClass::ConsumerGroup => {
                inventory_names(prefix, |filter| {
                    self.consumer_group_inventory_snapshot(cluster.clone(), filter, false, &page)
                })
                .await
            }
            CompletionClass::Broker => {
                let listed = self.list_brokers(ListBrokersArgs { cluster: cluster.name }).await?;
                let mut names = listed
                    .data
                    .brokers
                    .into_iter()
                    .map(|broker| broker.broker_name)
                    .collect::<Vec<_>>();
                names.sort();
                names.dedup();
                Ok(names)
            }
        }
    }
}

/// Reads an inventory whole, or narrowed by `prefix` when the whole of it is too large to keep.
async fn inventory_names<'a, Load, LoadFuture>(prefix: &'a str, load: Load) -> Result<Vec<String>, ToolFailure>
where
    Load: Fn(Option<&'a str>) -> LoadFuture,
    LoadFuture: std::future::Future<Output = Result<SnapshotView<Vec<String>>, ToolFailure>>,
{
    match load(None).await {
        Ok(snapshot) => Ok(snapshot.payload.data),
        Err(ToolFailure::Rejected(ToolRejection::ResultTooLarge)) if !prefix.trim().is_empty() => {
            Ok(load(Some(prefix)).await?.payload.data)
        }
        Err(ToolFailure::Rejected(ToolRejection::ResultTooLarge)) => Ok(Vec::new()),
        Err(error) => Err(error),
    }
}
