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

mod hash;
mod model;
mod repository;
mod service;

pub(crate) use hash::calculate_diff;
pub(crate) use hash::materialize_snapshot;
pub(crate) use hash::verify_diff;
pub(crate) use hash::verify_snapshot;
pub(crate) use model::AssetKey;
pub(crate) use model::AssetKind;
pub(crate) use model::AssetListQuery;
pub(crate) use model::AssetObservation;
pub(crate) use model::AssetPage;
pub(crate) use model::AssetSource;
pub(crate) use model::DiffEntity;
pub(crate) use model::IngestInventoryRequest;
pub(crate) use model::InventorySnapshot;
pub(crate) use model::NormalizedAsset;
pub(crate) use model::NormalizedTopologyEdge;
pub(crate) use model::TopologyDiff;
pub(crate) use model::TopologyDiffEntry;
pub(crate) use model::TopologyObservation;
pub(crate) use model::TopologyRelation;
pub(crate) use model::invalid_stored_inventory;
pub(crate) use repository::enforce_scope;
pub(crate) use service::AssetTopologyService;
pub(crate) use service::DashboardDeepLink;
pub(crate) use service::DashboardDeepLinkPolicy;
