// Copyright 2025 The RocketMQ Rust Authors
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

//! UI components for the RocketMQ Dashboard

#[path = "ui/dashboard_view.rs"]
pub mod dashboard_view;

#[path = "ui/icons.rs"]
pub mod icons;

#[path = "ui/nameserver_view.rs"]
pub mod nameserver_view;

#[path = "ui/cluster_view.rs"]
pub mod cluster_view;

#[path = "ui/topic_view.rs"]
pub mod topic_view;

#[path = "ui/consumer_view.rs"]
pub mod consumer_view;

#[path = "ui/producer_view.rs"]
pub mod producer_view;

#[path = "ui/pages.rs"]
pub mod pages;
