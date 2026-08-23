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

//! Thin dashboard compositions around the official GPUI component library.

#[path = "components/app_shell.rs"]
pub mod app_shell;
#[path = "components/data_table.rs"]
pub mod data_table;
#[path = "components/dialog.rs"]
pub mod dialog;
#[path = "components/page_header.rs"]
pub mod page_header;
#[path = "components/query_toolbar.rs"]
pub mod query_toolbar;
#[path = "components/sidebar.rs"]
pub mod sidebar;
#[path = "components/states.rs"]
pub mod states;
#[path = "components/status_badge.rs"]
pub mod status_badge;
#[path = "components/toast.rs"]
pub mod toast;
#[path = "components/topbar.rs"]
pub mod topbar;
