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

//! Asset registration for the GPUI dashboard.

/// Returns the official GPUI Component icon asset source.
///
/// RocketMQ-specific artwork will be added here only when it cannot be expressed with the
/// component library's built-in [`gpui_component::IconName`] icons.
pub fn component_assets() -> gpui_component_assets::Assets {
    gpui_component_assets::Assets
}
