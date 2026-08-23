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

//! Layout rules shared by the desktop shell and its narrow-window drawer.

use gpui::{Pixels, px};

/// Width at which the dashboard keeps its permanent navigation sidebar.
pub const SIDEBAR_BREAKPOINT: Pixels = px(1024.);

/// Returns whether a window should display the fixed sidebar rather than the Sheet drawer.
pub fn uses_fixed_sidebar(width: Pixels) -> bool {
    width >= SIDEBAR_BREAKPOINT
}

#[cfg(test)]
mod tests {
    use gpui::px;

    use super::uses_fixed_sidebar;

    #[test]
    fn breakpoint_keeps_sidebar_at_1024_and_uses_drawer_at_960_or_below() {
        assert!(uses_fixed_sidebar(px(1024.)));
        assert!(!uses_fixed_sidebar(px(1023.)));
        assert!(!uses_fixed_sidebar(px(960.)));
    }
}
