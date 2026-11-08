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

use gpui::{
    Hsla, InteractiveElement as _, ParentElement as _, PathBuilder, Styled as _, canvas, div, point, px, relative,
};
use rocketmq_dashboard_common::{PlacedTopologyNode, TopologyLayout};

/// Labels fit the existing node rectangle; the button tooltip/details retain the full identity.
pub(crate) fn node_label(placed: &PlacedTopologyNode, index: usize) -> gpui::Div {
    div()
        .debug_selector(move || format!("topology-node-label-{index}"))
        // Button padding and borders remain inside the smallest existing map scale.
        .w(px((placed.bounds.width - 28.).max(0.)))
        .min_w_0()
        .overflow_hidden()
        .flex()
        .flex_col()
        .gap_1()
        .text_xs()
        .line_height(relative(1.25))
        .child(div().w_full().truncate().child(format!(
            "{}{}",
            placed.node.id.kind_label(),
            if placed.stale { " · cached" } else { "" }
        )))
        .child(div().w_full().truncate().child(placed.node.label.clone()))
}

/// Painting consumes positions prepared by the Store. No topology aggregation runs in Render.
pub(crate) fn links(layout: &TopologyLayout, color: Hsla) -> impl gpui::IntoElement {
    let lines = layout.paths.clone();
    canvas(
        move |_, _, _| lines,
        move |bounds, lines, window, _| {
            for (from, to) in lines {
                let mut line = PathBuilder::stroke(px(1.));
                line.move_to(bounds.origin + point(px(from.x + from.width / 2.), px(from.y + from.height / 2.)));
                line.line_to(bounds.origin + point(px(to.x + to.width / 2.), px(to.y + to.height / 2.)));
                if let Ok(path) = line.build() {
                    window.paint_path(path, color);
                }
            }
        },
    )
    .absolute()
    .size_full()
}

pub(crate) fn board(width: f32, height: f32) -> gpui::Div {
    div().relative().w(px(width)).h(px(height.max(220.)))
}
