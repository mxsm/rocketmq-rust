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

use gpui::{Hsla, PathBuilder, Styled as _, canvas, div, point, px};
use rocketmq_dashboard_common::TopologyLayout;

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
