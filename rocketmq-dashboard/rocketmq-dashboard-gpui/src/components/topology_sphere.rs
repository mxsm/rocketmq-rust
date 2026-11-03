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

use std::sync::Arc;

use gpui::{App, Hsla, IntoElement, PathBuilder, Styled as _, canvas, point, px};
use rocketmq_dashboard_common::topology::sphere::{SphereProjection, SphereViewport};

/// Paints immutable geometry; committed canvas bounds prepare the next frame.
pub(crate) fn surface(
    projection: Option<Arc<SphereProjection>>,
    grid: Hsla,
    edge: Hsla,
    observe: impl FnOnce(SphereViewport, &mut App) + 'static,
) -> impl IntoElement {
    canvas(
        move |_, _, _| projection,
        move |bounds, projection, window, cx| {
            let viewport = SphereViewport {
                width: bounds.size.width.into(),
                height: bounds.size.height.into(),
            };
            // GPUI can prepaint speculative zero-width layouts before a committed paint.
            // Empty canvases have no visible content and must not invalidate usable geometry.
            if viewport.width <= 0. || viewport.height <= 0. {
                return;
            }
            observe(viewport, cx);
            let Some(projection) = projection.filter(|projection| projection.viewport == viewport) else {
                return;
            };
            for globe in &projection.globes {
                let mut path = PathBuilder::stroke(px(1.));
                for (from, to) in &globe.grid {
                    path.move_to(bounds.origin + point(px(from[0]), px(from[1])));
                    path.line_to(bounds.origin + point(px(to[0]), px(to[1])));
                }
                if let Ok(path) = path.build() {
                    window.paint_path(path, grid);
                }
            }
            for link in &projection.edges {
                let mut path = PathBuilder::stroke(px(1.));
                path.move_to(bounds.origin + point(px(link.from[0]), px(link.from[1])));
                path.line_to(bounds.origin + point(px(link.to[0]), px(link.to[1])));
                if let Ok(path) = path.build() {
                    window.paint_path(path, edge);
                }
            }
        },
    )
    .absolute()
    .left_0()
    .right_0()
    .top_0()
    .bottom_0()
}
