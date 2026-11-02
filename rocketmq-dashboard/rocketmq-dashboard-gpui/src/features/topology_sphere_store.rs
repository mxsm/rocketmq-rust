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

use rocketmq_dashboard_common::{TopologyLayout, TopologyNodeId, topology::sphere::*};

/// Geometry changes only on data, selection, viewport or explicit camera events.
/// This presentation owns no clock, background task, data source or separate selection.
pub(crate) struct TopologySphereStore {
    pub scene: SphereScene,
    pub camera: SphereCamera,
    pub viewport: SphereViewport,
    pub projection: Result<Arc<SphereProjection>, SphereUnavailable>,
    selected: Option<TopologyNodeId>,
}
impl Default for TopologySphereStore {
    fn default() -> Self {
        Self {
            scene: SphereScene::default(),
            camera: SphereCamera::default(),
            viewport: SphereViewport::default(),
            projection: Err(SphereUnavailable::InvalidViewport),
            selected: None,
        }
    }
}
impl TopologySphereStore {
    pub fn rebuild(&mut self, layout: &TopologyLayout, selected: Option<&TopologyNodeId>) {
        self.scene = sphere_scene(layout);
        self.selected = selected.cloned();
        self.project();
    }
    pub fn set_viewport(&mut self, viewport: SphereViewport) -> bool {
        let viewport = if viewport.width.is_finite() && viewport.height.is_finite() {
            viewport
        } else {
            SphereViewport::default()
        };
        if self.viewport == viewport {
            return false;
        }
        self.viewport = viewport;
        self.project();
        true
    }
    pub fn turn(&mut self, direction: SphereTurn) {
        self.camera.turn(direction);
        self.project();
    }
    pub fn zoom(&mut self, direction: SphereZoom) {
        self.camera.zoom(direction);
        self.project();
    }
    pub fn focus(&mut self) {
        if let Some(selected) = &self.selected {
            self.camera.focus(&self.scene, selected);
            self.project();
        }
    }
    pub fn reset(&mut self) {
        self.camera = SphereCamera::default();
        self.project();
    }
    fn project(&mut self) {
        self.projection = project_sphere(&self.scene, self.camera, self.viewport, self.selected.as_ref()).map(Arc::new);
    }
}
