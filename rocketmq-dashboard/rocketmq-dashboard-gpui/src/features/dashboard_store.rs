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

//! Five-resource Dashboard state with independent epochs and retained refresh data.

use std::time::SystemTime;

use rocketmq_dashboard_common::{BrokerCurrentMetric, HistoryPoint, TopicCurrentMetric};

use crate::{
    services::dashboard::DashboardOverviewLoad,
    state::{Loadable, RequestEpoch, UiError},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResourceRequest {
    epoch: RequestEpoch,
    revision: u64,
}

pub struct ResourceSlot<T> {
    pub state: Loadable<T>,
    epoch: RequestEpoch,
    last_updated_epoch_ms: Option<u64>,
}

impl<T> Default for ResourceSlot<T> {
    fn default() -> Self {
        Self {
            state: Loadable::Idle,
            epoch: RequestEpoch::initial(),
            last_updated_epoch_ms: None,
        }
    }
}

impl<T> ResourceSlot<T> {
    pub fn begin(&mut self, revision: u64) -> Option<ResourceRequest> {
        let epoch = self.epoch.advance().ok()?;
        self.state = std::mem::replace(&mut self.state, Loadable::Idle).begin();
        Some(ResourceRequest { epoch, revision })
    }

    pub fn finish(&mut self, request: ResourceRequest, revision: u64, result: Result<Option<T>, UiError>) -> bool {
        if request.revision != revision || !self.epoch.accepts(request.epoch) {
            return false;
        }
        let previous = std::mem::replace(&mut self.state, Loadable::Idle);
        self.state = match result {
            Ok(Some(value)) => {
                self.last_updated_epoch_ms = now_epoch_ms();
                Loadable::ready(value)
            }
            Ok(None) => Loadable::empty(),
            Err(error) => previous.fail(error),
        };
        true
    }

    pub fn last_updated_epoch_ms(&self) -> Option<u64> {
        self.last_updated_epoch_ms
    }

    pub fn invalidate(&mut self) {
        let _ = self.epoch.advance();
    }

    pub fn clear(&mut self) {
        let _ = self.epoch.advance();
        self.state = Loadable::Idle;
        self.last_updated_epoch_ms = None;
    }

    pub fn clear_with_error(&mut self, error: UiError) {
        let _ = self.epoch.advance();
        self.state = Loadable::Failed { previous: None, error };
        self.last_updated_epoch_ms = None;
    }
}

fn now_epoch_ms() -> Option<u64> {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
}

#[derive(Default)]
pub struct DashboardStore {
    pub overview: ResourceSlot<DashboardOverviewLoad>,
    pub topic_current: ResourceSlot<Vec<TopicCurrentMetric>>,
    pub broker_current: ResourceSlot<Vec<BrokerCurrentMetric>>,
    pub topic_history: ResourceSlot<Vec<HistoryPoint>>,
    pub broker_history: ResourceSlot<Vec<HistoryPoint>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DashboardLayout {
    Wide { metric_columns: usize },
    Compact { metric_columns: usize },
}

impl DashboardLayout {
    pub fn for_width(width: f32) -> Self {
        if width >= 1_200.0 {
            Self::Wide { metric_columns: 4 }
        } else {
            Self::Compact { metric_columns: 2 }
        }
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_dashboard_common::{EndpointAvailability, Observed};

    use crate::state::UiErrorCode;

    use super::*;

    fn overview(count: u64) -> DashboardOverviewLoad {
        DashboardOverviewLoad {
            overview: rocketmq_dashboard_common::DashboardOverview {
                broker_count: Observed::Observed(count),
                nameserver_availability: EndpointAvailability::Available,
                ..Default::default()
            },
            failed_resources: 0,
        }
    }

    #[test]
    fn five_resources_load_and_fail_independently() {
        let mut store = DashboardStore::default();
        let overview_request = store.overview.begin(3).expect("overview request");
        let topic_request = store.topic_current.begin(3).expect("topic request");
        assert!(store.overview.finish(overview_request, 3, Ok(Some(overview(2)))));
        assert!(store.topic_current.finish(
            topic_request,
            3,
            Err(UiError::new("topic failed", UiErrorCode::Connection, true))
        ));
        assert!(matches!(store.overview.state, Loadable::Ready(_)));
        assert!(matches!(store.topic_current.state, Loadable::Failed { .. }));
        assert!(matches!(store.broker_current.state, Loadable::Idle));
        assert!(matches!(store.topic_history.state, Loadable::Idle));
        assert!(matches!(store.broker_history.state, Loadable::Idle));
    }

    #[test]
    fn refresh_retains_data_and_stale_epoch_or_revision_cannot_replace_it() {
        let mut slot = ResourceSlot::default();
        let first = slot.begin(4).expect("first");
        assert!(slot.finish(first, 4, Ok(Some(overview(1)))));
        let stale = slot.begin(4).expect("stale");
        let current = slot.begin(4).expect("current");
        assert!(matches!(slot.state, Loadable::Refreshing(_)));
        assert!(!slot.finish(stale, 4, Ok(Some(overview(9)))));
        assert!(!slot.finish(current, 5, Ok(Some(overview(9)))));
        assert_eq!(
            slot.state.value().expect("retained").overview.broker_count,
            Observed::Observed(1)
        );
    }

    #[test]
    fn responsive_layout_is_deterministic_at_product_widths() {
        assert_eq!(
            DashboardLayout::for_width(1_440.0),
            DashboardLayout::Wide { metric_columns: 4 }
        );
        assert_eq!(
            DashboardLayout::for_width(960.0),
            DashboardLayout::Compact { metric_columns: 2 }
        );
    }
}
