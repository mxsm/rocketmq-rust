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

//! Coordinate-based History line chart that never connects gaps.

use gpui::{Hsla, ParentElement as _, PathBuilder, Styled as _, canvas, div, point, px};
use rocketmq_dashboard_common::{HistoryPoint, split_history_segments};

#[derive(Clone, Copy, Debug, PartialEq)]
struct ChartPoint {
    x: f32,
    y: f32,
}

pub fn render(points: &[HistoryPoint], max_gap_ms: u64, foreground: Hsla, muted: Hsla, border: Hsla) -> gpui::Div {
    if points.is_empty() {
        return div()
            .p_4()
            .text_sm()
            .text_color(muted)
            .child("Warming Up — no successful samples yet");
    }
    let points = points.to_vec();
    let (min_value, max_value) = value_bounds(&points);
    div()
        .w_full()
        .h(px(190.))
        .p_3()
        .rounded_lg()
        .border_1()
        .border_color(border)
        .flex()
        .flex_col()
        .gap_2()
        .child(
            div()
                .flex()
                .justify_between()
                .text_xs()
                .text_color(muted)
                .child(format!("{max_value:.2}"))
                .child(format!("{min_value:.2}")),
        )
        .child(
            canvas(
                move |bounds, _, _| {
                    let width = f32::from(bounds.size.width).max(1.0);
                    let height = f32::from(bounds.size.height).max(1.0);
                    project_chart_segments(&points, max_gap_ms, width, height)
                },
                move |bounds, segments, window, _| {
                    let mut axes = PathBuilder::stroke(px(1.));
                    axes.move_to(bounds.bottom_left());
                    axes.line_to(bounds.origin);
                    axes.line_to(bounds.bottom_right());
                    if let Ok(path) = axes.build() {
                        window.paint_path(path, border);
                    }
                    for segment in segments {
                        if segment.len() < 2 {
                            continue;
                        }
                        let mut line = PathBuilder::stroke(px(2.));
                        for (index, projected) in segment.into_iter().enumerate() {
                            let chart_point = bounds.origin + point(px(projected.x), px(projected.y));
                            if index == 0 {
                                line.move_to(chart_point);
                            } else {
                                line.line_to(chart_point);
                            }
                        }
                        if let Ok(path) = line.build() {
                            window.paint_path(path, foreground);
                        }
                    }
                },
            )
            .size_full(),
        )
}

fn value_bounds(points: &[HistoryPoint]) -> (f64, f64) {
    points
        .iter()
        .fold((f64::INFINITY, f64::NEG_INFINITY), |(min, max), point| {
            (min.min(point.value), max.max(point.value))
        })
}

fn project_chart_segments(points: &[HistoryPoint], max_gap_ms: u64, width: f32, height: f32) -> Vec<Vec<ChartPoint>> {
    let min_time = points.iter().map(|point| point.timestamp_epoch_ms).min().unwrap_or(0);
    let max_time = points
        .iter()
        .map(|point| point.timestamp_epoch_ms)
        .max()
        .unwrap_or(min_time);
    let (min_value, max_value) = value_bounds(points);
    let time_span = max_time.saturating_sub(min_time).max(1) as f64;
    let value_span = (max_value - min_value).max(f64::EPSILON);
    split_history_segments(points, max_gap_ms)
        .into_iter()
        .map(|segment| {
            segment
                .points
                .into_iter()
                .map(|point| ChartPoint {
                    x: (((point.timestamp_epoch_ms.saturating_sub(min_time) as f64) / time_span) * f64::from(width))
                        as f32,
                    y: (f64::from(height) - ((point.value - min_value) / value_span) * f64::from(height)) as f32,
                })
                .collect()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use rocketmq_dashboard_common::HistoryMetricKind;

    use super::*;

    fn history(timestamp: u64, value: f64) -> HistoryPoint {
        HistoryPoint {
            metric: HistoryMetricKind::TopicMessages,
            series_identity: "orders".into(),
            timestamp_epoch_ms: timestamp,
            value,
            source_revision: 1,
        }
    }

    #[test]
    fn chart_projects_real_coordinates_and_keeps_gap_segments_separate() {
        let projected = project_chart_segments(
            &[history(0, 10.0), history(60, 20.0), history(600, 15.0)],
            120,
            300.0,
            100.0,
        );
        assert_eq!(projected.len(), 2);
        assert_eq!(projected[0][0], ChartPoint { x: 0.0, y: 100.0 });
        assert!(projected[0][1].x > projected[0][0].x);
        assert_eq!(projected[0][1].y, 0.0);
        assert_eq!(projected[1].len(), 1);
    }
}
