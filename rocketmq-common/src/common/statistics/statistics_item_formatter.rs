// Copyright 2023 The RocketMQ Rust Authors
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

use std::sync::atomic::Ordering;

use crate::common::statistics::statistics_item::StatisticsItem;

pub struct StatisticsItemFormatter;

impl StatisticsItemFormatter {
    pub fn format(&self, stat_item: &StatisticsItem) -> String {
        let separator = "|";
        let mut sb = String::new();

        sb.push_str(stat_item.stat_kind());
        sb.push_str(separator);
        sb.push_str(stat_item.stat_object());
        sb.push_str(separator);

        for acc in stat_item.item_accumulates() {
            sb.push_str(&acc.load(Ordering::SeqCst).to_string());
            sb.push_str(separator);
        }

        sb.push_str(&stat_item.invoke_times().load(Ordering::SeqCst).to_string());

        sb
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn format_creates_correct_string() {
        let formatter = StatisticsItemFormatter;
        let item = StatisticsItem::new("kind", "object", vec!["item1", "item2"]);
        item.inc_items(vec![1, 2]);
        let result = formatter.format(&item);
        assert_eq!(result, "kind|object|1|2|1");
    }

    #[test]
    fn format_creates_correct_string_with_multiple_items() {
        let formatter = StatisticsItemFormatter;
        let item = StatisticsItem::new("kind", "object", vec!["item1", "item2", "item3"]);
        item.inc_items(vec![1, 2, 3]);
        let result = formatter.format(&item);
        assert_eq!(result, "kind|object|1|2|3|1");
    }
}
