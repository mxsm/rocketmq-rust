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

use tracing::info;

use crate::common::statistics::statistics_item::StatisticsItem;
use crate::common::statistics::statistics_item_formatter::StatisticsItemFormatter;

pub struct StatisticsItemPrinter<'a> {
    formatter: &'a StatisticsItemFormatter,
}

impl<'a> StatisticsItemPrinter<'a> {
    pub fn new(formatter: &'a StatisticsItemFormatter) -> Self {
        Self { formatter }
    }

    pub fn set_formatter(&mut self, formatter: &'a StatisticsItemFormatter) {
        self.formatter = formatter;
    }

    pub fn print(&self, prefix: &str, stat_item: &StatisticsItem, suffixes: &[&str]) {
        let suffix = suffixes.join("");

        let log_message = format!("{}{}{}", prefix, self.formatter.format(stat_item), suffix);

        info!("{}", log_message);
    }
}
