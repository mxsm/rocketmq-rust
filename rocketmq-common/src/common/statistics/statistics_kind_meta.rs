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

use crate::common::statistics::statistics_item_scheduled_printer::StatisticsItemScheduledPrinter;

pub struct StatisticsKindMeta {
    name: String,
    item_names: Vec<String>,
    scheduled_printer: StatisticsItemScheduledPrinter,
}

impl StatisticsKindMeta {
    pub fn new(name: String, item_names: Vec<String>, scheduled_printer: StatisticsItemScheduledPrinter) -> Self {
        StatisticsKindMeta {
            name,
            item_names,
            scheduled_printer,
        }
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn set_name(&mut self, name: String) {
        self.name = name;
    }

    pub fn get_item_names(&self) -> &Vec<String> {
        &self.item_names
    }

    pub fn set_item_names(&mut self, item_names: Vec<String>) {
        self.item_names = item_names;
    }

    pub fn get_scheduled_printer(&self) -> &StatisticsItemScheduledPrinter {
        &self.scheduled_printer
    }

    pub fn set_scheduled_printer(&mut self, scheduled_printer: StatisticsItemScheduledPrinter) {
        self.scheduled_printer = scheduled_printer;
    }
}
