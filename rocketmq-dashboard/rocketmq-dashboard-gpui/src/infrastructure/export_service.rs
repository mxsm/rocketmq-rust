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

//! Explicitly chosen CSV destinations written through the application's storage executor.

use crate::state::{UiError, UiErrorCode};
use rocketmq_dashboard_common::DlqCsv;
use rocketmq_runtime::ChildServiceContext;
use std::path::PathBuf;

pub(crate) async fn write_csv(context: ChildServiceContext, path: PathBuf, csv: DlqCsv) -> Result<usize, UiError> {
    context
        .storage_io()
        .spawn_io("gpui-dlq-csv-write", move || {
            std::fs::write(path, csv.as_bytes()).map(|()| csv.as_bytes().len())
        })
        .await
        .map_err(|source| UiError::caused_by("The CSV writer is unavailable.", UiErrorCode::Connection, false, source))?
        .map_err(|source| {
            UiError::caused_by(
                "Unable to write the CSV file. Choose a writable location and export again.",
                UiErrorCode::Configuration,
                true,
                source,
            )
        })
}
