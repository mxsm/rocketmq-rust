/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use crate::base::swappable::Swappable;

/// FileQueueLifeCycle contains life cycle methods of ConsumerQueue that is directly implemented by
/// FILE.
pub trait FileQueueLifeCycle: Swappable {
    /// Load from file.
    /// Returns true if loaded successfully.
    fn load(&self) -> bool;

    /// Recover from file.
    fn recover(&self);

    /// Check files.
    fn check_self(&self);

    /// Flush cache to file.
    /// `flush_least_pages`: The minimum number of pages to be flushed.
    /// Returns true if any data has been flushed.
    fn flush(&self, flush_least_pages: i32) -> bool;

    /// Destroy files.
    fn destroy(&self);

    /// Truncate dirty logic files starting at max commit log position.
    /// `max_commit_log_pos`: Max commit log position.
    fn truncate_dirty_logic_files(&self, max_commit_log_pos: i64);

    /// Delete expired files ending at min commit log position.
    /// `min_commit_log_pos`: Min commit log position.
    /// Returns deleted file numbers.
    fn delete_expired_file(&self, min_commit_log_pos: i64) -> i32;

    /// Roll to next file.
    /// `next_begin_offset`: Next begin offset.
    /// Returns the beginning offset of the next file.
    fn roll_next_file(&self, next_begin_offset: i64) -> i64;

    /// Is the first file available?
    /// Returns true if it's available.
    fn is_first_file_available(&self) -> bool;

    /// Does the first file exist?
    /// Returns true if it exists.
    fn is_first_file_exist(&self) -> bool;
}
