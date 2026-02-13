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

#[derive(Debug, Clone, Copy)]
pub struct ProcessQueueInfo {
    pub commit_offset: u64,
    pub cached_msg_min_offset: u64,
    pub cached_msg_max_offset: u64,
    pub cached_msg_count: u32,
    pub cached_msg_size_in_mib: u32,

    pub transaction_msg_min_offset: u64,
    pub transaction_msg_max_offset: u64,
    pub transaction_msg_count: u32,

    pub locked: bool,
    pub try_unlock_times: u64,
    pub last_lock_timestamp: u64,

    pub droped: bool,
    pub last_pull_timestamp: u64,
    pub last_consume_timestamp: u64,
}

impl std::fmt::Display for ProcessQueueInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ProcessQueueInfo [commit_offset: {}, cached_msg_min_offset: {}, cached_msg_max_offset: {}, \
             cached_msg_count: {}, cached_msg_size_in_mib: {}, transaction_msg_min_offset: {}, \
             transaction_msg_max_offset: {}, transaction_msg_count: {}, locked: {}, try_unlock_times: {}, \
             last_lock_timestamp: {}, droped: {}, last_pull_timestamp: {}, last_consume_timestamp: {}]",
            self.commit_offset,
            self.cached_msg_min_offset,
            self.cached_msg_max_offset,
            self.cached_msg_count,
            self.cached_msg_size_in_mib,
            self.transaction_msg_min_offset,
            self.transaction_msg_max_offset,
            self.transaction_msg_count,
            self.locked,
            self.try_unlock_times,
            self.last_lock_timestamp,
            self.droped,
            self.last_pull_timestamp,
            self.last_consume_timestamp
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_queue_init() {
        let process_queue_info = ProcessQueueInfo {
            commit_offset: 100,
            cached_msg_min_offset: 90,
            cached_msg_max_offset: 110,
            cached_msg_count: 20,
            cached_msg_size_in_mib: 5,
            transaction_msg_min_offset: 80,
            transaction_msg_max_offset: 120,
            transaction_msg_count: 40,
            locked: true,
            try_unlock_times: 3,
            last_lock_timestamp: 1620000000,
            droped: false,
            last_pull_timestamp: 1620000100,
            last_consume_timestamp: 1620000200,
        };

        assert_eq!(process_queue_info.commit_offset, 100);
        assert_eq!(process_queue_info.cached_msg_min_offset, 90);
        assert_eq!(process_queue_info.cached_msg_max_offset, 110);
        assert_eq!(process_queue_info.cached_msg_count, 20);
        assert_eq!(process_queue_info.cached_msg_size_in_mib, 5);
        assert_eq!(process_queue_info.transaction_msg_min_offset, 80);
        assert_eq!(process_queue_info.transaction_msg_max_offset, 120);
        assert_eq!(process_queue_info.transaction_msg_count, 40);
        assert_eq!(process_queue_info.locked, true);
        assert_eq!(process_queue_info.try_unlock_times, 3);
        assert_eq!(process_queue_info.last_lock_timestamp, 1620000000);
        assert_eq!(process_queue_info.droped, false);
        assert_eq!(process_queue_info.last_pull_timestamp, 1620000100);
        assert_eq!(process_queue_info.last_consume_timestamp, 1620000200);

        println!("{}", process_queue_info);
    }

    #[test]
    fn process_queue_clone() {
        let process_queue_info = ProcessQueueInfo {
            commit_offset: 100,
            cached_msg_min_offset: 90,
            cached_msg_max_offset: 110,
            cached_msg_count: 20,
            cached_msg_size_in_mib: 5,
            transaction_msg_min_offset: 80,
            transaction_msg_max_offset: 120,
            transaction_msg_count: 40,
            locked: true,
            try_unlock_times: 3,
            last_lock_timestamp: 1620000000,
            droped: false,
            last_pull_timestamp: 1620000100,
            last_consume_timestamp: 1620000200,
        };
        let cloned_process_queue_info = process_queue_info.clone();

        assert_eq!(
            process_queue_info.commit_offset,
            cloned_process_queue_info.commit_offset
        );
        assert_eq!(
            process_queue_info.cached_msg_min_offset,
            cloned_process_queue_info.cached_msg_min_offset
        );
        assert_eq!(
            process_queue_info.cached_msg_max_offset,
            cloned_process_queue_info.cached_msg_max_offset
        );
        assert_eq!(
            process_queue_info.cached_msg_count,
            cloned_process_queue_info.cached_msg_count
        );
        assert_eq!(
            process_queue_info.cached_msg_size_in_mib,
            cloned_process_queue_info.cached_msg_size_in_mib
        );
        assert_eq!(
            process_queue_info.transaction_msg_min_offset,
            cloned_process_queue_info.transaction_msg_min_offset
        );
        assert_eq!(
            process_queue_info.transaction_msg_max_offset,
            cloned_process_queue_info.transaction_msg_max_offset
        );
        assert_eq!(
            process_queue_info.transaction_msg_count,
            cloned_process_queue_info.transaction_msg_count
        );
        assert_eq!(process_queue_info.locked, cloned_process_queue_info.locked);
        assert_eq!(
            process_queue_info.try_unlock_times,
            cloned_process_queue_info.try_unlock_times
        );
        assert_eq!(
            process_queue_info.last_lock_timestamp,
            cloned_process_queue_info.last_lock_timestamp
        );
        assert_eq!(process_queue_info.droped, cloned_process_queue_info.droped);
        assert_eq!(
            process_queue_info.last_pull_timestamp,
            cloned_process_queue_info.last_pull_timestamp
        );
        assert_eq!(
            process_queue_info.last_consume_timestamp,
            cloned_process_queue_info.last_consume_timestamp
        );
    }

    #[test]
    fn process_queue_display() {
        let process_queue_info = ProcessQueueInfo {
            commit_offset: 100,
            cached_msg_min_offset: 90,
            cached_msg_max_offset: 110,
            cached_msg_count: 20,
            cached_msg_size_in_mib: 5,
            transaction_msg_min_offset: 80,
            transaction_msg_max_offset: 120,
            transaction_msg_count: 40,
            locked: true,
            try_unlock_times: 3,
            last_lock_timestamp: 1620000000,
            droped: false,
            last_pull_timestamp: 1620000100,
            last_consume_timestamp: 1620000200,
        };

        let display_output = format!("{}", process_queue_info);
        assert!(display_output.contains("ProcessQueueInfo"));
        assert!(display_output.contains("commit_offset: 100"));
        assert!(display_output.contains("cached_msg_min_offset: 90"));
        assert!(display_output.contains("cached_msg_max_offset: 110"));
        assert!(display_output.contains("cached_msg_count: 20"));
        assert!(display_output.contains("cached_msg_size_in_mib: 5"));
        assert!(display_output.contains("transaction_msg_min_offset: 80"));
        assert!(display_output.contains("transaction_msg_max_offset: 120"));
        assert!(display_output.contains("transaction_msg_count: 40"));
        assert!(display_output.contains("locked: true"));
        assert!(display_output.contains("try_unlock_times: 3"));
        assert!(display_output.contains("last_lock_timestamp: 1620000000"));
        assert!(display_output.contains("droped: false"));
        assert!(display_output.contains("last_pull_timestamp: 1620000100"));
        assert!(display_output.contains("last_consume_timestamp: 1620000200"));

        println!("{}", display_output);
    }
}
