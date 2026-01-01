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

#[inline]
pub(crate) fn get_store_path_consume_queue(root_dir: &str) -> String {
    format!("{}{}consumequeue", root_dir, std::path::MAIN_SEPARATOR,)
}

#[inline]
pub(crate) fn get_store_path_consume_queue_ext(root_dir: &str) -> String {
    format!("{}{}consumequeue_ext", root_dir, std::path::MAIN_SEPARATOR,)
}

#[inline]
pub(crate) fn get_store_path_batch_consume_queue(root_dir: &str) -> String {
    format!("{}{}batchconsumequeue", root_dir, std::path::MAIN_SEPARATOR,)
}

#[inline]
pub(crate) fn get_store_path_index(root_dir: &str) -> String {
    format!("{}{}index", root_dir, std::path::MAIN_SEPARATOR,)
}

#[inline]
pub(crate) fn get_store_checkpoint(root_dir: &str) -> String {
    format!("{}{}checkpoint", root_dir, std::path::MAIN_SEPARATOR,)
}

#[inline]
pub(crate) fn get_abort_file(root_dir: &str) -> String {
    format!("{}{}abort", root_dir, std::path::MAIN_SEPARATOR,)
}

#[inline]
pub(crate) fn get_lock_file(root_dir: &str) -> String {
    format!("{}{}lock", root_dir, std::path::MAIN_SEPARATOR,)
}

#[inline]
pub(crate) fn get_delay_offset_store_path(root_dir: &str) -> String {
    format!(
        "{}{}config{}delayOffset.json",
        root_dir,
        std::path::MAIN_SEPARATOR,
        std::path::MAIN_SEPARATOR,
    )
}

#[inline]
pub(crate) fn get_tran_state_table_store_path(root_dir: &str) -> String {
    format!(
        "{}{}transaction{}statetable",
        root_dir,
        std::path::MAIN_SEPARATOR,
        std::path::MAIN_SEPARATOR,
    )
}

#[inline]
pub(crate) fn get_tran_redo_log_store_path(root_dir: &str) -> String {
    format!(
        "{}{}transaction{}redolog",
        root_dir,
        std::path::MAIN_SEPARATOR,
        std::path::MAIN_SEPARATOR,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_store_path_consume_queue() {
        let root_dir = "/path/to/root";
        let expected_path = format!("{}{}consumequeue", root_dir, std::path::MAIN_SEPARATOR);
        assert_eq!(get_store_path_consume_queue(root_dir), expected_path);
    }

    #[test]
    fn test_get_store_path_consume_queue_ext() {
        let root_dir = "/path/to/root";
        let expected_path = format!("{}{}consumequeue_ext", root_dir, std::path::MAIN_SEPARATOR);
        assert_eq!(get_store_path_consume_queue_ext(root_dir), expected_path);
    }

    #[test]
    fn test_get_store_path_batch_consume_queue() {
        let root_dir = "/path/to/root";
        let expected_path = format!("{}{}batchconsumequeue", root_dir, std::path::MAIN_SEPARATOR);
        assert_eq!(get_store_path_batch_consume_queue(root_dir), expected_path);
    }

    #[test]
    fn test_get_store_path_index() {
        let root_dir = "/path/to/root";
        let expected_path = format!("{}{}index", root_dir, std::path::MAIN_SEPARATOR);
        assert_eq!(get_store_path_index(root_dir), expected_path);
    }

    #[test]
    fn test_get_store_checkpoint() {
        let root_dir = "/path/to/root";
        let expected_path = format!("{}{}checkpoint", root_dir, std::path::MAIN_SEPARATOR);
        assert_eq!(get_store_checkpoint(root_dir), expected_path);
    }

    #[test]
    fn test_get_abort_file() {
        let root_dir = "/path/to/root";
        let expected_path = format!("{}{}abort", root_dir, std::path::MAIN_SEPARATOR);
        assert_eq!(get_abort_file(root_dir), expected_path);
    }

    #[test]
    fn test_get_lock_file() {
        let root_dir = "/path/to/root";
        let expected_path = format!("{}{}lock", root_dir, std::path::MAIN_SEPARATOR);
        assert_eq!(get_lock_file(root_dir), expected_path);
    }

    #[test]
    fn test_get_delay_offset_store_path() {
        let root_dir = "/path/to/root";
        let expected_path = format!(
            "{}{}config{}delayOffset.json",
            root_dir,
            std::path::MAIN_SEPARATOR,
            std::path::MAIN_SEPARATOR
        );
        assert_eq!(get_delay_offset_store_path(root_dir), expected_path);
    }

    #[test]
    fn test_get_tran_state_table_store_path() {
        let root_dir = "/path/to/root";
        let expected_path = format!(
            "/path/to/root{}transaction{}statetable",
            std::path::MAIN_SEPARATOR,
            std::path::MAIN_SEPARATOR,
        );
        assert_eq!(get_tran_state_table_store_path(root_dir), expected_path);
    }

    #[test]
    fn test_get_tran_redo_log_store_path() {
        let root_dir = "/path/to/root";
        let expected_path = format!(
            "/path/to/root{}transaction{}redolog",
            std::path::MAIN_SEPARATOR,
            std::path::MAIN_SEPARATOR
        );
        assert_eq!(get_tran_redo_log_store_path(root_dir), expected_path);
    }
}
