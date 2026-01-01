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

use std::path::PathBuf;

pub fn get_store_path_consume_queue(root_dir: &str) -> String {
    PathBuf::from(root_dir)
        .join("consumequeue")
        .to_string_lossy()
        .into_owned()
}

pub fn get_store_path_consume_queue_ext(root_dir: &str) -> String {
    PathBuf::from(root_dir)
        .join("consumequeue_ext")
        .to_string_lossy()
        .into_owned()
}

pub fn get_store_path_batch_consume_queue(root_dir: &str) -> String {
    PathBuf::from(root_dir)
        .join("batchconsumequeue")
        .to_string_lossy()
        .into_owned()
}

pub fn get_store_path_index(root_dir: &str) -> String {
    PathBuf::from(root_dir).join("index").to_string_lossy().into_owned()
}

pub fn get_store_checkpoint(root_dir: &str) -> String {
    PathBuf::from(root_dir)
        .join("checkpoint")
        .to_string_lossy()
        .into_owned()
}

pub fn get_abort_file(root_dir: &str) -> String {
    PathBuf::from(root_dir).join("abort").to_string_lossy().into_owned()
}

pub fn get_lock_file(root_dir: &str) -> String {
    PathBuf::from(root_dir).join("lock").to_string_lossy().into_owned()
}

pub fn get_delay_offset_store_path(root_dir: &str) -> String {
    PathBuf::from(root_dir)
        .join("config")
        .join("delayOffset.json")
        .to_string_lossy()
        .into_owned()
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_get_store_paths() {
        let root_dir = "/tmp/test_storage";

        assert_eq!(
            get_store_path_consume_queue(root_dir),
            PathBuf::from(root_dir)
                .join("consumequeue")
                .to_string_lossy()
                .into_owned()
        );
        assert_eq!(
            get_store_path_consume_queue_ext(root_dir),
            PathBuf::from(root_dir)
                .join("consumequeue_ext")
                .to_string_lossy()
                .into_owned()
        );
        assert_eq!(
            get_store_path_batch_consume_queue(root_dir),
            PathBuf::from(root_dir)
                .join("batchconsumequeue")
                .to_string_lossy()
                .into_owned()
        );
        assert_eq!(
            get_store_path_index(root_dir),
            PathBuf::from(root_dir).join("index").to_string_lossy().into_owned()
        );
        assert_eq!(
            get_store_checkpoint(root_dir),
            PathBuf::from(root_dir)
                .join("checkpoint")
                .to_string_lossy()
                .into_owned()
        );
        assert_eq!(
            get_abort_file(root_dir),
            PathBuf::from(root_dir).join("abort").to_string_lossy().into_owned()
        );
        assert_eq!(
            get_lock_file(root_dir),
            PathBuf::from(root_dir).join("lock").to_string_lossy().into_owned()
        );
        assert_eq!(
            get_delay_offset_store_path(root_dir),
            PathBuf::from(root_dir)
                .join("config")
                .join("delayOffset.json")
                .to_string_lossy()
                .into_owned()
        );
    }
}
